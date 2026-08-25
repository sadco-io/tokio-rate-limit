//! Benchmark comparing DashMap alternatives for token bucket storage
//!
//! This benchmark tests alternative concurrent hashmap implementations to identify
//! potential replacements for DashMap that improve 2-4 thread performance.
//!
//! Run with: cargo bench --bench dashmap_alternatives

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

/// Atomic token state matching the real implementation
#[derive(Debug)]
struct AtomicTokenState {
    tokens: AtomicU64,
    last_refill_nanos: AtomicU64,
}

impl AtomicTokenState {
    fn new(capacity: u64, now_nanos: u64) -> Self {
        Self {
            tokens: AtomicU64::new(capacity * 1000), // SCALE = 1000
            last_refill_nanos: AtomicU64::new(now_nanos),
        }
    }

    /// Simplified token consumption for benchmarking
    fn try_consume(&self, capacity: u64, rate: u64, now_nanos: u64) -> bool {
        let scaled_capacity = capacity * 1000;

        loop {
            let current_tokens = self.tokens.load(Ordering::Relaxed);
            let last_refill = self.last_refill_nanos.load(Ordering::Relaxed);

            let elapsed_nanos = now_nanos.saturating_sub(last_refill);
            let elapsed_secs = elapsed_nanos as f64 / 1_000_000_000.0;
            let new_tokens_to_add = (elapsed_secs * (rate * 1000) as f64) as u64;

            let updated_tokens = current_tokens
                .saturating_add(new_tokens_to_add)
                .min(scaled_capacity);

            let token_cost = 1000;

            if updated_tokens >= token_cost {
                let new_tokens = updated_tokens - token_cost;
                let new_time = if new_tokens_to_add > 0 {
                    now_nanos
                } else {
                    last_refill
                };

                match self.tokens.compare_exchange_weak(
                    current_tokens,
                    new_tokens,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        if new_tokens_to_add > 0 {
                            let _ = self.last_refill_nanos.compare_exchange_weak(
                                last_refill,
                                new_time,
                                Ordering::AcqRel,
                                Ordering::Relaxed,
                            );
                        }
                        return true;
                    }
                    Err(_) => continue,
                }
            } else {
                return false;
            }
        }
    }
}

/// Get monotonic time in nanoseconds
fn now_nanos() -> u64 {
    static START: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
    let start = START.get_or_init(Instant::now);
    start.elapsed().as_nanos() as u64
}

/// Benchmark: DashMap baseline (current implementation)
fn bench_dashmap(c: &mut Criterion) {
    let mut group = c.benchmark_group("dashmap_alternatives/dashmap");

    for num_threads in [1, 2, 4, 8] {
        group.throughput(Throughput::Elements(1));

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}_threads", num_threads)),
            &num_threads,
            |b, &threads| {
                let map = Arc::new(dashmap::DashMap::<String, Arc<AtomicTokenState>>::new());

                // Pre-populate with 100 keys
                let now = now_nanos();
                for i in 0..100 {
                    let key = format!("key-{}", i);
                    map.insert(key, Arc::new(AtomicTokenState::new(1_000_000, now)));
                }

                b.iter_custom(|iters| {
                    let barrier = Arc::new(std::sync::Barrier::new(threads));
                    let mut handles = vec![];

                    for _ in 0..threads {
                        let map = Arc::clone(&map);
                        let barrier = Arc::clone(&barrier);

                        let handle = thread::spawn(move || {
                            barrier.wait();
                            let start = Instant::now();

                            for i in 0..iters {
                                let key = format!("key-{}", i % 100);
                                if let Some(state) = map.get(&key) {
                                    let now = now_nanos();
                                    black_box(state.try_consume(1_000_000, 100_000, now));
                                }
                            }

                            start.elapsed()
                        });

                        handles.push(handle);
                    }

                    handles
                        .into_iter()
                        .map(|h| h.join().unwrap())
                        .max()
                        .unwrap()
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: papaya (lock-free, read-optimized)
fn bench_papaya(c: &mut Criterion) {
    let mut group = c.benchmark_group("dashmap_alternatives/papaya");

    for num_threads in [1, 2, 4, 8] {
        group.throughput(Throughput::Elements(1));

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}_threads", num_threads)),
            &num_threads,
            |b, &threads| {
                let map = Arc::new(papaya::HashMap::<String, Arc<AtomicTokenState>>::new());
                let guard = map.guard();

                // Pre-populate with 100 keys
                let now = now_nanos();
                for i in 0..100 {
                    let key = format!("key-{}", i);
                    map.insert(key, Arc::new(AtomicTokenState::new(1_000_000, now)), &guard);
                }
                drop(guard);

                b.iter_custom(|iters| {
                    let barrier = Arc::new(std::sync::Barrier::new(threads));
                    let mut handles = vec![];

                    for _ in 0..threads {
                        let map = Arc::clone(&map);
                        let barrier = Arc::clone(&barrier);

                        let handle = thread::spawn(move || {
                            barrier.wait();
                            let start = Instant::now();
                            let guard = map.guard();

                            for i in 0..iters {
                                let key = format!("key-{}", i % 100);
                                if let Some(state) = map.get(&key, &guard) {
                                    let now = now_nanos();
                                    black_box(state.try_consume(1_000_000, 100_000, now));
                                }
                            }

                            start.elapsed()
                        });

                        handles.push(handle);
                    }

                    handles
                        .into_iter()
                        .map(|h| h.join().unwrap())
                        .max()
                        .unwrap()
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: scc::HashMap (fine-grained locking)
fn bench_scc(c: &mut Criterion) {
    let mut group = c.benchmark_group("dashmap_alternatives/scc");

    for num_threads in [1, 2, 4, 8] {
        group.throughput(Throughput::Elements(1));

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}_threads", num_threads)),
            &num_threads,
            |b, &threads| {
                let map = Arc::new(scc::HashMap::<String, Arc<AtomicTokenState>>::new());

                // Pre-populate with 100 keys
                let now = now_nanos();
                for i in 0..100 {
                    let key = format!("key-{}", i);
                    let _ = map.insert_sync(key, Arc::new(AtomicTokenState::new(1_000_000, now)));
                }

                b.iter_custom(|iters| {
                    let barrier = Arc::new(std::sync::Barrier::new(threads));
                    let mut handles = vec![];

                    for _ in 0..threads {
                        let map = Arc::clone(&map);
                        let barrier = Arc::clone(&barrier);

                        let handle = thread::spawn(move || {
                            barrier.wait();
                            let start = Instant::now();

                            for i in 0..iters {
                                let key = format!("key-{}", i % 100);
                                if let Some(entry) = map.read_sync(&key, |_, v| Arc::clone(v)) {
                                    let now = now_nanos();
                                    black_box(entry.try_consume(1_000_000, 100_000, now));
                                }
                            }

                            start.elapsed()
                        });

                        handles.push(handle);
                    }

                    handles
                        .into_iter()
                        .map(|h| h.join().unwrap())
                        .max()
                        .unwrap()
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: flurry (Java ConcurrentHashMap port)
fn bench_flurry(c: &mut Criterion) {
    let mut group = c.benchmark_group("dashmap_alternatives/flurry");

    for num_threads in [1, 2, 4, 8] {
        group.throughput(Throughput::Elements(1));

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}_threads", num_threads)),
            &num_threads,
            |b, &threads| {
                let map = Arc::new(flurry::HashMap::<String, Arc<AtomicTokenState>>::new());
                let guard = map.guard();

                // Pre-populate with 100 keys
                let now = now_nanos();
                for i in 0..100 {
                    let key = format!("key-{}", i);
                    map.insert(key, Arc::new(AtomicTokenState::new(1_000_000, now)), &guard);
                }
                drop(guard);

                b.iter_custom(|iters| {
                    let barrier = Arc::new(std::sync::Barrier::new(threads));
                    let mut handles = vec![];

                    for _ in 0..threads {
                        let map = Arc::clone(&map);
                        let barrier = Arc::clone(&barrier);

                        let handle = thread::spawn(move || {
                            barrier.wait();
                            let start = Instant::now();
                            let guard = map.guard();

                            for i in 0..iters {
                                let key = format!("key-{}", i % 100);
                                if let Some(state) = map.get(&key, &guard) {
                                    let now = now_nanos();
                                    black_box(state.try_consume(1_000_000, 100_000, now));
                                }
                            }

                            start.elapsed()
                        });

                        handles.push(handle);
                    }

                    handles
                        .into_iter()
                        .map(|h| h.join().unwrap())
                        .max()
                        .unwrap()
                });
            },
        );
    }

    group.finish();
}

/// Entry insertion benchmark (write-heavy scenario)
fn bench_insert_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("dashmap_alternatives/insert");

    for num_threads in [1, 2, 4, 8] {
        // DashMap insert
        group.bench_with_input(
            BenchmarkId::new("dashmap", format!("{}_threads", num_threads)),
            &num_threads,
            |b, &threads| {
                b.iter_custom(|iters| {
                    let map = Arc::new(dashmap::DashMap::<String, Arc<AtomicTokenState>>::new());
                    let barrier = Arc::new(std::sync::Barrier::new(threads));
                    let mut handles = vec![];

                    for thread_id in 0..threads {
                        let map = Arc::clone(&map);
                        let barrier = Arc::clone(&barrier);

                        let handle = thread::spawn(move || {
                            barrier.wait();
                            let start = Instant::now();
                            let now = now_nanos();

                            for i in 0..iters {
                                let key = format!("key-{}-{}", thread_id, i);
                                map.insert(key, Arc::new(AtomicTokenState::new(1_000_000, now)));
                            }

                            start.elapsed()
                        });

                        handles.push(handle);
                    }

                    handles
                        .into_iter()
                        .map(|h| h.join().unwrap())
                        .max()
                        .unwrap()
                });
            },
        );

        // papaya insert
        group.bench_with_input(
            BenchmarkId::new("papaya", format!("{}_threads", num_threads)),
            &num_threads,
            |b, &threads| {
                b.iter_custom(|iters| {
                    let map = Arc::new(papaya::HashMap::<String, Arc<AtomicTokenState>>::new());
                    let barrier = Arc::new(std::sync::Barrier::new(threads));
                    let mut handles = vec![];

                    for thread_id in 0..threads {
                        let map = Arc::clone(&map);
                        let barrier = Arc::clone(&barrier);

                        let handle = thread::spawn(move || {
                            barrier.wait();
                            let start = Instant::now();
                            let now = now_nanos();
                            let guard = map.guard();

                            for i in 0..iters {
                                let key = format!("key-{}-{}", thread_id, i);
                                map.insert(
                                    key,
                                    Arc::new(AtomicTokenState::new(1_000_000, now)),
                                    &guard,
                                );
                            }

                            start.elapsed()
                        });

                        handles.push(handle);
                    }

                    handles
                        .into_iter()
                        .map(|h| h.join().unwrap())
                        .max()
                        .unwrap()
                });
            },
        );

        // scc insert
        group.bench_with_input(
            BenchmarkId::new("scc", format!("{}_threads", num_threads)),
            &num_threads,
            |b, &threads| {
                b.iter_custom(|iters| {
                    let map = Arc::new(scc::HashMap::<String, Arc<AtomicTokenState>>::new());
                    let barrier = Arc::new(std::sync::Barrier::new(threads));
                    let mut handles = vec![];

                    for thread_id in 0..threads {
                        let map = Arc::clone(&map);
                        let barrier = Arc::clone(&barrier);

                        let handle = thread::spawn(move || {
                            barrier.wait();
                            let start = Instant::now();
                            let now = now_nanos();

                            for i in 0..iters {
                                let key = format!("key-{}-{}", thread_id, i);
                                let _ = map.insert_sync(
                                    key,
                                    Arc::new(AtomicTokenState::new(1_000_000, now)),
                                );
                            }

                            start.elapsed()
                        });

                        handles.push(handle);
                    }

                    handles
                        .into_iter()
                        .map(|h| h.join().unwrap())
                        .max()
                        .unwrap()
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_dashmap,
    bench_papaya,
    bench_scc,
    bench_flurry,
    bench_insert_operations,
);

criterion_main!(benches);
