//! Key cardinality scaling analysis
//!
//! Tests how performance scales with different numbers of unique keys.
//! Hypothesis: More keys = better shard distribution = better scaling

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::hint::black_box;
use std::sync::Arc;
use std::thread;
use tokio::runtime::Runtime;
use tokio_rate_limit::{RateLimiter, RateLimiterConfig};

/// Benchmark multi-threaded performance with varying key cardinalities
fn bench_key_cardinality(c: &mut Criterion) {
    let mut group = c.benchmark_group("key_cardinality");

    // Test different numbers of unique keys
    for num_keys in [10, 100, 1_000, 10_000, 100_000] {
        for num_threads in [1, 2, 4, 8] {
            group.throughput(Throughput::Elements(1));

            group.bench_with_input(
                BenchmarkId::new(
                    format!("{}_threads", num_threads),
                    format!("{}_keys", num_keys),
                ),
                &(num_keys, num_threads),
                |b, &(keys, threads)| {
                    let rt = Runtime::new().unwrap();
                    let limiter = Arc::new(rt.block_on(async {
                        RateLimiter::new(RateLimiterConfig {
                            requests_per_second: 1_000_000,
                            burst: 1_000_000,
                        })
                    }));

                    b.iter_custom(|iters| {
                        let barrier = Arc::new(std::sync::Barrier::new(threads));
                        let mut handles = vec![];

                        for t in 0..threads {
                            let limiter = Arc::clone(&limiter);
                            let barrier = Arc::clone(&barrier);

                            let handle = thread::spawn(move || {
                                let rt = Runtime::new().unwrap();
                                barrier.wait();

                                let start = std::time::Instant::now();
                                rt.block_on(async {
                                    for i in 0..iters {
                                        // Distribute keys across the keyspace
                                        let key_id = (i + t as u64 * iters) % keys;
                                        let key = format!("key-{}", key_id);
                                        let _ = black_box(limiter.check(black_box(&key)).await);
                                    }
                                });
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
    }

    group.finish();
}

/// Benchmark with hotspot workload (80/20 distribution)
fn bench_hotspot_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotspot_workload");

    for num_threads in [1, 2, 4, 8] {
        group.throughput(Throughput::Elements(1));

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}_threads", num_threads)),
            &num_threads,
            |b, &threads| {
                let rt = Runtime::new().unwrap();
                let limiter = Arc::new(rt.block_on(async {
                    RateLimiter::new(RateLimiterConfig {
                        requests_per_second: 1_000_000,
                        burst: 1_000_000,
                    })
                }));

                b.iter_custom(|iters| {
                    let barrier = Arc::new(std::sync::Barrier::new(threads));
                    let mut handles = vec![];

                    for t in 0..threads {
                        let limiter = Arc::clone(&limiter);
                        let barrier = Arc::clone(&barrier);

                        let handle = thread::spawn(move || {
                            let rt = Runtime::new().unwrap();
                            barrier.wait();

                            let start = std::time::Instant::now();
                            rt.block_on(async {
                                for i in 0..iters {
                                    // 80% of requests go to 20% of keys (0-19)
                                    // 20% of requests go to 80% of keys (20-99)
                                    let key_id = if (i % 10) < 8 {
                                        (i + t as u64 * iters) % 20
                                    } else {
                                        20 + ((i + t as u64 * iters) % 80)
                                    };
                                    let key = format!("key-{}", key_id);
                                    let _ = black_box(limiter.check(black_box(&key)).await);
                                }
                            });
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

/// Benchmark with per-thread keys (best case - no contention)
fn bench_per_thread_keys(c: &mut Criterion) {
    let mut group = c.benchmark_group("per_thread_keys");

    for num_threads in [1, 2, 4, 8] {
        group.throughput(Throughput::Elements(1));

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}_threads", num_threads)),
            &num_threads,
            |b, &threads| {
                let rt = Runtime::new().unwrap();
                let limiter = Arc::new(rt.block_on(async {
                    RateLimiter::new(RateLimiterConfig {
                        requests_per_second: 1_000_000,
                        burst: 1_000_000,
                    })
                }));

                b.iter_custom(|iters| {
                    let barrier = Arc::new(std::sync::Barrier::new(threads));
                    let mut handles = vec![];

                    for t in 0..threads {
                        let limiter = Arc::clone(&limiter);
                        let barrier = Arc::clone(&barrier);

                        let handle = thread::spawn(move || {
                            let rt = Runtime::new().unwrap();
                            barrier.wait();

                            let start = std::time::Instant::now();
                            rt.block_on(async {
                                // Each thread uses its own dedicated key
                                let key = format!("thread-{}-key", t);
                                for _ in 0..iters {
                                    let _ = black_box(limiter.check(black_box(&key)).await);
                                }
                            });
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
    bench_key_cardinality,
    bench_hotspot_workload,
    bench_per_thread_keys,
);

criterion_main!(benches);
