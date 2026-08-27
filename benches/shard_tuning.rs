//! DashMap shard count tuning benchmarks
//!
//! Run with: cargo bench --bench shard_tuning
//!
//! This benchmark helps identify the optimal shard count for DashMap to minimize
//! multi-threaded contention in the token bucket implementation.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use dashmap::DashMap;
use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

/// Simulates the AtomicTokenState used in TokenBucket
struct AtomicTokenState {
    tokens: AtomicU64,
    last_refill_nanos: AtomicU64,
}

impl AtomicTokenState {
    fn new(capacity: u64) -> Self {
        const SCALE: u64 = 1000;
        Self {
            tokens: AtomicU64::new(capacity.saturating_mul(SCALE)),
            last_refill_nanos: AtomicU64::new(0),
        }
    }

    /// Simplified try_consume that focuses on the CAS operation
    fn try_consume(&self) -> bool {
        const SCALE: u64 = 1000;
        loop {
            let current_tokens = self.tokens.load(Ordering::Relaxed);

            if current_tokens >= SCALE {
                let new_tokens = current_tokens - SCALE;
                match self.tokens.compare_exchange(
                    current_tokens,
                    new_tokens,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => return true,
                    Err(_) => continue,
                }
            } else {
                return false;
            }
        }
    }
}

/// Benchmark concurrent access with different shard counts
fn bench_shard_counts(c: &mut Criterion) {
    let mut group = c.benchmark_group("dashmap_shards");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(10));

    // Test different shard counts
    for num_shards in [16, 32, 64, 128, 256] {
        for num_threads in [1, 2, 4, 8, 16] {
            group.throughput(Throughput::Elements(1));

            group.bench_with_input(
                BenchmarkId::new(
                    format!("{}_shards", num_shards),
                    format!("{}_threads", num_threads),
                ),
                &(num_shards, num_threads),
                |b, &(shards, threads)| {
                    b.iter_custom(|iters| {
                        // Create DashMap with specific shard count
                        let tokens =
                            Arc::new(DashMap::with_capacity_and_shard_amount(1024, shards));

                        // Pre-populate with 100 keys (simulating real workload)
                        for i in 0..100 {
                            let key = format!("key-{}", i);
                            tokens.insert(key, AtomicTokenState::new(1_000_000));
                        }

                        let barrier = Arc::new(std::sync::Barrier::new(threads));
                        let mut handles = vec![];

                        for _ in 0..threads {
                            let tokens = Arc::clone(&tokens);
                            let barrier = Arc::clone(&barrier);

                            let handle = thread::spawn(move || {
                                barrier.wait();

                                let start = std::time::Instant::now();
                                for i in 0..iters {
                                    // Cycle through keys to simulate real workload
                                    let key = format!("key-{}", i % 100);

                                    // Access the DashMap entry and perform operation
                                    if let Some(state) = tokens.get(&key) {
                                        let _ = black_box(state.try_consume());
                                    }
                                }
                                start.elapsed()
                            });

                            handles.push(handle);
                        }

                        // Return the maximum time (slowest thread)
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

/// Benchmark that focuses on high-contention scenarios
fn bench_high_contention(c: &mut Criterion) {
    let mut group = c.benchmark_group("high_contention");
    group.sample_size(30);
    group.measurement_time(Duration::from_secs(8));

    // Focus on problematic thread counts (2, 4, 8)
    for num_shards in [16, 64, 128, 256] {
        for num_threads in [2, 4, 8] {
            group.throughput(Throughput::Elements(1));

            group.bench_with_input(
                BenchmarkId::new(
                    format!("{}_shards", num_shards),
                    format!("{}_threads", num_threads),
                ),
                &(num_shards, num_threads),
                |b, &(shards, threads)| {
                    b.iter_custom(|iters| {
                        // Use only 10 keys to increase contention
                        let tokens = Arc::new(DashMap::with_capacity_and_shard_amount(128, shards));

                        for i in 0..10 {
                            let key = format!("key-{}", i);
                            tokens.insert(key, AtomicTokenState::new(1_000_000));
                        }

                        let barrier = Arc::new(std::sync::Barrier::new(threads));
                        let mut handles = vec![];

                        for _ in 0..threads {
                            let tokens = Arc::clone(&tokens);
                            let barrier = Arc::clone(&barrier);

                            let handle = thread::spawn(move || {
                                barrier.wait();

                                let start = std::time::Instant::now();
                                for i in 0..iters {
                                    let key = format!("key-{}", i % 10);

                                    if let Some(state) = tokens.get(&key) {
                                        let _ = black_box(state.try_consume());
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
    }

    group.finish();
}

/// Benchmark single-threaded performance to ensure no regression
fn bench_single_threaded_baseline(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_threaded_baseline");

    for num_shards in [16, 64, 128, 256] {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}_shards", num_shards)),
            &num_shards,
            |b, &shards| {
                let tokens = Arc::new(DashMap::with_capacity_and_shard_amount(1024, shards));

                for i in 0..100 {
                    let key = format!("key-{}", i);
                    tokens.insert(key, AtomicTokenState::new(1_000_000));
                }

                b.iter(|| {
                    for i in 0..1000 {
                        let key = format!("key-{}", i % 100);
                        if let Some(state) = tokens.get(&key) {
                            let _ = black_box(state.try_consume());
                        }
                    }
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_shard_counts,
    bench_high_contention,
    bench_single_threaded_baseline,
);

criterion_main!(benches);
