//! Benchmark v0.6.0 optimization techniques
//!
//! Run with: cargo bench --bench v0_6_optimizations
//!
//! This benchmark compares:
//! 1. Baseline TokenBucket (flurry-based)
//! 2. SIMD-optimized TokenBucket
//! 3. Zero-copy TokenBucket
//! 4. Thread-local cached TokenBucket
//! 5. Combined optimizations
//!
//! Across different workload patterns:
//! - Single-threaded
//! - Multi-threaded (2, 4, 8, 16 threads)
//! - Low key cardinality (10, 100 keys)
//! - High key cardinality (10K, 100K keys)
//! - Hot keys (80/20 distribution)
//! - Cold keys (uniform distribution)

use criterion::{
    criterion_group, criterion_main, BenchmarkId, Criterion, PlotConfiguration, Throughput,
};
use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use tokio::runtime::Runtime;
use tokio_rate_limit::algorithm::{
    CachedTokenBucket, SimdTokenBucket, TokenBucket, ZeroCopyTokenBucket,
};
use tokio_rate_limit::Algorithm;

/// Generate key with specific distribution
fn generate_key(index: u64, cardinality: usize, hot_ratio: Option<f64>) -> String {
    if let Some(ratio) = hot_ratio {
        // Hot-key distribution: 80% of accesses go to 20% of keys
        if (index % 100) < (ratio * 100.0) as u64 {
            // Hot keys (20% of total)
            let hot_cardinality = (cardinality as f64 * 0.2) as usize;
            format!("hot-key-{}", index % hot_cardinality as u64)
        } else {
            // Cold keys (80% of total)
            format!("cold-key-{}", index % cardinality as u64)
        }
    } else {
        // Uniform distribution
        format!("key-{}", index % cardinality as u64)
    }
}

/// Benchmark: Single-threaded baseline comparison
fn single_threaded_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_threaded");
    group.plot_config(PlotConfiguration::default());

    let rt = Runtime::new().unwrap();

    // Baseline: TokenBucket
    group.bench_function("baseline_token_bucket", |b| {
        let bucket = TokenBucket::new(1_000_000, 1_000_000);
        b.to_async(&rt).iter(|| async {
            let result = bucket.check(black_box("test-key")).await;
            black_box(result)
        });
    });

    // SIMD-optimized
    group.bench_function("simd_token_bucket", |b| {
        let bucket = SimdTokenBucket::new(1_000_000, 1_000_000);
        b.to_async(&rt).iter(|| async {
            let result = bucket.check(black_box("test-key")).await;
            black_box(result)
        });
    });

    // Zero-copy
    group.bench_function("zerocopy_token_bucket", |b| {
        let bucket = ZeroCopyTokenBucket::new(1_000_000, 1_000_000);
        b.to_async(&rt).iter(|| async {
            let result = bucket.check(black_box("test-key")).await;
            black_box(result)
        });
    });

    // Cached
    group.bench_function("cached_token_bucket", |b| {
        let bucket = CachedTokenBucket::new(1_000_000, 1_000_000);
        b.to_async(&rt).iter(|| async {
            let result = bucket.check(black_box("test-key")).await;
            black_box(result)
        });
    });

    group.finish();
}

/// Benchmark: Multi-threaded performance across thread counts
fn multi_threaded_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("multi_threaded");
    group.plot_config(PlotConfiguration::default());

    for num_threads in [2, 4, 8, 16] {
        group.throughput(Throughput::Elements(1));

        // Baseline
        group.bench_with_input(
            BenchmarkId::new("baseline", format!("{}_threads", num_threads)),
            &num_threads,
            |b, &threads| {
                let rt = Runtime::new().unwrap();
                let bucket = Arc::new(TokenBucket::new(1_000_000, 1_000_000));

                b.iter_custom(|iters| {
                    let barrier = Arc::new(std::sync::Barrier::new(threads));
                    let mut handles = vec![];

                    for _ in 0..threads {
                        let bucket = Arc::clone(&bucket);
                        let barrier = Arc::clone(&barrier);

                        let handle = thread::spawn(move || {
                            let rt = Runtime::new().unwrap();
                            barrier.wait();

                            let start = std::time::Instant::now();
                            rt.block_on(async {
                                for i in 0..iters {
                                    let key = format!("thread-key-{}", i % 100);
                                    let _ = black_box(bucket.check(black_box(&key)).await);
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

        // SIMD
        group.bench_with_input(
            BenchmarkId::new("simd", format!("{}_threads", num_threads)),
            &num_threads,
            |b, &threads| {
                let rt = Runtime::new().unwrap();
                let bucket = Arc::new(SimdTokenBucket::new(1_000_000, 1_000_000));

                b.iter_custom(|iters| {
                    let barrier = Arc::new(std::sync::Barrier::new(threads));
                    let mut handles = vec![];

                    for _ in 0..threads {
                        let bucket = Arc::clone(&bucket);
                        let barrier = Arc::clone(&barrier);

                        let handle = thread::spawn(move || {
                            let rt = Runtime::new().unwrap();
                            barrier.wait();

                            let start = std::time::Instant::now();
                            rt.block_on(async {
                                for i in 0..iters {
                                    let key = format!("thread-key-{}", i % 100);
                                    let _ = black_box(bucket.check(black_box(&key)).await);
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

        // Zero-copy
        group.bench_with_input(
            BenchmarkId::new("zerocopy", format!("{}_threads", num_threads)),
            &num_threads,
            |b, &threads| {
                let rt = Runtime::new().unwrap();
                let bucket = Arc::new(ZeroCopyTokenBucket::new(1_000_000, 1_000_000));

                b.iter_custom(|iters| {
                    let barrier = Arc::new(std::sync::Barrier::new(threads));
                    let mut handles = vec![];

                    for _ in 0..threads {
                        let bucket = Arc::clone(&bucket);
                        let barrier = Arc::clone(&barrier);

                        let handle = thread::spawn(move || {
                            let rt = Runtime::new().unwrap();
                            barrier.wait();

                            let start = std::time::Instant::now();
                            rt.block_on(async {
                                for i in 0..iters {
                                    let key = format!("thread-key-{}", i % 100);
                                    let _ = black_box(bucket.check(black_box(&key)).await);
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

        // Cached
        group.bench_with_input(
            BenchmarkId::new("cached", format!("{}_threads", num_threads)),
            &num_threads,
            |b, &threads| {
                let rt = Runtime::new().unwrap();
                let bucket = Arc::new(CachedTokenBucket::new(1_000_000, 1_000_000));

                b.iter_custom(|iters| {
                    let barrier = Arc::new(std::sync::Barrier::new(threads));
                    let mut handles = vec![];

                    for _ in 0..threads {
                        let bucket = Arc::clone(&bucket);
                        let barrier = Arc::clone(&barrier);

                        let handle = thread::spawn(move || {
                            let rt = Runtime::new().unwrap();
                            barrier.wait();

                            let start = std::time::Instant::now();
                            rt.block_on(async {
                                for i in 0..iters {
                                    let key = format!("thread-key-{}", i % 100);
                                    let _ = black_box(bucket.check(black_box(&key)).await);
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

/// Benchmark: Key cardinality impact
fn key_cardinality_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("key_cardinality");
    group.plot_config(PlotConfiguration::default());

    let rt = Runtime::new().unwrap();
    let counter = Arc::new(AtomicU64::new(0));

    for cardinality in [10, 100, 1_000, 10_000] {
        // Baseline
        group.bench_with_input(
            BenchmarkId::new("baseline", format!("{}_keys", cardinality)),
            &cardinality,
            |b, &card| {
                let bucket = Arc::new(TokenBucket::new(1_000_000, 1_000_000));
                let counter = Arc::clone(&counter);

                b.to_async(&rt).iter(|| {
                    let bucket = Arc::clone(&bucket);
                    let counter = Arc::clone(&counter);
                    async move {
                        let idx = counter.fetch_add(1, Ordering::Relaxed);
                        let key = generate_key(idx, card, None);
                        let result = bucket.check(black_box(&key)).await;
                        black_box(result)
                    }
                });
            },
        );

        // Zero-copy
        group.bench_with_input(
            BenchmarkId::new("zerocopy", format!("{}_keys", cardinality)),
            &cardinality,
            |b, &card| {
                let bucket = Arc::new(ZeroCopyTokenBucket::new(1_000_000, 1_000_000));
                let counter = Arc::clone(&counter);

                b.to_async(&rt).iter(|| {
                    let bucket = Arc::clone(&bucket);
                    let counter = Arc::clone(&counter);
                    async move {
                        let idx = counter.fetch_add(1, Ordering::Relaxed);
                        let key = generate_key(idx, card, None);
                        let result = bucket.check(black_box(&key)).await;
                        black_box(result)
                    }
                });
            },
        );

        // Cached
        group.bench_with_input(
            BenchmarkId::new("cached", format!("{}_keys", cardinality)),
            &cardinality,
            |b, &card| {
                let bucket = Arc::new(CachedTokenBucket::new(1_000_000, 1_000_000));
                let counter = Arc::clone(&counter);

                b.to_async(&rt).iter(|| {
                    let bucket = Arc::clone(&bucket);
                    let counter = Arc::clone(&counter);
                    async move {
                        let idx = counter.fetch_add(1, Ordering::Relaxed);
                        let key = generate_key(idx, card, None);
                        let result = bucket.check(black_box(&key)).await;
                        black_box(result)
                    }
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: Hot key (80/20) distribution
fn hot_key_distribution(c: &mut Criterion) {
    let mut group = c.benchmark_group("hot_key_distribution");
    group.plot_config(PlotConfiguration::default());

    let rt = Runtime::new().unwrap();
    let counter = Arc::new(AtomicU64::new(0));

    // Baseline with hot keys
    group.bench_function("baseline_hot_keys", |b| {
        let bucket = Arc::new(TokenBucket::new(1_000_000, 1_000_000));
        let counter = Arc::clone(&counter);

        b.to_async(&rt).iter(|| {
            let bucket = Arc::clone(&bucket);
            let counter = Arc::clone(&counter);
            async move {
                let idx = counter.fetch_add(1, Ordering::Relaxed);
                let key = generate_key(idx, 1000, Some(0.8)); // 80% to 20% of keys
                let result = bucket.check(black_box(&key)).await;
                black_box(result)
            }
        });
    });

    // Cached with hot keys (should show improvement)
    group.bench_function("cached_hot_keys", |b| {
        let bucket = Arc::new(CachedTokenBucket::new(1_000_000, 1_000_000));
        let counter = Arc::clone(&counter);

        b.to_async(&rt).iter(|| {
            let bucket = Arc::clone(&bucket);
            let counter = Arc::clone(&counter);
            async move {
                let idx = counter.fetch_add(1, Ordering::Relaxed);
                let key = generate_key(idx, 1000, Some(0.8));
                let result = bucket.check(black_box(&key)).await;
                black_box(result)
            }
        });
    });

    // Zero-copy with hot keys
    group.bench_function("zerocopy_hot_keys", |b| {
        let bucket = Arc::new(ZeroCopyTokenBucket::new(1_000_000, 1_000_000));
        let counter = Arc::clone(&counter);

        b.to_async(&rt).iter(|| {
            let bucket = Arc::clone(&bucket);
            let counter = Arc::clone(&counter);
            async move {
                let idx = counter.fetch_add(1, Ordering::Relaxed);
                let key = generate_key(idx, 1000, Some(0.8));
                let result = bucket.check(black_box(&key)).await;
                black_box(result)
            }
        });
    });

    group.finish();
}

/// Benchmark: Cold key (uniform) distribution
fn cold_key_distribution(c: &mut Criterion) {
    let mut group = c.benchmark_group("cold_key_distribution");
    group.plot_config(PlotConfiguration::default());

    let rt = Runtime::new().unwrap();
    let counter = Arc::new(AtomicU64::new(0));

    // Baseline with cold keys
    group.bench_function("baseline_cold_keys", |b| {
        let bucket = Arc::new(TokenBucket::new(1_000_000, 1_000_000));
        let counter = Arc::clone(&counter);

        b.to_async(&rt).iter(|| {
            let bucket = Arc::clone(&bucket);
            let counter = Arc::clone(&counter);
            async move {
                let idx = counter.fetch_add(1, Ordering::Relaxed);
                let key = generate_key(idx, 10000, None); // Uniform distribution
                let result = bucket.check(black_box(&key)).await;
                black_box(result)
            }
        });
    });

    // Cached with cold keys (may show regression)
    group.bench_function("cached_cold_keys", |b| {
        let bucket = Arc::new(CachedTokenBucket::new(1_000_000, 1_000_000));
        let counter = Arc::clone(&counter);

        b.to_async(&rt).iter(|| {
            let bucket = Arc::clone(&bucket);
            let counter = Arc::clone(&counter);
            async move {
                let idx = counter.fetch_add(1, Ordering::Relaxed);
                let key = generate_key(idx, 10000, None);
                let result = bucket.check(black_box(&key)).await;
                black_box(result)
            }
        });
    });

    // Zero-copy with cold keys
    group.bench_function("zerocopy_cold_keys", |b| {
        let bucket = Arc::new(ZeroCopyTokenBucket::new(1_000_000, 1_000_000));
        let counter = Arc::clone(&counter);

        b.to_async(&rt).iter(|| {
            let bucket = Arc::clone(&bucket);
            let counter = Arc::clone(&counter);
            async move {
                let idx = counter.fetch_add(1, Ordering::Relaxed);
                let key = generate_key(idx, 10000, None);
                let result = bucket.check(black_box(&key)).await;
                black_box(result)
            }
        });
    });

    group.finish();
}

/// Benchmark: Memory allocation patterns
fn memory_allocation_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_allocation");
    group.plot_config(PlotConfiguration::default());

    let rt = Runtime::new().unwrap();

    // Baseline: allocates on every check
    group.bench_function("baseline_allocations", |b| {
        let bucket = TokenBucket::new(1_000_000, 1_000_000);
        b.to_async(&rt).iter(|| async {
            // This will allocate key.to_string() every time
            for i in 0..100 {
                let key = format!("key-{}", i % 10);
                let _ = bucket.check(black_box(&key)).await;
            }
        });
    });

    // Zero-copy: only allocates on first access per key
    group.bench_function("zerocopy_allocations", |b| {
        let bucket = ZeroCopyTokenBucket::new(1_000_000, 1_000_000);
        b.to_async(&rt).iter(|| async {
            for i in 0..100 {
                let key = format!("key-{}", i % 10);
                let _ = bucket.check(black_box(&key)).await;
            }
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    single_threaded_comparison,
    multi_threaded_comparison,
    key_cardinality_comparison,
    hot_key_distribution,
    cold_key_distribution,
    memory_allocation_comparison,
);

criterion_main!(benches);
