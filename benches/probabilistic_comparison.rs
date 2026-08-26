//! Comprehensive benchmark suite for probabilistic rate limiting.
//!
//! Run with: cargo bench --bench probabilistic_comparison
//!
//! This benchmark compares:
//! 1. Baseline TokenBucket (v0.6.0, flurry-based)
//! 2. Probabilistic with different sampling rates (1%, 5%, 10%, 20%)
//!
//! Across different scenarios:
//! - Single-threaded performance
//! - Multi-threaded scaling (2, 4, 8, 16 threads)
//! - Different key cardinalities (1, 100, 10K keys)
//! - Hot-key workloads (80/20 distribution)

use criterion::{
    criterion_group, criterion_main, BenchmarkId, Criterion, PlotConfiguration, Throughput,
};
use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use tokio::runtime::Runtime;
use tokio_rate_limit::algorithm::{ProbabilisticTokenBucket, TokenBucket};
use tokio_rate_limit::Algorithm;

/// Generate key with specific distribution
fn generate_key(index: u64, cardinality: usize, hot_ratio: Option<f64>) -> String {
    if let Some(ratio) = hot_ratio {
        // Hot-key distribution: 80% of accesses go to 20% of keys
        if (index % 100) < (ratio * 100.0) as u64 {
            let hot_cardinality = (cardinality as f64 * 0.2) as usize;
            format!("hot-key-{}", index % hot_cardinality as u64)
        } else {
            format!("cold-key-{}", index % cardinality as u64)
        }
    } else {
        format!("key-{}", index % cardinality as u64)
    }
}

/// Benchmark: Single-threaded comparison across sampling rates
fn single_threaded_sampling_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_threaded_sampling");
    group.plot_config(PlotConfiguration::default());
    group.throughput(Throughput::Elements(1));

    let rt = Runtime::new().unwrap();

    // Baseline: Deterministic TokenBucket
    group.bench_function("baseline_deterministic", |b| {
        let bucket = TokenBucket::new(1_000_000, 1_000_000);
        b.to_async(&rt).iter(|| async {
            let result = bucket.check(black_box("test-key")).await;
            black_box(result)
        });
    });

    // Probabilistic with different sampling rates
    let sampling_configs = vec![
        (1, "100%_deterministic"),
        (2, "50%_sampling"),
        (5, "20%_sampling"),
        (10, "10%_sampling"),
        (20, "5%_sampling"),
        (100, "1%_sampling"),
    ];

    for (sample_rate, label) in sampling_configs {
        group.bench_function(label, |b| {
            let bucket = ProbabilisticTokenBucket::new(1_000_000, 1_000_000, sample_rate);
            b.to_async(&rt).iter(|| async {
                let result = bucket.check(black_box("test-key")).await;
                black_box(result)
            });
        });
    }

    group.finish();
}

/// Benchmark: Multi-threaded scaling comparison
fn multi_threaded_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("multi_threaded_scaling");
    group.plot_config(PlotConfiguration::default());

    let thread_counts = vec![2, 4, 8];

    for num_threads in thread_counts {
        group.throughput(Throughput::Elements(1));

        // Baseline deterministic
        group.bench_with_input(
            BenchmarkId::new("baseline", format!("{}_threads", num_threads)),
            &num_threads,
            |b, &threads| {
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
                                    let key = format!("key-{}", i % 100);
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

        // 1% sampling
        group.bench_with_input(
            BenchmarkId::new("prob_1pct", format!("{}_threads", num_threads)),
            &num_threads,
            |b, &threads| {
                let bucket = Arc::new(ProbabilisticTokenBucket::new(1_000_000, 1_000_000, 100));

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
                                    let key = format!("key-{}", i % 100);
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

        // 5% sampling
        group.bench_with_input(
            BenchmarkId::new("prob_5pct", format!("{}_threads", num_threads)),
            &num_threads,
            |b, &threads| {
                let bucket = Arc::new(ProbabilisticTokenBucket::new(1_000_000, 1_000_000, 20));

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
                                    let key = format!("key-{}", i % 100);
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

        // 10% sampling
        group.bench_with_input(
            BenchmarkId::new("prob_10pct", format!("{}_threads", num_threads)),
            &num_threads,
            |b, &threads| {
                let bucket = Arc::new(ProbabilisticTokenBucket::new(1_000_000, 1_000_000, 10));

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
                                    let key = format!("key-{}", i % 100);
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

    let cardinalities = vec![1, 100, 10_000];

    for cardinality in cardinalities {
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

        // 1% sampling
        group.bench_with_input(
            BenchmarkId::new("prob_1pct", format!("{}_keys", cardinality)),
            &cardinality,
            |b, &card| {
                let bucket = Arc::new(ProbabilisticTokenBucket::new(1_000_000, 1_000_000, 100));
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

        // 10% sampling
        group.bench_with_input(
            BenchmarkId::new("prob_10pct", format!("{}_keys", cardinality)),
            &cardinality,
            |b, &card| {
                let bucket = Arc::new(ProbabilisticTokenBucket::new(1_000_000, 1_000_000, 10));
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

/// Benchmark: Hot key workload (80/20 distribution)
fn hot_key_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("hot_key_workload");
    group.plot_config(PlotConfiguration::default());

    let rt = Runtime::new().unwrap();
    let counter = Arc::new(AtomicU64::new(0));

    // Baseline with hot keys
    group.bench_function("baseline_80_20", |b| {
        let bucket = Arc::new(TokenBucket::new(1_000_000, 1_000_000));
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

    // 1% sampling with hot keys
    group.bench_function("prob_1pct_80_20", |b| {
        let bucket = Arc::new(ProbabilisticTokenBucket::new(1_000_000, 1_000_000, 100));
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

    // 5% sampling with hot keys
    group.bench_function("prob_5pct_80_20", |b| {
        let bucket = Arc::new(ProbabilisticTokenBucket::new(1_000_000, 1_000_000, 20));
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

    // 10% sampling with hot keys
    group.bench_function("prob_10pct_80_20", |b| {
        let bucket = Arc::new(ProbabilisticTokenBucket::new(1_000_000, 1_000_000, 10));
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

/// Benchmark: Cost-based rate limiting
fn cost_based_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("cost_based");
    group.plot_config(PlotConfiguration::default());

    let rt = Runtime::new().unwrap();

    // Baseline with cost
    group.bench_function("baseline_cost_10", |b| {
        let bucket = TokenBucket::new(1_000_000, 1_000_000);
        b.to_async(&rt).iter(|| async {
            let result = bucket.check_with_cost(black_box("test-key"), 10).await;
            black_box(result)
        });
    });

    // Probabilistic 1% with cost
    group.bench_function("prob_1pct_cost_10", |b| {
        let bucket = ProbabilisticTokenBucket::new(1_000_000, 1_000_000, 100);
        b.to_async(&rt).iter(|| async {
            let result = bucket.check_with_cost(black_box("test-key"), 10).await;
            black_box(result)
        });
    });

    // Probabilistic 10% with cost
    group.bench_function("prob_10pct_cost_10", |b| {
        let bucket = ProbabilisticTokenBucket::new(1_000_000, 1_000_000, 10);
        b.to_async(&rt).iter(|| async {
            let result = bucket.check_with_cost(black_box("test-key"), 10).await;
            black_box(result)
        });
    });

    group.finish();
}

/// Benchmark: Extreme throughput (single hot key)
fn extreme_throughput_single_key(c: &mut Criterion) {
    let mut group = c.benchmark_group("extreme_throughput_single_key");
    group.plot_config(PlotConfiguration::default());

    let rt = Runtime::new().unwrap();

    // This is where probabilistic really shines - single hot key

    group.bench_function("baseline", |b| {
        let bucket = TokenBucket::new(1_000_000, 1_000_000);
        b.to_async(&rt).iter(|| async {
            let result = bucket.check(black_box("hot-key")).await;
            black_box(result)
        });
    });

    group.bench_function("prob_1pct", |b| {
        let bucket = ProbabilisticTokenBucket::new(1_000_000, 1_000_000, 100);
        b.to_async(&rt).iter(|| async {
            let result = bucket.check(black_box("hot-key")).await;
            black_box(result)
        });
    });

    group.bench_function("prob_5pct", |b| {
        let bucket = ProbabilisticTokenBucket::new(1_000_000, 1_000_000, 20);
        b.to_async(&rt).iter(|| async {
            let result = bucket.check(black_box("hot-key")).await;
            black_box(result)
        });
    });

    group.bench_function("prob_10pct", |b| {
        let bucket = ProbabilisticTokenBucket::new(1_000_000, 1_000_000, 10);
        b.to_async(&rt).iter(|| async {
            let result = bucket.check(black_box("hot-key")).await;
            black_box(result)
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    single_threaded_sampling_comparison,
    multi_threaded_scaling,
    key_cardinality_comparison,
    hot_key_workload,
    cost_based_comparison,
    extreme_throughput_single_key,
);

criterion_main!(benches);
