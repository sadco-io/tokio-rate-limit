//! Comparison benchmarks vs existing solutions
//!
//! Run with: cargo bench --bench comparison
//!
//! Compares tokio-rate-limit against governor (popular existing crate)

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use governor::{Quota, RateLimiter as GovernorLimiter};
use std::hint::black_box;
use std::num::NonZeroU32;
use std::sync::Arc;
use tokio::runtime::Runtime;

use tokio_rate_limit::{RateLimiter as TokioRateLimiter, RateLimiterConfig};

/// Compare single-threaded performance: tokio-rate-limit vs governor
fn comparison_single_threaded(c: &mut Criterion) {
    let mut group = c.benchmark_group("comparison/single_threaded");

    let rt = Runtime::new().unwrap();

    // tokio-rate-limit
    group.bench_function("tokio_rate_limit", |b| {
        let limiter = rt.block_on(async {
            TokioRateLimiter::new(RateLimiterConfig {
                requests_per_second: 1_000_000,
                burst: 1_000_000,
            })
            .unwrap()
        });

        b.to_async(&rt).iter(|| async {
            let result = limiter.check(black_box("key"));
            black_box(result)
        });
    });

    // governor
    group.bench_function("governor", |b| {
        let limiter = Arc::new(GovernorLimiter::direct(
            Quota::per_second(NonZeroU32::new(1_000_000).unwrap())
                .allow_burst(NonZeroU32::new(1_000_000).unwrap()),
        ));

        b.to_async(&rt).iter(|| async {
            let result = limiter.check();
            black_box(result)
        });
    });

    group.finish();
}

/// Compare multi-threaded performance across different thread counts
fn comparison_concurrent(c: &mut Criterion) {
    let mut group = c.benchmark_group("comparison/concurrent");

    for num_threads in [2, 4, 8, 16] {
        // tokio-rate-limit
        group.bench_with_input(
            BenchmarkId::new("tokio_rate_limit", num_threads),
            &num_threads,
            |b, &threads| {
                let rt = Runtime::new().unwrap();
                let limiter = Arc::new(rt.block_on(async {
                    TokioRateLimiter::new(RateLimiterConfig {
                        requests_per_second: 1_000_000,
                        burst: 1_000_000,
                    })
                    .unwrap()
                }));

                b.iter_custom(|iters| {
                    let barrier = Arc::new(std::sync::Barrier::new(threads));
                    let mut handles = vec![];

                    for _ in 0..threads {
                        let limiter = Arc::clone(&limiter);
                        let barrier = Arc::clone(&barrier);

                        let handle = std::thread::spawn(move || {
                            let rt = Runtime::new().unwrap();
                            barrier.wait();

                            let start = std::time::Instant::now();
                            rt.block_on(async {
                                for i in 0..iters {
                                    let key = format!("key-{}", i % 100);
                                    let _ = black_box(limiter.check(black_box(&key)));
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

        // governor
        group.bench_with_input(
            BenchmarkId::new("governor", num_threads),
            &num_threads,
            |b, &threads| {
                let limiter = Arc::new(GovernorLimiter::direct(
                    Quota::per_second(NonZeroU32::new(1_000_000).unwrap())
                        .allow_burst(NonZeroU32::new(1_000_000).unwrap()),
                ));

                b.iter_custom(|iters| {
                    let barrier = Arc::new(std::sync::Barrier::new(threads));
                    let mut handles = vec![];

                    for _ in 0..threads {
                        let limiter = Arc::clone(&limiter);
                        let barrier = Arc::clone(&barrier);

                        let handle = std::thread::spawn(move || {
                            barrier.wait();

                            let start = std::time::Instant::now();
                            for _ in 0..iters {
                                let _ = black_box(limiter.check());
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

criterion_group!(benches, comparison_single_threaded, comparison_concurrent,);

criterion_main!(benches);
