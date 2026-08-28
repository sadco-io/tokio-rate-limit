//! Performance benchmarks for tokio-rate-limit
//!
//! Run with: cargo bench --bench rate_limit_performance
//!
//! These benchmarks prove the performance claims of the library.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tokio::runtime::Runtime;

// We'll implement these as we go
use tokio_rate_limit::{RateLimiter, RateLimiterConfig};

/// Benchmark: Single-threaded rate limit checks
///
/// Target: 10M+ ops/sec
fn single_threaded_checks(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let limiter = rt.block_on(async {
        RateLimiter::new(RateLimiterConfig {
            requests_per_second: 1_000_000, // High limit so we don't actually limit
            burst: 1_000_000,
        })
        .unwrap()
    });

    c.bench_function("rate_limit/single_threaded", |b| {
        b.to_async(&rt).iter(|| async {
            let result = limiter.check(black_box("test-key"));
            black_box(result)
        });
    });
}

/// Benchmark: Multi-threaded rate limit checks
///
/// Tests concurrent access patterns
fn multi_threaded_checks(c: &mut Criterion) {
    let mut group = c.benchmark_group("rate_limit/concurrent");

    for num_threads in [1, 2, 4, 8, 16] {
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
                    .unwrap()
                }));

                b.iter_custom(|iters| {
                    let barrier = Arc::new(std::sync::Barrier::new(threads));
                    let mut handles = vec![];

                    for _ in 0..threads {
                        let limiter = Arc::clone(&limiter);
                        let barrier = Arc::clone(&barrier);

                        let handle = thread::spawn(move || {
                            let rt = Runtime::new().unwrap();
                            barrier.wait();

                            let start = std::time::Instant::now();
                            rt.block_on(async {
                                for i in 0..iters {
                                    let key = format!("thread-key-{}", i % 100);
                                    let _ = black_box(limiter.check(black_box(&key)));
                                }
                            });
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

    group.finish();
}

/// Benchmark: Rate limit enforcement accuracy
///
/// Verify that rate limiting actually works correctly
fn rate_limit_enforcement(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let limiter = Arc::new(rt.block_on(async {
        RateLimiter::new(RateLimiterConfig {
            requests_per_second: 100,
            burst: 10,
        })
        .unwrap()
    }));

    let counter = Arc::new(AtomicU64::new(0));

    c.bench_function("rate_limit/enforcement", |b| {
        b.to_async(&rt).iter(|| {
            let counter = Arc::clone(&counter);
            let limiter = Arc::clone(&limiter);
            async move {
                // Use unique key per iteration to avoid refill between benchmark runs
                let iteration = counter.fetch_add(1, Ordering::Relaxed);
                let key = format!("enforcement-key-{}", iteration);

                // First 10 should succeed (burst)
                for _ in 0..10 {
                    assert!(limiter.check(black_box(&key)).permitted);
                }

                // Next ones should fail (exhausted burst)
                let result = limiter.check(black_box(&key));
                assert!(!result.permitted);

                // Wait for refill
                tokio::time::sleep(Duration::from_millis(100)).await;

                // Should work again
                let result = limiter.check(black_box(&key));
                black_box(result)
            }
        });
    });
}

/// Benchmark: Token bucket refill mechanism
///
/// Tests the refill logic performance
fn token_bucket_refill(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let limiter = Arc::new(rt.block_on(async {
        RateLimiter::new(RateLimiterConfig {
            requests_per_second: 1000,
            burst: 100,
        })
        .unwrap()
    }));

    let counter = Arc::new(AtomicU64::new(0));

    c.bench_function("rate_limit/refill", |b| {
        b.to_async(&rt).iter(|| {
            let counter = Arc::clone(&counter);
            let limiter = Arc::clone(&limiter);
            async move {
                // Use unique key per iteration
                let iteration = counter.fetch_add(1, Ordering::Relaxed);
                let key = format!("refill-key-{}", iteration);

                // Exhaust the bucket
                for _ in 0..100 {
                    let _ = limiter.check(black_box(&key));
                }

                // Wait for partial refill
                tokio::time::sleep(Duration::from_millis(50)).await;

                // Check again (should have refilled ~50 tokens)
                let result = limiter.check(black_box(&key));
                black_box(result)
            }
        });
    });
}

/// Benchmark: Memory usage per limiter instance
///
/// Measures the overhead of creating rate limiters
fn limiter_creation(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function("rate_limit/creation", |b| {
        b.to_async(&rt).iter(|| async {
            let limiter = RateLimiter::new(black_box(RateLimiterConfig {
                requests_per_second: 100,
                burst: 200,
            }));
            black_box(limiter)
        });
    });
}

criterion_group!(
    benches,
    single_threaded_checks,
    multi_threaded_checks,
    rate_limit_enforcement,
    token_bucket_refill,
    limiter_creation,
);

criterion_main!(benches);
