//! Algorithm comparison benchmarks: TokenBucket vs LeakyBucket
//!
//! Run with: cargo bench --bench algorithm_comparison
//!
//! This benchmark suite demonstrates the scenario-specific benefits of each algorithm:
//! - TokenBucket: Allows bursts, better for bursty workloads
//! - LeakyBucket: Enforces steady rate, better for backend protection

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio_rate_limit::algorithm::{LeakyBucket, TokenBucket};
use tokio_rate_limit::RateLimiter;

/// Benchmark 1: Raw performance comparison (single-threaded)
///
/// Tests the raw throughput of each algorithm with no rate limiting effects.
/// Both algorithms should perform similarly since they use the same architecture.
fn raw_performance_single_threaded(c: &mut Criterion) {
    let mut group = c.benchmark_group("algorithm/raw_performance/single_threaded");

    let rt = Runtime::new().unwrap();

    // TokenBucket - high limit to avoid actual rate limiting
    group.bench_function("token_bucket", |b| {
        let algorithm = TokenBucket::new(1_000_000, 1_000_000);
        let limiter = RateLimiter::from_algorithm(algorithm);

        b.to_async(&rt).iter(|| async {
            let result = limiter.check(black_box("test-key")).await;
            black_box(result)
        });
    });

    // LeakyBucket - high limit to avoid actual rate limiting
    group.bench_function("leaky_bucket", |b| {
        let algorithm = LeakyBucket::new(1_000_000, 1_000_000);
        let limiter = RateLimiter::from_algorithm(algorithm);

        b.to_async(&rt).iter(|| async {
            let result = limiter.check(black_box("test-key")).await;
            black_box(result)
        });
    });

    group.finish();
}

/// Benchmark 2: Raw performance comparison (multi-threaded)
///
/// Tests concurrent throughput of both algorithms.
fn raw_performance_multi_threaded(c: &mut Criterion) {
    let mut group = c.benchmark_group("algorithm/raw_performance/concurrent");

    for num_threads in [2, 4, 8] {
        group.throughput(Throughput::Elements(1));

        // TokenBucket
        group.bench_with_input(
            BenchmarkId::new("token_bucket", num_threads),
            &num_threads,
            |b, &threads| {
                let _rt = Runtime::new().unwrap();
                let algorithm = TokenBucket::new(1_000_000, 1_000_000);
                let limiter = Arc::new(RateLimiter::from_algorithm(algorithm));

                b.iter_custom(|iters| {
                    let barrier = Arc::new(std::sync::Barrier::new(threads));
                    let mut handles = vec![];

                    for _ in 0..threads {
                        let limiter = Arc::clone(&limiter);
                        let barrier = Arc::clone(&barrier);

                        let handle = thread::spawn(move || {
                            let _rt = Runtime::new().unwrap();
                            barrier.wait();

                            let start = std::time::Instant::now();
                            _rt.block_on(async {
                                for i in 0..iters {
                                    let key = format!("thread-key-{}", i % 100);
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

        // LeakyBucket
        group.bench_with_input(
            BenchmarkId::new("leaky_bucket", num_threads),
            &num_threads,
            |b, &threads| {
                let _rt = Runtime::new().unwrap();
                let algorithm = LeakyBucket::new(1_000_000, 1_000_000);
                let limiter = Arc::new(RateLimiter::from_algorithm(algorithm));

                b.iter_custom(|iters| {
                    let barrier = Arc::new(std::sync::Barrier::new(threads));
                    let mut handles = vec![];

                    for _ in 0..threads {
                        let limiter = Arc::clone(&limiter);
                        let barrier = Arc::clone(&barrier);

                        let handle = thread::spawn(move || {
                            let _rt = Runtime::new().unwrap();
                            barrier.wait();

                            let start = std::time::Instant::now();
                            _rt.block_on(async {
                                for i in 0..iters {
                                    let key = format!("thread-key-{}", i % 100);
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

/// Benchmark 3: Burst workload simulation
///
/// Simulates a bursty workload where requests come in rapid succession.
/// TokenBucket should excel here as it allows bursts up to capacity.
/// LeakyBucket will rate limit more aggressively, enforcing steady rate.
fn burst_workload_simulation(c: &mut Criterion) {
    let mut group = c.benchmark_group("algorithm/burst_workload");
    group.sample_size(50); // Fewer samples since this includes sleep

    let rt = Runtime::new().unwrap();

    // TokenBucket - should allow burst of 100 requests immediately
    group.bench_function("token_bucket", |b| {
        let counter = Arc::new(AtomicU64::new(0));

        b.to_async(&rt).iter(|| {
            let counter = Arc::clone(&counter);
            async move {
                // Fresh key per iteration to get clean burst behavior
                let iteration = counter.fetch_add(1, Ordering::Relaxed);
                let key = format!("burst-key-{}", iteration);

                let algorithm = TokenBucket::new(100, 100);
                let limiter = RateLimiter::from_algorithm(algorithm);

                // Send 100 requests as fast as possible (burst)
                let mut permitted_count = 0;
                for _ in 0..100 {
                    let decision = limiter.check(black_box(&key)).await.unwrap();
                    if decision.permitted {
                        permitted_count += 1;
                    }
                }

                black_box(permitted_count)
            }
        });
    });

    // LeakyBucket - should limit bursts, enforcing steady rate
    group.bench_function("leaky_bucket", |b| {
        let counter = Arc::new(AtomicU64::new(0));

        b.to_async(&rt).iter(|| {
            let counter = Arc::clone(&counter);
            async move {
                let iteration = counter.fetch_add(1, Ordering::Relaxed);
                let key = format!("burst-key-{}", iteration);

                let algorithm = LeakyBucket::new(100, 100);
                let limiter = RateLimiter::from_algorithm(algorithm);

                // Send 100 requests as fast as possible
                let mut permitted_count = 0;
                for _ in 0..100 {
                    let decision = limiter.check(black_box(&key)).await.unwrap();
                    if decision.permitted {
                        permitted_count += 1;
                    }
                }

                black_box(permitted_count)
            }
        });
    });

    group.finish();
}

/// Benchmark 4: Steady workload simulation
///
/// Simulates a steady workload where requests come at a constant rate.
/// Both algorithms should perform similarly, but we measure to confirm.
fn steady_workload_simulation(c: &mut Criterion) {
    let mut group = c.benchmark_group("algorithm/steady_workload");
    group.sample_size(20); // Fewer samples due to sleep

    let rt = Runtime::new().unwrap();

    // TokenBucket - steady requests at exactly the refill rate
    group.bench_function("token_bucket", |b| {
        let counter = Arc::new(AtomicU64::new(0));

        b.to_async(&rt).iter(|| {
            let counter = Arc::clone(&counter);
            async move {
                let iteration = counter.fetch_add(1, Ordering::Relaxed);
                let key = format!("steady-key-{}", iteration);

                let algorithm = TokenBucket::new(10, 100); // 100/sec = 10ms per request
                let limiter = RateLimiter::from_algorithm(algorithm);

                // Send 20 requests at steady rate (10ms apart)
                let mut permitted_count = 0;
                for i in 0..20 {
                    if i > 0 {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    let decision = limiter.check(black_box(&key)).await.unwrap();
                    if decision.permitted {
                        permitted_count += 1;
                    }
                }

                black_box(permitted_count)
            }
        });
    });

    // LeakyBucket - steady requests at exactly the leak rate
    group.bench_function("leaky_bucket", |b| {
        let counter = Arc::new(AtomicU64::new(0));

        b.to_async(&rt).iter(|| {
            let counter = Arc::clone(&counter);
            async move {
                let iteration = counter.fetch_add(1, Ordering::Relaxed);
                let key = format!("steady-key-{}", iteration);

                let algorithm = LeakyBucket::new(10, 100); // 100/sec = 10ms per request
                let limiter = RateLimiter::from_algorithm(algorithm);

                // Send 20 requests at steady rate (10ms apart)
                let mut permitted_count = 0;
                for i in 0..20 {
                    if i > 0 {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    let decision = limiter.check(black_box(&key)).await.unwrap();
                    if decision.permitted {
                        permitted_count += 1;
                    }
                }

                black_box(permitted_count)
            }
        });
    });

    group.finish();
}

/// Benchmark 5: Backend protection scenario
///
/// Simulates protecting a backend service that can handle a steady 50 RPS.
/// Tests behavior under load spikes where TokenBucket might overwhelm the backend
/// while LeakyBucket maintains steady rate.
fn backend_protection_scenario(c: &mut Criterion) {
    let mut group = c.benchmark_group("algorithm/backend_protection");
    group.sample_size(30);

    let rt = Runtime::new().unwrap();

    // TokenBucket - allows burst that could overwhelm backend
    group.bench_function("token_bucket_burst_impact", |b| {
        let counter = Arc::new(AtomicU64::new(0));

        b.to_async(&rt).iter(|| {
            let counter = Arc::clone(&counter);
            async move {
                let iteration = counter.fetch_add(1, Ordering::Relaxed);
                let key = format!("backend-key-{}", iteration);

                // Backend can handle 50/sec sustained, burst of 25
                let algorithm = TokenBucket::new(25, 50);
                let limiter = RateLimiter::from_algorithm(algorithm);

                // Simulate traffic spike: 50 requests immediately
                let mut immediate_permitted = 0;
                for _ in 0..50 {
                    let decision = limiter.check(black_box(&key)).await.unwrap();
                    if decision.permitted {
                        immediate_permitted += 1;
                    }
                }

                black_box(immediate_permitted)
            }
        });
    });

    // LeakyBucket - maintains steady rate, protecting backend
    group.bench_function("leaky_bucket_steady_protection", |b| {
        let counter = Arc::new(AtomicU64::new(0));

        b.to_async(&rt).iter(|| {
            let counter = Arc::clone(&counter);
            async move {
                let iteration = counter.fetch_add(1, Ordering::Relaxed);
                let key = format!("backend-key-{}", iteration);

                // Backend can handle 50/sec sustained
                let algorithm = LeakyBucket::new(25, 50);
                let limiter = RateLimiter::from_algorithm(algorithm);

                // Simulate traffic spike: 50 requests immediately
                let mut immediate_permitted = 0;
                for _ in 0..50 {
                    let decision = limiter.check(black_box(&key)).await.unwrap();
                    if decision.permitted {
                        immediate_permitted += 1;
                    }
                }

                black_box(immediate_permitted)
            }
        });
    });

    group.finish();
}

/// Benchmark 6: Cost-based rate limiting comparison
///
/// Tests performance of cost-based operations with both algorithms.
fn cost_based_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("algorithm/cost_based");

    let rt = Runtime::new().unwrap();

    for cost in [1, 10, 100] {
        // TokenBucket
        group.bench_with_input(BenchmarkId::new("token_bucket", cost), &cost, |b, &cost| {
            let algorithm = TokenBucket::new(1_000_000, 1_000_000);
            let limiter = RateLimiter::from_algorithm(algorithm);

            b.to_async(&rt).iter(|| async {
                let result = limiter
                    .check_with_cost(black_box("test-key"), black_box(cost))
                    .await;
                black_box(result)
            });
        });

        // LeakyBucket
        group.bench_with_input(BenchmarkId::new("leaky_bucket", cost), &cost, |b, &cost| {
            let algorithm = LeakyBucket::new(1_000_000, 1_000_000);
            let limiter = RateLimiter::from_algorithm(algorithm);

            b.to_async(&rt).iter(|| async {
                let result = limiter
                    .check_with_cost(black_box("test-key"), black_box(cost))
                    .await;
                black_box(result)
            });
        });
    }

    group.finish();
}

/// Benchmark 7: High key cardinality
///
/// Tests performance with many unique keys (simulating many users/tenants).
fn high_key_cardinality(c: &mut Criterion) {
    let mut group = c.benchmark_group("algorithm/high_cardinality");

    let rt = Runtime::new().unwrap();

    for num_keys in [100, 1_000, 10_000] {
        // TokenBucket
        group.bench_with_input(
            BenchmarkId::new("token_bucket", num_keys),
            &num_keys,
            |b, &num_keys| {
                let algorithm = TokenBucket::new(1_000_000, 1_000_000);
                let limiter = RateLimiter::from_algorithm(algorithm);

                b.to_async(&rt).iter(|| async {
                    // Rotate through keys
                    let key_idx = (std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_nanos()
                        % num_keys as u128) as usize;
                    let key = format!("user-{}", key_idx);

                    let result = limiter.check(black_box(&key)).await;
                    black_box(result)
                });
            },
        );

        // LeakyBucket
        group.bench_with_input(
            BenchmarkId::new("leaky_bucket", num_keys),
            &num_keys,
            |b, &num_keys| {
                let algorithm = LeakyBucket::new(1_000_000, 1_000_000);
                let limiter = RateLimiter::from_algorithm(algorithm);

                b.to_async(&rt).iter(|| async {
                    let key_idx = (std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_nanos()
                        % num_keys as u128) as usize;
                    let key = format!("user-{}", key_idx);

                    let result = limiter.check(black_box(&key)).await;
                    black_box(result)
                });
            },
        );
    }

    group.finish();
}

/// Benchmark 8: Rate limiting effectiveness
///
/// Measures how many requests get through when actually rate limited.
/// This is more of a behavioral test than a performance test.
fn rate_limiting_effectiveness(c: &mut Criterion) {
    let mut group = c.benchmark_group("algorithm/effectiveness");
    group.sample_size(50);

    let rt = Runtime::new().unwrap();

    // TokenBucket - allows initial burst then limits
    group.bench_function("token_bucket_limiting", |b| {
        let counter = Arc::new(AtomicU64::new(0));

        b.to_async(&rt).iter(|| {
            let counter = Arc::clone(&counter);
            async move {
                let iteration = counter.fetch_add(1, Ordering::Relaxed);
                let key = format!("limit-key-{}", iteration);

                let algorithm = TokenBucket::new(10, 100);
                let limiter = RateLimiter::from_algorithm(algorithm);

                // Send 50 requests rapidly (capacity is 10)
                let mut permitted = 0;
                let mut denied = 0;

                for _ in 0..50 {
                    let decision = limiter.check(black_box(&key)).await.unwrap();
                    if decision.permitted {
                        permitted += 1;
                    } else {
                        denied += 1;
                    }
                }

                black_box((permitted, denied))
            }
        });
    });

    // LeakyBucket - enforces steady rate from start
    group.bench_function("leaky_bucket_limiting", |b| {
        let counter = Arc::new(AtomicU64::new(0));

        b.to_async(&rt).iter(|| {
            let counter = Arc::clone(&counter);
            async move {
                let iteration = counter.fetch_add(1, Ordering::Relaxed);
                let key = format!("limit-key-{}", iteration);

                let algorithm = LeakyBucket::new(10, 100);
                let limiter = RateLimiter::from_algorithm(algorithm);

                // Send 50 requests rapidly (capacity is 10)
                let mut permitted = 0;
                let mut denied = 0;

                for _ in 0..50 {
                    let decision = limiter.check(black_box(&key)).await.unwrap();
                    if decision.permitted {
                        permitted += 1;
                    } else {
                        denied += 1;
                    }
                }

                black_box((permitted, denied))
            }
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    raw_performance_single_threaded,
    raw_performance_multi_threaded,
    burst_workload_simulation,
    steady_workload_simulation,
    backend_protection_scenario,
    cost_based_comparison,
    high_key_cardinality,
    rate_limiting_effectiveness,
);

criterion_main!(benches);
