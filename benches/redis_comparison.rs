//! Redis comparison benchmark
//!
//! Compares in-memory rate limiter with Redis-backed implementation

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use redis::RedisError;
use std::sync::Arc;
use std::thread;
use tokio::runtime::Runtime;
use tokio_rate_limit::{RateLimiter, RateLimiterConfig};

/// Redis-backed token bucket implementation using Lua script
async fn redis_rate_limit_check(
    conn: &mut redis::aio::ConnectionManager,
    key: &str,
    capacity: u64,
    rate: u64,
) -> Result<bool, RedisError> {
    // Token bucket Lua script for atomic operations
    let script = r#"
        local key = KEYS[1]
        local capacity = tonumber(ARGV[1])
        local rate = tonumber(ARGV[2])
        local now = tonumber(ARGV[3])

        -- Get current state or initialize
        local state = redis.call('HMGET', key, 'tokens', 'last_refill')
        local tokens = tonumber(state[1])
        local last_refill = tonumber(state[2])

        -- Initialize if key doesn't exist
        if not tokens then
            tokens = capacity
            last_refill = now
        end

        -- Calculate refill
        local elapsed_sec = (now - last_refill) / 1000000000.0
        local new_tokens = math.min(capacity, tokens + (elapsed_sec * rate))

        -- Try to consume one token
        if new_tokens >= 1 then
            redis.call('HMSET', key, 'tokens', new_tokens - 1, 'last_refill', now)
            redis.call('EXPIRE', key, 3600)  -- 1 hour TTL
            return 1
        else
            return 0
        end
    "#;

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64;

    let result: i32 = redis::Script::new(script)
        .key(key)
        .arg(capacity)
        .arg(rate)
        .arg(now)
        .invoke_async(conn)
        .await?;

    Ok(result == 1)
}

/// Benchmark: In-memory single-threaded
fn bench_in_memory_single(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let limiter = rt.block_on(async {
        RateLimiter::new(RateLimiterConfig {
            requests_per_second: 1_000_000,
            burst: 1_000_000,
        })
    });

    c.bench_function("in_memory/single_threaded", |b| {
        b.to_async(&rt).iter(|| async {
            let permitted = limiter.check(black_box("test-key")).await.unwrap();
            black_box(permitted)
        });
    });
}

/// Benchmark: Redis single-threaded
fn bench_redis_single(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    // Try to connect to Redis
    let client_result = rt.block_on(async { redis::Client::open("redis://127.0.0.1:6379/") });

    if client_result.is_err() {
        eprintln!("Skipping Redis benchmark: Redis not available");
        return;
    }

    let client = client_result.unwrap();

    c.bench_function("redis/single_threaded", |b| {
        b.to_async(&rt).iter(|| {
            let client = client.clone();
            async move {
                let mut conn = redis::aio::ConnectionManager::new(client).await.unwrap();
                let permitted =
                    redis_rate_limit_check(&mut conn, black_box("test-key"), 1_000_000, 1_000_000)
                        .await
                        .unwrap();
                black_box(permitted)
            }
        });
    });
}

/// Benchmark: In-memory multi-threaded
fn bench_in_memory_concurrent(c: &mut Criterion) {
    let mut group = c.benchmark_group("in_memory/concurrent");

    for num_threads in [2, 4, 8, 16] {
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
                                    let key = format!("key-{}", (i + t as u64 * iters) % 100);
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

/// Benchmark: Redis multi-threaded
fn bench_redis_concurrent(c: &mut Criterion) {
    let mut group = c.benchmark_group("redis/concurrent");

    let rt = Runtime::new().unwrap();

    // Check if Redis is available
    let client_result = rt.block_on(async { redis::Client::open("redis://127.0.0.1:6379/") });

    if client_result.is_err() {
        eprintln!("Skipping Redis concurrent benchmark: Redis not available");
        return;
    }

    let client = client_result.unwrap();

    for num_threads in [2, 4, 8, 16] {
        group.throughput(Throughput::Elements(1));

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}_threads", num_threads)),
            &num_threads,
            |b, &threads| {
                b.iter_custom(|iters| {
                    let barrier = Arc::new(std::sync::Barrier::new(threads));
                    let client = client.clone();
                    let mut handles = vec![];

                    for t in 0..threads {
                        let barrier = Arc::clone(&barrier);
                        let client = client.clone();

                        let handle = thread::spawn(move || {
                            let rt = Runtime::new().unwrap();
                            let mut conn = rt.block_on(async {
                                redis::aio::ConnectionManager::new(client).await.unwrap()
                            });

                            barrier.wait();
                            let start = std::time::Instant::now();

                            rt.block_on(async {
                                for i in 0..iters {
                                    let key = format!("redis-key-{}", (i + t as u64 * iters) % 100);
                                    let _ = black_box(
                                        redis_rate_limit_check(
                                            &mut conn, &key, 1_000_000, 1_000_000,
                                        )
                                        .await,
                                    );
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
    bench_in_memory_single,
    bench_redis_single,
    bench_in_memory_concurrent,
    bench_redis_concurrent,
);

criterion_main!(benches);
