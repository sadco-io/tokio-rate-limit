//! Empirical cost attribution for the `ProbabilisticTokenBucket` hot path.
//!
//! Run with: `cargo bench --bench component_breakdown`
//!
//! `perf` is not reliably available on this host (WSL2), so this bench
//! isolates each component of the per-request path and times it directly:
//! clock reads, hashing, the flurry guard/lookup, the per-key atomics, the
//! `async_trait` future boxing, and the decision construction. The point is to
//! answer "what does the unsampled fast path actually spend its time on?"
//! with measurements rather than intuition.
//!
//! Timings are wall-clock over large iteration counts with `black_box`
//! fences; they are attribution-grade, not criterion-grade.

use flurry::HashMap as FlurryHashMap;
use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};
use tokio_rate_limit::algorithm::{ProbabilisticTokenBucket, TokenBucket};
use tokio_rate_limit::Algorithm;

fn time<F: FnMut()>(name: &str, iters: u64, mut f: F) -> f64 {
    for _ in 0..iters / 10 {
        f();
    }
    let start = Instant::now();
    for _ in 0..iters {
        f();
    }
    let ns = start.elapsed().as_nanos() as f64 / iters as f64;
    println!("{name:<44} {ns:8.2} ns/op");
    ns
}

fn contended<F>(name: &str, threads: usize, iters: u64, f: F)
where
    F: Fn() + Send + Sync + 'static,
{
    let f = Arc::new(f);
    let barrier = Arc::new(Barrier::new(threads + 1));
    let mut handles = Vec::new();
    for _ in 0..threads {
        let f = Arc::clone(&f);
        let barrier = Arc::clone(&barrier);
        handles.push(std::thread::spawn(move || {
            barrier.wait();
            for _ in 0..iters {
                f();
            }
        }));
    }
    barrier.wait();
    let start = Instant::now();
    for handle in handles {
        handle.join().unwrap();
    }
    let elapsed = start.elapsed();
    let per_op = elapsed.as_nanos() as f64 / iters as f64;
    let aggregate = (threads as u64 * iters) as f64 / elapsed.as_secs_f64() / 1e6;
    println!("{name:<44} {per_op:8.2} ns/op/thread ({aggregate:7.2} Mops/s aggregate)");
}

fn fnv_hash(key: &str) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in key.bytes() {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn full_check<A: Algorithm>(name: &str, algorithm: &A, iters: u64) -> f64 {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let elapsed = runtime.block_on(async {
        for _ in 0..iters / 10 {
            black_box(algorithm.check(black_box("hot-key")));
        }
        let start = Instant::now();
        for _ in 0..iters {
            black_box(algorithm.check(black_box("hot-key")));
        }
        start.elapsed()
    });
    let ns = elapsed.as_nanos() as f64 / iters as f64;
    println!("{name:<44} {ns:8.2} ns/op");
    ns
}

fn full_check_contended<A>(name: &str, algorithm: Arc<A>, threads: usize, iters: u64)
where
    A: Algorithm + Send + Sync + 'static,
{
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .enable_all()
        .build()
        .unwrap();
    let elapsed = runtime.block_on(async move {
        let start = Instant::now();
        let mut handles = Vec::new();
        for _ in 0..threads {
            let algorithm = Arc::clone(&algorithm);
            handles.push(tokio::spawn(async move {
                for _ in 0..iters {
                    black_box(algorithm.check(black_box("hot-key")));
                }
            }));
        }
        for handle in handles {
            handle.await.unwrap();
        }
        start.elapsed()
    });
    let per_op = elapsed.as_nanos() as f64 / iters as f64;
    let aggregate = (threads as u64 * iters) as f64 / elapsed.as_secs_f64() / 1e6;
    println!("{name:<44} {per_op:8.2} ns/op/thread ({aggregate:7.2} Mops/s aggregate)");
}

fn main() {
    println!("\n=== single-thread component costs ===");

    time("std Instant::now", 5_000_000, || {
        black_box(Instant::now());
    });
    time("tokio Instant::now", 5_000_000, || {
        black_box(tokio::time::Instant::now());
    });
    let reference = tokio::time::Instant::now();
    time("reference.elapsed().as_nanos", 5_000_000, || {
        black_box(reference.elapsed().as_nanos() as u64);
    });

    time("fnv shard hash (7-byte key)", 10_000_000, || {
        black_box(fnv_hash(black_box("hot-key")));
    });

    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hash, Hasher};
    let random_state = RandomState::new();
    time(
        "siphash (default map hasher, 7-byte key)",
        10_000_000,
        || {
            let mut hasher = random_state.build_hasher();
            black_box("hot-key").hash(&mut hasher);
            black_box(hasher.finish());
        },
    );

    let map: FlurryHashMap<String, Arc<AtomicU64>> = FlurryHashMap::new();
    {
        let guard = map.guard();
        map.insert("hot-key".to_string(), Arc::new(AtomicU64::new(0)), &guard);
    }
    time("flurry guard() alone", 5_000_000, || {
        black_box(map.guard());
    });
    time("flurry guard + get (no clone)", 5_000_000, || {
        let guard = map.guard();
        black_box(map.get(black_box("hot-key"), &guard));
    });
    time("flurry guard + get + Arc clone/drop", 5_000_000, || {
        let guard = map.guard();
        let value = map.get(black_box("hot-key"), &guard).unwrap().clone();
        black_box(value);
    });

    let counter = AtomicU64::new(0);
    time("uncontended fetch_add (relaxed)", 10_000_000, || {
        black_box(counter.fetch_add(1, Ordering::Relaxed));
    });
    time("uncontended store (relaxed)", 10_000_000, || {
        counter.store(black_box(42), Ordering::Relaxed);
    });

    time("u128 refill arithmetic", 10_000_000, || {
        let elapsed = black_box(123_456u64);
        let rate = black_box(1_000_000_000_000u64);
        let added = (u128::from(elapsed) * u128::from(rate)) / 1_000_000_000u128;
        black_box(added.min(i64::MAX as u128) as i64);
    });

    thread_local! {
        static STATE: std::cell::Cell<u64> = const { std::cell::Cell::new(0x1234_5678_9abc_def0) };
    }
    time("thread-local xorshift", 10_000_000, || {
        STATE.with(|state| {
            let mut x = state.get();
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            state.set(x);
            black_box(x);
        });
    });

    time("decision float math (reset calc)", 10_000_000, || {
        let remaining = black_box(1234u64);
        let capacity = black_box(1_000_000_000u64);
        let rate = black_box(1_000_000_000u64);
        let to_refill = capacity.saturating_sub(remaining);
        let secs = to_refill as f64 / rate as f64;
        black_box(Duration::from_secs_f64(secs.max(0.001)));
    });

    time("Box::pin a small future (async_trait)", 5_000_000, || {
        let fut: std::pin::Pin<Box<dyn std::future::Future<Output = u64>>> =
            Box::pin(async { black_box(42u64) });
        black_box(&fut);
    });

    println!("\n=== full check(), single thread ===");
    let token_bucket = TokenBucket::new(1_000_000_000, 1_000_000_000);
    full_check("TokenBucket::check", &token_bucket, 3_000_000);
    for sample_rate in [1u32, 10, 100] {
        let bucket = ProbabilisticTokenBucket::new(1_000_000_000, 1_000_000_000, sample_rate);
        full_check(
            &format!("ProbabilisticTokenBucket sr={sample_rate}"),
            &bucket,
            3_000_000,
        );
    }

    println!("\n=== contended (8 threads, one shared cache line) ===");
    let shared = Arc::new(AtomicU64::new(0));
    {
        let shared = Arc::clone(&shared);
        contended("shared fetch_add (relaxed)", 8, 2_000_000, move || {
            black_box(shared.fetch_add(1, Ordering::Relaxed));
        });
    }
    {
        let shared = Arc::clone(&shared);
        contended("shared store (relaxed)", 8, 2_000_000, move || {
            shared.store(black_box(7), Ordering::Relaxed);
        });
    }
    {
        let shared = Arc::clone(&shared);
        contended("shared load (relaxed)", 8, 2_000_000, move || {
            black_box(shared.load(Ordering::Relaxed));
        });
    }
    {
        let shared = Arc::clone(&shared);
        contended("shared CAS success loop", 8, 2_000_000, move || {
            let mut current = shared.load(Ordering::Relaxed);
            loop {
                match shared.compare_exchange_weak(
                    current,
                    current.wrapping_add(1),
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(actual) => current = actual,
                }
            }
        });
    }
    {
        let shared = Arc::clone(&shared);
        contended(
            "Arc clone + drop (shared refcount)",
            8,
            2_000_000,
            move || {
                black_box(Arc::clone(&shared));
            },
        );
    }

    println!("\n=== full check(), 8 threads, one hot key ===");
    full_check_contended(
        "TokenBucket::check",
        Arc::new(TokenBucket::new(1_000_000_000, 1_000_000_000)),
        8,
        1_000_000,
    );
    for sample_rate in [1u32, 100] {
        full_check_contended(
            &format!("ProbabilisticTokenBucket sr={sample_rate}"),
            Arc::new(ProbabilisticTokenBucket::new(
                1_000_000_000,
                1_000_000_000,
                sample_rate,
            )),
            8,
            1_000_000,
        );
    }
    println!();
}
