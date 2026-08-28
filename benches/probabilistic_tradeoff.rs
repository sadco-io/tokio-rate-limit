//! Performance *and* accuracy benchmark for `ProbabilisticTokenBucket`.
//!
//! Run with: `cargo bench --bench probabilistic_tradeoff`
//!
//! The other probabilistic benchmark in this repo (`probabilistic_comparison`)
//! measures throughput only. Throughput on its own is not a meaningful number
//! for a sampling rate limiter -- a bucket that admits everything is infinitely
//! fast -- so this benchmark reports both halves of the trade-off:
//!
//! 1. **Accuracy.** A deterministic, virtual-clock report printed before the
//!    timing runs: for each sampling rate and offered load, how far the
//!    admitted count and the deny rate land from the deterministic
//!    `TokenBucket`. This is what makes a throughput claim honest.
//! 2. **Throughput.** Criterion groups for the uncontended single-thread path,
//!    for contention on one hot key, and for key cardinality.
//!
//! The accuracy report uses a paused tokio clock advanced by hand, so it is a
//! function of the algorithm alone and is reproducible on any machine.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::runtime::Builder;
use tokio_rate_limit::algorithm::{ProbabilisticTokenBucket, TokenBucket};
use tokio_rate_limit::Algorithm;

/// Sampling rates reported throughout. `1` is the no-sampling control: it is
/// the deterministic algorithm reached through the probabilistic type, so it
/// isolates the cost of the type itself from the cost of sampling.
const SAMPLE_RATES: [u32; 4] = [1, 10, 20, 100];

// ---------------------------------------------------------------------------
// Accuracy report
// ---------------------------------------------------------------------------

/// Offered load as a multiple of the configured limit.
const LOADS: [(u64, &str); 4] = [(1, "0.5x"), (2, "1x"), (4, "2x"), (20, "10x")];

const LIMIT: u64 = 1_000;
const CAPACITY: u64 = 2_000;
const WINDOW_SECS: u64 = 10;

/// Number of distinct keys in the interleaving section.
const KEYS: usize = 10;

/// Drives one algorithm at a fixed offered rate over a virtual window,
/// spreading the requests over `keys` keys round-robin.
async fn drive<A: Algorithm>(algorithm: &A, offered_per_sec: u64, keys: usize) -> (u64, u64) {
    let offered = offered_per_sec * WINDOW_SECS;
    let interval = Duration::from_nanos(1_000_000_000 / offered_per_sec);
    let key_names: Vec<String> = (0..keys).map(|i| format!("key-{i}")).collect();

    let mut allowed = 0u64;
    for i in 0..offered {
        let key = &key_names[(i as usize) % keys];
        if algorithm.check(key).permitted {
            allowed += 1;
        }
        tokio::time::advance(interval).await;
    }
    (allowed, offered)
}

fn accuracy_report(_c: &mut Criterion) {
    let runtime = Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .unwrap();

    println!(
        "\n=== accuracy: capacity={CAPACITY} limit={LIMIT}/s window={WINDOW_SECS}s, single key ==="
    );
    println!(
        "{:<6} {:>8} {:>10} {:>10} {:>9} {:>12} {:>12} {:>10}",
        "load",
        "offered",
        "baseline",
        "sampled",
        "admit err",
        "deny base",
        "deny sampled",
        "deny err"
    );

    for (numerator, load_label) in LOADS {
        let offered_per_sec = LIMIT * numerator / 2;

        let baseline_allowed = runtime.block_on(async {
            let baseline = TokenBucket::new(CAPACITY, LIMIT);
            drive(&baseline, offered_per_sec, 1).await.0
        });

        for sample_rate in SAMPLE_RATES {
            let (allowed, offered) = runtime.block_on(async {
                let probabilistic = ProbabilisticTokenBucket::new(CAPACITY, LIMIT, sample_rate);
                drive(&probabilistic, offered_per_sec, 1).await
            });

            let admit_error = (allowed as f64 - baseline_allowed as f64) / baseline_allowed as f64;
            let deny_baseline = 1.0 - baseline_allowed as f64 / offered as f64;
            let deny_sampled = 1.0 - allowed as f64 / offered as f64;

            println!(
                "{:<6} {:>8} {:>10} {:>10} {:>8.2}% {:>11.1}% {:>11.1}% {:>9.1}pp   sample_rate={}",
                load_label,
                offered,
                baseline_allowed,
                allowed,
                admit_error * 100.0,
                deny_baseline * 100.0,
                deny_sampled * 100.0,
                (deny_sampled - deny_baseline) * 100.0,
                sample_rate,
            );
        }
    }

    // Sampling is counted per key, so interleaving several keys must not
    // change the sampled fraction any individual key sees. Each key here is
    // given the same (well-sized) configuration as the single-key rows above
    // and the same per-key offered rate, so the only difference is the
    // interleaving.
    println!("\n=== accuracy: same per-key configuration, 10 keys visited round-robin ===");
    println!(
        "{:<6} {:>9} {:>10} {:>10} {:>9}",
        "load", "offered", "baseline", "sampled", "admit err"
    );
    for (numerator, load_label) in LOADS {
        let offered_per_sec = KEYS as u64 * LIMIT * numerator / 2;

        let baseline_allowed = runtime.block_on(async {
            let baseline = TokenBucket::new(CAPACITY, LIMIT);
            drive(&baseline, offered_per_sec, KEYS).await.0
        });

        for sample_rate in SAMPLE_RATES {
            let (allowed, offered) = runtime.block_on(async {
                let probabilistic = ProbabilisticTokenBucket::new(CAPACITY, LIMIT, sample_rate);
                drive(&probabilistic, offered_per_sec, KEYS).await
            });
            let admit_error = (allowed as f64 - baseline_allowed as f64) / baseline_allowed as f64;
            println!(
                "{:<6} {:>9} {:>10} {:>10} {:>8.2}%   sample_rate={}",
                load_label,
                offered,
                baseline_allowed,
                allowed,
                admit_error * 100.0,
                sample_rate,
            );
        }
    }

    // The documented sizing constraint, measured. A sampled request debits a
    // whole `sample_rate * cost` lump, so a bucket that is not much larger than
    // one lump is corrected too coarsely to track the deterministic bucket.
    // This is the failure mode to warn users about, and the direction of the
    // error (denying more, not less) is the safe one.
    println!("\n=== accuracy vs sizing: capacity / (sample_rate * cost), 2x offered load ===");
    println!(
        "{:<10} {:>12} {:>10} {:>10} {:>9}",
        "capacity", "lumps held", "baseline", "sampled", "admit err"
    );
    for capacity in [200u64, 1_000, 2_000, 10_000, 100_000] {
        let sample_rate = 100u32;
        let baseline_allowed = runtime.block_on(async {
            let baseline = TokenBucket::new(capacity, LIMIT);
            drive(&baseline, LIMIT * 2, 1).await.0
        });
        let (allowed, _) = runtime.block_on(async {
            let probabilistic = ProbabilisticTokenBucket::new(capacity, LIMIT, sample_rate);
            drive(&probabilistic, LIMIT * 2, 1).await
        });
        let admit_error = (allowed as f64 - baseline_allowed as f64) / baseline_allowed as f64;
        println!(
            "{:<10} {:>12} {:>10} {:>10} {:>8.2}%",
            capacity,
            capacity / u64::from(sample_rate),
            baseline_allowed,
            allowed,
            admit_error * 100.0,
        );
    }
    println!();
}

// ---------------------------------------------------------------------------
// Throughput
// ---------------------------------------------------------------------------

/// Buckets are sized so that nothing is ever denied: the timing runs measure
/// the cost of the decision, not the cost of rejecting.
const BENCH_CAPACITY: u64 = 1_000_000_000;
const BENCH_RATE: u64 = 1_000_000_000;

fn single_thread(c: &mut Criterion) {
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();

    let mut group = c.benchmark_group("probabilistic/single_thread");
    group.throughput(Throughput::Elements(1));

    group.bench_function("token_bucket_baseline", |b| {
        let bucket = TokenBucket::new(BENCH_CAPACITY, BENCH_RATE);
        b.to_async(&runtime)
            .iter(|| async { black_box(bucket.check(black_box("hot-key"))) });
    });

    for sample_rate in SAMPLE_RATES {
        group.bench_with_input(
            BenchmarkId::new("probabilistic", sample_rate),
            &sample_rate,
            |b, &sample_rate| {
                let bucket = ProbabilisticTokenBucket::new(BENCH_CAPACITY, BENCH_RATE, sample_rate);
                b.to_async(&runtime)
                    .iter(|| async { black_box(bucket.check(black_box("hot-key"))) });
            },
        );
    }

    // With an idle TTL the clock is read on every request (the TTL
    // bookkeeping needs the timestamp), so the clock-skipping fast path does
    // not apply. This row prices that.
    group.bench_function("probabilistic_ttl/100", |b| {
        let bucket = ProbabilisticTokenBucket::with_ttl(
            BENCH_CAPACITY,
            BENCH_RATE,
            100,
            std::time::Duration::from_secs(3600),
        );
        b.to_async(&runtime)
            .iter(|| async { black_box(bucket.check(black_box("hot-key"))) });
    });
    group.finish();
}

/// Throughput when every request is *denied*.
///
/// The unsampled fast path can only skip the clock while the credited level
/// covers a whole lump; a bucket pinned at empty takes the slow confirm path
/// (clock read + refill estimate) on every request. A deny-heavy workload --
/// the DDoS case this type is documented for -- must therefore be priced
/// separately from the healthy-bucket numbers above.
fn deny_path(c: &mut Criterion) {
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();

    let mut group = c.benchmark_group("probabilistic/deny_path");
    group.throughput(Throughput::Elements(1));

    // Tiny bucket, negligible refill: after the first handful of requests
    // everything is denied.
    const DENY_CAPACITY: u64 = 10;
    const DENY_RATE: u64 = 1;

    group.bench_function("token_bucket_baseline", |b| {
        let bucket = TokenBucket::new(DENY_CAPACITY, DENY_RATE);
        b.to_async(&runtime)
            .iter(|| async { black_box(bucket.check(black_box("hot-key"))) });
    });

    for sample_rate in [1u32, 100] {
        group.bench_with_input(
            BenchmarkId::new("probabilistic", sample_rate),
            &sample_rate,
            |b, &sample_rate| {
                let bucket = ProbabilisticTokenBucket::new(DENY_CAPACITY, DENY_RATE, sample_rate);
                b.to_async(&runtime)
                    .iter(|| async { black_box(bucket.check(black_box("hot-key"))) });
            },
        );
    }
    group.finish();
}

/// Runs `iterations` checks on each of `threads` tokio worker threads against a
/// single shared key, and returns the wall-clock time for all of them.
fn contended_run<A>(algorithm: Arc<A>, threads: usize, iterations: u64) -> Duration
where
    A: Algorithm + Send + Sync + 'static,
{
    let runtime = Builder::new_multi_thread()
        .worker_threads(threads)
        .enable_all()
        .build()
        .unwrap();

    runtime.block_on(async move {
        let start = Instant::now();
        let mut handles = Vec::with_capacity(threads);
        for _ in 0..threads {
            let algorithm = Arc::clone(&algorithm);
            handles.push(tokio::spawn(async move {
                for _ in 0..iterations {
                    black_box(algorithm.check(black_box("hot-key")).permitted);
                }
            }));
        }
        for handle in handles {
            handle.await.unwrap();
        }
        start.elapsed()
    })
}

fn contention(c: &mut Criterion) {
    let mut group = c.benchmark_group("probabilistic/contended_hot_key");
    group.sample_size(20);

    for threads in [1usize, 2, 4, 8] {
        // One "element" per thread per iteration: criterion then reports
        // aggregate operations/second across all threads.
        group.throughput(Throughput::Elements(threads as u64));

        group.bench_with_input(
            BenchmarkId::new("token_bucket_baseline", threads),
            &threads,
            |b, &threads| {
                b.iter_custom(|iterations| {
                    let bucket = Arc::new(TokenBucket::new(BENCH_CAPACITY, BENCH_RATE));
                    contended_run(bucket, threads, iterations)
                });
            },
        );

        for sample_rate in SAMPLE_RATES {
            group.bench_with_input(
                BenchmarkId::new(format!("probabilistic_{sample_rate}"), threads),
                &threads,
                |b, &threads| {
                    b.iter_custom(|iterations| {
                        let bucket = Arc::new(ProbabilisticTokenBucket::new(
                            BENCH_CAPACITY,
                            BENCH_RATE,
                            sample_rate,
                        ));
                        contended_run(bucket, threads, iterations)
                    });
                },
            );
        }
    }
    group.finish();
}

fn key_cardinality(c: &mut Criterion) {
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();

    let mut group = c.benchmark_group("probabilistic/key_cardinality");
    group.throughput(Throughput::Elements(1));

    for cardinality in [1usize, 1_000] {
        let keys: Vec<String> = (0..cardinality).map(|i| format!("key-{i}")).collect();
        let keys = Arc::new(keys);

        group.bench_with_input(
            BenchmarkId::new("token_bucket_baseline", cardinality),
            &cardinality,
            |b, _| {
                let bucket = TokenBucket::new(BENCH_CAPACITY, BENCH_RATE);
                let keys = Arc::clone(&keys);
                let mut index = 0usize;
                b.to_async(&runtime).iter(|| {
                    index = index.wrapping_add(1);
                    let key = keys[index % keys.len()].clone();
                    let bucket = &bucket;
                    async move { black_box(bucket.check(black_box(&key))) }
                });
            },
        );

        for sample_rate in [1u32, 100] {
            group.bench_with_input(
                BenchmarkId::new(format!("probabilistic_{sample_rate}"), cardinality),
                &cardinality,
                |b, _| {
                    let bucket =
                        ProbabilisticTokenBucket::new(BENCH_CAPACITY, BENCH_RATE, sample_rate);
                    let keys = Arc::clone(&keys);
                    let mut index = 0usize;
                    b.to_async(&runtime).iter(|| {
                        index = index.wrapping_add(1);
                        let key = keys[index % keys.len()].clone();
                        let bucket = &bucket;
                        async move { black_box(bucket.check(black_box(&key))) }
                    });
                },
            );
        }
    }
    group.finish();
}

criterion_group!(
    benches,
    accuracy_report,
    single_thread,
    deny_path,
    contention,
    key_cardinality
);
criterion_main!(benches);
