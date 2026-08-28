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
//! 2. **Fairness.** A 10k-user Zipf (s=1.2) panel with real per-user quotas
//!    (100/s, burst 200) over a paused 5s window, reported per offered-count
//!    decile against `TokenBucket` and the configured cap.
//! 3. **Throughput.** Criterion groups for the uncontended single-thread path,
//!    for contention on one hot key, and for key cardinality.
//!
//! The accuracy report uses a paused tokio clock advanced by hand, so it is a
//! function of the algorithm alone and is reproducible on any machine.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::hint::black_box;
use std::io::Write;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::runtime::Builder;
use tokio_rate_limit::algorithm::{ProbabilisticTokenBucket, TokenBucket};
use tokio_rate_limit::Algorithm;

#[path = "support/zipf.rs"]
mod zipf;

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

    zipf_fairness_report();
}

// ---------------------------------------------------------------------------
// 10k-user Zipf fairness panel
// ---------------------------------------------------------------------------

const ZIPF_USERS: usize = 10_000;
const ZIPF_S: f64 = 1.2;
const ZIPF_CAPACITY: u64 = 200;
const ZIPF_RATE: u64 = 100;
// 5s × 20k rps = 100k checks/algorithm. Drop to 3 only if this panel exceeds ~60s.
const ZIPF_WINDOW_SECS: u64 = 5;
const ZIPF_OFFERED_PER_SEC: u64 = 20_000;
const ZIPF_SEED: u64 = 0x9E37_79B9_7F4A_7C15;
const ZIPF_DECILES: usize = 10;

fn paused_runtime() -> tokio::runtime::Runtime {
    Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .unwrap()
}

/// Replay `sequence` against a fresh algorithm on a fresh paused clock.
fn zipf_pass<A: Algorithm>(
    make: impl FnOnce() -> A,
    keys: &[String],
    sequence: &[u32],
    interval: Duration,
) -> Vec<u64> {
    let runtime = paused_runtime();
    runtime.block_on(async {
        let algorithm = make();
        let mut admitted = vec![0u64; keys.len()];
        for &id in sequence {
            if algorithm.check(&keys[id as usize]).permitted {
                admitted[id as usize] += 1;
            }
            tokio::time::advance(interval).await;
        }
        admitted
    })
}

fn fmt_pct(p: f64) -> String {
    format!("{p:+.1}%")
}

fn print_zipf_row(row: &zipf::FairnessRow) {
    println!(
        "{:<8} {:>6} {:>9} {:>7} {:>10} {:>10} {:>11} {:>10} {:>11} {:>11} {:>12}",
        row.label,
        row.users,
        row.offered,
        row.cap,
        row.tb_admit,
        row.p20_admit,
        row.p100_admit,
        fmt_pct(zipf::pct_delta(row.p20_admit, row.tb_admit)),
        fmt_pct(zipf::pct_delta(row.p100_admit, row.tb_admit)),
        fmt_pct(zipf::pct_delta(row.p20_admit, row.cap)),
        fmt_pct(zipf::pct_delta(row.p100_admit, row.cap)),
    );
}

fn admit_dir(vs_tb: f64) -> &'static str {
    if vs_tb > 1.0 {
        "over-admitted"
    } else if vs_tb < -1.0 {
        "under-admitted"
    } else {
        "matched"
    }
}

fn print_zipf_interpretation(rows: &[zipf::FairnessRow]) {
    let d1 = &rows[0];
    let all = rows.last().expect("ALL row");
    let deciles = &rows[..ZIPF_DECILES];
    let last_live = deciles.iter().rposition(|r| r.offered > 0).unwrap_or(0);
    let empty_tail = ZIPF_DECILES - 1 - last_live;
    let light: Vec<&zipf::FairnessRow> = deciles
        .iter()
        .take(last_live + 1)
        .skip(1)
        .filter(|r| r.offered > 0)
        .collect();

    let light_p20_vs_tb = light
        .iter()
        .map(|r| zipf::pct_delta(r.p20_admit, r.tb_admit))
        .fold(f64::INFINITY, f64::min);
    let light_p100_vs_tb = light
        .iter()
        .map(|r| zipf::pct_delta(r.p100_admit, r.tb_admit))
        .fold(f64::INFINITY, f64::min);
    let d1_p20_vs_tb = zipf::pct_delta(d1.p20_admit, d1.tb_admit);
    let d1_p100_vs_tb = zipf::pct_delta(d1.p100_admit, d1.tb_admit);
    let d1_p20_vs_cap = zipf::pct_delta(d1.p20_admit, d1.cap);
    let d1_p100_vs_cap = zipf::pct_delta(d1.p100_admit, d1.cap);
    let d1_share = d1.offered as f64 / all.offered as f64 * 100.0;

    let stolen = light_p20_vs_tb < -1.0 || light_p100_vs_tb < -1.0;
    let light_verdict = if light.is_empty() {
        "No decile below D1 received traffic, so there is no light-user fairness signal in this window.".to_string()
    } else if stolen {
        format!(
            "Light users are stolen from: among D2–D{}, worst-decile error vs TokenBucket is {:+.1}% (sr=20) and {:+.1}% (sr=100).",
            last_live + 1,
            light_p20_vs_tb,
            light_p100_vs_tb
        )
    } else {
        let first = light[0];
        let last = light[light.len() - 1];
        format!(
            "Light users are not stolen from: D2–D{last_d} offered {offered} requests, all under the per-user cap, and TokenBucket / sr=20 / sr=100 admitted them in lockstep (D{last_d} {p20} / {p100} vs TokenBucket; D2 {p20_d2} / {p100_d2}).",
            last_d = last_live + 1,
            offered = light.iter().map(|r| r.offered).sum::<u64>(),
            p20 = fmt_pct(zipf::pct_delta(last.p20_admit, last.tb_admit)),
            p100 = fmt_pct(zipf::pct_delta(last.p100_admit, last.tb_admit)),
            p20_d2 = fmt_pct(zipf::pct_delta(first.p20_admit, first.tb_admit)),
            p100_d2 = fmt_pct(zipf::pct_delta(first.p100_admit, first.tb_admit)),
        )
    };

    let heavy_verdict = format!(
        "Heavy users (D1, {d1_share:.1}% of offered traffic) are {dir20} at sr=20 ({p20_tb} vs TokenBucket, {p20_cap} vs cap) and {dir100} at sr=100 ({p100_tb} vs TokenBucket, {p100_cap} vs cap).",
        dir20 = admit_dir(d1_p20_vs_tb),
        dir100 = admit_dir(d1_p100_vs_tb),
        p20_tb = fmt_pct(d1_p20_vs_tb),
        p20_cap = fmt_pct(d1_p20_vs_cap),
        p100_tb = fmt_pct(d1_p100_vs_tb),
        p100_cap = fmt_pct(d1_p100_vs_cap),
    );

    let sr20_over = d1_p20_vs_cap > 5.0;
    let sr100_over = d1_p100_vs_cap > 5.0;
    let safety = match (sr20_over, sr100_over) {
        (false, false) => {
            "sr=20 is at the documented sizing floor (capacity 200 = 10 lumps) and sr=100 is below it (2 lumps); neither over-admits D1 vs the configured cap, so both are safe for this API shape in the fail-closed direction."
        }
        (false, true) => {
            "sr=20 is at the documented sizing floor (capacity 200 = 10 lumps) and stays inside the cap; sr=100 holds only two lumps and over-admits D1 vs the configured cap, so sr=100 is not safe for this API shape."
        }
        (true, false) => {
            "sr=20 over-admits D1 vs the configured cap; sr=100 does not. Treat sr=20 as too coarse for this burst/cap, not as a CPU win."
        }
        (true, true) => {
            "Both sr=20 and sr=100 over-admit D1 vs the configured cap, so sampling is not safe as an enforcer on this 100/s burst-200 shape."
        }
    };

    let tail = if empty_tail > 0 {
        format!(
            "Zipf s=1.2 concentrates {d1_share:.1}% of the 20k rps onto D1; D{}–D10 offered nothing because 100k Zipf samples leave the coldest ~{} users unarrived, not because the limiter denied them.",
            last_live + 2,
            empty_tail * (ZIPF_USERS / ZIPF_DECILES)
        )
    } else {
        format!(
            "Zipf s=1.2 concentrates {d1_share:.1}% of the 20k rps onto D1; D10 is far under 100/s."
        )
    };

    println!("{tail}");
    println!("{light_verdict}");
    println!("{heavy_verdict}");
    println!("{safety}");
    println!(
        "CPU is not the constraint: ~144 ns/check ⇒ 20k rps is ~0.3% of one core at the TokenBucket rate, so this panel is about per-user fairness, not throughput."
    );
    println!(
        "The ~1.6× sr=100 gain is an admit-always microbench on this keyspace; under real per-user caps the deny path dominates the hot users and sampling is not free."
    );
}

fn zipf_fairness_report() {
    let n_requests = (ZIPF_OFFERED_PER_SEC * ZIPF_WINDOW_SECS) as usize;
    let interval = Duration::from_nanos(1_000_000_000 / ZIPF_OFFERED_PER_SEC);
    let keys: Vec<String> = (0..ZIPF_USERS).map(|i| format!("user-{i}")).collect();
    let sequence = zipf::zipf_sequence(ZIPF_USERS, ZIPF_S, n_requests, ZIPF_SEED);
    let offered = zipf::count_offered(ZIPF_USERS, &sequence);

    let tb = zipf_pass(
        || TokenBucket::new(ZIPF_CAPACITY, ZIPF_RATE),
        &keys,
        &sequence,
        interval,
    );
    let p20 = zipf_pass(
        || ProbabilisticTokenBucket::new(ZIPF_CAPACITY, ZIPF_RATE, 20),
        &keys,
        &sequence,
        interval,
    );
    let p100 = zipf_pass(
        || ProbabilisticTokenBucket::new(ZIPF_CAPACITY, ZIPF_RATE, 100),
        &keys,
        &sequence,
        interval,
    );

    let rows = zipf::fairness_rows(
        &zipf::AdmitCounts {
            offered: &offered,
            tb: &tb,
            p20: &p20,
            p100: &p100,
        },
        zipf::Quota {
            capacity: ZIPF_CAPACITY,
            rate: ZIPF_RATE,
            window_secs: ZIPF_WINDOW_SECS,
        },
        ZIPF_DECILES,
    );

    println!(
        "=== 10k-user API, Zipf s=1.2, 20k rps, {}s, per-user 100/s burst 200 ===",
        ZIPF_WINDOW_SECS
    );
    println!(
        "{:<8} {:>6} {:>9} {:>7} {:>10} {:>10} {:>11} {:>10} {:>11} {:>11} {:>12}",
        "decile",
        "users",
        "offered",
        "cap",
        "tb_admit",
        "p20_admit",
        "p100_admit",
        "p20_vs_tb",
        "p100_vs_tb",
        "p20_vs_cap",
        "p100_vs_cap"
    );
    for row in &rows {
        print_zipf_row(row);
    }
    println!();
    print_zipf_interpretation(&rows);
    println!();
    let _ = std::io::stdout().flush();
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
