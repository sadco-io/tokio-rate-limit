//! Statistical accuracy suite for `ProbabilisticTokenBucket`.
//!
//! The single-run assertions live in `probabilistic_effective_limit.rs` and
//! `probabilistic_accuracy.rs`. The tests here characterize the *distribution*
//! of the admitted count over many independent runs -- mean, standard
//! deviation and range against the deterministic `TokenBucket` baseline --
//! which is what an accuracy claim for a randomized algorithm actually needs.
//!
//! The heavy tests are `#[ignore]`d so the normal test run stays fast; run
//! them explicitly (release mode, or they take minutes):
//!
//! ```text
//! cargo test --release --test probabilistic_statistics -- --ignored --nocapture
//! ```
//!
//! The virtual clock is advanced by hand, so the results are a function of the
//! algorithm alone and reproducible on any machine (up to RNG seeding).

use std::time::Duration;
use tokio_rate_limit::algorithm::{ProbabilisticTokenBucket, TokenBucket};
use tokio_rate_limit::Algorithm;

const LIMIT: u64 = 1_000;
const CAPACITY: u64 = 2_000;
const WINDOW: Duration = Duration::from_secs(10);
const RUNS: usize = 100;

/// Drives `algorithm` at a fixed offered rate over a virtual window.
async fn drive<A: Algorithm>(
    algorithm: &A,
    key: &str,
    offered_per_sec: u64,
    window: Duration,
) -> u64 {
    let offered = offered_per_sec * window.as_secs();
    let interval = Duration::from_nanos(1_000_000_000 / offered_per_sec);

    let mut allowed = 0u64;
    for _ in 0..offered {
        if algorithm.check(key).await.unwrap().permitted {
            allowed += 1;
        }
        tokio::time::advance(interval).await;
    }
    allowed
}

struct Stats {
    mean: f64,
    sd: f64,
    min: i64,
    max: i64,
}

fn stats_of(samples: &[i64]) -> Stats {
    let n = samples.len() as f64;
    let mean = samples.iter().sum::<i64>() as f64 / n;
    let var = samples
        .iter()
        .map(|&s| (s as f64 - mean).powi(2))
        .sum::<f64>()
        / (n - 1.0);
    Stats {
        mean,
        sd: var.sqrt(),
        min: *samples.iter().min().unwrap(),
        max: *samples.iter().max().unwrap(),
    }
}

/// Admitted count vs the deterministic baseline, distribution over [`RUNS`]
/// independent runs, at 0.25x/1x/2x/10x offered load and `sample_rate` 10/100.
///
/// The bars asserted (mean within 1.5% of baseline, worst single run within
/// 9%) are deliberately looser than the *typical* measurement (mean within
/// ~1%, worst run within ~5%) because the worst-of-100 statistic has a heavy
/// tail: at 2x load with `sample_rate = 100` the per-run sd is ~230 requests
/// (~1.9% of baseline), so a 3-sigma run -- roughly one suite execution in
/// four -- lands near 7%. Identical tails were measured on the pre-existing
/// algorithm (the fast path provably does not change the decision function),
/// so the bar guards against regressions, not against luck; the pre-fix
/// defect admitted 100% of a 10x overload and exceeds it by an order of
/// magnitude.
#[tokio::test(start_paused = true)]
#[ignore = "statistical characterization; run explicitly in release mode"]
async fn stats_admitted_vs_baseline() {
    let loads: [(u64, &str); 4] = [
        (LIMIT / 4, "0.25x"),
        (LIMIT, "1x"),
        (LIMIT * 2, "2x"),
        (LIMIT * 10, "10x"),
    ];

    println!(
        "\ncapacity={CAPACITY} limit={LIMIT}/s window={}s runs={RUNS}",
        WINDOW.as_secs()
    );
    println!(
        "{:<6} {:>4} {:>9} {:>10} {:>8} {:>8} {:>8} {:>8}",
        "load", "sr", "baseline", "mean diff", "mean %", "sd", "min", "max"
    );

    for (offered_per_sec, label) in loads {
        let baseline = TokenBucket::new(CAPACITY, LIMIT);
        let baseline_allowed = drive(&baseline, "baseline", offered_per_sec, WINDOW).await;

        // sample_rate = 1 must reproduce the deterministic baseline exactly;
        // it is a deterministic path, so a few runs suffice.
        for run in 0..3 {
            let exact = ProbabilisticTokenBucket::new(CAPACITY, LIMIT, 1);
            let allowed = drive(&exact, &format!("exact-{run}"), offered_per_sec, WINDOW).await;
            assert_eq!(
                allowed, baseline_allowed,
                "sample_rate=1 must equal TokenBucket exactly at load {label}"
            );
        }

        for sample_rate in [10u32, 100] {
            let mut diffs = Vec::with_capacity(RUNS);
            for run in 0..RUNS {
                let probabilistic = ProbabilisticTokenBucket::new(CAPACITY, LIMIT, sample_rate);
                let allowed = drive(
                    &probabilistic,
                    &format!("p-{sample_rate}-{run}"),
                    offered_per_sec,
                    WINDOW,
                )
                .await;
                diffs.push(allowed as i64 - baseline_allowed as i64);
            }
            let stats = stats_of(&diffs);
            let mean_pct = stats.mean / baseline_allowed as f64 * 100.0;
            println!(
                "{label:<6} {sample_rate:>4} {baseline_allowed:>9} {:>10.1} {mean_pct:>7.2}% {:>8.1} {:>8} {:>8}",
                stats.mean, stats.sd, stats.min, stats.max
            );

            let mean_bar = baseline_allowed as f64 * 0.015;
            assert!(
                stats.mean.abs() <= mean_bar,
                "load {label} sr={sample_rate}: |mean diff| {:.1} exceeds {:.1}",
                stats.mean.abs(),
                mean_bar
            );
            let worst = stats.min.unsigned_abs().max(stats.max.unsigned_abs());
            let worst_bar = (baseline_allowed as f64 * 0.09) as u64;
            assert!(
                worst <= worst_bar,
                "load {label} sr={sample_rate}: worst |diff| {worst} exceeds {worst_bar}"
            );
        }
    }
}

/// A burst with no refill must never exceed `capacity`, at any sampling rate,
/// over [`RUNS`] independent runs -- and must still admit a useful burst
/// (worst case one lump short, from the random sampler phase).
#[tokio::test(start_paused = true)]
#[ignore = "statistical characterization; run explicitly in release mode"]
async fn stats_burst_cap() {
    const BURST_CAPACITY: u64 = 200;
    const BURST: usize = 20_000;

    println!("\nburst {BURST} against capacity {BURST_CAPACITY}, {RUNS} runs");
    println!("{:>4} {:>8} {:>8} {:>8}", "sr", "mean", "min", "max");

    for sample_rate in [1u32, 10, 100] {
        let mut allowed_counts = Vec::with_capacity(RUNS);
        for run in 0..RUNS {
            let bucket = ProbabilisticTokenBucket::new(BURST_CAPACITY, 100, sample_rate);
            let key = format!("burst-{sample_rate}-{run}");
            let mut allowed = 0i64;
            for _ in 0..BURST {
                if bucket.check(&key).await.unwrap().permitted {
                    allowed += 1;
                }
            }
            allowed_counts.push(allowed);
        }
        let stats = stats_of(&allowed_counts);
        println!(
            "{sample_rate:>4} {:>8.1} {:>8} {:>8}",
            stats.mean, stats.min, stats.max
        );

        assert!(
            stats.max as u64 <= BURST_CAPACITY,
            "sr={sample_rate}: burst admitted {} > capacity {BURST_CAPACITY}",
            stats.max
        );
        let floor = BURST_CAPACITY.saturating_sub(u64::from(sample_rate));
        assert!(
            stats.min as u64 >= floor,
            "sr={sample_rate}: burst admitted only {}, expected at least {floor}",
            stats.min
        );
    }
}

/// Regression guard for the clock-skipping fast path: refill accrued while a
/// key sits *below* one lump must still be visible to unsampled requests.
///
/// A drained bucket left idle refills off-clock; when traffic resumes, the
/// first requests are almost all unsampled. If the fast path ever consulted
/// only the credited level (and skipped the clock when the level does *not*
/// cover the ramp), these requests would all be denied despite a full bucket.
/// The fast path may skip the clock only in the `level >= ramp` case, where
/// the decision cannot change; this test pins that boundary.
#[tokio::test(start_paused = true)]
async fn idle_refill_is_seen_by_unsampled_requests() {
    const CAP: u64 = 200;
    for sample_rate in [10u32, 100] {
        let bucket = ProbabilisticTokenBucket::new(CAP, 100, sample_rate);
        let key = "resume";

        // Drain to (at least) empty.
        for _ in 0..20_000 {
            bucket.check(key).await.unwrap();
        }

        // Idle long enough to refill to capacity.
        tokio::time::advance(Duration::from_secs(5)).await;

        // The resumed burst must be admitted immediately, not only after the
        // next sampled request happens to credit the refill.
        let mut allowed = 0u64;
        for _ in 0..(CAP as usize) {
            if bucket.check(key).await.unwrap().permitted {
                allowed += 1;
            }
        }
        assert!(
            allowed >= CAP - u64::from(sample_rate),
            "sr={sample_rate}: only {allowed} of {CAP} admitted after idle refill"
        );
        assert!(
            allowed <= CAP,
            "sr={sample_rate}: {allowed} admitted exceeds capacity {CAP} after idle refill"
        );
    }
}
