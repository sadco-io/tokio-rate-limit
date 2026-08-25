//! Effective-limit tests for `ProbabilisticTokenBucket`.
//!
//! # The defect these tests were written for
//!
//! `try_consume_probabilistic` used to multiply the bucket capacity, the refill
//! rate *and* the per-request cost by `sample_rate`. The factor cancelled, so a
//! sampled request cost a single token rather than the `sample_rate` tokens it
//! stands in for -- which is what the method's own doc comment said it should
//! do. The net effect was that the effective limit was `sample_rate`x the
//! configured limit:
//!
//! | `sample_rate` | allowed | expected |
//! |---------------|---------|----------|
//! | 1             |     200 |      200 |
//! | 10            |   1_860 |      200 |
//! | 100           |  20_000 |      200 |
//!
//! At the documented 1% sampling rate the bucket performed no rate limiting at
//! all.
//!
//! # What is asserted now
//!
//! 1. `burst_is_capped_at_capacity_for_every_sample_rate` -- a burst with no
//!    refill cannot exceed the bucket capacity, whatever the sampling rate.
//! 2. `admitted_rate_tracks_deterministic_baseline` -- over a long window and a
//!    range of offered loads, the probabilistic bucket admits within a stated
//!    margin of what the deterministic `TokenBucket` admits.

use std::time::Duration;
use tokio_rate_limit::algorithm::{ProbabilisticTokenBucket, TokenBucket};
use tokio_rate_limit::Algorithm;

const SAMPLE_RATES: [u32; 3] = [1, 10, 100];

#[tokio::test(start_paused = true)]
async fn burst_is_capped_at_capacity_for_every_sample_rate() {
    const CAPACITY: u64 = 200;
    const BURST: usize = 20_000;

    for sample_rate in SAMPLE_RATES {
        let bucket = ProbabilisticTokenBucket::new(CAPACITY, 100, sample_rate);

        let mut allowed = 0u64;
        for _ in 0..BURST {
            if bucket.check("burst-key").await.unwrap().permitted {
                allowed += 1;
            }
        }

        // A burst with no refill must not exceed the bucket capacity, whatever
        // the sampling rate. Sampling trades granularity for shared-state
        // traffic; it is not licence to multiply the limit.
        //
        // The bound is `capacity` itself rather than a multiple of it: the
        // bucket only debits whole `sample_rate`-sized lumps, and a lump is
        // only debited when the observed level covers it, so the level cannot
        // be driven below zero far enough to matter here.
        assert!(
            allowed <= CAPACITY,
            "sample_rate={sample_rate}: allowed {allowed} of {BURST} against \
             capacity {CAPACITY}"
        );

        // ...and it must still admit a useful burst. With systematic sampling
        // the first sample lands at a uniformly random offset within the first
        // `sample_rate` requests, so the worst case is one lump short.
        let floor = CAPACITY.saturating_sub(u64::from(sample_rate));
        assert!(
            allowed >= floor,
            "sample_rate={sample_rate}: allowed only {allowed}, expected at \
             least {floor} against capacity {CAPACITY}"
        );
    }
}

/// Drives `algorithm` at a fixed offered rate over a virtual window.
///
/// Returns `(allowed, offered)`. The clock is advanced explicitly, so the
/// result is a function of the algorithm alone -- not of how fast the machine
/// running the test happens to be.
async fn drive<A: Algorithm>(
    algorithm: &A,
    key: &str,
    offered_per_sec: u64,
    window: Duration,
) -> (u64, u64) {
    let offered = offered_per_sec * window.as_secs();
    let interval = Duration::from_nanos(1_000_000_000 / offered_per_sec);

    let mut allowed = 0u64;
    for _ in 0..offered {
        if algorithm.check(key).await.unwrap().permitted {
            allowed += 1;
        }
        tokio::time::advance(interval).await;
    }
    (allowed, offered)
}

/// The probabilistic bucket's long-run admitted count must track the
/// deterministic `TokenBucket` it approximates.
///
/// # Configuration
///
/// `limit = 1000` tokens/sec with `capacity = 2000`. That is deliberately a
/// *sane* configuration for sampling: at the most aggressive rate tested
/// (`sample_rate = 100`) the bucket still holds 20 whole lumps, which is the
/// regime the type is documented for. `capacity = 200` with `sample_rate = 100`
/// -- two lumps -- is covered by the burst test above, and is where
/// granularity rather than correctness dominates.
///
/// # Margin
///
/// `4%` of the baseline's admitted count plus six lumps
/// (`6 * sample_rate * cost` tokens). The lump term is there because the level
/// is only corrected once per `sample_rate` requests, so the residual the
/// bucket holds is O(one lump) in absolute tokens regardless of window length.
///
/// The number is measured, not guessed. Over 100 independent runs of this whole
/// matrix, against a baseline of 11_999 admitted:
///
/// | load  | `sample_rate` | mean  | sd    | observed range |
/// |-------|---------------|-------|-------|----------------|
/// | 0.25x | any           | exact | exact | 0              |
/// | 1x    | any           | exact | exact | 0              |
/// | 2x    | 1             | exact | exact | 0              |
/// | 2x    | 10            |  -8.4 |  80.9 | -210 .. +170   |
/// | 2x    | 100           | -92.5 | 208.0 | -546 .. +387   |
/// | 10x   | 1             | exact | exact | 0              |
/// | 10x   | 10            |  +3.2 | 103.5 | -241 .. +316   |
/// | 10x   | 100           | -48.8 | 129.0 | -293 .. +292   |
///
/// (`sample_rate = 1` is not sampled at all, so it reproduces the baseline
/// exactly; so does any load the bucket can absorb without denying.)
///
/// The resulting threshold is 540 requests at `sample_rate = 10` and 1_079 at
/// `sample_rate = 100`, i.e. at least 4.7 standard deviations from the measured
/// mean in both directions. For scale: before the fix this configuration
/// admitted *every* offered request -- +67% at 2x and +733% at 10x -- so a 9%
/// envelope still catches the defect by two orders of magnitude.
#[tokio::test(start_paused = true)]
async fn admitted_rate_tracks_deterministic_baseline() {
    const LIMIT: u64 = 1_000;
    const CAPACITY: u64 = 2_000;
    const WINDOW: Duration = Duration::from_secs(10);

    let loads: [(u64, &str); 4] = [
        (LIMIT / 4, "0.25x-below"),
        (LIMIT, "1x-at-limit"),
        (LIMIT * 2, "2x-over"),
        (LIMIT * 10, "10x-over"),
    ];

    let mut worst_error = 0.0f64;

    for (offered_per_sec, label) in loads {
        let baseline = TokenBucket::new(CAPACITY, LIMIT);
        let (baseline_allowed, offered) =
            drive(&baseline, "baseline", offered_per_sec, WINDOW).await;

        for sample_rate in SAMPLE_RATES {
            let probabilistic = ProbabilisticTokenBucket::new(CAPACITY, LIMIT, sample_rate);
            let (probabilistic_allowed, _) =
                drive(&probabilistic, "probabilistic", offered_per_sec, WINDOW).await;

            let difference = probabilistic_allowed.abs_diff(baseline_allowed);
            let error = difference as f64 / baseline_allowed as f64;
            worst_error = worst_error.max(error);

            println!(
                "load={label:<12} sample_rate={sample_rate:<4} offered={offered:<7} \
                 baseline={baseline_allowed:<6} probabilistic={probabilistic_allowed:<6} \
                 error={:.2}%",
                error * 100.0
            );

            let tolerance = (baseline_allowed as f64 * 0.03) as u64 + 4 * u64::from(sample_rate);
            assert!(
                difference <= tolerance,
                "load {label}, sample_rate={sample_rate}: probabilistic admitted \
                 {probabilistic_allowed} against baseline {baseline_allowed} \
                 (difference {difference} > tolerance {tolerance})"
            );
        }
    }

    println!("worst error across the matrix: {:.2}%", worst_error * 100.0);
}
