//! Probabilistic token bucket rate limiting algorithm.
//!
//! This algorithm reduces the amount of shared-state read-modify-write traffic by
//! only debiting the bucket on a sampled fraction of requests. A sampled request
//! pays for the whole group it stands in for, so the *expected* debit per request
//! is exactly the same as the deterministic [`TokenBucket`](super::TokenBucket).
//!
//! # How it stays accurate
//!
//! Four properties, each of which is load-bearing:
//!
//! 1. **Sampling is systematic and counted per key.** Each key carries its own
//!    request counter, starting from a random phase; the requests where
//!    `tick % sample_rate == 0` are sampled. The group a sampled request
//!    represents is therefore *exactly* `sample_rate` requests, however the
//!    traffic is interleaved across threads and keys. An independent coin flip
//!    per request would make the group size geometric; a counter shared across
//!    keys would alias against periodic key patterns.
//! 2. **Admission is probabilistic near empty.** Because debits arrive in lumps
//!    of `sample_rate * cost` tokens, a hard `tokens >= cost` threshold cannot
//!    produce a proportional deny rate: the bucket would be either "obviously
//!    full" (admit everything) or "obviously empty" (deny everything) for a
//!    whole inter-sample interval. Instead a request is admitted with
//!    probability `min(1, tokens / lump)`. The admitted rate is then a
//!    continuous, monotone function of the fill level, and the bucket settles
//!    where `offered_rate * admit_probability == refill_rate` -- so the
//!    long-run admitted rate converges to `min(offered_rate, refill_rate)`,
//!    which is exactly what the deterministic bucket does.
//! 3. **A sampled request observes the bucket at a random point in its
//!    window.** A sampled request necessarily arrives at the *end* of the
//!    refill window it represents, so the level it sees is systematically
//!    fuller than what its group saw. Since that observation also gates the
//!    debit, using it directly makes the bucket debit faster than it admits.
//!    Rolling the observation back by a uniformly random fraction of the
//!    accrued refill removes the bias.
//! 4. **The debit is `lump * admit_probability`, charged deterministically**,
//!    rather than a whole lump gated on a coin flip. Both are unbiased, but
//!    gating a whole lump makes the token level a random walk with steps of one
//!    lump, and that walk dominates the error.
//!
//! # Trade-off
//!
//! - `sample_rate = 1` is exactly the deterministic token bucket (no sampling,
//!   no randomness, hard threshold).
//! - Higher sample rates do less shared-state work per request but make the
//!   bucket coarser: the level is only corrected once per `sample_rate`
//!   requests, so the residual the bucket holds is O(one lump) in absolute
//!   tokens, and short observation windows are noisier.
//! - Sampling does **not** remove all shared-state traffic. Every request still
//!   performs the key lookup, the TTL timestamp store and the sampling counter
//!   increment; what sampling removes is the refill arithmetic and the
//!   compare-and-swap loop. See `benches/probabilistic_tradeoff.rs` for what
//!   that is actually worth.
//!
//! # When to Use
//!
//! - Very high request rates against a small number of hot keys.
//! - Soft rate limiting where a bounded overshoot is acceptable.
//!
//! # When NOT to Use
//!
//! - Billing, metering, or strict compliance (use [`TokenBucket`](super::TokenBucket)).
//! - Buckets whose `capacity` is not comfortably larger than
//!   `sample_rate * cost` -- there the lump granularity dominates. A good rule
//!   of thumb is `capacity >= 10 * sample_rate * cost`.

use crate::algorithm::Algorithm;
use crate::error::Result;
use crate::limiter::RateLimitDecision;
use async_trait::async_trait;
use flurry::HashMap as FlurryHashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

/// Scaling factor for sub-token precision.
const SCALE: u64 = 1000;

/// Nanoseconds in one second.
const NANOS_PER_SEC: u128 = 1_000_000_000;

/// Maximum burst capacity to prevent overflow.
///
/// Token counts are held in an `i64` (they may go transiently negative by at
/// most one lump), so the bound is derived from `i64::MAX` rather than
/// `u64::MAX`.
const MAX_BURST: u64 = (i64::MAX as u64) / (2 * SCALE);

/// Maximum refill rate per second to prevent overflow.
const MAX_RATE_PER_SEC: u64 = (i64::MAX as u64) / (2 * SCALE);

/// Number of shards for the HashMap.
const NUM_SHARDS: usize = 256;

// Fast random number generator state (thread-local).
// Using xorshift64 for speed: https://en.wikipedia.org/wiki/Xorshift
thread_local! {
    static RNG_STATE: std::cell::Cell<u64> = std::cell::Cell::new(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    );
}

/// Fast thread-local random number generator.
/// Uses xorshift64 algorithm for minimal overhead.
#[inline]
fn fast_random() -> u64 {
    RNG_STATE.with(|state| {
        let mut x = state.get();
        if x == 0 {
            x = 1;
        }
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        state.set(x);
        x
    })
}

/// Scales a token count for sub-token precision, saturating at `i64::MAX`.
#[inline]
fn scaled(tokens: u64) -> i64 {
    tokens.saturating_mul(SCALE).min(i64::MAX as u64) as i64
}

/// Tokens (already scaled) accrued over `elapsed_nanos` at `rate_scaled` per second.
#[inline]
fn refill_tokens(elapsed_nanos: u64, rate_scaled: u64) -> i64 {
    if elapsed_nanos == 0 || rate_scaled == 0 {
        return 0;
    }
    let added = (u128::from(elapsed_nanos) * u128::from(rate_scaled)) / NANOS_PER_SEC;
    added.min(i64::MAX as u128) as i64
}

/// Inverse of [`refill_tokens`]: the elapsed time that `added` tokens account for.
///
/// Used so that the fractional remainder of a refill is carried forward instead
/// of being discarded, which would let the bucket drift slow.
#[inline]
fn nanos_for_tokens(added: i64, rate_scaled: u64) -> u64 {
    if added <= 0 || rate_scaled == 0 {
        return 0;
    }
    let nanos = (added as u128 * NANOS_PER_SEC) / u128::from(rate_scaled);
    nanos.min(u128::from(u64::MAX)) as u64
}

/// A uniformly random amount in `[0, accrued]`.
///
/// Used to place a sampled request at a random point inside the refill window
/// it represents, rather than always at the end of it.
#[inline]
fn random_fraction_of(accrued: i64) -> i64 {
    if accrued <= 0 {
        return 0;
    }
    // xorshift64 has good high bits; take 32 of them as the fraction.
    let fraction = u128::from(fast_random() >> 32);
    ((accrued as u128 * fraction) >> 32) as i64
}

/// Admission decision for a bucket holding `available` scaled tokens.
///
/// Admits unconditionally once the bucket covers a whole lump, and otherwise
/// admits with probability `available / lump`. The ramp is what turns the
/// lumpy debit stream into a proportional deny rate: at equilibrium the bucket
/// sits at the fill level where `offered_rate * available / lump == refill_rate`,
/// so the admitted rate is the refill rate.
#[inline]
fn admit(available: i64, lump: i64) -> bool {
    if available >= lump {
        return true;
    }
    if available <= 0 {
        return false;
    }
    // `lump` is >= 1 here because `available` is >= 1 and < `lump`.
    (fast_random() % (lump as u64)) < (available as u64)
}

/// Clamps a signed scaled token count to the unscaled, non-negative count
/// reported to callers.
#[inline]
fn remaining_of(available: i64) -> u64 {
    (available.max(0) as u64) / SCALE
}

/// Atomic state for a probabilistic token bucket.
struct AtomicProbabilisticState {
    /// Available tokens, scaled by [`SCALE`].
    ///
    /// Signed on purpose. A sampled request debits a whole lump
    /// (`sample_rate * cost`), which can overdraw a bucket that was admitting
    /// on the ramp. Allowing the count to go transiently negative (by strictly
    /// less than one lump) is what keeps the estimator unbiased -- clamping at
    /// zero would silently forgive part of every overdraft and make the limiter
    /// systematically too permissive under sustained overload.
    tokens: AtomicI64,

    /// Last refill timestamp in nanoseconds
    last_refill_nanos: AtomicU64,

    /// Last access timestamp for TTL tracking
    last_access_nanos: AtomicU64,

    /// Per-key systematic sampler tick.
    ///
    /// Incremented once per request to this key; the requests where
    /// `tick % sample_rate == 0` are the sampled ones. Being per *key* rather
    /// than per thread is what makes the group size exactly `sample_rate`
    /// however the requests are interleaved across threads or keys.
    ///
    /// A shared per-thread counter was tried first and is not viable: it
    /// aliases catastrophically against periodic key patterns. With 100 keys
    /// visited round-robin and `sample_rate = 100`, `tick % 100 == 0` lands on
    /// the same key every time, so one key is sampled on every request and the
    /// other 99 are never sampled at all -- measured 90_109 admitted against a
    /// deterministic baseline of 11_900.
    ///
    /// An independent Bernoulli draw per request avoids the aliasing but makes
    /// the group size geometric rather than exactly `sample_rate`, and the
    /// debit is capped at one lump: long gaps then under-debit. Measured at 2x
    /// overload with 1% sampling that over-admitted by 26% on a single key and
    /// swung between -33% and +38% across 100 keys.
    ///
    /// The counter starts at a random phase so the sampled positions cannot be
    /// predicted by a client trying to dodge them.
    request_tick: AtomicU64,
}

impl AtomicProbabilisticState {
    fn new(capacity: u64, now_nanos: u64) -> Self {
        Self {
            tokens: AtomicI64::new(scaled(capacity)),
            last_refill_nanos: AtomicU64::new(now_nanos),
            last_access_nanos: AtomicU64::new(now_nanos),
            request_tick: AtomicU64::new(fast_random()),
        }
    }

    /// Returns `true` if this request should perform the shared-state update.
    ///
    /// Exactly one request in `sample_rate` returns `true`, counted per key.
    #[inline]
    fn should_sample(&self, sample_rate: u32) -> bool {
        let tick = self.request_tick.fetch_add(1, Ordering::Relaxed);
        tick % u64::from(sample_rate) == 0
    }

    /// Credits elapsed time to the bucket.
    ///
    /// The elapsed interval is *claimed* with a CAS on `last_refill_nanos`
    /// before the tokens are credited, so two threads refilling concurrently
    /// cannot both credit the same interval. Only the whole tokens covered by
    /// the interval are claimed; the sub-token remainder stays on the clock and
    /// is picked up by the next refill.
    fn apply_refill(&self, capacity_scaled: i64, rate_scaled: u64, now_nanos: u64) {
        loop {
            let last = self.last_refill_nanos.load(Ordering::Relaxed);
            let added = refill_tokens(now_nanos.saturating_sub(last), rate_scaled);
            if added == 0 {
                return;
            }
            let claimed = nanos_for_tokens(added, rate_scaled);
            if self
                .last_refill_nanos
                .compare_exchange_weak(
                    last,
                    last.saturating_add(claimed),
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                )
                .is_err()
            {
                continue;
            }
            let _ = self
                .tokens
                .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |current| {
                    if current >= capacity_scaled {
                        None
                    } else {
                        Some(current.saturating_add(added).min(capacity_scaled))
                    }
                });
            return;
        }
    }

    /// Read-only estimate of the current fill level, including refill that has
    /// accrued but has not been credited yet.
    ///
    /// Two relaxed loads and no read-modify-write -- this is the whole point of
    /// the unsampled path.
    #[inline]
    fn estimate(&self, capacity_scaled: i64, rate_scaled: u64, now_nanos: u64) -> i64 {
        let current = self.tokens.load(Ordering::Relaxed);
        let last = self.last_refill_nanos.load(Ordering::Relaxed);
        let added = refill_tokens(now_nanos.saturating_sub(last), rate_scaled);
        current.saturating_add(added).min(capacity_scaled)
    }

    /// Exact (unsampled) consumption, used when `sample_rate == 1`.
    fn consume_exact(
        &self,
        capacity_scaled: i64,
        rate_scaled: u64,
        now_nanos: u64,
        cost_scaled: i64,
    ) -> (bool, u64) {
        self.apply_refill(capacity_scaled, rate_scaled, now_nanos);
        match self
            .tokens
            .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |current| {
                if current >= cost_scaled {
                    Some(current - cost_scaled)
                } else {
                    None
                }
            }) {
            Ok(previous) => (true, remaining_of(previous - cost_scaled)),
            Err(current) => (false, remaining_of(current)),
        }
    }

    /// Try to consume tokens probabilistically.
    ///
    /// Only performs the read-modify-write 1 request in `sample_rate` of the
    /// time. A sampled request debits `sample_rate * cost` tokens, standing in
    /// for the group it represents. Every request -- sampled or not -- is
    /// admitted with probability `min(1, tokens / (sample_rate * cost))`, which
    /// is what keeps the debit rate equal to the admitted rate and the deny
    /// rate proportional to the overload.
    ///
    /// Returns `(permitted, remaining_tokens)`.
    fn try_consume_probabilistic(
        &self,
        capacity: u64,
        refill_rate_per_second: u64,
        now_nanos: u64,
        cost: u64,
        sample_rate: u32,
    ) -> (bool, u64) {
        // Update last access time (Relaxed is fine for TTL tracking)
        self.last_access_nanos.store(now_nanos, Ordering::Relaxed);

        let capacity_scaled = scaled(capacity);
        let rate_scaled = refill_rate_per_second.saturating_mul(SCALE);
        let cost_scaled = scaled(cost);

        if sample_rate <= 1 {
            return self.consume_exact(capacity_scaled, rate_scaled, now_nanos, cost_scaled);
        }

        // What one sampled request pays on behalf of its group.
        let lump = cost_scaled.saturating_mul(i64::from(sample_rate));
        // The admission ramp is never wider than the bucket itself: a bucket
        // that cannot physically hold a whole lump would otherwise never reach
        // probability 1 and would deny even when completely full.
        let ramp = lump.min(capacity_scaled).max(1);

        if !self.should_sample(sample_rate) {
            // NON-SAMPLED PATH: two relaxed loads, no read-modify-write.
            let available = self.estimate(capacity_scaled, rate_scaled, now_nanos);
            return (admit(available, ramp), remaining_of(available));
        }

        // SAMPLED PATH.
        //
        // The sampled request is, by construction, the *last* request of the
        // group it represents: it arrives once the whole inter-sample refill
        // has accrued, while the requests it stands in for saw the bucket
        // somewhere between the previous sample and now. Deciding on the
        // end-of-window fill level therefore systematically over-states the
        // group's fill, and -- because that same decision gates the debit --
        // makes the bucket debit faster than it admits. (Measured before this
        // correction: 347 admitted where the deterministic bucket admits 700,
        // at 2x the configured rate with 1% sampling.)
        //
        // Taking the decision at a uniformly random point inside the accrual
        // window restores the balance: the sampled request's observation is
        // then drawn from the same distribution as its group's, so
        // E[debit rate] == cost * E[admit rate] at every fill level.
        let current = self.tokens.load(Ordering::Relaxed);
        let last_refill = self.last_refill_nanos.load(Ordering::Relaxed);
        let accrued = refill_tokens(now_nanos.saturating_sub(last_refill), rate_scaled);
        let observed = current
            .saturating_add(random_fraction_of(accrued))
            .min(capacity_scaled);

        self.apply_refill(capacity_scaled, rate_scaled, now_nanos);

        // The debit is the *expected* consumption of the whole group, charged
        // deterministically -- `lump * admit_probability` -- rather than a whole
        // lump gated on this one request's coin flip.
        //
        // Both are unbiased, but the deterministic form has far lower variance:
        // gating a whole lump on a coin flip makes the token level a random walk
        // with steps of `lump`, and that walk is the dominant error term.
        // Measured over 25 runs at 1% sampling and 10x overload, the coin-flip
        // form spread the admitted count over -10.9%..+15.4% of the
        // deterministic baseline; charging `lump * p` holds it inside 1%.
        //
        // `lump * p` is `observed` in the usual case (where `ramp == lump`), so
        // an overloaded bucket is drained to exactly empty rather than into
        // debt.
        let debit = if observed >= ramp {
            lump
        } else if observed <= 0 {
            0
        } else {
            ((i128::from(lump) * i128::from(observed)) / i128::from(ramp)) as i64
        };
        let previous = if debit > 0 {
            self.tokens
                .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |tokens| {
                    Some(tokens.saturating_sub(debit))
                })
                .unwrap_or(observed)
        } else {
            self.tokens.load(Ordering::Relaxed)
        };

        (
            admit(observed, ramp),
            remaining_of(previous.saturating_sub(debit)),
        )
    }
}

/// Probabilistic token bucket with fixed sampling rate.
///
/// Only one request in `sample_rate` performs the shared-state read-modify-write;
/// that request debits `sample_rate * cost` tokens on behalf of the group it
/// stands in for. Admission is probabilistic on the ramp (see the module docs),
/// so the long-run admitted rate converges to `min(offered_rate, refill_rate)`
/// exactly as the deterministic bucket does.
///
/// # Accuracy
///
/// The estimator is unbiased: the expected debit per admitted request is `cost`
/// for every sampling rate. What sampling costs is *granularity*, not
/// correctness:
///
/// - The token level is corrected once per `sample_rate` requests, so a burst
///   against a full bucket can overshoot `capacity` by up to about one lump
///   (`sample_rate * cost` tokens).
/// - Short observation windows are noisier, in proportion to how few samples
///   they contain.
///
/// `sample_rate = 1` disables sampling entirely and is exactly the deterministic
/// token bucket.
///
/// # Use Cases
///
/// Ideal for:
/// - Very high request rates concentrated on a few hot keys
/// - Soft rate limiting (DDoS protection) where bounded overshoot is fine
///
/// Not suitable for:
/// - Billing/metering (requires exact counts)
/// - Strict compliance scenarios
/// - Buckets whose capacity is not comfortably larger than `sample_rate * cost`
pub struct ProbabilisticTokenBucket {
    capacity: u64,
    refill_rate_per_second: u64,
    reference_instant: Instant,
    idle_ttl: Option<Duration>,
    shards: Vec<Arc<FlurryHashMap<String, Arc<AtomicProbabilisticState>>>>,

    /// Sampling rate: 1 in N requests are sampled.
    /// - 100 = 1% sampling
    /// - 20 = 5% sampling
    /// - 10 = 10% sampling
    sample_rate: u32,
}

impl ProbabilisticTokenBucket {
    /// Gets the shard index for a given key.
    #[inline]
    fn get_shard_index(key: &str) -> usize {
        let mut hash: u64 = 0xcbf29ce484222325;
        for byte in key.bytes() {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
        (hash as usize) & (NUM_SHARDS - 1)
    }

    #[inline]
    fn get_shard(&self, key: &str) -> &Arc<FlurryHashMap<String, Arc<AtomicProbabilisticState>>> {
        let index = Self::get_shard_index(key);
        &self.shards[index]
    }

    /// Creates a new probabilistic token bucket with the specified sampling rate.
    ///
    /// # Arguments
    ///
    /// * `capacity` - Maximum tokens (burst size)
    /// * `refill_rate_per_second` - Tokens added per second
    /// * `sample_rate` - 1 in N requests are sampled (e.g., 100 = 1% sampling)
    ///
    /// # Choosing a sampling rate
    ///
    /// The estimator is unbiased at every sampling rate; what changes is
    /// granularity. Keep `capacity` well above `sample_rate * cost` -- a good
    /// rule of thumb is `capacity >= 10 * sample_rate * cost` -- otherwise the
    /// bucket is corrected too rarely relative to its own size and bursts
    /// overshoot noticeably.
    ///
    /// - 1: no sampling, exactly the deterministic token bucket
    /// - 10 (10%): a good default when the bucket holds hundreds of tokens
    /// - 100 (1%): only for large buckets and very high request rates
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // 1% sampling: 100 req/sec limit, 1% of requests perform the debit
    /// let bucket = ProbabilisticTokenBucket::new(200, 100, 100);
    ///
    /// // 5% sampling: finer granularity, still cheap
    /// let bucket = ProbabilisticTokenBucket::new(200, 100, 20);
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if `sample_rate` is zero.
    pub fn new(capacity: u64, refill_rate_per_second: u64, sample_rate: u32) -> Self {
        assert!(sample_rate >= 1, "Sample rate must be at least 1");

        let safe_capacity = capacity.min(MAX_BURST);
        let safe_rate = refill_rate_per_second.min(MAX_RATE_PER_SEC);

        let shards = (0..NUM_SHARDS)
            .map(|_| Arc::new(FlurryHashMap::new()))
            .collect();

        Self {
            capacity: safe_capacity,
            refill_rate_per_second: safe_rate,
            reference_instant: Instant::now(),
            idle_ttl: None,
            shards,
            sample_rate,
        }
    }

    /// Creates a new probabilistic token bucket with TTL-based eviction.
    ///
    /// # Panics
    ///
    /// Panics if `sample_rate` is zero.
    pub fn with_ttl(
        capacity: u64,
        refill_rate_per_second: u64,
        sample_rate: u32,
        idle_ttl: Duration,
    ) -> Self {
        let mut bucket = Self::new(capacity, refill_rate_per_second, sample_rate);
        bucket.idle_ttl = Some(idle_ttl);
        bucket
    }

    #[inline]
    fn now_nanos(&self) -> u64 {
        self.reference_instant.elapsed().as_nanos() as u64
    }

    fn cleanup_idle(&self, now_nanos: u64) {
        if let Some(ttl) = self.idle_ttl {
            let ttl_nanos = ttl.as_nanos() as u64;

            for shard in &self.shards {
                let guard = shard.guard();
                let keys_to_remove: Vec<String> = shard
                    .iter(&guard)
                    .filter_map(|(key, state)| {
                        let last_access = state.last_access_nanos.load(Ordering::Relaxed);
                        let age = now_nanos.saturating_sub(last_access);
                        if age >= ttl_nanos {
                            Some(key.clone())
                        } else {
                            None
                        }
                    })
                    .collect();

                for key in keys_to_remove {
                    shard.remove(&key, &guard);
                }
            }
        }
    }

    /// Get the configured sampling rate.
    pub fn sample_rate(&self) -> u32 {
        self.sample_rate
    }

    /// Get the total number of keys across all shards.
    #[cfg(test)]
    fn len(&self) -> usize {
        self.shards.iter().map(|shard| shard.len()).sum()
    }
}

impl super::private::Sealed for ProbabilisticTokenBucket {}

#[async_trait]
impl Algorithm for ProbabilisticTokenBucket {
    async fn check(&self, key: &str) -> Result<RateLimitDecision> {
        let now = self.now_nanos();

        // Probabilistic cleanup (1% of sampled requests)
        if self.idle_ttl.is_some() && (fast_random() % (self.sample_rate as u64 * 100)) == 0 {
            self.cleanup_idle(now);
        }

        let shard = self.get_shard(key);
        let guard = shard.guard();
        let state = match shard.get(key, &guard) {
            Some(state) => state.clone(),
            None => {
                let new_state = Arc::new(AtomicProbabilisticState::new(self.capacity, now));
                let key_string = key.to_string();
                match shard.try_insert(key_string, new_state.clone(), &guard) {
                    Ok(_) => new_state,
                    Err(current) => current.current.clone(),
                }
            }
        };

        let (permitted, remaining) = state.try_consume_probabilistic(
            self.capacity,
            self.refill_rate_per_second,
            now,
            1,
            self.sample_rate,
        );

        let retry_after = if !permitted {
            let tokens_needed = 1u64.saturating_sub(remaining);
            let seconds_to_wait = if self.refill_rate_per_second > 0 {
                (tokens_needed as f64 / self.refill_rate_per_second as f64).ceil()
            } else {
                1.0
            };
            Some(Duration::from_secs_f64(seconds_to_wait.max(0.001)))
        } else {
            None
        };

        let reset = if self.refill_rate_per_second > 0 && remaining < self.capacity {
            let tokens_to_refill = self.capacity.saturating_sub(remaining);
            let seconds_to_full = tokens_to_refill as f64 / self.refill_rate_per_second as f64;
            Some(Duration::from_secs_f64(seconds_to_full.max(0.001)))
        } else if remaining >= self.capacity {
            Some(Duration::from_secs(0))
        } else {
            None
        };

        Ok(RateLimitDecision {
            permitted,
            retry_after,
            remaining: Some(remaining),
            limit: self.capacity,
            reset,
        })
    }

    async fn check_with_cost(&self, key: &str, cost: u64) -> Result<RateLimitDecision> {
        let now = self.now_nanos();

        if self.idle_ttl.is_some() && (fast_random() % (self.sample_rate as u64 * 100)) == 0 {
            self.cleanup_idle(now);
        }

        let shard = self.get_shard(key);
        let guard = shard.guard();
        let state = match shard.get(key, &guard) {
            Some(state) => state.clone(),
            None => {
                let new_state = Arc::new(AtomicProbabilisticState::new(self.capacity, now));
                let key_string = key.to_string();
                match shard.try_insert(key_string, new_state.clone(), &guard) {
                    Ok(_) => new_state,
                    Err(current) => current.current.clone(),
                }
            }
        };

        let (permitted, remaining) = state.try_consume_probabilistic(
            self.capacity,
            self.refill_rate_per_second,
            now,
            cost,
            self.sample_rate,
        );

        let retry_after = if !permitted {
            let tokens_needed = cost.saturating_sub(remaining);
            let seconds_to_wait = if self.refill_rate_per_second > 0 {
                (tokens_needed as f64 / self.refill_rate_per_second as f64).ceil()
            } else {
                1.0
            };
            Some(Duration::from_secs_f64(seconds_to_wait.max(0.001)))
        } else {
            None
        };

        let reset = if self.refill_rate_per_second > 0 && remaining < self.capacity {
            let tokens_to_refill = self.capacity.saturating_sub(remaining);
            let seconds_to_full = tokens_to_refill as f64 / self.refill_rate_per_second as f64;
            Some(Duration::from_secs_f64(seconds_to_full.max(0.001)))
        } else if remaining >= self.capacity {
            Some(Duration::from_secs(0))
        } else {
            None
        };

        Ok(RateLimitDecision {
            permitted,
            retry_after,
            remaining: Some(remaining),
            limit: self.capacity,
            reset,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_basic_functionality() {
        // Use 100% sampling (sample_rate=1) for deterministic testing
        let bucket = ProbabilisticTokenBucket::new(10, 100, 1);

        // First 10 requests should succeed
        for _ in 0..10 {
            let decision = bucket.check("test-key").await.unwrap();
            assert!(decision.permitted);
        }

        // 11th should fail
        let decision = bucket.check("test-key").await.unwrap();
        assert!(!decision.permitted);
    }

    #[tokio::test]
    async fn test_multiple_keys() {
        let bucket = ProbabilisticTokenBucket::new(2, 10, 1);

        bucket.check("key1").await.unwrap();
        bucket.check("key1").await.unwrap();
        let decision = bucket.check("key1").await.unwrap();
        assert!(!decision.permitted);

        let decision = bucket.check("key2").await.unwrap();
        assert!(decision.permitted);
    }

    #[tokio::test(start_paused = true)]
    async fn test_refill() {
        let bucket = ProbabilisticTokenBucket::new(5, 10, 1);

        // Exhaust bucket
        for _ in 0..5 {
            bucket.check("test-key").await.unwrap();
        }

        let decision = bucket.check("test-key").await.unwrap();
        assert!(!decision.permitted);

        // Wait for refill
        tokio::time::advance(Duration::from_millis(100)).await;

        let decision = bucket.check("test-key").await.unwrap();
        assert!(decision.permitted);
    }

    #[tokio::test]
    async fn test_probabilistic_sampling() {
        // With high sample rate, most requests should be fast path
        let bucket = ProbabilisticTokenBucket::new(1_000_000, 1_000_000, 100);

        // Run many requests - should not panic or deadlock
        for i in 0..1000 {
            let key = format!("key-{}", i % 10);
            let _ = bucket.check(&key).await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_cost_based() {
        let bucket = ProbabilisticTokenBucket::new(100, 100, 1);

        // Consume 50 tokens
        let decision = bucket.check_with_cost("test-key", 50).await.unwrap();
        assert!(decision.permitted);
        assert!(decision.remaining.unwrap() >= 40 && decision.remaining.unwrap() <= 50);

        // Consume another 50
        let decision = bucket.check_with_cost("test-key", 50).await.unwrap();
        assert!(decision.permitted);

        // Should be exhausted
        let decision = bucket.check_with_cost("test-key", 50).await.unwrap();
        assert!(!decision.permitted);
    }

    #[tokio::test(start_paused = true)]
    async fn test_ttl_eviction() {
        let bucket = ProbabilisticTokenBucket::with_ttl(10, 100, 1, Duration::from_secs(1));

        bucket.check("key1").await.unwrap();
        assert_eq!(bucket.len(), 1);

        tokio::time::advance(Duration::from_secs(2)).await;

        // Trigger cleanup
        for _ in 0..200 {
            bucket.check("key2").await.unwrap();
        }

        // key1 should eventually be evicted
        let count = bucket.len();
        assert!((1..=2).contains(&count));
    }
}
