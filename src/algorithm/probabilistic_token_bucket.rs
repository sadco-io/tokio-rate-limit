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
//!    traffic is interleaved across threads and keys.
//! 2. **Admission is probabilistic near empty.** A request is admitted with
//!    probability `min(1, tokens / lump)`. The admitted rate is then a
//!    continuous, monotone function of the fill level, and the bucket settles
//!    where `offered_rate * admit_probability == refill_rate` -- so the
//!    long-run admitted rate converges to `min(offered_rate, refill_rate)`.
//! 3. **A sampled request observes the bucket at a random point in its
//!    window.** Rolling the observation back by a uniformly random fraction of
//!    accrued refill removes end-of-window upward bias.
//! 4. **The debit is `lump * admit_probability`, charged deterministically**,
//!    rather than a whole lump gated on a coin flip, eliminating random-walk
//!    variance.
//!
//! # Trade-off
//!
//! - `sample_rate = 1` is deterministic (no sampling, no randomness, hard threshold).
//! - Higher sample rates do less shared-state work per request on hot keys (especially
//!   when skipping the monotonic clock read on healthy buckets), at the cost of
//!   `sample_rate * cost` lump granularity.
//! - Keep `capacity >= 10 * sample_rate * cost` for smooth behavior.

use crate::algorithm::internal::{
    duration_to_nanos_sat, fast_random, nanos_for_tokens as nanos_for_tokens_u64,
    refill_tokens as refill_tokens_u64, time_until_full, token_decision, zero_cost_decision,
    ShardedMap, SCALE,
};
use crate::algorithm::{Algorithm, RateLimitKey, ToKey};
use crate::limiter::RateLimitDecision;
use std::borrow::Borrow;
use std::fmt;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::Duration;
use tokio::time::Instant;

/// Maximum burst capacity (`i64::MAX / (2 * SCALE)`).
const MAX_BURST: u64 = (i64::MAX as u64) / (2 * SCALE);

/// Maximum refill rate per second (`i64::MAX / (2 * SCALE)`).
const MAX_RATE_PER_SEC: u64 = (i64::MAX as u64) / (2 * SCALE);

/// Scales a token count for sub-token precision, saturating at `i64::MAX`.
#[inline]
fn scaled(tokens: u64) -> i64 {
    tokens.saturating_mul(SCALE).min(i64::MAX as u64) as i64
}

/// Tokens (already scaled) accrued over `elapsed_nanos` at `rate_scaled` per second.
#[inline]
fn refill_tokens(elapsed_nanos: u64, rate_scaled: u64) -> i64 {
    refill_tokens_u64(elapsed_nanos, rate_scaled).min(i64::MAX as u64) as i64
}

/// Inverse of [`refill_tokens`]: the elapsed time that `added` tokens account for.
#[inline]
fn nanos_for_tokens(added: i64, rate_scaled: u64) -> u64 {
    if added <= 0 {
        return 0;
    }
    nanos_for_tokens_u64(added as u64, rate_scaled)
}

/// A uniformly random amount in `[0, accrued]`.
#[inline]
fn random_fraction_of(accrued: i64) -> i64 {
    if accrued <= 0 {
        return 0;
    }
    let fraction = u128::from(fast_random() >> 32);
    ((accrued as u128 * fraction) >> 32) as i64
}

/// Admission decision for a bucket holding `available` scaled tokens.
#[inline]
fn admit(available: i64, ramp: i64) -> bool {
    if available >= ramp {
        return true;
    }
    if available <= 0 {
        return false;
    }
    (fast_random() % (ramp as u64)) < (available as u64)
}

/// Clamps a signed scaled token count to the unscaled, non-negative count.
#[inline]
fn remaining_of(available: i64) -> u64 {
    (available.max(0) as u64) / SCALE
}

/// A timestamp source that reads the clock at most once per request.
struct LazyNow<'a> {
    reference: &'a Instant,
    cached: std::cell::Cell<Option<u64>>,
}

impl<'a> LazyNow<'a> {
    #[inline]
    fn new(reference: &'a Instant) -> Self {
        Self {
            reference,
            cached: std::cell::Cell::new(None),
        }
    }

    #[inline]
    fn get(&self) -> u64 {
        match self.cached.get() {
            Some(now) => now,
            None => {
                let now = duration_to_nanos_sat(self.reference.elapsed());
                self.cached.set(Some(now));
                now
            }
        }
    }
}

/// The per-key fields written on (nearly) every request, isolated on their own
/// 128-byte cache-line pair.
#[repr(align(128))]
struct HotWrites {
    request_tick: AtomicU64,
    last_access_nanos: AtomicU64,
}

/// Atomic state for a probabilistic token bucket.
#[repr(C)]
struct AtomicProbabilisticState {
    tokens: AtomicI64,
    last_refill_nanos: AtomicU64,
    hot: HotWrites,
}

impl AtomicProbabilisticState {
    fn new(capacity: u64, now_nanos: u64) -> Self {
        Self {
            tokens: AtomicI64::new(scaled(capacity)),
            last_refill_nanos: AtomicU64::new(now_nanos),
            hot: HotWrites {
                request_tick: AtomicU64::new(fast_random()),
                last_access_nanos: AtomicU64::new(now_nanos),
            },
        }
    }

    #[inline]
    fn should_sample(&self, sample_rate: u32) -> bool {
        let tick = self.hot.request_tick.fetch_add(1, Ordering::Relaxed);
        tick % u64::from(sample_rate) == 0
    }

    #[inline]
    fn cas_tokens(&self, mut f: impl FnMut(i64) -> Option<i64>) -> Result<i64, i64> {
        let mut current = self.tokens.load(Ordering::Relaxed);
        while let Some(next) = f(current) {
            match self.tokens.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(prev) => return Ok(prev),
                Err(actual) => current = actual,
            }
        }
        Err(current)
    }

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
            let _ = self.cas_tokens(|current| {
                if current >= capacity_scaled {
                    None
                } else {
                    Some(current.saturating_add(added).min(capacity_scaled))
                }
            });
            return;
        }
    }

    #[inline]
    fn estimate(&self, capacity_scaled: i64, rate_scaled: u64, now_nanos: u64) -> i64 {
        let current = self.tokens.load(Ordering::Acquire);
        let last = self.last_refill_nanos.load(Ordering::Relaxed);
        let added = refill_tokens(now_nanos.saturating_sub(last), rate_scaled);
        current.saturating_add(added).min(capacity_scaled)
    }

    #[inline]
    fn is_evictable(
        &self,
        capacity_scaled: i64,
        rate_scaled: u64,
        now_nanos: u64,
        ttl_nanos: u64,
    ) -> bool {
        let last_access = self.hot.last_access_nanos.load(Ordering::Relaxed);
        if now_nanos.saturating_sub(last_access) < ttl_nanos {
            return false;
        }
        self.estimate(capacity_scaled, rate_scaled, now_nanos) >= capacity_scaled
    }

    fn consume_exact(
        &self,
        capacity_scaled: i64,
        rate_scaled: u64,
        now_nanos: u64,
        cost_scaled: i64,
    ) -> (bool, u64) {
        self.apply_refill(capacity_scaled, rate_scaled, now_nanos);
        match self.cas_tokens(|current| {
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

    fn try_consume_probabilistic(
        &self,
        capacity: u64,
        refill_rate_per_second: u64,
        now: &LazyNow<'_>,
        cost: u64,
        sample_rate: u32,
        track_access: bool,
    ) -> (bool, u64) {
        if track_access {
            self.hot
                .last_access_nanos
                .store(now.get(), Ordering::Relaxed);
        }

        let capacity_scaled = scaled(capacity);
        let rate_scaled = refill_rate_per_second.saturating_mul(SCALE);
        let cost_scaled = scaled(cost);

        if sample_rate <= 1 {
            return self.consume_exact(capacity_scaled, rate_scaled, now.get(), cost_scaled);
        }

        let lump = cost_scaled.saturating_mul(i64::from(sample_rate));
        let ramp = lump.min(capacity_scaled).max(1);

        if !self.should_sample(sample_rate) {
            let current = self.tokens.load(Ordering::Relaxed);
            if current >= ramp {
                return (true, remaining_of(current));
            }
            let available = self.estimate(capacity_scaled, rate_scaled, now.get());
            return (admit(available, ramp), remaining_of(available));
        }

        let now_nanos = now.get();
        let current = self.tokens.load(Ordering::Acquire);
        let last_refill = self.last_refill_nanos.load(Ordering::Relaxed);
        let accrued = refill_tokens(now_nanos.saturating_sub(last_refill), rate_scaled);
        let observed = current
            .saturating_add(random_fraction_of(accrued))
            .min(capacity_scaled);

        self.apply_refill(capacity_scaled, rate_scaled, now_nanos);

        let debit = if observed >= ramp {
            lump
        } else if observed <= 0 {
            0
        } else {
            ((i128::from(lump) * i128::from(observed)) / i128::from(ramp)) as i64
        };
        let previous = if debit > 0 {
            self.cas_tokens(|tokens| Some(tokens.saturating_sub(debit)))
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
/// stands in for.
pub struct ProbabilisticTokenBucket<K: RateLimitKey = String> {
    capacity: u64,
    refill_rate_per_second: u64,
    reference_instant: Instant,
    idle_ttl: Option<Duration>,
    shards: ShardedMap<K, AtomicProbabilisticState>,
    sample_rate: u32,
    _key: PhantomData<fn() -> K>,
}

impl ProbabilisticTokenBucket<String> {
    /// Creates a new `String`-keyed probabilistic token bucket with the specified sampling rate.
    ///
    /// # Examples
    ///
    /// ```
    /// use tokio_rate_limit::algorithm::{Algorithm, ProbabilisticTokenBucket};
    ///
    /// // 5% sampling (1 in 20 requests performs shared RMW)
    /// let bucket = ProbabilisticTokenBucket::new(2_000, 100, 20);
    /// assert!(bucket.check("hot-key").permitted);
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if `sample_rate` is zero.
    pub fn new(capacity: u64, refill_rate_per_second: u64, sample_rate: u32) -> Self {
        Self::new_with_key(capacity, refill_rate_per_second, sample_rate)
    }

    /// Creates a new `String`-keyed probabilistic token bucket with TTL-based idle eviction.
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
        Self::with_ttl_and_key(capacity, refill_rate_per_second, sample_rate, idle_ttl)
    }
}

impl<K: RateLimitKey> ProbabilisticTokenBucket<K> {
    /// Creates a probabilistic token bucket keyed by an arbitrary [`RateLimitKey`] type `K`.
    ///
    /// # Panics
    ///
    /// Panics if `sample_rate` is zero.
    pub fn new_with_key(capacity: u64, refill_rate_per_second: u64, sample_rate: u32) -> Self {
        assert!(sample_rate >= 1, "Sample rate must be at least 1");

        let safe_capacity = capacity.min(MAX_BURST);
        let safe_rate = refill_rate_per_second.min(MAX_RATE_PER_SEC);

        Self {
            capacity: safe_capacity,
            refill_rate_per_second: safe_rate,
            reference_instant: Instant::now(),
            idle_ttl: None,
            shards: ShardedMap::new(),
            sample_rate,
            _key: PhantomData,
        }
    }

    /// Creates a generic-keyed probabilistic token bucket with TTL-based idle eviction.
    ///
    /// # Panics
    ///
    /// Panics if `sample_rate` is zero.
    pub fn with_ttl_and_key(
        capacity: u64,
        refill_rate_per_second: u64,
        sample_rate: u32,
        idle_ttl: Duration,
    ) -> Self {
        let mut bucket = Self::new_with_key(capacity, refill_rate_per_second, sample_rate);
        bucket.idle_ttl = Some(idle_ttl);
        bucket
    }

    /// Get the configured sampling rate.
    #[inline]
    pub fn sample_rate(&self) -> u32 {
        self.sample_rate
    }

    /// Returns the configured maximum burst capacity.
    #[inline]
    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    fn check_impl<Q>(&self, key: &Q, cost: u64) -> RateLimitDecision
    where
        K: Borrow<Q>,
        Q: Hash + Ord + ToKey<K> + ?Sized,
    {
        if cost == 0 {
            return zero_cost_decision(self.capacity);
        }

        if cost > self.capacity {
            let peeked = self.peek_impl(key);
            return RateLimitDecision {
                permitted: false,
                retry_after: None,
                remaining: peeked.remaining,
                limit: self.capacity,
                reset: peeked.reset,
            };
        }

        let now = LazyNow::new(&self.reference_instant);
        let track_access = self.idle_ttl.is_some();

        if let Some(ttl) = self.idle_ttl {
            let sample_mask =
                (u64::from(self.sample_rate).saturating_mul(16)).next_power_of_two() - 1;
            let now_nanos = now.get();
            let ttl_nanos = duration_to_nanos_sat(ttl);
            let capacity_scaled = scaled(self.capacity);
            let rate_scaled = self.refill_rate_per_second.saturating_mul(SCALE);
            self.shards.maybe_cleanup(sample_mask, |state| {
                state.is_evictable(capacity_scaled, rate_scaled, now_nanos, ttl_nanos)
            });
        }

        let (permitted, remaining) = self.shards.with_or_insert(
            key,
            || AtomicProbabilisticState::new(self.capacity, now.get()),
            |state| {
                state.try_consume_probabilistic(
                    self.capacity,
                    self.refill_rate_per_second,
                    &now,
                    cost,
                    self.sample_rate,
                    track_access,
                )
            },
        );

        token_decision(
            permitted,
            remaining,
            self.capacity,
            self.refill_rate_per_second,
            cost,
        )
    }

    fn peek_impl<Q>(&self, key: &Q) -> RateLimitDecision
    where
        K: Borrow<Q>,
        Q: Hash + Ord + ?Sized,
    {
        let now_nanos = duration_to_nanos_sat(self.reference_instant.elapsed());
        let capacity_scaled = scaled(self.capacity);
        let rate_scaled = self.refill_rate_per_second.saturating_mul(SCALE);
        match self.shards.with_existing(key, |state| {
            remaining_of(state.estimate(capacity_scaled, rate_scaled, now_nanos))
        }) {
            Some(remaining) => token_decision(
                remaining >= 1 && self.capacity >= 1,
                remaining,
                self.capacity,
                self.refill_rate_per_second,
                1,
            ),
            None => RateLimitDecision {
                permitted: self.capacity >= 1,
                retry_after: None,
                remaining: Some(self.capacity),
                limit: self.capacity,
                reset: time_until_full(0, self.refill_rate_per_second),
            },
        }
    }
}

impl<K: RateLimitKey> fmt::Debug for ProbabilisticTokenBucket<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProbabilisticTokenBucket")
            .field("capacity", &self.capacity)
            .field("refill_rate_per_second", &self.refill_rate_per_second)
            .field("sample_rate", &self.sample_rate)
            .field("idle_ttl", &self.idle_ttl)
            .field("tracked_keys", &self.shards.len())
            .finish()
    }
}

impl<K: RateLimitKey> super::private::Sealed for ProbabilisticTokenBucket<K> {}

impl<K: RateLimitKey> Algorithm for ProbabilisticTokenBucket<K> {
    type Key = K;

    #[inline]
    fn check_with_cost<Q>(&self, key: &Q, cost: u64) -> RateLimitDecision
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Ord + fmt::Debug + ToKey<Self::Key> + ?Sized,
    {
        self.check_impl(key, cost)
    }

    fn peek<Q>(&self, key: &Q) -> RateLimitDecision
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Ord + ?Sized,
    {
        self.peek_impl(key)
    }

    fn reset<Q>(&self, key: &Q) -> bool
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Ord + ?Sized,
    {
        self.shards.remove(key)
    }

    fn clear(&self) {
        self.shards.clear();
    }

    fn len(&self) -> usize {
        self.shards.len()
    }

    fn cleanup(&self) -> usize {
        let Some(ttl) = self.idle_ttl else {
            return 0;
        };
        let now_nanos = duration_to_nanos_sat(self.reference_instant.elapsed());
        let ttl_nanos = duration_to_nanos_sat(ttl);
        let capacity_scaled = scaled(self.capacity);
        let rate_scaled = self.refill_rate_per_second.saturating_mul(SCALE);
        self.shards.cleanup_all(|state| {
            state.is_evictable(capacity_scaled, rate_scaled, now_nanos, ttl_nanos)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_basic_functionality() {
        let bucket = ProbabilisticTokenBucket::new(10, 100, 1);

        for _ in 0..10 {
            let decision = bucket.check("test-key");
            assert!(decision.permitted);
        }

        let decision = bucket.check("test-key");
        assert!(!decision.permitted);
    }

    #[tokio::test]
    async fn test_multiple_keys() {
        let bucket = ProbabilisticTokenBucket::new(2, 10, 1);

        let _ = bucket.check("key1");
        let _ = bucket.check("key1");
        let decision = bucket.check("key1");
        assert!(!decision.permitted);

        let decision = bucket.check("key2");
        assert!(decision.permitted);
    }

    #[tokio::test(start_paused = true)]
    async fn test_refill() {
        let bucket = ProbabilisticTokenBucket::new(5, 10, 1);

        for _ in 0..5 {
            let _ = bucket.check("test-key");
        }

        let decision = bucket.check("test-key");
        assert!(!decision.permitted);

        tokio::time::advance(Duration::from_millis(100)).await;

        let decision = bucket.check("test-key");
        assert!(decision.permitted);
    }

    #[tokio::test]
    async fn test_probabilistic_sampling() {
        let bucket = ProbabilisticTokenBucket::new(1_000_000, 1_000_000, 100);

        for i in 0..1000 {
            let key = format!("key-{}", i % 10);
            let _ = bucket.check(&key);
        }
    }

    #[tokio::test]
    async fn test_cost_based() {
        let bucket = ProbabilisticTokenBucket::new(100, 100, 1);

        let decision = bucket.check_with_cost("test-key", 50);
        assert!(decision.permitted);
        assert!(decision.remaining.unwrap() >= 40 && decision.remaining.unwrap() <= 50);

        let decision = bucket.check_with_cost("test-key", 50);
        assert!(decision.permitted);

        let decision = bucket.check_with_cost("test-key", 50);
        assert!(!decision.permitted);

        // Cost exceeding capacity returns retry_after: None
        let unfulfillable = bucket.check_with_cost("test-key", 101);
        assert!(!unfulfillable.permitted);
        assert_eq!(unfulfillable.retry_after, None);
    }

    #[tokio::test(start_paused = true)]
    async fn test_ttl_eviction() {
        let bucket = ProbabilisticTokenBucket::with_ttl(10, 100, 1, Duration::from_secs(1));

        let _ = bucket.check("key1");
        assert_eq!(bucket.len(), 1);

        tokio::time::advance(Duration::from_secs(2)).await;

        for _ in 0..300 {
            let _ = bucket.check("key2");
        }

        assert_eq!(bucket.len(), 1);
    }
}
