//! Leaky bucket rate limiting algorithm implementation.

use crate::algorithm::internal::{
    duration_to_nanos_sat, zero_cost_decision, AtomicTatState, ShardedMap, TatParams,
    MAX_TAT_BURST, MAX_TAT_RATE_PER_SEC,
};
use crate::algorithm::{Algorithm, RateLimitKey, ToKey};
use crate::limiter::RateLimitDecision;
use std::borrow::Borrow;
use std::fmt;
use std::hash::Hash;
use std::marker::PhantomData;
use std::time::Duration;
use tokio::time::Instant;

/// Maximum capacity (`u64::MAX / 2`).
const MAX_CAPACITY: u64 = MAX_TAT_BURST;

/// Maximum leak rate per second (`1_000_000_000`).
const MAX_LEAK_RATE: u64 = MAX_TAT_RATE_PER_SEC;

/// Mask for amortized round-robin shard cleanup (`1` in `16` checks sweeps `16` shards).
const CLEANUP_SAMPLE_MASK: u64 = 15;

/// Leaky bucket (as a meter) rate limiting algorithm.
///
/// Implements the ITU-T I.371 / ATM Forum Generic Cell Rate Algorithm (GCRA)
/// leaky-bucket-as-a-meter traffic policer over 256 lock-free shards.
///
/// Each key starts empty (`0` water level, `capacity` remaining space). Incoming
/// requests add `cost` units of water; water drains continuously at `leak_rate_per_second`.
/// If adding `cost` would exceed `capacity`, the request is rejected without modifying
/// the bucket and `retry_after` reports the exact duration until enough water drains
/// to accommodate `cost`.
///
/// # Pacing vs Tolerance
///
/// - Setting `capacity = 1` enforces strict zero-burst inter-arrival spacing (`1 / leak_rate`).
/// - Setting `capacity > 1` allows up to `capacity` queued units before overflow while
///   enforcing the long-term `leak_rate_per_second` ceiling.
///
/// # Examples
///
/// ```
/// use tokio_rate_limit::algorithm::LeakyBucket;
/// use tokio_rate_limit::RateLimiter;
///
/// let algorithm = LeakyBucket::new(50, 100);
/// let limiter = RateLimiter::from_algorithm(algorithm);
/// let decision = limiter.check("client-123");
/// assert!(decision.permitted);
/// ```
pub struct LeakyBucket<K: RateLimitKey = String> {
    params: TatParams,
    leak_rate_per_second: u64,
    reference_instant: Instant,
    idle_ttl: Option<Duration>,
    shards: ShardedMap<K, AtomicTatState>,
    _key: PhantomData<fn() -> K>,
}

impl LeakyBucket<String> {
    /// Creates a new `String`-keyed leaky bucket with the specified capacity and leak rate.
    ///
    /// # Examples
    ///
    /// ```
    /// use tokio_rate_limit::algorithm::{Algorithm, LeakyBucket};
    ///
    /// let bucket = LeakyBucket::new(50, 100);
    /// assert!(bucket.check("client-1").permitted);
    /// ```
    pub fn new(capacity: u64, leak_rate_per_second: u64) -> Self {
        Self::new_with_key(capacity, leak_rate_per_second)
    }

    /// Creates a new `String`-keyed leaky bucket with TTL-based idle eviction.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use tokio_rate_limit::algorithm::{Algorithm, LeakyBucket};
    ///
    /// let bucket = LeakyBucket::with_ttl(50, 100, Duration::from_secs(3600));
    /// assert!(bucket.check("client-1").permitted);
    /// ```
    pub fn with_ttl(capacity: u64, leak_rate_per_second: u64, idle_ttl: Duration) -> Self {
        Self::with_ttl_and_key(capacity, leak_rate_per_second, idle_ttl)
    }

    /// Creates a new `String`-keyed leaky bucket with an exact per-unit leak interval.
    pub fn with_period(capacity: u64, leak_interval: Duration) -> Self {
        Self::with_period_and_key(capacity, leak_interval)
    }

    /// Creates a new `String`-keyed leaky bucket with an exact per-unit leak interval and idle TTL.
    pub fn with_period_and_ttl(capacity: u64, leak_interval: Duration, idle_ttl: Duration) -> Self {
        Self::with_period_and_ttl_and_key(capacity, leak_interval, idle_ttl)
    }
}

impl<K: RateLimitKey> LeakyBucket<K> {
    /// Creates a leaky bucket keyed by an arbitrary [`RateLimitKey`] type `K`.
    pub fn new_with_key(capacity: u64, leak_rate_per_second: u64) -> Self {
        let safe_capacity = capacity.min(MAX_CAPACITY);
        let safe_rate = leak_rate_per_second.min(MAX_LEAK_RATE);
        let (params, _, _) = TatParams::from_rate_per_sec(safe_capacity, safe_rate);
        Self {
            params,
            leak_rate_per_second: safe_rate,
            reference_instant: Instant::now(),
            idle_ttl: None,
            shards: ShardedMap::new(),
            _key: PhantomData,
        }
    }

    /// Creates a generic-keyed leaky bucket with TTL-based idle eviction.
    pub fn with_ttl_and_key(capacity: u64, leak_rate_per_second: u64, idle_ttl: Duration) -> Self {
        let mut bucket = Self::new_with_key(capacity, leak_rate_per_second);
        bucket.idle_ttl = Some(idle_ttl);
        bucket
    }

    /// Creates a generic-keyed leaky bucket with an exact per-unit leak interval.
    pub fn with_period_and_key(capacity: u64, leak_interval: Duration) -> Self {
        let params = TatParams::from_emission_interval(capacity, leak_interval);
        let rate_approx = 1_000_000_000u64
            .checked_div(params.emission_interval_nanos)
            .unwrap_or(0);
        Self {
            params,
            leak_rate_per_second: rate_approx,
            reference_instant: Instant::now(),
            idle_ttl: None,
            shards: ShardedMap::new(),
            _key: PhantomData,
        }
    }

    /// Creates a generic-keyed leaky bucket with an exact per-unit leak interval and idle TTL.
    pub fn with_period_and_ttl_and_key(
        capacity: u64,
        leak_interval: Duration,
        idle_ttl: Duration,
    ) -> Self {
        let mut bucket = Self::with_period_and_key(capacity, leak_interval);
        bucket.idle_ttl = Some(idle_ttl);
        bucket
    }

    /// Returns the configured bucket capacity.
    #[inline]
    pub fn capacity(&self) -> u64 {
        self.params.capacity
    }

    /// Returns the configured per-unit leak interval, or `None` if rate is 0.
    #[inline]
    pub fn leak_interval(&self) -> Option<Duration> {
        if self.params.emission_interval_nanos == 0 {
            None
        } else {
            Some(Duration::from_nanos(self.params.emission_interval_nanos))
        }
    }

    #[inline]
    fn now_nanos(&self) -> u64 {
        duration_to_nanos_sat(self.reference_instant.elapsed())
    }

    fn check_impl<Q>(&self, key: &Q, cost: u64) -> RateLimitDecision
    where
        K: Borrow<Q>,
        Q: Hash + Ord + ToKey<K> + ?Sized,
    {
        if cost == 0 {
            return zero_cost_decision(self.params.capacity);
        }

        let now = self.now_nanos();

        if cost > self.params.capacity {
            let existing_tat = self.shards.with_existing(key, |state| state.raw_tat());
            return self.params.unfulfillable_decision(existing_tat, now);
        }

        let track_access = if let Some(ttl) = self.idle_ttl {
            let ttl_nanos = duration_to_nanos_sat(ttl);
            let emission = self.params.emission_interval_nanos;
            self.shards.maybe_cleanup(CLEANUP_SAMPLE_MASK, |state| {
                state.is_evictable(now, ttl_nanos, emission)
            });
            true
        } else {
            false
        };

        self.shards.with_or_insert(
            key,
            || AtomicTatState::new(now),
            |state| state.try_consume(&self.params, now, cost, track_access),
        )
    }
}

impl<K: RateLimitKey> fmt::Debug for LeakyBucket<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LeakyBucket")
            .field("capacity", &self.params.capacity)
            .field("leak_rate_per_second", &self.leak_rate_per_second)
            .field(
                "emission_interval_nanos",
                &self.params.emission_interval_nanos,
            )
            .field("idle_ttl", &self.idle_ttl)
            .field("tracked_keys", &self.shards.len())
            .finish()
    }
}

impl<K: RateLimitKey> super::private::Sealed for LeakyBucket<K> {}

impl<K: RateLimitKey> Algorithm for LeakyBucket<K> {
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
        let now = self.now_nanos();
        self.shards
            .with_existing(key, |state| state.peek(&self.params, now))
            .unwrap_or_else(|| self.params.untouched_decision())
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
        let now = self.now_nanos();
        let ttl_nanos = duration_to_nanos_sat(ttl);
        let emission = self.params.emission_interval_nanos;
        self.shards
            .cleanup_all(|state| state.is_evictable(now, ttl_nanos, emission))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_leaky_bucket_basic() {
        let bucket = LeakyBucket::new(10, 100);

        let decision = bucket.check("test-key");
        assert!(decision.permitted, "First request should be permitted");

        let mut permitted_count = 1;
        for _ in 0..20 {
            let decision = bucket.check("test-key");
            if decision.permitted {
                permitted_count += 1;
            }
        }

        assert!(
            permitted_count <= 11,
            "Should have rate limited some requests, but allowed {}",
            permitted_count
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_leaky_bucket_leak_rate() {
        let bucket = LeakyBucket::new(5, 10);

        for i in 0..5 {
            let decision = bucket.check("test-key");
            assert!(decision.permitted, "Request {} should be permitted", i + 1);
        }

        let decision = bucket.check("test-key");
        assert!(!decision.permitted, "Should be rate limited when full");

        tokio::time::advance(Duration::from_millis(100)).await;

        let decision = bucket.check("test-key");
        assert!(decision.permitted, "Request should be permitted after leak");
    }

    #[tokio::test(start_paused = true)]
    async fn test_leaky_bucket_partial_fill_retry_after() {
        // Capacity 10, leak rate 10/sec (100ms per unit).
        let bucket = LeakyBucket::new(10, 10);

        // Add 7 units -> 3 remaining capacity.
        let d = bucket.check_with_cost("k", 7);
        assert!(d.permitted);
        assert_eq!(d.remaining, Some(3));

        // Requesting cost 5 needs only 2 units to drain (200ms), NOT 5 units (500ms)!
        let d = bucket.check_with_cost("k", 5);
        assert!(!d.permitted);
        assert_eq!(d.remaining, Some(3));
        assert_eq!(d.retry_after, Some(Duration::from_millis(200)));

        // Unfulfillable cost > capacity returns retry_after: None.
        let d_huge = bucket.check_with_cost("k", 11);
        assert!(!d_huge.permitted);
        assert_eq!(d_huge.retry_after, None);
    }

    #[tokio::test]
    async fn test_leaky_bucket_multiple_keys() {
        let bucket = LeakyBucket::new(2, 10);

        let _ = bucket.check("key1");
        let _ = bucket.check("key1");
        let decision = bucket.check("key1");
        assert!(!decision.permitted, "key1 should be rate limited");

        let decision = bucket.check("key2");
        assert!(decision.permitted, "key2 should be permitted");
    }

    #[tokio::test(start_paused = true)]
    async fn test_leaky_bucket_cost() {
        let bucket = LeakyBucket::new(10, 10);

        let decision = bucket.check_with_cost("test-key", 5);
        assert!(decision.permitted, "Cost 5 request should be permitted");

        let decision = bucket.check_with_cost("test-key", 6);
        assert!(!decision.permitted, "Cost 6 request should be denied");

        let decision = bucket.check_with_cost("test-key", 5);
        assert!(decision.permitted, "Cost 5 request should still work");
    }

    #[tokio::test(start_paused = true)]
    async fn test_leaky_bucket_ttl() {
        let bucket = LeakyBucket::with_ttl(10, 100, Duration::from_secs(1));

        let _ = bucket.check("key1");
        assert_eq!(bucket.len(), 1);

        tokio::time::advance(Duration::from_secs(2)).await;

        for _ in 0..300 {
            let _ = bucket.check("key2");
        }

        assert_eq!(bucket.len(), 1);
    }
}
