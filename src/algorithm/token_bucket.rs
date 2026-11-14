//! Token bucket rate limiting algorithm implementation.

use crate::algorithm::internal::{
    duration_to_nanos_sat, zero_cost_decision, AtomicTatState, ShardedMap, TatParams,
};
use crate::algorithm::{Algorithm, RateLimitKey, ToKey};
use crate::limiter::RateLimitDecision;
use std::borrow::Borrow;
use std::fmt;
use std::hash::Hash;
use std::marker::PhantomData;
use std::time::Duration;
use tokio::time::Instant;

/// Mask for amortized round-robin shard cleanup (`1` in `16` checks sweeps `16` shards).
const CLEANUP_SAMPLE_MASK: u64 = 15;

/// Token bucket rate limiting algorithm backed by single-word lock-free GCRA
/// Theoretical Arrival Time (TAT) accounting.
///
/// The token bucket algorithm maintains a per-key quota that permits bursts up
/// to `capacity` tokens and replenishes continuously at a constant rate.
///
/// # Algorithm Details
///
/// - **Capacity**: Maximum number of tokens (burst size)
/// - **Replenish Rate**: Tokens added per second (or exact inter-token [`Duration`] via [`TokenBucket::with_period`])
/// - **Concurrency**:
///   - Per-key state uses a single `AtomicU64` Theoretical Arrival Time updated via CAS
///   - Key lookup uses 256 `flurry` lock-free shards routed with per-instance OS-seeded `ahash`
/// - **Memory Management**:
///   - Optional `idle_ttl` evicts idle keys via bounded round-robin shard sweeps once their
///     quota has fully recovered (preventing premature eviction from resetting an exhausted quota),
///     or deterministically via [`Algorithm::cleanup`].
pub struct TokenBucket<K: RateLimitKey = String> {
    params: TatParams,
    refill_rate_per_second: u64,
    reference_instant: Instant,
    idle_ttl: Option<Duration>,
    shards: ShardedMap<K, AtomicTatState>,
    _key: PhantomData<fn() -> K>,
}

impl TokenBucket<String> {
    /// Creates a new `String`-keyed token bucket with the specified capacity and refill rate.
    ///
    /// # Arguments
    ///
    /// * `capacity` - Maximum number of tokens (burst size), clamped to `u64::MAX / 2`
    /// * `refill_rate_per_second` - Tokens added per second, clamped to `1_000_000_000`
    ///
    /// # Examples
    ///
    /// ```
    /// use tokio_rate_limit::algorithm::{Algorithm, TokenBucket};
    ///
    /// // 100 requests per second with burst of 200
    /// let bucket = TokenBucket::new(200, 100);
    /// assert!(bucket.check("client-1").permitted);
    /// ```
    pub fn new(capacity: u64, refill_rate_per_second: u64) -> Self {
        Self::new_with_key(capacity, refill_rate_per_second)
    }

    /// Creates a new `String`-keyed token bucket with TTL-based idle eviction.
    ///
    /// Idle keys are only evicted once both `idle_ttl` has elapsed since last access
    /// and their token bucket has recovered to full capacity, preventing TTL eviction
    /// from acting as a quota reset bypass.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use tokio_rate_limit::algorithm::{Algorithm, TokenBucket};
    ///
    /// let bucket = TokenBucket::with_ttl(200, 100, Duration::from_secs(3600));
    /// assert!(bucket.check("client-1").permitted);
    /// ```
    pub fn with_ttl(capacity: u64, refill_rate_per_second: u64, idle_ttl: Duration) -> Self {
        Self::with_ttl_and_key(capacity, refill_rate_per_second, idle_ttl)
    }

    /// Creates a new `String`-keyed token bucket with an exact per-token replenish interval.
    ///
    /// Useful for sub-1-req/sec limits such as per-minute or per-hour quotas.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use tokio_rate_limit::algorithm::{Algorithm, TokenBucket};
    ///
    /// // Burst of 5, replenishing 1 token every 6 seconds (10 per minute)
    /// let bucket = TokenBucket::with_period(5, Duration::from_secs(6));
    /// assert!(bucket.check("client-1").permitted);
    /// ```
    pub fn with_period(capacity: u64, replenish_interval: Duration) -> Self {
        Self::with_period_and_key(capacity, replenish_interval)
    }

    /// Creates a new `String`-keyed token bucket with an exact per-token replenish interval and idle TTL.
    pub fn with_period_and_ttl(
        capacity: u64,
        replenish_interval: Duration,
        idle_ttl: Duration,
    ) -> Self {
        Self::with_period_and_ttl_and_key(capacity, replenish_interval, idle_ttl)
    }
}

impl<K: RateLimitKey> TokenBucket<K> {
    /// Creates a token bucket keyed by an arbitrary [`RateLimitKey`] type `K`
    /// (such as [`std::net::IpAddr`], `u64`, or a custom composite struct).
    ///
    /// # Examples
    ///
    /// ```
    /// use std::net::{IpAddr, Ipv4Addr};
    /// use tokio_rate_limit::algorithm::{Algorithm, TokenBucket};
    ///
    /// let bucket = TokenBucket::<IpAddr>::new_with_key(100, 50);
    /// let ip = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
    /// assert!(bucket.check(&ip).permitted);
    /// ```
    pub fn new_with_key(capacity: u64, refill_rate_per_second: u64) -> Self {
        let (params, _, safe_rate) = TatParams::from_rate_per_sec(capacity, refill_rate_per_second);
        Self {
            params,
            refill_rate_per_second: safe_rate,
            reference_instant: Instant::now(),
            idle_ttl: None,
            shards: ShardedMap::new(),
            _key: PhantomData,
        }
    }

    /// Creates a generic-keyed token bucket with TTL-based idle eviction.
    pub fn with_ttl_and_key(
        capacity: u64,
        refill_rate_per_second: u64,
        idle_ttl: Duration,
    ) -> Self {
        let mut bucket = Self::new_with_key(capacity, refill_rate_per_second);
        bucket.idle_ttl = Some(idle_ttl);
        bucket
    }

    /// Creates a generic-keyed token bucket with an exact per-token replenish interval.
    pub fn with_period_and_key(capacity: u64, replenish_interval: Duration) -> Self {
        let params = TatParams::from_emission_interval(capacity, replenish_interval);
        let rate_approx = 1_000_000_000u64
            .checked_div(params.emission_interval_nanos)
            .unwrap_or(0);
        Self {
            params,
            refill_rate_per_second: rate_approx,
            reference_instant: Instant::now(),
            idle_ttl: None,
            shards: ShardedMap::new(),
            _key: PhantomData,
        }
    }

    /// Creates a generic-keyed token bucket with an exact per-token replenish interval and idle TTL.
    pub fn with_period_and_ttl_and_key(
        capacity: u64,
        replenish_interval: Duration,
        idle_ttl: Duration,
    ) -> Self {
        let mut bucket = Self::with_period_and_key(capacity, replenish_interval);
        bucket.idle_ttl = Some(idle_ttl);
        bucket
    }

    /// Returns the configured maximum burst capacity.
    #[inline]
    pub fn capacity(&self) -> u64 {
        self.params.capacity
    }

    /// Returns the configured per-token replenish interval, or `None` if rate is 0.
    #[inline]
    pub fn emission_interval(&self) -> Option<Duration> {
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

impl<K: RateLimitKey> fmt::Debug for TokenBucket<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TokenBucket")
            .field("capacity", &self.params.capacity)
            .field("refill_rate_per_second", &self.refill_rate_per_second)
            .field(
                "emission_interval_nanos",
                &self.params.emission_interval_nanos,
            )
            .field("idle_ttl", &self.idle_ttl)
            .field("tracked_keys", &self.shards.len())
            .finish()
    }
}

impl<K: RateLimitKey> super::private::Sealed for TokenBucket<K> {}

impl<K: RateLimitKey> Algorithm for TokenBucket<K> {
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
    async fn test_token_bucket_basic() {
        let bucket = TokenBucket::new(10, 100);

        for _ in 0..10 {
            let decision = bucket.check("test-key");
            assert!(decision.permitted, "Request should be permitted");
        }

        let decision = bucket.check("test-key");
        assert!(!decision.permitted, "Request should be rate limited");
    }

    #[tokio::test]
    async fn test_token_bucket_refill() {
        let bucket = TokenBucket::new(5, 10);

        for _ in 0..5 {
            let _ = bucket.check("test-key");
        }

        let decision = bucket.check("test-key");
        assert!(!decision.permitted);

        tokio::time::sleep(Duration::from_millis(100)).await;

        let decision = bucket.check("test-key");
        assert!(
            decision.permitted,
            "Request should be permitted after refill"
        );
    }

    #[tokio::test]
    async fn test_token_bucket_multiple_keys() {
        let bucket = TokenBucket::new(2, 10);

        let _ = bucket.check("key1");
        let _ = bucket.check("key1");
        let decision = bucket.check("key1");
        assert!(!decision.permitted, "key1 should be rate limited");

        let decision = bucket.check("key2");
        assert!(decision.permitted, "key2 should be permitted");
    }

    #[tokio::test(start_paused = true)]
    async fn test_token_bucket_refill_deterministic() {
        let bucket = TokenBucket::new(10, 100);

        for i in 0..10 {
            let decision = bucket.check("test-key");
            assert!(decision.permitted, "Request {} should be permitted", i + 1);
        }

        let decision = bucket.check("test-key");
        assert!(!decision.permitted, "Request should be rate limited");
        assert_eq!(decision.remaining, Some(0));

        tokio::time::advance(Duration::from_millis(100)).await;

        for i in 0..10 {
            let decision = bucket.check("test-key");
            assert!(
                decision.permitted,
                "Request {} should be permitted after refill",
                i + 1
            );
        }

        let decision = bucket.check("test-key");
        assert!(!decision.permitted, "Request should be rate limited again");
    }

    #[tokio::test(start_paused = true)]
    async fn test_token_bucket_partial_refill() {
        let bucket = TokenBucket::new(100, 100);

        for _ in 0..50 {
            let _ = bucket.check("test-key");
        }

        tokio::time::advance(Duration::from_millis(200)).await;

        for i in 0..20 {
            let decision = bucket.check("test-key");
            assert!(decision.permitted, "Request {} should be permitted", i + 1);
        }

        let decision = bucket.check("test-key");
        assert!(decision.permitted, "Should still have tokens");
        assert_eq!(decision.remaining, Some(49));
    }

    #[tokio::test(start_paused = true)]
    async fn test_token_bucket_capacity_cap() {
        let bucket = TokenBucket::new(50, 100);

        tokio::time::advance(Duration::from_secs(1)).await;

        for i in 0..50 {
            let decision = bucket.check("test-key");
            assert!(decision.permitted, "Request {} should be permitted", i + 1);
        }

        let decision = bucket.check("test-key");
        assert!(!decision.permitted, "Should be rate limited at capacity");
    }

    #[tokio::test(start_paused = true)]
    async fn test_contended_refill_exact_gcra() {
        use std::sync::Arc;
        let bucket = Arc::new(TokenBucket::new(100, 1_000));
        for _ in 0..100 {
            assert!(bucket.check("hot").permitted);
        }
        tokio::time::advance(Duration::from_millis(50)).await;
        // 50ms at 1000/s is EXACTLY 50 tokens — single-word TAT CAS admits
        // neither more nor fewer than 50 under contention.
        let mut handles = Vec::new();
        for _ in 0..8 {
            let bucket = bucket.clone();
            handles.push(tokio::spawn(async move {
                let mut n = 0u64;
                for _ in 0..20 {
                    if bucket.check("hot").permitted {
                        n += 1;
                    }
                }
                n
            }));
        }
        let mut total = 0u64;
        for h in handles {
            total += h.await.unwrap();
        }
        assert_eq!(
            total, 50,
            "single-atomic GCRA must admit exactly 50 tokens under contention, got {total}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_retry_after_accurate() {
        let bucket = TokenBucket::new(1, 10);

        let decision = bucket.check("test-key");
        assert!(decision.permitted);

        let decision = bucket.check("test-key");
        assert!(!decision.permitted);
        assert_eq!(decision.remaining, Some(0));
        assert_eq!(decision.retry_after, Some(Duration::from_millis(100)));
    }

    #[tokio::test]
    async fn test_cost_exceeds_capacity_returns_none_retry_after() {
        let bucket = TokenBucket::new(10, 100);
        let decision = bucket.check_with_cost("test-key", 11);
        assert!(!decision.permitted);
        assert_eq!(decision.retry_after, None);
        assert_eq!(decision.remaining, Some(10));
        // Untouched bucket must not even have been inserted on unfulfillable cost.
        assert_eq!(bucket.len(), 0);
    }

    #[tokio::test]
    async fn test_overflow_protection() {
        use crate::algorithm::internal::{MAX_TAT_BURST, MAX_TAT_RATE_PER_SEC};
        let bucket = TokenBucket::new(u64::MAX, u64::MAX);

        assert_eq!(bucket.capacity(), MAX_TAT_BURST);
        assert_eq!(bucket.refill_rate_per_second, MAX_TAT_RATE_PER_SEC);

        let decision = bucket.check("test-key");
        assert!(decision.permitted, "Should work with clamped values");
    }

    #[tokio::test]
    async fn test_saturating_arithmetic() {
        use crate::algorithm::internal::{MAX_TAT_BURST, MAX_TAT_RATE_PER_SEC};
        let bucket = TokenBucket::new(MAX_TAT_BURST, MAX_TAT_RATE_PER_SEC);

        for _ in 0..100 {
            let _ = bucket.check("test-key");
        }

        let _decision = bucket.check("test-key");
    }

    #[tokio::test(start_paused = true)]
    async fn test_ttl_eviction() {
        let bucket = TokenBucket::with_ttl(10, 100, Duration::from_secs(1));

        let _ = bucket.check("key1");
        assert_eq!(bucket.len(), 1);

        tokio::time::advance(Duration::from_secs(2)).await;

        for _ in 0..300 {
            let _ = bucket.check("key2");
        }

        assert_eq!(bucket.len(), 1, "expired key1 should have been swept");
    }

    #[tokio::test(start_paused = true)]
    async fn test_ttl_does_not_reset_exhausted_quota() {
        // 10 burst, 1 token/sec (takes 10s to recover), 1s idle TTL.
        let bucket = TokenBucket::with_ttl(10, 1, Duration::from_secs(1));

        for _ in 0..10 {
            assert!(bucket.check("attacker").permitted);
        }
        assert!(!bucket.check("attacker").permitted);

        // Advance 2 seconds: idle_ttl (1s) has expired, ONLY 2 tokens have refilled (8 still missing).
        tokio::time::advance(Duration::from_secs(2)).await;

        // Deterministic full cleanup must NOT evict "attacker" while still in debt!
        assert_eq!(bucket.cleanup(), 0);
        assert_eq!(bucket.len(), 1);

        // Attacker should only get 2 refilled tokens, NOT a fresh burst of 10!
        assert!(bucket.check("attacker").permitted);
        assert!(bucket.check("attacker").permitted);
        assert!(!bucket.check("attacker").permitted);

        // Once full recovery (10s) + TTL (1s) elapses, cleanup evicts cleanly.
        tokio::time::advance(Duration::from_secs(11)).await;
        assert_eq!(bucket.cleanup(), 1);
        assert_eq!(bucket.len(), 0);
    }

    #[tokio::test]
    async fn test_no_ttl_keeps_keys() {
        let bucket = TokenBucket::new(10, 100);

        for i in 0..10 {
            let _ = bucket.check(&format!("key{}", i));
        }

        assert_eq!(bucket.len(), 10);
    }
}
