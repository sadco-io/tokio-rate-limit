//! Leaky bucket rate limiting algorithm implementation.

use crate::algorithm::internal::{
    nanos_for_tokens, refill_tokens, should_cleanup, wait_for_tokens, zero_cost_decision, SCALE,
};
use crate::algorithm::Algorithm;
use crate::limiter::RateLimitDecision;
use flurry::HashMap as FlurryHashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

/// Maximum capacity to prevent overflow in scaled arithmetic.
const MAX_CAPACITY: u64 = u64::MAX / (2 * SCALE);

/// Maximum leak rate per second to prevent overflow.
const MAX_LEAK_RATE: u64 = u64::MAX / (2 * SCALE);

/// Atomic state for a leaky bucket.
///
/// Unlike token bucket which allows bursts, leaky bucket enforces a steady rate
/// by "leaking" tokens at a constant rate. Requests add tokens to the bucket,
/// and if the bucket would overflow, the request is denied.
struct AtomicLeakyState {
    /// Current number of tokens in the bucket (scaled by 1000 for sub-token precision)
    /// Lower is better - represents pending requests that haven't "leaked" yet
    tokens: AtomicU64,

    /// Last leak timestamp in nanoseconds since the tokio runtime started
    last_leak_nanos: AtomicU64,

    /// Last access timestamp in nanoseconds, used for TTL-based eviction
    last_access_nanos: AtomicU64,
}

impl AtomicLeakyState {
    /// Creates a new leaky bucket state starting empty.
    fn new(now_nanos: u64) -> Self {
        Self {
            tokens: AtomicU64::new(0), // Start empty
            last_leak_nanos: AtomicU64::new(now_nanos),
            last_access_nanos: AtomicU64::new(now_nanos),
        }
    }

    /// Attempts to add tokens to the bucket (i.e., make a request).
    ///
    /// This method performs automatic leaking based on elapsed time and uses
    /// lock-free compare-and-swap loops for token updates.
    ///
    /// # Arguments
    ///
    /// * `capacity` - Maximum bucket capacity (water level)
    /// * `leak_rate_per_second` - Rate at which tokens leak out
    /// * `now_nanos` - Current time in nanoseconds
    /// * `cost` - Number of tokens to add (request cost)
    ///
    /// Returns `(permitted, remaining_capacity)`
    fn try_add(
        &self,
        capacity: u64,
        leak_rate_per_second: u64,
        now_nanos: u64,
        cost: u64,
    ) -> (bool, u64) {
        self.last_access_nanos.store(now_nanos, Ordering::Relaxed);

        let scaled_capacity = capacity.saturating_mul(SCALE);
        let token_cost = cost.saturating_mul(SCALE);
        let rate_scaled = leak_rate_per_second.saturating_mul(SCALE);

        self.apply_leak(rate_scaled, now_nanos);

        loop {
            let current = self.tokens.load(Ordering::Relaxed);
            let new_tokens = current.saturating_add(token_cost);
            if new_tokens > scaled_capacity {
                let remaining_capacity = scaled_capacity.saturating_sub(current) / SCALE;
                return (false, remaining_capacity);
            }
            match self.tokens.compare_exchange_weak(
                current,
                new_tokens,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    let remaining_capacity = scaled_capacity.saturating_sub(new_tokens) / SCALE;
                    return (true, remaining_capacity);
                }
                Err(_) => continue,
            }
        }
    }

    /// Claim the elapsed interval, then subtract leaked occupancy.
    fn apply_leak(&self, rate_scaled: u64, now_nanos: u64) {
        loop {
            let last = self.last_leak_nanos.load(Ordering::Relaxed);
            let leaked = refill_tokens(now_nanos.saturating_sub(last), rate_scaled);
            if leaked == 0 {
                return;
            }
            let claimed = nanos_for_tokens(leaked, rate_scaled);
            if self
                .last_leak_nanos
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
                    Some(current.saturating_sub(leaked))
                });
            return;
        }
    }
}

/// Leaky bucket rate limiting algorithm.
///
/// The leaky bucket algorithm enforces a steady rate by "leaking" tokens at a constant rate.
/// Unlike token bucket which allows bursts, leaky bucket smooths traffic by maintaining
/// a consistent flow rate.
///
/// # Algorithm Details
///
/// - **Capacity**: Maximum bucket size (water level)
/// - **Leak Rate**: Tokens removed per second (steady outflow)
/// - **Request Handling**: Each request adds tokens; if bucket overflows, request is denied
/// - **Traffic Smoothing**: Enforces steady rate without bursts
///
/// # Comparison with Token Bucket
///
/// | Feature | Token Bucket | Leaky Bucket |
/// |---------|--------------|--------------|
/// | **Bursts** | Allowed up to capacity | Not allowed |
/// | **Rate Enforcement** | Average rate over time | Strict steady rate |
/// | **Traffic Pattern** | Bursty | Smooth |
/// | **Use Case** | Public APIs, user requests | Backend protection, QPS limits |
///
/// # Use Cases
///
/// - **Backend Protection**: Prevent overwhelming downstream services with consistent load
/// - **Strict QPS Enforcement**: When you need exactly N requests/sec, no more, no less
/// - **Traffic Smoothing**: Convert bursty traffic into steady stream
/// - **Fair Queuing**: Ensure no client can monopolize resources with bursts
///
/// # Performance
///
/// - Uses same lock-free architecture as TokenBucket
/// - Expected: Similar performance to TokenBucket (15M+ ops/sec single-threaded)
/// - Minimal overhead compared to token bucket
///
/// # Examples
///
/// ```
/// use tokio_rate_limit::algorithm::LeakyBucket;
/// use tokio_rate_limit::RateLimiter;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// // 100 requests/sec steady rate, capacity of 50
/// let algorithm = LeakyBucket::new(50, 100);
/// let limiter = RateLimiter::from_algorithm(algorithm);
///
/// // Requests that would cause bursts are denied
/// let decision = limiter.check("client-123");
/// # Ok(())
/// # }
/// ```
pub struct LeakyBucket {
    /// Maximum tokens the bucket can hold
    capacity: u64,

    /// Number of tokens leaked per second (steady rate)
    leak_rate_per_second: u64,

    /// Reference instant for time measurements.
    reference_instant: Instant,

    /// Time-to-live for idle keys.
    idle_ttl: Option<Duration>,

    /// Per-key leaky bucket state.
    buckets: Arc<FlurryHashMap<String, Arc<AtomicLeakyState>>>,
}

impl LeakyBucket {
    /// Creates a new leaky bucket with the specified capacity and leak rate.
    ///
    /// # Arguments
    ///
    /// * `capacity` - Maximum bucket size, clamped to MAX_CAPACITY if exceeded
    /// * `leak_rate_per_second` - Tokens leaked per second, clamped to MAX_LEAK_RATE if exceeded
    ///
    /// # Examples
    ///
    /// ```
    /// use tokio_rate_limit::algorithm::LeakyBucket;
    ///
    /// // 100 requests/sec with capacity of 50
    /// let bucket = LeakyBucket::new(50, 100);
    /// ```
    pub fn new(capacity: u64, leak_rate_per_second: u64) -> Self {
        let safe_capacity = capacity.min(MAX_CAPACITY);
        let safe_rate = leak_rate_per_second.min(MAX_LEAK_RATE);

        Self {
            capacity: safe_capacity,
            leak_rate_per_second: safe_rate,
            reference_instant: Instant::now(),
            idle_ttl: None,
            buckets: Arc::new(FlurryHashMap::new()),
        }
    }

    /// Creates a new leaky bucket with TTL-based eviction.
    ///
    /// # Arguments
    ///
    /// * `capacity` - Maximum bucket size
    /// * `leak_rate_per_second` - Tokens leaked per second
    /// * `idle_ttl` - Duration after which idle keys are evicted
    ///
    /// # Examples
    ///
    /// ```
    /// use tokio_rate_limit::algorithm::LeakyBucket;
    /// use std::time::Duration;
    ///
    /// // Evict keys idle for more than 1 hour
    /// let bucket = LeakyBucket::with_ttl(50, 100, Duration::from_secs(3600));
    /// ```
    pub fn with_ttl(capacity: u64, leak_rate_per_second: u64, idle_ttl: Duration) -> Self {
        let mut bucket = Self::new(capacity, leak_rate_per_second);
        bucket.idle_ttl = Some(idle_ttl);
        bucket
    }

    /// Get current time in nanoseconds since the reference instant.
    #[inline]
    fn now_nanos(&self) -> u64 {
        self.reference_instant.elapsed().as_nanos() as u64
    }

    /// Cleanup idle keys based on TTL configuration.
    fn cleanup_idle(&self, now_nanos: u64) {
        if let Some(ttl) = self.idle_ttl {
            let ttl_nanos = ttl.as_nanos() as u64;

            let guard = self.buckets.guard();
            let keys_to_remove: Vec<String> = self
                .buckets
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
                self.buckets.remove(&key, &guard);
            }
        }
    }

    fn check_impl(&self, key: &str, cost: u64) -> RateLimitDecision {
        if cost == 0 {
            return zero_cost_decision(self.capacity);
        }

        let now = self.now_nanos();
        if self.idle_ttl.is_some() && should_cleanup() {
            self.cleanup_idle(now);
        }

        let guard = self.buckets.guard();
        let state = match self.buckets.get(key, &guard) {
            Some(state) => state,
            None => {
                let new_state = Arc::new(AtomicLeakyState::new(now));
                match self.buckets.try_insert(key.to_string(), new_state, &guard) {
                    Ok(inserted) => inserted,
                    Err(not_inserted) => not_inserted.current,
                }
            }
        };

        let (permitted, remaining_capacity) =
            state.try_add(self.capacity, self.leak_rate_per_second, now, cost);

        let retry_after = if !permitted {
            Some(wait_for_tokens(cost, self.leak_rate_per_second))
        } else {
            None
        };
        let current_level = self.capacity.saturating_sub(remaining_capacity);
        let reset = if self.leak_rate_per_second > 0 && current_level > 0 {
            Some(wait_for_tokens(current_level, self.leak_rate_per_second))
        } else {
            Some(Duration::from_secs(0))
        };

        RateLimitDecision {
            permitted,
            retry_after,
            remaining: Some(remaining_capacity),
            limit: self.capacity,
            reset,
        }
    }
}

impl super::private::Sealed for LeakyBucket {}

impl Algorithm for LeakyBucket {
    fn check(&self, key: &str) -> RateLimitDecision {
        self.check_impl(key, 1)
    }

    fn check_with_cost(&self, key: &str, cost: u64) -> RateLimitDecision {
        self.check_impl(key, cost)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_leaky_bucket_basic() {
        let bucket = LeakyBucket::new(10, 100);

        // First request should succeed (bucket starts empty)
        let decision = bucket.check("test-key");
        assert!(decision.permitted, "First request should be permitted");

        // Multiple rapid requests should eventually be rate limited
        // since leaky bucket doesn't allow bursts
        let mut permitted_count = 1;
        for _ in 0..20 {
            let decision = bucket.check("test-key");
            if decision.permitted {
                permitted_count += 1;
            }
        }

        // Should have rate limited some requests (capacity is 10, we sent 21 total)
        assert!(
            permitted_count <= 11,
            "Should have rate limited some requests, but allowed {}",
            permitted_count
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_leaky_bucket_leak_rate() {
        let bucket = LeakyBucket::new(5, 10); // 5 capacity, 10 per second leak rate

        // Fill the bucket to capacity
        for i in 0..5 {
            let decision = bucket.check("test-key");
            assert!(decision.permitted, "Request {} should be permitted", i + 1);
        }

        // Should be rate limited now (bucket full)
        let decision = bucket.check("test-key");
        assert!(!decision.permitted, "Should be rate limited when full");

        // Wait 100ms (should leak 1 token at 10/sec)
        tokio::time::advance(Duration::from_millis(100)).await;

        // Should work again
        let decision = bucket.check("test-key");
        assert!(decision.permitted, "Request should be permitted after leak");
    }

    #[tokio::test]
    async fn test_leaky_bucket_multiple_keys() {
        let bucket = LeakyBucket::new(2, 10);

        // Key 1: fill bucket
        let _ = bucket.check("key1");
        let _ = bucket.check("key1");
        let decision = bucket.check("key1");
        assert!(!decision.permitted, "key1 should be rate limited");

        // Key 2: should still work (separate bucket)
        let decision = bucket.check("key2");
        assert!(decision.permitted, "key2 should be permitted");
    }

    #[tokio::test]
    async fn test_leaky_bucket_cost() {
        let bucket = LeakyBucket::new(10, 10);

        // Request with cost 5 should work
        let decision = bucket.check_with_cost("test-key", 5);
        assert!(decision.permitted, "Cost 5 request should be permitted");

        // Request with cost 6 should fail (5 + 6 > 10)
        let decision = bucket.check_with_cost("test-key", 6);
        assert!(!decision.permitted, "Cost 6 request should be denied");

        // Request with cost 5 should still work (still at 5)
        let decision = bucket.check_with_cost("test-key", 5);
        assert!(decision.permitted, "Cost 5 request should still work");
    }

    #[tokio::test(start_paused = true)]
    async fn test_leaky_bucket_ttl() {
        let bucket = LeakyBucket::with_ttl(10, 100, Duration::from_secs(1));

        // Access key1
        let _ = bucket.check("key1");
        assert_eq!(bucket.buckets.len(), 1);

        // Advance time past TTL
        tokio::time::advance(Duration::from_secs(2)).await;

        // Access key2 multiple times to trigger cleanup
        for _ in 0..200 {
            let _ = bucket.check("key2");
        }

        // key1 should eventually be evicted
        let count = bucket.buckets.len();
        assert!(
            (1..=2).contains(&count),
            "Expected 1-2 keys after TTL, got {}",
            count
        );
    }
}
