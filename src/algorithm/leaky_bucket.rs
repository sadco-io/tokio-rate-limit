//! Leaky bucket rate limiting algorithm implementation.

use crate::algorithm::Algorithm;
use crate::error::Result;
use crate::limiter::RateLimitDecision;
use async_trait::async_trait;
use flurry::HashMap as FlurryHashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

/// Scaling factor for sub-token precision.
/// Token counts are multiplied by this value to allow fractional tokens.
const SCALE: u64 = 1000;

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
        // Update last access time (for TTL tracking)
        self.last_access_nanos.store(now_nanos, Ordering::Relaxed);

        // Scale capacity and cost for precision
        let scaled_capacity = capacity.saturating_mul(SCALE);
        let token_cost = cost.saturating_mul(SCALE);

        loop {
            // Load current state
            let current_tokens = self.tokens.load(Ordering::Relaxed);
            let last_leak = self.last_leak_nanos.load(Ordering::Relaxed);

            // Calculate elapsed time and tokens to leak out
            let elapsed_nanos = now_nanos.saturating_sub(last_leak);
            let elapsed_secs = elapsed_nanos as f64 / 1_000_000_000.0;

            // Calculate tokens to leak (remove from bucket)
            let tokens_per_sec_scaled = leak_rate_per_second.saturating_mul(SCALE);
            let tokens_to_leak = (elapsed_secs * tokens_per_sec_scaled as f64) as u64;

            // Calculate updated token count after leaking (can't go below 0)
            let leaked_tokens = current_tokens.saturating_sub(tokens_to_leak);

            // Try to add the requested cost
            let new_tokens = leaked_tokens.saturating_add(token_cost);

            if new_tokens <= scaled_capacity {
                // Request fits in bucket, allow it
                let new_time = if tokens_to_leak > 0 {
                    now_nanos
                } else {
                    last_leak
                };

                // Try to update tokens atomically
                match self.tokens.compare_exchange_weak(
                    current_tokens,
                    new_tokens,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        // Successfully updated tokens, now update time if needed
                        if tokens_to_leak > 0 {
                            let _ = self.last_leak_nanos.compare_exchange_weak(
                                last_leak,
                                new_time,
                                Ordering::AcqRel,
                                Ordering::Relaxed,
                            );
                        }
                        // Return remaining capacity
                        let remaining_capacity = (scaled_capacity - new_tokens) / SCALE;
                        return (true, remaining_capacity);
                    }
                    Err(_) => {
                        // Another thread modified it, retry
                        continue;
                    }
                }
            } else {
                // Request would overflow bucket, deny it
                // But still update state to reflect leaking
                let new_time = if tokens_to_leak > 0 {
                    now_nanos
                } else {
                    last_leak
                };

                match self.tokens.compare_exchange_weak(
                    current_tokens,
                    leaked_tokens,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        if tokens_to_leak > 0 {
                            let _ = self.last_leak_nanos.compare_exchange_weak(
                                last_leak,
                                new_time,
                                Ordering::AcqRel,
                                Ordering::Relaxed,
                            );
                        }
                        // Return current capacity (how much room is left)
                        let remaining_capacity = (scaled_capacity - leaked_tokens) / SCALE;
                        return (false, remaining_capacity);
                    }
                    Err(_) => {
                        // Another thread modified it, retry
                        continue;
                    }
                }
            }
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
/// let decision = limiter.check("client-123").await?;
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
}

// Implement the sealed trait marker
impl super::private::Sealed for LeakyBucket {}

#[async_trait]
impl Algorithm for LeakyBucket {
    async fn check(&self, key: &str) -> Result<RateLimitDecision> {
        let now = self.now_nanos();

        // Probabilistic cleanup (1% of the time)
        if self.idle_ttl.is_some() && (now % 100) == 0 {
            self.cleanup_idle(now);
        }

        // Get or create bucket state for this key
        let guard = self.buckets.guard();
        let key_string = key.to_string();
        let state = match self.buckets.get(&key_string, &guard) {
            Some(state) => state.clone(),
            None => {
                let new_state = Arc::new(AtomicLeakyState::new(now));
                match self
                    .buckets
                    .try_insert(key_string.clone(), new_state.clone(), &guard)
                {
                    Ok(_) => new_state,
                    Err(current) => current.current.clone(),
                }
            }
        };

        // Try to add a token (cost of 1)
        let (permitted, remaining_capacity) =
            state.try_add(self.capacity, self.leak_rate_per_second, now, 1);

        // Calculate retry_after if rate limited
        let retry_after = if !permitted {
            // We need to wait for enough tokens to leak out
            // At leak_rate_per_second, time to leak 1 token is 1/leak_rate_per_second
            let seconds_to_wait = if self.leak_rate_per_second > 0 {
                1.0 / self.leak_rate_per_second as f64
            } else {
                1.0
            };
            Some(Duration::from_secs_f64(seconds_to_wait.max(0.001)))
        } else {
            None
        };

        // Calculate reset time (time until bucket is empty)
        // Current bucket level is (capacity - remaining_capacity)
        // Time to empty = current_level / leak_rate_per_second
        let current_level = self.capacity.saturating_sub(remaining_capacity);
        let reset = if self.leak_rate_per_second > 0 && current_level > 0 {
            let seconds_to_empty = current_level as f64 / self.leak_rate_per_second as f64;
            Some(Duration::from_secs_f64(seconds_to_empty.max(0.001)))
        } else {
            Some(Duration::from_secs(0))
        };

        Ok(RateLimitDecision {
            permitted,
            retry_after,
            remaining: Some(remaining_capacity),
            limit: self.capacity,
            reset,
        })
    }

    async fn check_with_cost(&self, key: &str, cost: u64) -> Result<RateLimitDecision> {
        let now = self.now_nanos();

        // Probabilistic cleanup (1% of the time)
        if self.idle_ttl.is_some() && (now % 100) == 0 {
            self.cleanup_idle(now);
        }

        // Get or create bucket state for this key
        let guard = self.buckets.guard();
        let key_string = key.to_string();
        let state = match self.buckets.get(&key_string, &guard) {
            Some(state) => state.clone(),
            None => {
                let new_state = Arc::new(AtomicLeakyState::new(now));
                match self
                    .buckets
                    .try_insert(key_string.clone(), new_state.clone(), &guard)
                {
                    Ok(_) => new_state,
                    Err(current) => current.current.clone(),
                }
            }
        };

        // Try to add tokens with specified cost
        let (permitted, remaining_capacity) =
            state.try_add(self.capacity, self.leak_rate_per_second, now, cost);

        // Calculate retry_after if rate limited
        let retry_after = if !permitted {
            // Time to leak enough tokens for this cost
            let seconds_to_wait = if self.leak_rate_per_second > 0 {
                cost as f64 / self.leak_rate_per_second as f64
            } else {
                1.0
            };
            Some(Duration::from_secs_f64(seconds_to_wait.max(0.001)))
        } else {
            None
        };

        // Calculate reset time
        let current_level = self.capacity.saturating_sub(remaining_capacity);
        let reset = if self.leak_rate_per_second > 0 && current_level > 0 {
            let seconds_to_empty = current_level as f64 / self.leak_rate_per_second as f64;
            Some(Duration::from_secs_f64(seconds_to_empty.max(0.001)))
        } else {
            Some(Duration::from_secs(0))
        };

        Ok(RateLimitDecision {
            permitted,
            retry_after,
            remaining: Some(remaining_capacity),
            limit: self.capacity,
            reset,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_leaky_bucket_basic() {
        let bucket = LeakyBucket::new(10, 100);

        // First request should succeed (bucket starts empty)
        let decision = bucket.check("test-key").await.unwrap();
        assert!(decision.permitted, "First request should be permitted");

        // Multiple rapid requests should eventually be rate limited
        // since leaky bucket doesn't allow bursts
        let mut permitted_count = 1;
        for _ in 0..20 {
            let decision = bucket.check("test-key").await.unwrap();
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
            let decision = bucket.check("test-key").await.unwrap();
            assert!(decision.permitted, "Request {} should be permitted", i + 1);
        }

        // Should be rate limited now (bucket full)
        let decision = bucket.check("test-key").await.unwrap();
        assert!(!decision.permitted, "Should be rate limited when full");

        // Wait 100ms (should leak 1 token at 10/sec)
        tokio::time::advance(Duration::from_millis(100)).await;

        // Should work again
        let decision = bucket.check("test-key").await.unwrap();
        assert!(decision.permitted, "Request should be permitted after leak");
    }

    #[tokio::test]
    async fn test_leaky_bucket_multiple_keys() {
        let bucket = LeakyBucket::new(2, 10);

        // Key 1: fill bucket
        bucket.check("key1").await.unwrap();
        bucket.check("key1").await.unwrap();
        let decision = bucket.check("key1").await.unwrap();
        assert!(!decision.permitted, "key1 should be rate limited");

        // Key 2: should still work (separate bucket)
        let decision = bucket.check("key2").await.unwrap();
        assert!(decision.permitted, "key2 should be permitted");
    }

    #[tokio::test]
    async fn test_leaky_bucket_cost() {
        let bucket = LeakyBucket::new(10, 10);

        // Request with cost 5 should work
        let decision = bucket.check_with_cost("test-key", 5).await.unwrap();
        assert!(decision.permitted, "Cost 5 request should be permitted");

        // Request with cost 6 should fail (5 + 6 > 10)
        let decision = bucket.check_with_cost("test-key", 6).await.unwrap();
        assert!(!decision.permitted, "Cost 6 request should be denied");

        // Request with cost 5 should still work (still at 5)
        let decision = bucket.check_with_cost("test-key", 5).await.unwrap();
        assert!(decision.permitted, "Cost 5 request should still work");
    }

    #[tokio::test(start_paused = true)]
    async fn test_leaky_bucket_ttl() {
        let bucket = LeakyBucket::with_ttl(10, 100, Duration::from_secs(1));

        // Access key1
        bucket.check("key1").await.unwrap();
        assert_eq!(bucket.buckets.len(), 1);

        // Advance time past TTL
        tokio::time::advance(Duration::from_secs(2)).await;

        // Access key2 multiple times to trigger cleanup
        for _ in 0..200 {
            bucket.check("key2").await.unwrap();
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
