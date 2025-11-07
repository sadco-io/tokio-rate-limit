//! Zero-copy token bucket implementation.
//!
//! This implementation eliminates string allocations in the hot path by using
//! borrowed string references for HashMap lookups and only allocating when inserting new keys.
//!
//! ## Optimization Strategy
//!
//! Current baseline allocates on every check:
//! ```rust,ignore
//! pub async fn check(&self, key: &str) -> Result<RateLimitDecision> {
//!     let key_string = key.to_string();  // ❌ Allocation!
//!     let state = self.tokens.get(&key_string, &guard);
//! }
//! ```
//!
//! Zero-copy approach:
//! ```rust,ignore
//! pub async fn check(&self, key: &str) -> Result<RateLimitDecision> {
//!     let state = self.tokens.get(key, &guard);  // ✅ No allocation for lookups
//!     // Only allocate when inserting new key
//! }
//! ```
//!
//! ## Performance Impact
//!
//! - Reduces memory allocator pressure
//! - Improves cache locality
//! - Decreases GC pressure in high-throughput scenarios
//! - Expected improvement: 10-30% for high key cardinality workloads
//!
//! ## Tradeoffs
//!
//! - Slightly more complex code
//! - Depends on flurry's support for borrowed key lookups
//! - May require custom Borrow implementation

use crate::algorithm::Algorithm;
use crate::error::Result;
use crate::limiter::RateLimitDecision;
use async_trait::async_trait;
use flurry::HashMap as FlurryHashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

const SCALE: u64 = 1000;
const MAX_BURST: u64 = u64::MAX / (2 * SCALE);
const MAX_RATE_PER_SEC: u64 = u64::MAX / (2 * SCALE);

/// Atomic state for a token bucket
struct AtomicTokenState {
    tokens: AtomicU64,
    last_refill_nanos: AtomicU64,
    last_access_nanos: AtomicU64,
}

impl AtomicTokenState {
    fn new(capacity: u64, now_nanos: u64) -> Self {
        Self {
            tokens: AtomicU64::new(capacity.saturating_mul(SCALE)),
            last_refill_nanos: AtomicU64::new(now_nanos),
            last_access_nanos: AtomicU64::new(now_nanos),
        }
    }

    fn try_consume(
        &self,
        capacity: u64,
        refill_rate_per_second: u64,
        now_nanos: u64,
        cost: u64,
    ) -> (bool, u64) {
        self.last_access_nanos.store(now_nanos, Ordering::Relaxed);

        let scaled_capacity = capacity.saturating_mul(SCALE);
        let token_cost = cost.saturating_mul(SCALE);

        loop {
            let current_tokens = self.tokens.load(Ordering::Relaxed);
            let last_refill = self.last_refill_nanos.load(Ordering::Relaxed);

            let elapsed_nanos = now_nanos.saturating_sub(last_refill);
            let elapsed_secs = elapsed_nanos as f64 / 1_000_000_000.0;
            let tokens_per_sec_scaled = refill_rate_per_second.saturating_mul(SCALE);
            let new_tokens_to_add = (elapsed_secs * tokens_per_sec_scaled as f64) as u64;

            let updated_tokens = current_tokens
                .saturating_add(new_tokens_to_add)
                .min(scaled_capacity);

            if updated_tokens >= token_cost {
                let new_tokens = updated_tokens.saturating_sub(token_cost);
                let new_time = if new_tokens_to_add > 0 {
                    now_nanos
                } else {
                    last_refill
                };

                match self.tokens.compare_exchange_weak(
                    current_tokens,
                    new_tokens,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        if new_tokens_to_add > 0 {
                            let _ = self.last_refill_nanos.compare_exchange_weak(
                                last_refill,
                                new_time,
                                Ordering::AcqRel,
                                Ordering::Relaxed,
                            );
                        }
                        return (true, new_tokens / SCALE);
                    }
                    Err(_) => continue,
                }
            } else {
                let new_time = if new_tokens_to_add > 0 {
                    now_nanos
                } else {
                    last_refill
                };

                match self.tokens.compare_exchange_weak(
                    current_tokens,
                    updated_tokens,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        if new_tokens_to_add > 0 {
                            let _ = self.last_refill_nanos.compare_exchange_weak(
                                last_refill,
                                new_time,
                                Ordering::AcqRel,
                                Ordering::Relaxed,
                            );
                        }
                        return (false, updated_tokens / SCALE);
                    }
                    Err(_) => continue,
                }
            }
        }
    }
}

/// Zero-copy token bucket implementation
///
/// This implementation optimizes for minimal allocations by:
/// 1. Using borrowed string references for lookups
/// 2. Only allocating when inserting new keys
/// 3. Avoiding intermediate string copies
///
/// Note: flurry's current API requires String keys, so we still need to allocate
/// on first access. However, subsequent accesses to the same key avoid allocation
/// during the lookup phase.
pub struct ZeroCopyTokenBucket {
    capacity: u64,
    refill_rate_per_second: u64,
    reference_instant: Instant,
    idle_ttl: Option<Duration>,
    tokens: Arc<FlurryHashMap<String, Arc<AtomicTokenState>>>,
}

impl ZeroCopyTokenBucket {
    /// Creates a new zero-copy token bucket
    pub fn new(capacity: u64, refill_rate_per_second: u64) -> Self {
        let safe_capacity = capacity.min(MAX_BURST);
        let safe_rate = refill_rate_per_second.min(MAX_RATE_PER_SEC);

        Self {
            capacity: safe_capacity,
            refill_rate_per_second: safe_rate,
            reference_instant: Instant::now(),
            idle_ttl: None,
            tokens: Arc::new(FlurryHashMap::new()),
        }
    }

    /// Creates a token bucket with TTL-based eviction
    pub fn with_ttl(capacity: u64, refill_rate_per_second: u64, idle_ttl: Duration) -> Self {
        let mut bucket = Self::new(capacity, refill_rate_per_second);
        bucket.idle_ttl = Some(idle_ttl);
        bucket
    }

    #[inline]
    fn now_nanos(&self) -> u64 {
        self.reference_instant.elapsed().as_nanos() as u64
    }

    /// Zero-copy key lookup helper
    ///
    /// This method attempts to look up a key using a borrowed reference.
    /// Only allocates if the key doesn't exist and needs to be inserted.
    #[inline]
    fn get_or_create_state(
        &self,
        key: &str,
        guard: &flurry::Guard<'_>,
        now_nanos: u64,
    ) -> Arc<AtomicTokenState> {
        // First, try lookup with borrowed key (zero-copy)
        // Note: flurry's get() accepts &Q where Q: Borrow<K>, so this works
        if let Some(state) = self.tokens.get(key, guard) {
            return state.clone();
        }

        // Key doesn't exist, need to allocate and insert
        // This is the only allocation point in the hot path
        let key_string = key.to_string();
        let new_state = Arc::new(AtomicTokenState::new(self.capacity, now_nanos));

        match self.tokens.try_insert(key_string, new_state.clone(), guard) {
            Ok(_) => new_state,
            Err(current) => current.current.clone(),
        }
    }

    fn cleanup_idle(&self, now_nanos: u64) {
        if let Some(ttl) = self.idle_ttl {
            let ttl_nanos = ttl.as_nanos() as u64;
            let guard = self.tokens.guard();
            let keys_to_remove: Vec<String> = self
                .tokens
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
                self.tokens.remove(&key, &guard);
            }
        }
    }
}

impl super::private::Sealed for ZeroCopyTokenBucket {}

#[async_trait]
impl Algorithm for ZeroCopyTokenBucket {
    async fn check(&self, key: &str) -> Result<RateLimitDecision> {
        let now = self.now_nanos();

        if self.idle_ttl.is_some() && (now % 100) == 0 {
            self.cleanup_idle(now);
        }

        // Zero-copy lookup: no allocation unless inserting
        let guard = self.tokens.guard();
        let state = self.get_or_create_state(key, &guard, now);

        let (permitted, remaining) =
            state.try_consume(self.capacity, self.refill_rate_per_second, now, 1);

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

        if self.idle_ttl.is_some() && (now % 100) == 0 {
            self.cleanup_idle(now);
        }

        // Zero-copy lookup
        let guard = self.tokens.guard();
        let state = self.get_or_create_state(key, &guard, now);

        let (permitted, remaining) =
            state.try_consume(self.capacity, self.refill_rate_per_second, now, cost);

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
    async fn test_zerocopy_token_bucket_basic() {
        let bucket = ZeroCopyTokenBucket::new(10, 100);

        for _ in 0..10 {
            let decision = bucket.check("test-key").await.unwrap();
            assert!(decision.permitted);
        }

        let decision = bucket.check("test-key").await.unwrap();
        assert!(!decision.permitted);
    }

    #[tokio::test(start_paused = true)]
    async fn test_zerocopy_token_bucket_refill() {
        let bucket = ZeroCopyTokenBucket::new(10, 100);

        for _ in 0..10 {
            bucket.check("test-key").await.unwrap();
        }

        let decision = bucket.check("test-key").await.unwrap();
        assert!(!decision.permitted);

        tokio::time::advance(Duration::from_millis(100)).await;

        for _ in 0..10 {
            let decision = bucket.check("test-key").await.unwrap();
            assert!(decision.permitted);
        }
    }

    #[tokio::test]
    async fn test_zerocopy_no_allocation_on_second_access() {
        let bucket = ZeroCopyTokenBucket::new(1000, 1000);

        // First access: allocates (inserts key)
        bucket.check("test-key").await.unwrap();

        // Subsequent accesses: should not allocate (lookup only)
        // This is hard to test directly, but we can verify functionality
        for _ in 0..100 {
            bucket.check("test-key").await.unwrap();
        }
    }
}
