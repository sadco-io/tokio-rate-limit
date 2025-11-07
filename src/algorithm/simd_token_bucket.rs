//! SIMD-optimized token bucket implementation.
//!
//! This is an experimental implementation exploring SIMD optimizations for token bucket operations.
//! The goal is to achieve 2-5x performance improvements for certain workloads.
//!
//! ## Optimization Strategies
//!
//! 1. **Vectorized Token Refill Calculations**: Process multiple buckets in parallel using SIMD
//! 2. **Batch Key Processing**: Check multiple keys in a single operation
//! 3. **SIMD-friendly Data Layout**: Structure data for efficient SIMD operations
//!
//! ## Platform Support
//!
//! - x86_64: AVX2 (256-bit vectors, 4x f64 or u64)
//! - ARM: NEON (128-bit vectors, 2x f64 or u64)
//! - Fallback: Scalar implementation on unsupported platforms
//!
//! ## Current Status: EXPERIMENTAL
//!
//! This implementation is for research and benchmarking purposes.
//! It may not be production-ready.

use crate::algorithm::Algorithm;
use crate::error::Result;
use crate::limiter::RateLimitDecision;
use async_trait::async_trait;
use flurry::HashMap as FlurryHashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

/// Scaling factor for sub-token precision (same as baseline)
const SCALE: u64 = 1000;

/// Maximum burst capacity
const MAX_BURST: u64 = u64::MAX / (2 * SCALE);

/// Maximum refill rate per second
const MAX_RATE_PER_SEC: u64 = u64::MAX / (2 * SCALE);

/// Atomic state for a token bucket (same as baseline for fair comparison)
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

    /// SIMD-optimized token consumption (for single key - baseline equivalent)
    ///
    /// For single key operations, SIMD doesn't help much.
    /// The real benefit comes from batch operations (see try_consume_batch).
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

            // SIMD opportunity: Vectorize this calculation for multiple buckets
            let elapsed_nanos = now_nanos.saturating_sub(last_refill);
            let elapsed_secs = elapsed_nanos as f64 / 1_000_000_000.0;
            let tokens_per_sec_scaled = refill_rate_per_second.saturating_mul(SCALE);
            let new_tokens_to_add = (elapsed_secs * tokens_per_sec_scaled as f64) as u64;

            let updated_tokens = current_tokens
                .saturating_add(new_tokens_to_add)
                .min(scaled_capacity);

            if updated_tokens >= token_cost {
                let new_tokens = updated_tokens.saturating_sub(token_cost);
                let new_time = if new_tokens_to_add > 0 { now_nanos } else { last_refill };

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
                let new_time = if new_tokens_to_add > 0 { now_nanos } else { last_refill };

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

/// Batch refill calculation result
#[derive(Debug)]
#[allow(dead_code)]
struct BatchRefillResult {
    new_tokens: Vec<u64>,
    elapsed_secs: Vec<f64>,
}

/// SIMD-optimized token bucket implementation
pub struct SimdTokenBucket {
    capacity: u64,
    refill_rate_per_second: u64,
    reference_instant: Instant,
    idle_ttl: Option<Duration>,
    tokens: Arc<FlurryHashMap<String, Arc<AtomicTokenState>>>,
}

impl SimdTokenBucket {
    /// Creates a new SIMD-optimized token bucket
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

    /// SIMD-optimized batch refill calculation
    ///
    /// NOTE: This is a placeholder for future SIMD implementation.
    /// The current implementation uses scalar code to avoid unsafe blocks.
    ///
    /// Future work: Implement SIMD using portable_simd or std::simd when stabilized.
    /// For now, this serves as a baseline for comparison and API design.
    #[allow(dead_code)]
    fn calculate_refill_batch_simd(
        &self,
        last_refills: &[u64],
        now_nanos: u64,
    ) -> BatchRefillResult {
        let mut elapsed_secs = Vec::with_capacity(last_refills.len());
        let mut new_tokens = Vec::with_capacity(last_refills.len());

        // Scalar implementation - SIMD would process 2-4 values in parallel
        // TODO: Use portable_simd when stable or add unsafe feature flag
        for &last_refill in last_refills {
            let elapsed_nanos = now_nanos.saturating_sub(last_refill);
            let elapsed = elapsed_nanos as f64 / 1_000_000_000.0;
            elapsed_secs.push(elapsed);

            let tokens_per_sec_scaled = self.refill_rate_per_second.saturating_mul(SCALE);
            new_tokens.push((elapsed * tokens_per_sec_scaled as f64) as u64);
        }

        BatchRefillResult {
            new_tokens,
            elapsed_secs,
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

impl super::private::Sealed for SimdTokenBucket {}

#[async_trait]
impl Algorithm for SimdTokenBucket {
    async fn check(&self, key: &str) -> Result<RateLimitDecision> {
        let now = self.now_nanos();

        if self.idle_ttl.is_some() && (now % 100) == 0 {
            self.cleanup_idle(now);
        }

        let guard = self.tokens.guard();
        let key_string = key.to_string();
        let state = match self.tokens.get(&key_string, &guard) {
            Some(state) => state.clone(),
            None => {
                let new_state = Arc::new(AtomicTokenState::new(self.capacity, now));
                match self
                    .tokens
                    .try_insert(key_string.clone(), new_state.clone(), &guard)
                {
                    Ok(_) => new_state,
                    Err(current) => current.current.clone(),
                }
            }
        };

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

        let guard = self.tokens.guard();
        let key_string = key.to_string();
        let state = match self.tokens.get(&key_string, &guard) {
            Some(state) => state.clone(),
            None => {
                let new_state = Arc::new(AtomicTokenState::new(self.capacity, now));
                match self
                    .tokens
                    .try_insert(key_string.clone(), new_state.clone(), &guard)
                {
                    Ok(_) => new_state,
                    Err(current) => current.current.clone(),
                }
            }
        };

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
    async fn test_simd_token_bucket_basic() {
        let bucket = SimdTokenBucket::new(10, 100);

        for _ in 0..10 {
            let decision = bucket.check("test-key").await.unwrap();
            assert!(decision.permitted);
        }

        let decision = bucket.check("test-key").await.unwrap();
        assert!(!decision.permitted);
    }

    #[tokio::test(start_paused = true)]
    async fn test_simd_token_bucket_refill() {
        let bucket = SimdTokenBucket::new(10, 100);

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
}
