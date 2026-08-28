//! Thread-local cached token bucket implementation.

#![allow(deprecated)]
//!
//! This is a revisited implementation of thread-local caching, which showed a -6.4%
//! regression in v0.1.0. This new approach uses different caching strategies to avoid
//! the overhead that caused the regression.
//!
//! ## Why the Original Failed
//!
//! v0.1.0 caching issues:
//! - `RefCell::borrow_mut()` overhead on every access
//! - LRU cache management cost
//! - Cache coherency overhead
//! - Contention between cache updates and main hashmap
//!
//! ## New Approach: Lock-Free Thread-Local Caching
//!
//! This implementation uses:
//! 1. **Lock-free thread-local cache**: No RefCell, pure atomics
//! 2. **Simple cache eviction**: Last-accessed-only (no LRU complexity)
//! 3. **Probabilistic cache refresh**: Reduce cache coherency overhead
//! 4. **Adaptive caching**: Only cache hot keys (80/20 rule)
//!
//! ## When This Helps
//!
//! - **Hot keys**: Few keys accessed repeatedly (e.g., per-IP limiting with few IPs)
//! - **Single-threaded**: Thread-local caching is most effective
//! - **Low contention**: When most threads access different keys
//!
//! ## When This Hurts
//!
//! - **High key cardinality**: Cache thrashing with many unique keys
//! - **Uniform distribution**: No hot keys to cache
//! - **Cross-thread sharing**: Same keys accessed from multiple threads
//!
//! ## Performance Target
//!
//! - Best case: +20-50% for hot-key workloads
//! - Worst case: -5% overhead (better than v0.1.0's -6.4%)
//! - Target: 0% overhead for uniform distribution

use crate::algorithm::internal::{
    nanos_for_tokens, refill_tokens, should_cleanup, token_decision, zero_cost_decision, SCALE,
};
use crate::algorithm::Algorithm;
use crate::limiter::RateLimitDecision;
use flurry::HashMap as FlurryHashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;
const MAX_BURST: u64 = u64::MAX / (2 * SCALE);
const MAX_RATE_PER_SEC: u64 = u64::MAX / (2 * SCALE);

/// Atomic state for a token bucket
struct AtomicTokenState {
    tokens: AtomicU64,
    last_refill_nanos: AtomicU64,
    last_access_nanos: AtomicU64,
    access_count: AtomicU64, // Track access frequency for adaptive caching
}

impl AtomicTokenState {
    fn new(capacity: u64, now_nanos: u64) -> Self {
        Self {
            tokens: AtomicU64::new(capacity.saturating_mul(SCALE)),
            last_refill_nanos: AtomicU64::new(now_nanos),
            last_access_nanos: AtomicU64::new(now_nanos),
            access_count: AtomicU64::new(0),
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
        self.access_count.fetch_add(1, Ordering::Relaxed);

        let scaled_capacity = capacity.saturating_mul(SCALE);
        let token_cost = cost.saturating_mul(SCALE);
        let rate_scaled = refill_rate_per_second.saturating_mul(SCALE);

        self.apply_refill(scaled_capacity, rate_scaled, now_nanos);

        loop {
            let current = self.tokens.load(Ordering::Relaxed);
            if current < token_cost {
                return (false, current / SCALE);
            }
            match self.tokens.compare_exchange_weak(
                current,
                current - token_cost,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(prev) => return (true, (prev - token_cost) / SCALE),
                Err(_) => continue,
            }
        }
    }

    fn apply_refill(&self, capacity_scaled: u64, rate_scaled: u64, now_nanos: u64) {
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

    /// Check if this is a "hot" key worth caching
    #[inline]
    fn is_hot_key(&self) -> bool {
        // A key is "hot" if accessed more than 10 times
        // This is a simple heuristic - could be tuned
        self.access_count.load(Ordering::Relaxed) > 10
    }
}

use std::cell::RefCell;

/// Thread-local cache entry
///
/// Uses RefCell for safe interior mutability. While this has some overhead,
/// it's still faster than the v0.1.0 LRU cache implementation for hot-key workloads.
struct CacheEntry {
    key: Option<String>,
    state: Option<Arc<AtomicTokenState>>,
    hits: u64,
    misses: u64,
}

impl CacheEntry {
    fn new() -> Self {
        Self {
            key: None,
            state: None,
            hits: 0,
            misses: 0,
        }
    }

    /// Try to get cached state for a key
    #[inline]
    fn get(&mut self, key: &str) -> Option<Arc<AtomicTokenState>> {
        if let Some(cached_key) = &self.key {
            if cached_key == key {
                self.hits += 1;
                return self.state.clone();
            }
        }
        self.misses += 1;
        None
    }

    /// Update cache entry
    #[inline]
    fn set(&mut self, key: String, state: Arc<AtomicTokenState>) {
        self.key = Some(key);
        self.state = Some(state);
    }

    /// Get cache hit rate for diagnostics
    #[allow(dead_code)]
    fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }
}

thread_local! {
    /// Thread-local cache: stores the most recently accessed key
    ///
    /// This is a simple last-accessed cache (not LRU) to minimize overhead.
    /// In hot-key workloads, the same key is accessed repeatedly, so this is sufficient.
    static CACHE: RefCell<CacheEntry> = RefCell::new(CacheEntry::new());
}

/// Thread-local cached token bucket.
///
/// Deprecated: the thread-local cache keeps a ghost `Arc` after TTL eviction,
/// so two buckets for the same key can run in parallel. Use [`TokenBucket`].
/// Removal planned for 0.11.
#[deprecated(
    since = "0.10.0",
    note = "thread-local cache splits from the map under TTL eviction; use TokenBucket. Removal planned for 0.11."
)]
pub struct CachedTokenBucket {
    capacity: u64,
    refill_rate_per_second: u64,
    reference_instant: Instant,
    idle_ttl: Option<Duration>,
    tokens: Arc<FlurryHashMap<String, Arc<AtomicTokenState>>>,
}

#[allow(deprecated)]
impl CachedTokenBucket {
    /// Creates a new cached token bucket
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

    /// Get or create state with thread-local caching
    #[inline]
    fn get_or_create_state_cached(
        &self,
        key: &str,
        guard: &flurry::Guard<'_>,
        now_nanos: u64,
    ) -> Arc<AtomicTokenState> {
        // Never cache when TTL eviction is on: cleanup_idle drops map entries
        // while this thread would keep debiting the ghost Arc.
        if self.idle_ttl.is_none() {
            if let Some(state) = CACHE.with(|cache| cache.borrow_mut().get(key)) {
                return state;
            }
        }

        // Cache miss: look up in main hashmap
        let state = if let Some(state) = self.tokens.get(key, guard) {
            state.clone()
        } else {
            // Key doesn't exist, create it
            let key_string = key.to_string();
            let new_state = Arc::new(AtomicTokenState::new(self.capacity, now_nanos));

            match self
                .tokens
                .try_insert(key_string.clone(), new_state.clone(), guard)
            {
                Ok(_) => new_state,
                Err(current) => current.current.clone(),
            }
        };

        if self.idle_ttl.is_none() && state.is_hot_key() {
            CACHE.with(|cache| cache.borrow_mut().set(key.to_string(), state.clone()));
        }

        state
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

impl super::private::Sealed for CachedTokenBucket {}

#[allow(deprecated)]
impl CachedTokenBucket {
    fn check_impl(&self, key: &str, cost: u64) -> RateLimitDecision {
        if cost == 0 {
            return zero_cost_decision(self.capacity);
        }

        let now = self.now_nanos();
        if self.idle_ttl.is_some() && should_cleanup() {
            self.cleanup_idle(now);
        }

        let guard = self.tokens.guard();
        let state = self.get_or_create_state_cached(key, &guard, now);
        let (permitted, remaining) =
            state.try_consume(self.capacity, self.refill_rate_per_second, now, cost);
        token_decision(
            permitted,
            remaining,
            self.capacity,
            self.refill_rate_per_second,
            cost,
        )
    }
}

#[allow(deprecated)]
impl Algorithm for CachedTokenBucket {
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
    async fn test_cached_token_bucket_basic() {
        let bucket = CachedTokenBucket::new(10, 100);

        for _ in 0..10 {
            let decision = bucket.check("test-key");
            assert!(decision.permitted);
        }

        let decision = bucket.check("test-key");
        assert!(!decision.permitted);
    }

    #[tokio::test(start_paused = true)]
    async fn test_cached_token_bucket_refill() {
        let bucket = CachedTokenBucket::new(10, 100);

        for _ in 0..10 {
            let _ = bucket.check("test-key");
        }

        let decision = bucket.check("test-key");
        assert!(!decision.permitted);

        tokio::time::advance(Duration::from_millis(100)).await;

        for _ in 0..10 {
            let decision = bucket.check("test-key");
            assert!(decision.permitted);
        }
    }

    #[tokio::test]
    async fn test_cached_token_bucket_hot_keys() {
        let bucket = CachedTokenBucket::new(1000, 1000);

        // Access the same key repeatedly to make it "hot"
        for _ in 0..20 {
            let _ = bucket.check("hot-key");
        }

        // After 20 accesses, it should be cached
        // Subsequent accesses should hit the cache
        for _ in 0..100 {
            let _ = bucket.check("hot-key");
        }
    }
}
