//! Rate limiting algorithms.

mod leaky_bucket;
mod token_bucket;

mod cached_token_bucket;
mod probabilistic_token_bucket;

pub(crate) mod internal;

pub use leaky_bucket::LeakyBucket;
pub use token_bucket::TokenBucket;

/// Thread-local cached token bucket. Deprecated: the cache splits from the
/// map under TTL eviction. Prefer [`TokenBucket`].
#[allow(deprecated)]
pub use cached_token_bucket::CachedTokenBucket;

pub use probabilistic_token_bucket::ProbabilisticTokenBucket;

use crate::limiter::RateLimitDecision;

/// Private module for the sealed trait pattern.
///
/// This prevents external implementations of the Algorithm trait while maintaining
/// flexibility for internal algorithm implementations.
mod private {
    pub trait Sealed {}
}

/// Trait for rate limiting algorithms.
///
/// Implementations of this trait define how rate limiting decisions are made.
/// The trait is synchronous: every in-tree algorithm is a pure atomic/hashmap
/// operation. `RateLimiter::acquire` is the async wrapper that sleeps on
/// `retry_after`.
///
/// # Sealed Trait
///
/// This trait is sealed and cannot be implemented outside of this crate.
///
/// # Available Algorithms
///
/// - [`TokenBucket`] — bursts up to capacity, refills at a constant rate (default)
/// - [`LeakyBucket`] — enforces a steady rate, smooths traffic
/// - [`ProbabilisticTokenBucket`] — samples 1 in `sample_rate` requests; soft limiting
/// - `CachedTokenBucket` — deprecated; use [`TokenBucket`]
pub trait Algorithm: Send + Sync + private::Sealed {
    /// Checks if a request for the given key should be permitted.
    ///
    /// # Arguments
    ///
    /// * `key` - A string identifier for the client/resource being rate limited
    fn check(&self, key: &str) -> RateLimitDecision;

    /// Checks if a request with the given cost should be permitted.
    ///
    /// Cost represents the number of tokens to consume and must be > 0.
    /// A cost of 0 is rejected without consuming quota.
    ///
    /// The default implementation delegates to [`check`](Self::check) and
    /// ignores `cost`. Concrete algorithms override this.
    fn check_with_cost(&self, key: &str, cost: u64) -> RateLimitDecision {
        let _ = cost;
        self.check(key)
    }
}
