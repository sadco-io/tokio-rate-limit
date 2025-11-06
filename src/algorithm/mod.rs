//! Rate limiting algorithms.

use crate::error::Result;
use crate::limiter::RateLimitDecision;
use async_trait::async_trait;

mod leaky_bucket;
mod token_bucket;

pub use leaky_bucket::LeakyBucket;
pub use token_bucket::TokenBucket;

/// Private module for the sealed trait pattern.
///
/// This prevents external implementations of the Algorithm trait while maintaining
/// flexibility for internal algorithm implementations. This allows us to make
/// breaking changes to the trait in the future without requiring a semver major bump.
mod private {
    pub trait Sealed {}
}

/// Trait for rate limiting algorithms.
///
/// Implementations of this trait define how rate limiting decisions are made.
/// The trait is async to allow for potential I/O operations in custom implementations.
///
/// # Sealed Trait
///
/// This trait is sealed and cannot be implemented outside of this crate. This design
/// allows us to add new methods or change the trait in minor version updates without
/// breaking semver guarantees. If you need a custom algorithm, please open an issue
/// to discuss adding it to the library.
///
/// # Available Algorithms
///
/// - [`TokenBucket`] - Allows bursts up to capacity, refills at constant rate
/// - [`LeakyBucket`] - Enforces steady rate, smooths traffic (NEW in v0.3.0)
///
/// # Planned Algorithms
///
/// - Sliding Window (future) - More precise rate limiting
#[async_trait]
pub trait Algorithm: Send + Sync + private::Sealed {
    /// Checks if a request for the given key should be permitted.
    ///
    /// # Arguments
    ///
    /// * `key` - A string identifier for the client/resource being rate limited
    ///
    /// # Returns
    ///
    /// A `RateLimitDecision` indicating whether the request is permitted and
    /// additional metadata about the rate limit status.
    async fn check(&self, key: &str) -> Result<RateLimitDecision>;

    /// Checks if a request with the given cost should be permitted.
    ///
    /// Cost represents the number of tokens to consume. The default implementation
    /// uses a cost of 1 (equivalent to `check()`), but algorithms can override this
    /// to support weighted rate limiting.
    ///
    /// # Arguments
    ///
    /// * `key` - A string identifier for the client/resource being rate limited
    /// * `cost` - Number of tokens to consume (must be > 0)
    ///
    /// # Returns
    ///
    /// A `RateLimitDecision` indicating whether the request is permitted and
    /// additional metadata about the rate limit status.
    ///
    /// # Default Behavior
    ///
    /// The default implementation rejects requests with cost > 1 if there aren't
    /// enough remaining tokens. Algorithms should override this for proper
    /// weighted rate limiting support.
    async fn check_with_cost(&self, key: &str, cost: u64) -> Result<RateLimitDecision> {
        if cost == 1 {
            self.check(key).await
        } else {
            // Default: check if we have enough tokens for the cost
            let mut decision = self.check(key).await?;
            if cost > 1 && decision.remaining.unwrap_or(0) < cost {
                decision.permitted = false;
            }
            Ok(decision)
        }
    }
}
