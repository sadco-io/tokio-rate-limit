//! Rate limiting algorithms.

use crate::error::Result;
use crate::limiter::RateLimitDecision;
use async_trait::async_trait;

mod token_bucket;

pub use token_bucket::TokenBucket;

/// Trait for rate limiting algorithms.
///
/// Implementations of this trait define how rate limiting decisions are made.
/// The trait is async to allow for potential I/O operations in custom implementations.
#[async_trait]
pub trait Algorithm: Send + Sync {
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
}
