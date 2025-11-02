//! Rate limiter implementation and configuration.

use crate::algorithm::{Algorithm, TokenBucket};
use crate::error::{Error, Result};
use std::time::Duration;

/// Configuration for creating a rate limiter.
#[derive(Debug, Clone, Copy)]
pub struct RateLimiterConfig {
    /// Maximum number of requests allowed per second.
    pub requests_per_second: u64,

    /// Maximum burst size (bucket capacity).
    pub burst: u64,
}

impl RateLimiterConfig {
    /// Validates the configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - `requests_per_second` is 0
    /// - `burst` is 0
    /// - `burst` is less than `requests_per_second`
    pub fn validate(&self) -> Result<()> {
        if self.requests_per_second == 0 {
            return Err(Error::Config(
                "requests_per_second must be greater than 0".to_string(),
            ));
        }
        if self.burst == 0 {
            return Err(Error::Config("burst must be greater than 0".to_string()));
        }
        if self.burst < self.requests_per_second {
            return Err(Error::Config(
                "burst must be greater than or equal to requests_per_second".to_string(),
            ));
        }
        Ok(())
    }
}

/// Builder for creating a rate limiter with a fluent API.
///
/// # Examples
///
/// ```
/// use tokio_rate_limit::RateLimiter;
///
/// let limiter = RateLimiter::builder()
///     .requests_per_second(100)
///     .burst(200)
///     .build()
///     .unwrap();
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct RateLimiterBuilder {
    requests_per_second: Option<u64>,
    burst: Option<u64>,
}

impl RateLimiterBuilder {
    /// Creates a new builder with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the requests per second rate limit.
    ///
    /// # Arguments
    ///
    /// * `rate` - Maximum number of requests allowed per second
    ///
    /// # Examples
    ///
    /// ```
    /// use tokio_rate_limit::RateLimiter;
    ///
    /// let limiter = RateLimiter::builder()
    ///     .requests_per_second(100)
    ///     .burst(200)
    ///     .build()
    ///     .unwrap();
    /// ```
    pub fn requests_per_second(mut self, rate: u64) -> Self {
        self.requests_per_second = Some(rate);
        self
    }

    /// Sets the burst size (maximum tokens in the bucket).
    ///
    /// The burst size must be at least equal to `requests_per_second`.
    /// A higher burst allows for short traffic spikes.
    ///
    /// # Arguments
    ///
    /// * `burst` - Maximum burst size
    ///
    /// # Examples
    ///
    /// ```
    /// use tokio_rate_limit::RateLimiter;
    ///
    /// // Allow bursts up to 2x the rate
    /// let limiter = RateLimiter::builder()
    ///     .requests_per_second(100)
    ///     .burst(200)
    ///     .build()
    ///     .unwrap();
    /// ```
    pub fn burst(mut self, burst: u64) -> Self {
        self.burst = Some(burst);
        self
    }

    /// Builds the rate limiter with the configured settings.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Either `requests_per_second` or `burst` is not set
    /// - The configuration validation fails
    ///
    /// # Examples
    ///
    /// ```
    /// use tokio_rate_limit::RateLimiter;
    ///
    /// let limiter = RateLimiter::builder()
    ///     .requests_per_second(100)
    ///     .burst(200)
    ///     .build()
    ///     .unwrap();
    /// ```
    pub fn build(self) -> Result<RateLimiter> {
        let requests_per_second = self
            .requests_per_second
            .ok_or_else(|| Error::Config("requests_per_second must be set".to_string()))?;

        let burst = self
            .burst
            .ok_or_else(|| Error::Config("burst must be set".to_string()))?;

        let config = RateLimiterConfig {
            requests_per_second,
            burst,
        };

        config.validate()?;

        Ok(RateLimiter::new(config))
    }
}

/// Result of a rate limit check.
#[derive(Debug, Clone)]
pub struct RateLimitDecision {
    /// Whether the request is permitted.
    pub permitted: bool,

    /// How long to wait before retrying (if rate limited).
    pub retry_after: Option<Duration>,

    /// Number of remaining requests in the current window.
    pub remaining: Option<u64>,

    /// The configured rate limit.
    pub limit: u64,
}

/// A rate limiter that tracks requests per key and enforces limits.
pub struct RateLimiter {
    algorithm: Box<dyn Algorithm>,
}

impl RateLimiter {
    /// Creates a new rate limiter with the given configuration.
    ///
    /// # Examples
    ///
    /// ```
    /// use tokio_rate_limit::{RateLimiter, RateLimiterConfig};
    ///
    /// let limiter = RateLimiter::new(RateLimiterConfig {
    ///     requests_per_second: 100,
    ///     burst: 200,
    /// });
    /// ```
    pub fn new(config: RateLimiterConfig) -> Self {
        Self {
            algorithm: Box::new(TokenBucket::new(config.burst, config.requests_per_second)),
        }
    }

    /// Creates a new builder for configuring a rate limiter.
    ///
    /// # Examples
    ///
    /// ```
    /// use tokio_rate_limit::RateLimiter;
    ///
    /// let limiter = RateLimiter::builder()
    ///     .requests_per_second(100)
    ///     .burst(200)
    ///     .build()
    ///     .unwrap();
    /// ```
    pub fn builder() -> RateLimiterBuilder {
        RateLimiterBuilder::new()
    }

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
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use tokio_rate_limit::{RateLimiter, RateLimiterConfig};
    /// # async fn example() {
    /// let limiter = RateLimiter::new(RateLimiterConfig {
    ///     requests_per_second: 100,
    ///     burst: 200,
    /// });
    ///
    /// let decision = limiter.check("client-123").await.unwrap();
    /// if decision.permitted {
    ///     // Process the request
    /// } else {
    ///     // Reject with 429 Too Many Requests
    /// }
    /// # }
    /// ```
    pub async fn check(&self, key: &str) -> Result<RateLimitDecision> {
        self.algorithm.check(key).await
    }
}
