//! Rate limiter implementation and configuration.

use crate::algorithm::{Algorithm, TokenBucket};
use crate::error::{Error, Result};
use std::time::Duration;

#[cfg(feature = "observability")]
use tracing::{debug, info, instrument};

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

    /// Time until the bucket fully refills (for IETF RateLimit-Reset header).
    /// This is the duration until the token bucket returns to maximum capacity.
    pub reset: Option<Duration>,
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
    #[cfg_attr(feature = "observability", instrument(skip(self), fields(key = %key)))]
    pub async fn check(&self, key: &str) -> Result<RateLimitDecision> {
        let decision = self.algorithm.check(key).await?;

        #[cfg(feature = "observability")]
        {
            if decision.permitted {
                debug!(
                    remaining = decision.remaining,
                    limit = decision.limit,
                    "Rate limit check: PERMITTED"
                );
            } else {
                info!(
                    remaining = decision.remaining,
                    limit = decision.limit,
                    retry_after_ms = decision.retry_after.map(|d| d.as_millis()),
                    "Rate limit check: DENIED"
                );
            }
        }

        Ok(decision)
    }

    /// Checks if a request with the given cost should be permitted.
    ///
    /// Cost represents the number of tokens to consume. This enables weighted
    /// rate limiting where different operations consume different amounts of quota.
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
    /// // Light operation costs 1 token
    /// let decision = limiter.check_with_cost("client-123", 1).await.unwrap();
    ///
    /// // Heavy operation costs 10 tokens
    /// let decision = limiter.check_with_cost("client-123", 10).await.unwrap();
    /// # }
    /// ```
    #[cfg_attr(feature = "observability", instrument(skip(self), fields(key = %key, cost = cost)))]
    pub async fn check_with_cost(&self, key: &str, cost: u64) -> Result<RateLimitDecision> {
        let decision = self.algorithm.check_with_cost(key, cost).await?;

        #[cfg(feature = "observability")]
        {
            if decision.permitted {
                debug!(
                    remaining = decision.remaining,
                    limit = decision.limit,
                    cost = cost,
                    "Rate limit check with cost: PERMITTED"
                );
            } else {
                info!(
                    remaining = decision.remaining,
                    limit = decision.limit,
                    cost = cost,
                    retry_after_ms = decision.retry_after.map(|d| d.as_millis()),
                    "Rate limit check with cost: DENIED"
                );
            }
        }

        Ok(decision)
    }

    /// Try to acquire permission without blocking.
    ///
    /// This is an alias for `check()` provided for API consistency with
    /// other rate limiting libraries.
    ///
    /// # Arguments
    ///
    /// * `key` - A string identifier for the client/resource being rate limited
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
    /// let decision = limiter.try_acquire("client-123").await.unwrap();
    /// if decision.permitted {
    ///     // Process the request
    /// }
    /// # }
    /// ```
    pub async fn try_acquire(&self, key: &str) -> Result<RateLimitDecision> {
        self.check(key).await
    }

    /// Try to acquire permission with a specific cost.
    ///
    /// This is an alias for `check_with_cost()` provided for API consistency.
    ///
    /// # Arguments
    ///
    /// * `key` - A string identifier for the client/resource being rate limited
    /// * `cost` - Number of tokens to consume
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
    /// let decision = limiter.try_acquire_n("client-123", 5).await.unwrap();
    /// # }
    /// ```
    pub async fn try_acquire_n(&self, key: &str, cost: u64) -> Result<RateLimitDecision> {
        self.check_with_cost(key, cost).await
    }

    /// Acquire permission, blocking until tokens are available or timeout.
    ///
    /// This method will retry until either:
    /// - Tokens become available (returns Ok with permitted=true)
    /// - The timeout expires (returns Ok with permitted=false)
    ///
    /// # Arguments
    ///
    /// * `key` - A string identifier for the client/resource being rate limited
    /// * `timeout` - Maximum time to wait for tokens
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use tokio_rate_limit::{RateLimiter, RateLimiterConfig};
    /// # use std::time::Duration;
    /// # async fn example() {
    /// let limiter = RateLimiter::new(RateLimiterConfig {
    ///     requests_per_second: 100,
    ///     burst: 200,
    /// });
    ///
    /// // Wait up to 5 seconds for tokens
    /// let decision = limiter
    ///     .acquire_timeout("client-123", Duration::from_secs(5))
    ///     .await
    ///     .unwrap();
    ///
    /// if decision.permitted {
    ///     // Process the request
    /// }
    /// # }
    /// ```
    #[cfg_attr(feature = "observability", instrument(skip(self), fields(key = %key, timeout_ms = timeout.as_millis())))]
    pub async fn acquire_timeout(
        &self,
        key: &str,
        timeout: Duration,
    ) -> Result<RateLimitDecision> {
        let start = tokio::time::Instant::now();

        loop {
            let decision = self.check(key).await?;

            if decision.permitted {
                #[cfg(feature = "observability")]
                debug!("Acquired tokens after waiting");
                return Ok(decision);
            }

            let elapsed = start.elapsed();
            if elapsed >= timeout {
                #[cfg(feature = "observability")]
                info!("Timeout expired while waiting for tokens");
                return Ok(decision); // Return denied decision
            }

            // Sleep for retry_after or a small amount
            let sleep_time = decision
                .retry_after
                .unwrap_or(Duration::from_millis(10))
                .min(timeout - elapsed);

            tokio::time::sleep(sleep_time).await;
        }
    }

    /// Acquire permission, blocking indefinitely until tokens are available.
    ///
    /// This method will retry until tokens become available. Use with caution
    /// as it can block indefinitely if the rate limit is never satisfied.
    ///
    /// # Arguments
    ///
    /// * `key` - A string identifier for the client/resource being rate limited
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
    /// // Wait indefinitely for tokens
    /// let decision = limiter.acquire("client-123").await.unwrap();
    /// // Will always be permitted when this returns
    /// assert!(decision.permitted);
    /// # }
    /// ```
    #[cfg_attr(feature = "observability", instrument(skip(self), fields(key = %key)))]
    pub async fn acquire(&self, key: &str) -> Result<RateLimitDecision> {
        loop {
            let decision = self.check(key).await?;

            if decision.permitted {
                #[cfg(feature = "observability")]
                debug!("Acquired tokens");
                return Ok(decision);
            }

            // Sleep for retry_after or a small amount
            let sleep_time = decision.retry_after.unwrap_or(Duration::from_millis(10));

            #[cfg(feature = "observability")]
            debug!(sleep_ms = sleep_time.as_millis(), "Waiting for tokens to refill");

            tokio::time::sleep(sleep_time).await;
        }
    }
}
