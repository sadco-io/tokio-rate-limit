//! Rate limiter implementation, builder, and configuration.

use crate::algorithm::{Algorithm, RateLimitKey, ToKey, TokenBucket};
use crate::error::{Error, Result};
use std::borrow::Borrow;
use std::fmt;
use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;

#[cfg(feature = "observability")]
use tracing::{debug, info, instrument};

/// Configuration for creating a token-bucket [`RateLimiter`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct RateLimiterConfig {
    /// Maximum number of requests allowed per second.
    pub requests_per_second: u64,

    /// Maximum burst size (bucket capacity).
    ///
    /// May be less than, equal to, or greater than `requests_per_second`
    /// (for example `requests_per_second: 100, burst: 10` caps instantaneous
    /// bursts at 10 while sustaining 100 requests/sec).
    pub burst: u64,
}

impl RateLimiterConfig {
    /// Validates the configuration.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] if `requests_per_second == 0` or `burst == 0`.
    pub fn validate(&self) -> Result<()> {
        if self.requests_per_second == 0 {
            return Err(Error::Config(
                "requests_per_second must be greater than 0".to_string(),
            ));
        }
        if self.burst == 0 {
            return Err(Error::Config("burst must be greater than 0".to_string()));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
enum RateSpec {
    PerSecond(u64),
    PerMinute(u64),
    PerHour(u64),
    Interval(Duration),
}

/// Builder for creating a [`RateLimiter`] with a fluent API.
///
/// Supports per-second, per-minute, per-hour, or explicit replenish-interval quotas,
/// optional idle TTL eviction, and custom key types via [`build_with_key`](Self::build_with_key).
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use tokio_rate_limit::RateLimiter;
///
/// let limiter = RateLimiter::builder()
///     .requests_per_second(100)
///     .burst(200)
///     .idle_ttl(Duration::from_secs(3600))
///     .build()
///     .unwrap();
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct RateLimiterBuilder {
    rate: Option<RateSpec>,
    rate_conflict: bool,
    burst: Option<u64>,
    idle_ttl: Option<Duration>,
}

impl RateLimiterBuilder {
    /// Creates a new builder with default (unset) values.
    pub fn new() -> Self {
        Self::default()
    }

    fn set_rate(mut self, spec: RateSpec) -> Self {
        if self.rate.is_some() {
            self.rate_conflict = true;
        }
        self.rate = Some(spec);
        self
    }

    /// Sets the sustained rate limit in requests per second.
    pub fn requests_per_second(self, rate: u64) -> Self {
        self.set_rate(RateSpec::PerSecond(rate))
    }

    /// Sets the sustained rate limit in requests per minute (supports sub-1-req/s quotas).
    ///
    /// # Examples
    ///
    /// ```
    /// use tokio_rate_limit::RateLimiter;
    ///
    /// // 10 requests per minute (1 token every 6 seconds), burst up to 3
    /// let limiter = RateLimiter::builder()
    ///     .per_minute(10)
    ///     .burst(3)
    ///     .build()
    ///     .unwrap();
    /// ```
    pub fn per_minute(self, requests_per_minute: u64) -> Self {
        self.set_rate(RateSpec::PerMinute(requests_per_minute))
    }

    /// Sets the sustained rate limit in requests per hour (supports low-rate quotas).
    ///
    /// # Examples
    ///
    /// ```
    /// use tokio_rate_limit::RateLimiter;
    ///
    /// // 100 requests per hour, burst up to 10
    /// let limiter = RateLimiter::builder()
    ///     .per_hour(100)
    ///     .burst(10)
    ///     .build()
    ///     .unwrap();
    /// ```
    pub fn per_hour(self, requests_per_hour: u64) -> Self {
        self.set_rate(RateSpec::PerHour(requests_per_hour))
    }

    /// Sets the exact duration required to replenish a single token.
    pub fn replenish_interval(self, interval: Duration) -> Self {
        self.set_rate(RateSpec::Interval(interval))
    }

    /// Sets the burst size (maximum tokens in the bucket).
    pub fn burst(mut self, burst: u64) -> Self {
        self.burst = Some(burst);
        self
    }

    /// Enables automatic TTL eviction for idle keys after `idle_ttl`.
    ///
    /// Keys are only evicted once both `idle_ttl` has elapsed and their quota
    /// has fully recovered, preventing eviction from acting as a quota bypass.
    pub fn idle_ttl(mut self, idle_ttl: Duration) -> Self {
        self.idle_ttl = Some(idle_ttl);
        self
    }

    /// Builds a `String`-keyed [`RateLimiter`] with the configured settings.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] if rate or burst is unset or zero.
    pub fn build(self) -> Result<RateLimiter<TokenBucket<String>>> {
        self.build_with_key::<String>()
    }

    /// Builds a [`RateLimiter`] keyed by an arbitrary [`RateLimitKey`] type `K`
    /// (such as [`std::net::IpAddr`] or `u64`).
    ///
    /// # Examples
    ///
    /// ```
    /// use std::net::{IpAddr, Ipv4Addr};
    /// use tokio_rate_limit::RateLimiter;
    ///
    /// let limiter = RateLimiter::builder()
    ///     .requests_per_second(50)
    ///     .burst(100)
    ///     .build_with_key::<IpAddr>()
    ///     .unwrap();
    ///
    /// let ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
    /// assert!(limiter.check(&ip).permitted);
    /// ```
    pub fn build_with_key<K: RateLimitKey>(self) -> Result<RateLimiter<TokenBucket<K>>> {
        if self.rate_conflict {
            return Err(Error::Config(
                "conflicting rate specifications: set only one of requests_per_second, per_minute, per_hour, or replenish_interval"
                    .to_string(),
            ));
        }

        let burst = self
            .burst
            .ok_or_else(|| Error::Config("burst must be set".to_string()))?;
        if burst == 0 {
            return Err(Error::Config("burst must be greater than 0".to_string()));
        }

        let rate_spec = self.rate.ok_or_else(|| {
            Error::Config(
                "rate must be set (via requests_per_second, per_minute, per_hour, or replenish_interval)"
                    .to_string(),
            )
        })?;

        let bucket = match (rate_spec, self.idle_ttl) {
            (RateSpec::PerSecond(0), _) => {
                return Err(Error::Config(
                    "requests_per_second must be greater than 0".to_string(),
                ));
            }
            (RateSpec::PerSecond(rps), None) => TokenBucket::new_with_key(burst, rps),
            (RateSpec::PerSecond(rps), Some(ttl)) => TokenBucket::with_ttl_and_key(burst, rps, ttl),
            (RateSpec::PerMinute(0), _) => {
                return Err(Error::Config(
                    "per_minute must be greater than 0".to_string(),
                ));
            }
            (RateSpec::PerMinute(rpm), ttl) => {
                let nanos = (60_000_000_000u128 / u128::from(rpm)).max(1) as u64;
                let interval = Duration::from_nanos(nanos);
                match ttl {
                    Some(ttl) => TokenBucket::with_period_and_ttl_and_key(burst, interval, ttl),
                    None => TokenBucket::with_period_and_key(burst, interval),
                }
            }
            (RateSpec::PerHour(0), _) => {
                return Err(Error::Config("per_hour must be greater than 0".to_string()));
            }
            (RateSpec::PerHour(rph), ttl) => {
                let nanos = (3_600_000_000_000u128 / u128::from(rph)).max(1) as u64;
                let interval = Duration::from_nanos(nanos);
                match ttl {
                    Some(ttl) => TokenBucket::with_period_and_ttl_and_key(burst, interval, ttl),
                    None => TokenBucket::with_period_and_key(burst, interval),
                }
            }
            (RateSpec::Interval(d), _) if d.is_zero() => {
                return Err(Error::Config(
                    "replenish_interval must be greater than zero".to_string(),
                ));
            }
            (RateSpec::Interval(interval), Some(ttl)) => {
                TokenBucket::with_period_and_ttl_and_key(burst, interval, ttl)
            }
            (RateSpec::Interval(interval), None) => {
                TokenBucket::with_period_and_key(burst, interval)
            }
        };

        Ok(RateLimiter::from_algorithm(bucket))
    }
}

/// Result of a rate limit check.
///
/// `check` consumes quota when it returns `permitted: true`. Ignoring the
/// decision is almost always a bug.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct RateLimitDecision {
    /// Whether the request is permitted.
    pub permitted: bool,

    /// How long to wait before retrying (if rate limited and fulfillable).
    ///
    /// `None` when `permitted == true`, when `cost == 0`, or when `cost > limit`
    /// (because waiting can never satisfy a cost larger than the bucket's capacity).
    pub retry_after: Option<Duration>,

    /// Number of remaining whole tokens in the bucket.
    pub remaining: Option<u64>,

    /// The configured bucket capacity / limit.
    pub limit: u64,

    /// Time until the bucket returns to full capacity (for IETF `RateLimit-Reset`).
    pub reset: Option<Duration>,
}

/// A cheaply-cloneable rate limiter that tracks requests per key and enforces quotas.
///
/// Internally wraps the [`Algorithm`] in an [`Arc`], so cloning a `RateLimiter` is an
/// inexpensive atomic reference-count increment (`Arc` wrapping at call sites is optional).
pub struct RateLimiter<A: Algorithm = TokenBucket> {
    algorithm: Arc<A>,
}

impl<A: Algorithm> Clone for RateLimiter<A> {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            algorithm: Arc::clone(&self.algorithm),
        }
    }
}

impl<A: Algorithm> fmt::Debug for RateLimiter<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RateLimiter")
            .field("algorithm", &*self.algorithm)
            .finish()
    }
}

impl<A: Algorithm> From<Arc<RateLimiter<A>>> for RateLimiter<A> {
    #[inline]
    fn from(limiter: Arc<RateLimiter<A>>) -> Self {
        (*limiter).clone()
    }
}

impl<A: Algorithm> From<&Arc<RateLimiter<A>>> for RateLimiter<A> {
    #[inline]
    fn from(limiter: &Arc<RateLimiter<A>>) -> Self {
        (**limiter).clone()
    }
}

impl<A: Algorithm> From<&RateLimiter<A>> for RateLimiter<A> {
    #[inline]
    fn from(limiter: &RateLimiter<A>) -> Self {
        limiter.clone()
    }
}

/// Conversion trait allowing middleware layers to accept either a [`RateLimiter`]
/// directly or an `Arc<RateLimiter>` without type-inference ambiguity.
pub trait IntoRateLimiter<A: Algorithm> {
    /// Converts `self` into a [`RateLimiter`].
    fn into_rate_limiter(self) -> RateLimiter<A>;
}

impl<A: Algorithm> IntoRateLimiter<A> for RateLimiter<A> {
    #[inline]
    fn into_rate_limiter(self) -> RateLimiter<A> {
        self
    }
}

impl<A: Algorithm> IntoRateLimiter<A> for &RateLimiter<A> {
    #[inline]
    fn into_rate_limiter(self) -> RateLimiter<A> {
        self.clone()
    }
}

impl<A: Algorithm> IntoRateLimiter<A> for Arc<RateLimiter<A>> {
    #[inline]
    fn into_rate_limiter(self) -> RateLimiter<A> {
        (*self).clone()
    }
}

impl<A: Algorithm> IntoRateLimiter<A> for &Arc<RateLimiter<A>> {
    #[inline]
    fn into_rate_limiter(self) -> RateLimiter<A> {
        (**self).clone()
    }
}

#[cfg(feature = "metrics-support")]
#[inline]
fn record_decision_metrics(decision: &RateLimitDecision) {
    if decision.permitted {
        metrics::counter!("tokio_rate_limit.requests.allowed").increment(1);
    } else {
        metrics::counter!("tokio_rate_limit.requests.denied").increment(1);
    }
    if let Some(remaining) = decision.remaining {
        metrics::histogram!("tokio_rate_limit.remaining_tokens").record(remaining as f64);
    }
}

impl RateLimiter<TokenBucket<String>> {
    /// Creates a new `String`-keyed token-bucket rate limiter with the given configuration.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] if `requests_per_second == 0` or `burst == 0`.
    ///
    /// # Examples
    ///
    /// ```
    /// use tokio_rate_limit::{RateLimiter, RateLimiterConfig};
    ///
    /// let limiter = RateLimiter::new(RateLimiterConfig {
    ///     requests_per_second: 100,
    ///     burst: 200,
    /// })
    /// .unwrap();
    /// ```
    pub fn new(config: RateLimiterConfig) -> Result<Self> {
        config.validate()?;
        Ok(Self::from_algorithm(TokenBucket::new(
            config.burst,
            config.requests_per_second,
        )))
    }

    /// Creates a new [`RateLimiterBuilder`] for configuring a token-bucket rate limiter.
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
}

impl<A: Algorithm> RateLimiter<A> {
    /// Creates a new rate limiter wrapping the given algorithm.
    ///
    /// # Examples
    ///
    /// ```
    /// use tokio_rate_limit::RateLimiter;
    /// use tokio_rate_limit::algorithm::LeakyBucket;
    ///
    /// let limiter = RateLimiter::from_algorithm(LeakyBucket::new(50, 100));
    /// assert!(limiter.check("client-123").permitted);
    /// ```
    pub fn from_algorithm(algorithm: A) -> Self {
        Self {
            algorithm: Arc::new(algorithm),
        }
    }

    /// Returns a shared reference to the underlying [`Algorithm`].
    #[inline]
    pub fn algorithm(&self) -> &A {
        &self.algorithm
    }

    /// Checks if a single-token (`cost = 1`) request for `key` should be permitted.
    ///
    /// # Examples
    ///
    /// ```
    /// use tokio_rate_limit::{RateLimiter, RateLimiterConfig};
    ///
    /// let limiter = RateLimiter::new(RateLimiterConfig {
    ///     requests_per_second: 100,
    ///     burst: 200,
    /// })
    /// .unwrap();
    ///
    /// let decision = limiter.check("client-123");
    /// assert!(decision.permitted);
    /// ```
    #[cfg_attr(feature = "observability", instrument(skip(self), fields(key = ?key)))]
    #[inline]
    pub fn check<Q>(&self, key: &Q) -> RateLimitDecision
    where
        A::Key: Borrow<Q>,
        Q: Hash + Ord + fmt::Debug + ToKey<A::Key> + ?Sized,
    {
        let decision = self.algorithm.check(key);

        #[cfg(feature = "metrics-support")]
        record_decision_metrics(&decision);

        #[cfg(feature = "observability")]
        {
            if decision.permitted {
                debug!(
                    remaining = decision.remaining,
                    limit = decision.limit,
                    "Rate limit check: PERMITTED"
                );
            } else {
                debug!(
                    remaining = decision.remaining,
                    limit = decision.limit,
                    retry_after_ms = decision.retry_after.map(|d| d.as_millis()),
                    "Rate limit check: DENIED"
                );
            }
        }

        decision
    }

    /// Checks if a request with the given `cost` should be permitted.
    ///
    /// - `cost == 0` is rejected without consuming quota (`retry_after: None`).
    /// - `cost > limit` is rejected without consuming quota (`retry_after: None`).
    ///
    /// # Examples
    ///
    /// ```
    /// use tokio_rate_limit::{RateLimiter, RateLimiterConfig};
    ///
    /// let limiter = RateLimiter::new(RateLimiterConfig {
    ///     requests_per_second: 100,
    ///     burst: 200,
    /// })
    /// .unwrap();
    ///
    /// assert!(limiter.check_with_cost("client-123", 10).permitted);
    /// ```
    #[cfg_attr(feature = "observability", instrument(skip(self), fields(key = ?key, cost = cost)))]
    #[inline]
    pub fn check_with_cost<Q>(&self, key: &Q, cost: u64) -> RateLimitDecision
    where
        A::Key: Borrow<Q>,
        Q: Hash + Ord + fmt::Debug + ToKey<A::Key> + ?Sized,
    {
        let decision = self.algorithm.check_with_cost(key, cost);

        #[cfg(feature = "metrics-support")]
        record_decision_metrics(&decision);

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
                debug!(
                    remaining = decision.remaining,
                    limit = decision.limit,
                    cost = cost,
                    retry_after_ms = decision.retry_after.map(|d| d.as_millis()),
                    "Rate limit check with cost: DENIED"
                );
            }
        }

        decision
    }

    /// Inspects the current rate limit status for `key` without consuming quota
    /// or allocating map state if `key` is not currently tracked.
    #[inline]
    pub fn peek<Q>(&self, key: &Q) -> RateLimitDecision
    where
        A::Key: Borrow<Q>,
        Q: Hash + Ord + ?Sized,
    {
        self.algorithm.peek(key)
    }

    /// Resets the rate limit state for `key`, restoring full quota.
    ///
    /// Returns `true` if `key` had tracked state that was removed.
    #[inline]
    pub fn reset<Q>(&self, key: &Q) -> bool
    where
        A::Key: Borrow<Q>,
        Q: Hash + Ord + ?Sized,
    {
        self.algorithm.reset(key)
    }

    /// Clears all tracked keys from the limiter.
    #[inline]
    pub fn clear(&self) {
        self.algorithm.clear();
    }

    /// Returns the approximate number of keys currently tracked by the limiter.
    #[inline]
    pub fn len(&self) -> usize {
        self.algorithm.len()
    }

    /// Returns `true` if no keys are currently tracked.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.algorithm.is_empty()
    }

    /// Immediately sweeps all shards for expired idle entries (when `idle_ttl` is set)
    /// and returns the number of evicted keys.
    #[inline]
    pub fn cleanup(&self) -> usize {
        self.algorithm.cleanup()
    }

    /// Non-blocking alias for [`check`](Self::check).
    #[inline]
    pub fn try_acquire<Q>(&self, key: &Q) -> RateLimitDecision
    where
        A::Key: Borrow<Q>,
        Q: Hash + Ord + fmt::Debug + ToKey<A::Key> + ?Sized,
    {
        self.check(key)
    }

    /// Non-blocking alias for [`check_with_cost`](Self::check_with_cost).
    #[inline]
    pub fn try_acquire_n<Q>(&self, key: &Q, cost: u64) -> RateLimitDecision
    where
        A::Key: Borrow<Q>,
        Q: Hash + Ord + fmt::Debug + ToKey<A::Key> + ?Sized,
    {
        self.check_with_cost(key, cost)
    }

    /// Acquires permission for a single token (`cost = 1`), waiting up to `timeout`.
    ///
    /// Returns immediately with `permitted == false` if the quota can never be
    /// satisfied (`retry_after.is_none()`).
    #[cfg_attr(feature = "observability", instrument(skip(self), fields(key = ?key, timeout_ms = timeout.as_millis())))]
    pub async fn acquire_timeout<Q>(&self, key: &Q, timeout: Duration) -> RateLimitDecision
    where
        A::Key: Borrow<Q>,
        Q: Hash + Ord + fmt::Debug + ToKey<A::Key> + Sync + ?Sized,
    {
        self.acquire_timeout_with_cost(key, 1, timeout).await
    }

    /// Acquires permission for `cost` tokens, waiting up to `timeout`.
    ///
    /// Returns immediately with `permitted == false` if `cost == 0` or `cost > limit`
    /// (`retry_after.is_none()`), avoiding futile sleep loops.
    #[cfg_attr(feature = "observability", instrument(skip(self), fields(key = ?key, cost = cost, timeout_ms = timeout.as_millis())))]
    pub async fn acquire_timeout_with_cost<Q>(
        &self,
        key: &Q,
        cost: u64,
        timeout: Duration,
    ) -> RateLimitDecision
    where
        A::Key: Borrow<Q>,
        Q: Hash + Ord + fmt::Debug + ToKey<A::Key> + Sync + ?Sized,
    {
        let start = tokio::time::Instant::now();

        loop {
            let decision = self.check_with_cost(key, cost);

            if decision.permitted {
                #[cfg(feature = "observability")]
                debug!("Acquired tokens after waiting");
                return decision;
            }

            let Some(retry_after) = decision.retry_after else {
                return decision;
            };

            let elapsed = start.elapsed();
            if elapsed >= timeout {
                #[cfg(feature = "observability")]
                info!("Timeout expired while waiting for tokens");
                return decision;
            }

            let sleep_time = retry_after.min(timeout - elapsed);
            tokio::time::sleep(sleep_time).await;
        }
    }

    /// Acquires permission for a single token (`cost = 1`), waiting until available.
    ///
    /// If the bucket cannot ever satisfy the request (for example `limit == 0`),
    /// returns immediately with `permitted: false` rather than hanging forever.
    #[cfg_attr(feature = "observability", instrument(skip(self), fields(key = ?key)))]
    pub async fn acquire<Q>(&self, key: &Q) -> RateLimitDecision
    where
        A::Key: Borrow<Q>,
        Q: Hash + Ord + fmt::Debug + ToKey<A::Key> + Sync + ?Sized,
    {
        self.acquire_with_cost(key, 1).await
    }

    /// Acquires permission for `cost` tokens, waiting until available.
    ///
    /// If `cost == 0` or `cost > limit` (`retry_after.is_none()`), returns immediately
    /// with `permitted: false` rather than hanging forever.
    #[cfg_attr(feature = "observability", instrument(skip(self), fields(key = ?key, cost = cost)))]
    pub async fn acquire_with_cost<Q>(&self, key: &Q, cost: u64) -> RateLimitDecision
    where
        A::Key: Borrow<Q>,
        Q: Hash + Ord + fmt::Debug + ToKey<A::Key> + Sync + ?Sized,
    {
        loop {
            let decision = self.check_with_cost(key, cost);

            if decision.permitted {
                #[cfg(feature = "observability")]
                debug!("Acquired tokens");
                return decision;
            }

            let Some(sleep_time) = decision.retry_after else {
                return decision;
            };

            #[cfg(feature = "observability")]
            debug!(
                sleep_ms = sleep_time.as_millis(),
                "Waiting for tokens to refill"
            );

            tokio::time::sleep(sleep_time).await;
        }
    }
}
