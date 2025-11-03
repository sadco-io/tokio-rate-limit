//! Axum middleware integration for rate limiting.
//!
//! This module provides middleware for integrating the rate limiter with Axum web applications.
//!
//! # Example
//!
//! ```no_run
//! use axum::{Router, routing::get};
//! use tokio_rate_limit::{RateLimiter, middleware::RateLimitLayer};
//! use std::sync::Arc;
//!
//! # async fn example() {
//! let limiter = Arc::new(RateLimiter::builder()
//!     .requests_per_second(100)
//!     .burst(200)
//!     .build()
//!     .unwrap());
//!
//! let app: Router = Router::new()
//!     .route("/", get(|| async { "Hello, World!" }))
//!     .layer(RateLimitLayer::new(limiter));
//! # }
//! ```

use crate::{RateLimitDecision, RateLimiter};
use axum::{
    body::Body,
    extract::ConnectInfo,
    http::{Request, Response, StatusCode},
    response::IntoResponse,
};
use std::net::SocketAddr;
use std::sync::Arc;
use std::task::{Context, Poll};
use tower::{Layer, Service};

/// A trait for extracting rate limit keys from requests.
///
/// Implement this trait to customize how rate limit keys are extracted
/// from incoming requests. The default implementation uses the client's IP address.
pub trait KeyExtractor: Send + Sync + 'static {
    /// Extracts a rate limit key from the request.
    ///
    /// # Arguments
    ///
    /// * `req` - The incoming HTTP request
    ///
    /// # Returns
    ///
    /// A string key to use for rate limiting, or None if the request should not be rate limited.
    fn extract(&self, req: &Request<Body>) -> Option<String>;
}

/// Default key extractor that uses the client's IP address.
#[derive(Clone, Default)]
pub struct IpKeyExtractor;

impl KeyExtractor for IpKeyExtractor {
    fn extract(&self, req: &Request<Body>) -> Option<String> {
        req.extensions()
            .get::<ConnectInfo<SocketAddr>>()
            .map(|ConnectInfo(addr)| addr.ip().to_string())
    }
}

/// A custom key extractor that uses a closure.
///
/// # Example
///
/// ```no_run
/// use tokio_rate_limit::middleware::CustomKeyExtractor;
/// use axum::http::Request;
/// use axum::body::Body;
///
/// let extractor = CustomKeyExtractor::new(|req: &Request<Body>| {
///     // Extract user ID from a header
///     req.headers()
///         .get("X-User-Id")
///         .and_then(|v| v.to_str().ok())
///         .map(|s| s.to_string())
/// });
/// ```
#[derive(Clone)]
pub struct CustomKeyExtractor<F>
where
    F: Fn(&Request<Body>) -> Option<String> + Send + Sync + Clone + 'static,
{
    extractor: F,
}

impl<F> CustomKeyExtractor<F>
where
    F: Fn(&Request<Body>) -> Option<String> + Send + Sync + Clone + 'static,
{
    /// Creates a new custom key extractor with the given function.
    pub fn new(extractor: F) -> Self {
        Self { extractor }
    }
}

impl<F> KeyExtractor for CustomKeyExtractor<F>
where
    F: Fn(&Request<Body>) -> Option<String> + Send + Sync + Clone + 'static,
{
    fn extract(&self, req: &Request<Body>) -> Option<String> {
        (self.extractor)(req)
    }
}

/// Tower layer for adding rate limiting to an Axum application.
///
/// # Example
///
/// ```no_run
/// use axum::{Router, routing::get};
/// use tokio_rate_limit::{RateLimiter, middleware::RateLimitLayer};
/// use std::sync::Arc;
///
/// # async fn example() {
/// let limiter = Arc::new(RateLimiter::builder()
///     .requests_per_second(100)
///     .burst(200)
///     .build()
///     .unwrap());
///
/// let app: Router = Router::new()
///     .route("/", get(|| async { "Hello, World!" }))
///     .layer(RateLimitLayer::new(limiter));
/// # }
/// ```
#[derive(Clone)]
pub struct RateLimitLayer<E = IpKeyExtractor>
where
    E: KeyExtractor,
{
    limiter: Arc<RateLimiter>,
    extractor: E,
}

impl RateLimitLayer<IpKeyExtractor> {
    /// Creates a new rate limit layer with the default IP-based key extraction.
    pub fn new(limiter: Arc<RateLimiter>) -> Self {
        Self {
            limiter,
            extractor: IpKeyExtractor,
        }
    }
}

impl<E> RateLimitLayer<E>
where
    E: KeyExtractor,
{
    /// Creates a new rate limit layer with a custom key extractor.
    pub fn with_extractor(limiter: Arc<RateLimiter>, extractor: E) -> Self {
        Self { limiter, extractor }
    }
}

impl<S, E> Layer<S> for RateLimitLayer<E>
where
    E: KeyExtractor + Clone,
{
    type Service = RateLimitService<S, E>;

    fn layer(&self, inner: S) -> Self::Service {
        RateLimitService {
            inner,
            limiter: self.limiter.clone(),
            extractor: self.extractor.clone(),
        }
    }
}

/// The rate limiting middleware service.
#[derive(Clone)]
pub struct RateLimitService<S, E = IpKeyExtractor>
where
    E: KeyExtractor,
{
    inner: S,
    limiter: Arc<RateLimiter>,
    extractor: E,
}

impl<S, E> Service<Request<Body>> for RateLimitService<S, E>
where
    S: Service<Request<Body>, Response = Response<Body>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    E: KeyExtractor + Clone,
{
    type Response = Response<Body>;
    type Error = S::Error;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        let limiter = self.limiter.clone();
        let extractor = self.extractor.clone();
        let mut inner = self.inner.clone();

        Box::pin(async move {
            // Extract the rate limit key
            let key = match extractor.extract(&req) {
                Some(k) => k,
                None => {
                    // No key available, allow the request
                    return inner.call(req).await;
                }
            };

            // Check rate limit
            let decision = match limiter.check(&key).await {
                Ok(d) => d,
                Err(_) => {
                    // Error checking rate limit, allow the request to be safe
                    return inner.call(req).await;
                }
            };

            if decision.permitted {
                // Request is allowed, add rate limit headers and pass through
                #[cfg(feature = "metrics-support")]
                {
                    metrics::counter!("tokio_rate_limit.requests.allowed").increment(1);
                    if let Some(remaining) = decision.remaining {
                        metrics::histogram!("tokio_rate_limit.remaining_tokens").record(remaining as f64);
                    }
                }

                let mut response = inner.call(req).await?;
                add_rate_limit_headers(&mut response, &decision);
                Ok(response)
            } else {
                // Request is rate limited
                #[cfg(feature = "metrics-support")]
                {
                    metrics::counter!("tokio_rate_limit.requests.denied").increment(1);
                    if let Some(remaining) = decision.remaining {
                        metrics::histogram!("tokio_rate_limit.remaining_tokens").record(remaining as f64);
                    }
                }

                Ok(rate_limit_response(&decision))
            }
        })
    }
}

/// Adds rate limit headers to a response.
///
/// This function adds both IETF standard headers and legacy X-RateLimit-* headers
/// for backward compatibility:
///
/// IETF Standard Headers (draft-ietf-httpapi-ratelimit-headers):
/// - RateLimit-Limit: Maximum requests allowed
/// - RateLimit-Remaining: Requests remaining in current window
/// - RateLimit-Reset: Seconds until bucket is full
///
/// Legacy Headers (backward compatibility):
/// - X-RateLimit-Limit: Maximum requests allowed
/// - X-RateLimit-Remaining: Requests remaining in current window
fn add_rate_limit_headers(response: &mut Response<Body>, decision: &RateLimitDecision) {
    let headers = response.headers_mut();

    // IETF Standard Headers
    headers.insert(
        "RateLimit-Limit",
        decision.limit.to_string().parse().unwrap(),
    );

    if let Some(remaining) = decision.remaining {
        headers.insert(
            "RateLimit-Remaining",
            remaining.to_string().parse().unwrap(),
        );
    }

    if let Some(reset) = decision.reset {
        let reset_seconds = reset.as_secs();
        headers.insert(
            "RateLimit-Reset",
            reset_seconds.to_string().parse().unwrap(),
        );
    }

    // Legacy X-RateLimit-* headers for backward compatibility
    headers.insert(
        "X-RateLimit-Limit",
        decision.limit.to_string().parse().unwrap(),
    );

    if let Some(remaining) = decision.remaining {
        headers.insert(
            "X-RateLimit-Remaining",
            remaining.to_string().parse().unwrap(),
        );
    }
}

/// Creates a 429 Too Many Requests response with rate limit headers.
///
/// This function adds both IETF standard headers and legacy headers:
///
/// IETF Standard Headers:
/// - RateLimit-Limit: Maximum requests allowed
/// - RateLimit-Remaining: Requests remaining (typically 0)
/// - RateLimit-Reset: Seconds until bucket is full
///
/// Legacy Headers:
/// - X-RateLimit-Limit: Maximum requests allowed
/// - X-RateLimit-Remaining: Requests remaining (typically 0)
/// - Retry-After: Seconds to wait before retrying
fn rate_limit_response(decision: &RateLimitDecision) -> Response<Body> {
    let mut response = (StatusCode::TOO_MANY_REQUESTS, "Rate limit exceeded").into_response();

    let headers = response.headers_mut();

    // IETF Standard Headers
    headers.insert(
        "RateLimit-Limit",
        decision.limit.to_string().parse().unwrap(),
    );

    if let Some(remaining) = decision.remaining {
        headers.insert(
            "RateLimit-Remaining",
            remaining.to_string().parse().unwrap(),
        );
    }

    if let Some(reset) = decision.reset {
        let reset_seconds = reset.as_secs();
        headers.insert(
            "RateLimit-Reset",
            reset_seconds.to_string().parse().unwrap(),
        );
    }

    // Legacy X-RateLimit-* headers for backward compatibility
    headers.insert(
        "X-RateLimit-Limit",
        decision.limit.to_string().parse().unwrap(),
    );

    if let Some(remaining) = decision.remaining {
        headers.insert(
            "X-RateLimit-Remaining",
            remaining.to_string().parse().unwrap(),
        );
    }

    // Add Retry-After header (legacy, but still widely used)
    if let Some(retry_after) = decision.retry_after {
        let seconds = retry_after.as_secs();
        headers.insert("Retry-After", seconds.to_string().parse().unwrap());
    }

    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Body,
        http::{Request, StatusCode},
        routing::get,
        Router,
    };
    use tower::ServiceExt;

    #[tokio::test]
    async fn test_rate_limit_allows_within_limit() {
        let limiter = Arc::new(
            RateLimiter::builder()
                .requests_per_second(10)
                .burst(10)
                .build()
                .unwrap(),
        );

        let app = Router::new().route("/", get(|| async { "Hello" })).layer(
            RateLimitLayer::with_extractor(
                limiter,
                CustomKeyExtractor::new(|_| Some("test-key".to_string())),
            ),
        );

        // First request should succeed
        let response = app
            .clone()
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.headers().contains_key("X-RateLimit-Limit"));
        assert!(response.headers().contains_key("X-RateLimit-Remaining"));
    }

    #[tokio::test]
    async fn test_rate_limit_blocks_over_limit() {
        let limiter = Arc::new(
            RateLimiter::builder()
                .requests_per_second(2)
                .burst(2)
                .build()
                .unwrap(),
        );

        let app = Router::new().route("/", get(|| async { "Hello" })).layer(
            RateLimitLayer::with_extractor(
                limiter,
                CustomKeyExtractor::new(|_| Some("test-key".to_string())),
            ),
        );

        // First two requests should succeed
        for _ in 0..2 {
            let response = app
                .clone()
                .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
        }

        // Third request should be rate limited
        let response = app
            .clone()
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert!(response.headers().contains_key("X-RateLimit-Limit"));
        assert!(response.headers().contains_key("Retry-After"));
    }

    #[tokio::test]
    async fn test_custom_key_extractor() {
        let limiter = Arc::new(
            RateLimiter::builder()
                .requests_per_second(1)
                .burst(1)
                .build()
                .unwrap(),
        );

        let app = Router::new().route("/", get(|| async { "Hello" })).layer(
            RateLimitLayer::with_extractor(
                limiter,
                CustomKeyExtractor::new(|req| {
                    req.headers()
                        .get("X-User-Id")
                        .and_then(|v| v.to_str().ok())
                        .map(|s| s.to_string())
                }),
            ),
        );

        // Request with user1 should succeed
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/")
                    .header("X-User-Id", "user1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Request with user2 should also succeed (different key)
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/")
                    .header("X-User-Id", "user2")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Second request with user1 should be rate limited
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/")
                    .header("X-User-Id", "user1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    }
}
