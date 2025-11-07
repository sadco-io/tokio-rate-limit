//! Tonic middleware integration for gRPC rate limiting.
//!
//! This module provides Tower middleware for integrating the rate limiter with Tonic gRPC services.
//! Unlike HTTP interceptors, this implementation uses Tower's Layer and Service traits to provide
//! comprehensive rate limiting with access to both requests and responses.
//!
//! # Example
//!
//! ```ignore
//! use tonic::transport::Server;
//! use tokio_rate_limit::{RateLimiter, tonic_middleware::GrpcRateLimitLayer};
//! use std::sync::Arc;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let limiter = Arc::new(RateLimiter::builder()
//!     .requests_per_second(100)
//!     .burst(200)
//!     .build()?);
//!
//! let layer = GrpcRateLimitLayer::new(limiter);
//!
//! // Apply to your gRPC server
//! Server::builder()
//!     .layer(layer)
//!     .add_service(your_service)  // Add your gRPC service
//!     .serve("[::1]:50051".parse()?)
//!     .await?;
//! # Ok(())
//! # }
//! ```

use crate::{RateLimitDecision, RateLimiter};
use std::sync::Arc;
use std::task::{Context, Poll};
use tonic::body::BoxBody;
use tonic::{Code, Status};
use tower::{Layer, Service};

/// A trait for extracting rate limit keys from gRPC requests.
///
/// Implement this trait to customize how rate limit keys are extracted
/// from incoming gRPC requests. The default implementation uses the method path.
pub trait GrpcKeyExtractor: Send + Sync + 'static {
    /// Extracts a rate limit key from the gRPC request.
    ///
    /// # Arguments
    ///
    /// * `req` - The incoming HTTP/2 request (gRPC uses HTTP/2)
    ///
    /// # Returns
    ///
    /// A string key to use for rate limiting, or None if the request should not be rate limited.
    fn extract(&self, req: &http::Request<BoxBody>) -> Option<String>;
}

/// Default key extractor that uses the gRPC method path.
///
/// This allows per-method rate limiting, where different RPC methods
/// can have different rate limits.
///
/// # Example
///
/// For a request to `/helloworld.Greeter/SayHello`, the key will be:
/// `helloworld.Greeter/SayHello`
#[derive(Clone, Default)]
pub struct MethodKeyExtractor;

impl GrpcKeyExtractor for MethodKeyExtractor {
    fn extract(&self, req: &http::Request<BoxBody>) -> Option<String> {
        // Extract the gRPC method path from the URI
        // Format: /{package}.{service}/{method}
        let path = req.uri().path();
        if let Some(stripped) = path.strip_prefix('/') {
            Some(stripped.to_string())
        } else {
            Some(path.to_string())
        }
    }
}

/// Key extractor that uses client IP address for rate limiting.
///
/// This limits requests per client IP, regardless of which method is called.
#[derive(Clone, Default)]
pub struct IpKeyExtractor;

impl GrpcKeyExtractor for IpKeyExtractor {
    fn extract(&self, req: &http::Request<BoxBody>) -> Option<String> {
        // In a real implementation, you'd extract this from connection info
        // For now, we check headers that might contain the client IP
        req.headers()
            .get("x-forwarded-for")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.split(',').next().unwrap_or(s).trim().to_string())
            .or_else(|| {
                req.headers()
                    .get("x-real-ip")
                    .and_then(|v| v.to_str().ok())
                    .map(|s| s.to_string())
            })
    }
}

/// Key extractor that uses metadata from the gRPC request.
///
/// This allows extracting custom identifiers from gRPC metadata headers,
/// such as user IDs, API keys, or tenant IDs.
///
/// # Example
///
/// ```no_run
/// use tokio_rate_limit::tonic_middleware::MetadataKeyExtractor;
///
/// // Extract rate limit key from "user-id" metadata
/// let extractor = MetadataKeyExtractor::new("user-id");
/// ```
#[derive(Clone)]
pub struct MetadataKeyExtractor {
    header_name: String,
}

impl MetadataKeyExtractor {
    /// Creates a new metadata key extractor.
    ///
    /// # Arguments
    ///
    /// * `header_name` - The metadata header name to extract (e.g., "user-id", "x-api-key")
    pub fn new(header_name: impl Into<String>) -> Self {
        Self {
            header_name: header_name.into(),
        }
    }
}

impl GrpcKeyExtractor for MetadataKeyExtractor {
    fn extract(&self, req: &http::Request<BoxBody>) -> Option<String> {
        req.headers()
            .get(&self.header_name)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string())
    }
}

/// A custom key extractor that uses a closure.
///
/// # Example
///
/// ```no_run
/// use tokio_rate_limit::tonic_middleware::CustomGrpcKeyExtractor;
///
/// let extractor = CustomGrpcKeyExtractor::new(|req| {
///     // Combine method path and user ID
///     let method = req.uri().path().trim_start_matches('/');
///     let user = req.headers()
///         .get("user-id")
///         .and_then(|v| v.to_str().ok())?;
///     Some(format!("{}:{}", method, user))
/// });
/// ```
#[derive(Clone)]
pub struct CustomGrpcKeyExtractor<F>
where
    F: Fn(&http::Request<BoxBody>) -> Option<String> + Send + Sync + Clone + 'static,
{
    extractor: F,
}

impl<F> CustomGrpcKeyExtractor<F>
where
    F: Fn(&http::Request<BoxBody>) -> Option<String> + Send + Sync + Clone + 'static,
{
    /// Creates a new custom key extractor with the given function.
    pub fn new(extractor: F) -> Self {
        Self { extractor }
    }
}

impl<F> GrpcKeyExtractor for CustomGrpcKeyExtractor<F>
where
    F: Fn(&http::Request<BoxBody>) -> Option<String> + Send + Sync + Clone + 'static,
{
    fn extract(&self, req: &http::Request<BoxBody>) -> Option<String> {
        (self.extractor)(req)
    }
}

/// Tower layer for adding rate limiting to a Tonic gRPC service.
///
/// This layer wraps gRPC services with rate limiting middleware. It uses Tower's
/// Layer trait, making it composable with other middleware.
///
/// # Example
///
/// ```ignore
/// use tonic::transport::Server;
/// use tokio_rate_limit::{RateLimiter, tonic_middleware::GrpcRateLimitLayer};
/// use std::sync::Arc;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let limiter = Arc::new(RateLimiter::builder()
///     .requests_per_second(100)
///     .burst(200)
///     .build()?);
///
/// Server::builder()
///     .layer(GrpcRateLimitLayer::new(limiter))
///     .add_service(your_service)  // Add your gRPC service
///     .serve("[::1]:50051".parse()?)
///     .await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct GrpcRateLimitLayer<E = MethodKeyExtractor>
where
    E: GrpcKeyExtractor,
{
    limiter: Arc<RateLimiter>,
    extractor: E,
}

impl GrpcRateLimitLayer<MethodKeyExtractor> {
    /// Creates a new gRPC rate limit layer with the default method-based key extraction.
    pub fn new(limiter: Arc<RateLimiter>) -> Self {
        Self {
            limiter,
            extractor: MethodKeyExtractor,
        }
    }
}

impl<E> GrpcRateLimitLayer<E>
where
    E: GrpcKeyExtractor,
{
    /// Creates a new gRPC rate limit layer with a custom key extractor.
    pub fn with_extractor(limiter: Arc<RateLimiter>, extractor: E) -> Self {
        Self { limiter, extractor }
    }
}

impl<S, E> Layer<S> for GrpcRateLimitLayer<E>
where
    E: GrpcKeyExtractor + Clone,
{
    type Service = GrpcRateLimitService<S, E>;

    fn layer(&self, inner: S) -> Self::Service {
        GrpcRateLimitService {
            inner,
            limiter: self.limiter.clone(),
            extractor: self.extractor.clone(),
        }
    }
}

/// The gRPC rate limiting middleware service.
#[derive(Clone)]
pub struct GrpcRateLimitService<S, E = MethodKeyExtractor>
where
    E: GrpcKeyExtractor,
{
    inner: S,
    limiter: Arc<RateLimiter>,
    extractor: E,
}

impl<S, E> Service<http::Request<BoxBody>> for GrpcRateLimitService<S, E>
where
    S: Service<http::Request<BoxBody>, Response = http::Response<BoxBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<Box<dyn std::error::Error + Send + Sync>> + 'static,
    E: GrpcKeyExtractor + Clone,
{
    type Response = http::Response<BoxBody>;
    type Error = Box<dyn std::error::Error + Send + Sync>;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, req: http::Request<BoxBody>) -> Self::Future {
        let limiter = self.limiter.clone();
        let extractor = self.extractor.clone();
        let mut inner = self.inner.clone();

        Box::pin(async move {
            // Extract the rate limit key
            let key = match extractor.extract(&req) {
                Some(k) => k,
                None => {
                    // No key available, allow the request
                    return inner.call(req).await.map_err(Into::into);
                }
            };

            // Check rate limit
            let decision = match limiter.check(&key).await {
                Ok(d) => d,
                Err(_) => {
                    // Error checking rate limit, allow the request to be safe
                    return inner.call(req).await.map_err(Into::into);
                }
            };

            if decision.permitted {
                // Request is allowed, add rate limit metadata and pass through
                #[cfg(feature = "metrics-support")]
                {
                    metrics::counter!("tokio_rate_limit.grpc.requests.allowed").increment(1);
                    if let Some(remaining) = decision.remaining {
                        metrics::histogram!("tokio_rate_limit.grpc.remaining_tokens")
                            .record(remaining as f64);
                    }
                }

                let response = inner.call(req).await.map_err(Into::into)?;
                Ok(add_rate_limit_trailer(response, &decision))
            } else {
                // Request is rate limited
                #[cfg(feature = "metrics-support")]
                {
                    metrics::counter!("tokio_rate_limit.grpc.requests.denied").increment(1);
                    if let Some(remaining) = decision.remaining {
                        metrics::histogram!("tokio_rate_limit.grpc.remaining_tokens")
                            .record(remaining as f64);
                    }
                }

                Ok(rate_limit_error_response(&decision))
            }
        })
    }
}

/// Adds rate limit information to the response as gRPC trailers.
///
/// gRPC uses HTTP/2 trailers to send metadata after the response body.
/// This function adds rate limit information that clients can inspect.
fn add_rate_limit_trailer(
    mut response: http::Response<BoxBody>,
    decision: &RateLimitDecision,
) -> http::Response<BoxBody> {
    let headers = response.headers_mut();

    // Add rate limit information as headers (will become trailers in gRPC)
    headers.insert(
        "x-ratelimit-limit",
        decision.limit.to_string().parse().unwrap(),
    );

    if let Some(remaining) = decision.remaining {
        headers.insert(
            "x-ratelimit-remaining",
            remaining.to_string().parse().unwrap(),
        );
    }

    if let Some(reset) = decision.reset {
        headers.insert(
            "x-ratelimit-reset",
            reset.as_secs().to_string().parse().unwrap(),
        );
    }

    response
}

/// Creates a gRPC error response with RESOURCE_EXHAUSTED status.
///
/// According to gRPC specifications, RESOURCE_EXHAUSTED (code 8) is the
/// appropriate status code for rate limiting. This indicates that some
/// resource has been exhausted (in this case, the rate limit quota).
///
/// The response includes:
/// - Status: RESOURCE_EXHAUSTED
/// - Message: "Rate limit exceeded"
/// - Metadata: Rate limit information (limit, remaining, retry-after)
fn rate_limit_error_response(decision: &RateLimitDecision) -> http::Response<BoxBody> {
    let mut status = Status::resource_exhausted("Rate limit exceeded");

    // Add rate limit metadata to the status
    let metadata = status.metadata_mut();

    if let Ok(value) = decision.limit.to_string().parse() {
        metadata.insert("x-ratelimit-limit", value);
    }

    if let Some(remaining) = decision.remaining {
        if let Ok(value) = remaining.to_string().parse() {
            metadata.insert("x-ratelimit-remaining", value);
        }
    }

    if let Some(retry_after) = decision.retry_after {
        if let Ok(value) = retry_after.as_secs().to_string().parse() {
            metadata.insert("retry-after", value);
        }
    }

    if let Some(reset) = decision.reset {
        if let Ok(value) = reset.as_secs().to_string().parse() {
            metadata.insert("x-ratelimit-reset", value);
        }
    }

    status.to_http()
}

/// Helper function to convert a tonic::Code to an http::StatusCode.
///
/// This is used internally by the error response generation.
trait StatusExt {
    fn to_http(self) -> http::Response<BoxBody>;
}

impl StatusExt for Status {
    fn to_http(self) -> http::Response<BoxBody> {
        let mut response = http::Response::new(BoxBody::default());
        *response.status_mut() = self.code().to_http_status();

        // Add gRPC status headers
        response.headers_mut().insert(
            "grpc-status",
            (self.code() as i32).to_string().parse().unwrap(),
        );

        if let Ok(value) = self.message().parse() {
            response.headers_mut().insert("grpc-message", value);
        }

        // Copy metadata to response headers
        // Note: tonic metadata uses a different type system than http headers
        // We need to convert them appropriately
        for key_and_value in self.metadata().iter() {
            match key_and_value {
                tonic::metadata::KeyAndValueRef::Ascii(key, value) => {
                    let Ok(header_name) = http::header::HeaderName::from_bytes(key.as_ref()) else {
                        continue;
                    };
                    let Ok(header_value) = http::header::HeaderValue::from_bytes(value.as_ref())
                    else {
                        continue;
                    };
                    let entry = response.headers_mut().entry(header_name);
                    if let http::header::Entry::Vacant(e) = entry {
                        e.insert(header_value);
                    }
                }
                tonic::metadata::KeyAndValueRef::Binary(key, value) => {
                    let Ok(header_name) = http::header::HeaderName::from_bytes(key.as_ref()) else {
                        continue;
                    };
                    let Ok(header_value) = http::header::HeaderValue::from_bytes(value.as_ref())
                    else {
                        continue;
                    };
                    let entry = response.headers_mut().entry(header_name);
                    if let http::header::Entry::Vacant(e) = entry {
                        e.insert(header_value);
                    }
                }
            }
        }

        response
    }
}

trait CodeExt {
    fn to_http_status(&self) -> http::StatusCode;
}

impl CodeExt for Code {
    fn to_http_status(&self) -> http::StatusCode {
        match self {
            Code::Ok => http::StatusCode::OK,
            Code::Cancelled => http::StatusCode::from_u16(499).unwrap(), // Client Closed Request
            Code::Unknown => http::StatusCode::INTERNAL_SERVER_ERROR,
            Code::InvalidArgument => http::StatusCode::BAD_REQUEST,
            Code::DeadlineExceeded => http::StatusCode::GATEWAY_TIMEOUT,
            Code::NotFound => http::StatusCode::NOT_FOUND,
            Code::AlreadyExists => http::StatusCode::CONFLICT,
            Code::PermissionDenied => http::StatusCode::FORBIDDEN,
            Code::ResourceExhausted => http::StatusCode::TOO_MANY_REQUESTS,
            Code::FailedPrecondition => http::StatusCode::PRECONDITION_FAILED,
            Code::Aborted => http::StatusCode::CONFLICT,
            Code::OutOfRange => http::StatusCode::BAD_REQUEST,
            Code::Unimplemented => http::StatusCode::NOT_IMPLEMENTED,
            Code::Internal => http::StatusCode::INTERNAL_SERVER_ERROR,
            Code::Unavailable => http::StatusCode::SERVICE_UNAVAILABLE,
            Code::DataLoss => http::StatusCode::INTERNAL_SERVER_ERROR,
            Code::Unauthenticated => http::StatusCode::UNAUTHORIZED,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    #[test]
    fn test_method_key_extractor() {
        let extractor = MethodKeyExtractor;
        let req = http::Request::builder()
            .uri("http://example.com/helloworld.Greeter/SayHello")
            .body(BoxBody::default())
            .unwrap();

        let key = extractor.extract(&req);
        assert_eq!(key, Some("helloworld.Greeter/SayHello".to_string()));
    }

    #[test]
    fn test_method_key_extractor_no_leading_slash() {
        let extractor = MethodKeyExtractor;
        let req = http::Request::builder()
            .uri("http://example.com/test.Service/Method")
            .body(BoxBody::default())
            .unwrap();

        let key = extractor.extract(&req);
        assert_eq!(key, Some("test.Service/Method".to_string()));
    }

    #[test]
    fn test_metadata_key_extractor() {
        let extractor = MetadataKeyExtractor::new("user-id");
        let req = http::Request::builder()
            .uri("http://example.com/test")
            .header("user-id", "user-123")
            .body(BoxBody::default())
            .unwrap();

        let key = extractor.extract(&req);
        assert_eq!(key, Some("user-123".to_string()));
    }

    #[test]
    fn test_metadata_key_extractor_missing_header() {
        let extractor = MetadataKeyExtractor::new("user-id");
        let req = http::Request::builder()
            .uri("http://example.com/test")
            .body(BoxBody::default())
            .unwrap();

        let key = extractor.extract(&req);
        assert_eq!(key, None);
    }

    #[test]
    fn test_ip_key_extractor() {
        let extractor = IpKeyExtractor;
        let req = http::Request::builder()
            .uri("http://example.com/test")
            .header("x-forwarded-for", "192.168.1.1, 10.0.0.1")
            .body(BoxBody::default())
            .unwrap();

        let key = extractor.extract(&req);
        assert_eq!(key, Some("192.168.1.1".to_string()));
    }

    #[test]
    fn test_ip_key_extractor_single_ip() {
        let extractor = IpKeyExtractor;
        let req = http::Request::builder()
            .uri("http://example.com/test")
            .header("x-forwarded-for", "192.168.1.1")
            .body(BoxBody::default())
            .unwrap();

        let key = extractor.extract(&req);
        assert_eq!(key, Some("192.168.1.1".to_string()));
    }

    #[test]
    fn test_ip_key_extractor_x_real_ip() {
        let extractor = IpKeyExtractor;
        let req = http::Request::builder()
            .uri("http://example.com/test")
            .header("x-real-ip", "10.0.0.1")
            .body(BoxBody::default())
            .unwrap();

        let key = extractor.extract(&req);
        assert_eq!(key, Some("10.0.0.1".to_string()));
    }

    #[test]
    fn test_ip_key_extractor_no_headers() {
        let extractor = IpKeyExtractor;
        let req = http::Request::builder()
            .uri("http://example.com/test")
            .body(BoxBody::default())
            .unwrap();

        let key = extractor.extract(&req);
        assert_eq!(key, None);
    }

    #[test]
    fn test_custom_key_extractor() {
        let extractor = CustomGrpcKeyExtractor::new(|req| {
            let method = req.uri().path().trim_start_matches('/');
            Some(format!("custom:{}", method))
        });

        let req = http::Request::builder()
            .uri("http://example.com/test.Service/Method")
            .body(BoxBody::default())
            .unwrap();

        let key = extractor.extract(&req);
        assert_eq!(key, Some("custom:test.Service/Method".to_string()));
    }

    #[test]
    fn test_custom_key_extractor_returns_none() {
        let extractor = CustomGrpcKeyExtractor::new(|_req| None);

        let req = http::Request::builder()
            .uri("http://example.com/test")
            .body(BoxBody::default())
            .unwrap();

        let key = extractor.extract(&req);
        assert_eq!(key, None);
    }

    #[test]
    fn test_code_to_http_status() {
        use http::StatusCode;

        assert_eq!(Code::Ok.to_http_status(), StatusCode::OK);
        assert_eq!(
            Code::ResourceExhausted.to_http_status(),
            StatusCode::TOO_MANY_REQUESTS
        );
        assert_eq!(Code::NotFound.to_http_status(), StatusCode::NOT_FOUND);
        assert_eq!(
            Code::InvalidArgument.to_http_status(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            Code::Unauthenticated.to_http_status(),
            StatusCode::UNAUTHORIZED
        );
    }

    #[test]
    fn test_rate_limit_error_response() {
        let decision = crate::RateLimitDecision {
            permitted: false,
            limit: 100,
            remaining: Some(0),
            reset: Some(std::time::Duration::from_secs(60)),
            retry_after: Some(std::time::Duration::from_secs(1)),
        };

        let response = rate_limit_error_response(&decision);

        // Check status code
        assert_eq!(response.status(), http::StatusCode::TOO_MANY_REQUESTS);

        // Check grpc-status header
        let grpc_status = response
            .headers()
            .get("grpc-status")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<i32>().ok());
        assert_eq!(grpc_status, Some(Code::ResourceExhausted as i32));

        // Check rate limit headers
        assert_eq!(
            response
                .headers()
                .get("x-ratelimit-limit")
                .and_then(|v| v.to_str().ok()),
            Some("100")
        );
        assert_eq!(
            response
                .headers()
                .get("x-ratelimit-remaining")
                .and_then(|v| v.to_str().ok()),
            Some("0")
        );
        assert_eq!(
            response
                .headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok()),
            Some("1")
        );
    }

    #[test]
    fn test_add_rate_limit_trailer() {
        let decision = crate::RateLimitDecision {
            permitted: true,
            limit: 100,
            remaining: Some(75),
            reset: Some(std::time::Duration::from_secs(60)),
            retry_after: None,
        };

        let response = http::Response::new(BoxBody::default());
        let response = add_rate_limit_trailer(response, &decision);

        assert_eq!(
            response
                .headers()
                .get("x-ratelimit-limit")
                .and_then(|v| v.to_str().ok()),
            Some("100")
        );
        assert_eq!(
            response
                .headers()
                .get("x-ratelimit-remaining")
                .and_then(|v| v.to_str().ok()),
            Some("75")
        );
        assert_eq!(
            response
                .headers()
                .get("x-ratelimit-reset")
                .and_then(|v| v.to_str().ok()),
            Some("60")
        );
    }

    // Mock service for testing
    #[derive(Clone)]
    struct MockService;

    impl MockService {
        fn new() -> Self {
            Self
        }
    }

    impl Service<http::Request<BoxBody>> for MockService {
        type Response = http::Response<BoxBody>;
        type Error = Box<dyn std::error::Error + Send + Sync>;
        type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: http::Request<BoxBody>) -> Self::Future {
            Box::pin(async move { Ok(http::Response::new(BoxBody::default())) })
        }
    }

    #[tokio::test]
    async fn test_rate_limit_service_allows_requests() {
        let limiter = Arc::new(
            RateLimiter::builder()
                .requests_per_second(10)
                .burst(20)
                .build()
                .unwrap(),
        );

        let layer = GrpcRateLimitLayer::new(limiter);
        let mut service = layer.layer(MockService::new());

        let req = http::Request::builder()
            .uri("http://example.com/test.Service/Method")
            .body(BoxBody::default())
            .unwrap();

        let response = service.call(req).await.unwrap();
        assert_eq!(response.status(), http::StatusCode::OK);

        // Check for rate limit headers
        assert!(response.headers().get("x-ratelimit-limit").is_some());
        assert!(response.headers().get("x-ratelimit-remaining").is_some());
    }

    #[tokio::test]
    async fn test_rate_limit_service_denies_requests() {
        // Create a very restrictive rate limiter
        let limiter = Arc::new(
            RateLimiter::builder()
                .requests_per_second(1)
                .burst(1)
                .build()
                .unwrap(),
        );

        let layer = GrpcRateLimitLayer::new(limiter);
        let mut service = layer.layer(MockService::new());

        // First request should succeed
        let req = http::Request::builder()
            .uri("http://example.com/test.Service/Method")
            .body(BoxBody::default())
            .unwrap();

        let response = service.call(req).await.unwrap();
        assert_eq!(response.status(), http::StatusCode::OK);

        // Second immediate request should be rate limited
        let req = http::Request::builder()
            .uri("http://example.com/test.Service/Method")
            .body(BoxBody::default())
            .unwrap();

        let response = service.call(req).await.unwrap();
        assert_eq!(response.status(), http::StatusCode::TOO_MANY_REQUESTS);

        // Check for rate limit headers
        assert!(response.headers().get("x-ratelimit-limit").is_some());
        assert!(response.headers().get("retry-after").is_some());
    }

    #[tokio::test]
    async fn test_rate_limit_service_no_key_extracted() {
        let limiter = Arc::new(
            RateLimiter::builder()
                .requests_per_second(1)
                .burst(1)
                .build()
                .unwrap(),
        );

        // Use a custom extractor that returns None
        let extractor = CustomGrpcKeyExtractor::new(|_req| None);
        let layer = GrpcRateLimitLayer::with_extractor(limiter, extractor);
        let mut service = layer.layer(MockService::new());

        let req = http::Request::builder()
            .uri("http://example.com/test")
            .body(BoxBody::default())
            .unwrap();

        // Should allow request when no key is extracted
        let response = service.call(req).await.unwrap();
        assert_eq!(response.status(), http::StatusCode::OK);
    }

    #[tokio::test]
    async fn test_rate_limit_service_custom_extractor() {
        let limiter = Arc::new(
            RateLimiter::builder()
                .requests_per_second(1)
                .burst(1)
                .build()
                .unwrap(),
        );

        // Custom extractor that combines method and user-id
        let extractor = CustomGrpcKeyExtractor::new(|req| {
            let method = req.uri().path().trim_start_matches('/');
            let user = req.headers().get("user-id")?.to_str().ok()?;
            Some(format!("{}:{}", method, user))
        });

        let layer = GrpcRateLimitLayer::with_extractor(limiter, extractor);
        let mut service = layer.layer(MockService::new());

        // Request with user-1
        let req = http::Request::builder()
            .uri("http://example.com/test.Service/Method")
            .header("user-id", "user-1")
            .body(BoxBody::default())
            .unwrap();

        let response = service.call(req).await.unwrap();
        assert_eq!(response.status(), http::StatusCode::OK);

        // Second request with user-1 should be rate limited
        let req = http::Request::builder()
            .uri("http://example.com/test.Service/Method")
            .header("user-id", "user-1")
            .body(BoxBody::default())
            .unwrap();

        let response = service.call(req).await.unwrap();
        assert_eq!(response.status(), http::StatusCode::TOO_MANY_REQUESTS);

        // Request with user-2 should be allowed (different key)
        let req = http::Request::builder()
            .uri("http://example.com/test.Service/Method")
            .header("user-id", "user-2")
            .body(BoxBody::default())
            .unwrap();

        let response = service.call(req).await.unwrap();
        assert_eq!(response.status(), http::StatusCode::OK);
    }

    #[tokio::test]
    async fn test_rate_limit_service_different_methods() {
        let limiter = Arc::new(
            RateLimiter::builder()
                .requests_per_second(1)
                .burst(1)
                .build()
                .unwrap(),
        );

        let layer = GrpcRateLimitLayer::new(limiter);
        let mut service = layer.layer(MockService::new());

        // Request to Method1
        let req = http::Request::builder()
            .uri("http://example.com/test.Service/Method1")
            .body(BoxBody::default())
            .unwrap();

        let response = service.call(req).await.unwrap();
        assert_eq!(response.status(), http::StatusCode::OK);

        // Second request to Method1 should be rate limited
        let req = http::Request::builder()
            .uri("http://example.com/test.Service/Method1")
            .body(BoxBody::default())
            .unwrap();

        let response = service.call(req).await.unwrap();
        assert_eq!(response.status(), http::StatusCode::TOO_MANY_REQUESTS);

        // Request to Method2 should be allowed (different key)
        let req = http::Request::builder()
            .uri("http://example.com/test.Service/Method2")
            .body(BoxBody::default())
            .unwrap();

        let response = service.call(req).await.unwrap();
        assert_eq!(response.status(), http::StatusCode::OK);
    }
}
