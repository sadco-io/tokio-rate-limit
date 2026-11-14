//! Tonic middleware integration for gRPC rate limiting.
//!
//! Provides Tower `Layer` and `Service` implementations for Tonic gRPC servers with:
//! - Spec-compliant gRPC over HTTP/2 wire status (`HTTP 200 OK` + `grpc-status: 8`
//!   [`tonic::Code::ResourceExhausted`]) so gRPC clients and proxies receive the exact
//!   `RESOURCE_EXHAUSTED` code and rate-limit metadata instead of `UNAVAILABLE`.
//! - Zero-allocation borrowed method/metadata/IP key extraction and unboxed futures.
//! - Per-RPC cost hooks (`.with_cost(...)`) and custom `Status` hooks (`.on_rejected(...)`).
//! - Proper Tower `poll_ready` readiness swapping for stateful inner layers.
//!
//! # Example
//!
//! ```ignore
//! use tonic::transport::Server;
//! use tokio_rate_limit::{RateLimiter, tonic_middleware::GrpcRateLimitLayer};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let limiter = RateLimiter::builder()
//!     .requests_per_second(100)
//!     .burst(200)
//!     .build()?;
//!
//! Server::builder()
//!     .layer(GrpcRateLimitLayer::new(limiter))
//!     .add_service(your_service)
//!     .serve("[::1]:50051".parse()?)
//!     .await?;
//! # Ok(())
//! # }
//! ```

use crate::algorithm::internal::http_seconds_ceil;
pub use crate::algorithm::internal::StackIp;
use crate::algorithm::{Algorithm, RateLimitKey, ToKey, TokenBucket};
use crate::{IntoRateLimiter, RateLimitDecision, RateLimiter};
use http::{header::HeaderName, HeaderValue};
use pin_project_lite::pin_project;
use std::borrow::{Borrow, Cow};
use std::fmt;
use std::future::Future;
use std::hash::Hash;
use std::net::{IpAddr, SocketAddr};
use std::ops::Deref;
use std::pin::Pin;
use std::task::{Context, Poll};
use tonic::body::Body;
use tonic::Status;
use tower::{Layer, Service};

const HDR_X_RATELIMIT_LIMIT: HeaderName = HeaderName::from_static("x-ratelimit-limit");
const HDR_X_RATELIMIT_REMAINING: HeaderName = HeaderName::from_static("x-ratelimit-remaining");
const HDR_X_RATELIMIT_RESET: HeaderName = HeaderName::from_static("x-ratelimit-reset");
const HDR_RETRY_AFTER: HeaderName = HeaderName::from_static("retry-after");

/// Borrowed, stack-formatted, or owned string key extracted from a gRPC request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GrpcExtractedKey<'a> {
    /// Borrowed string slice from the gRPC URI path or metadata headers (zero allocation).
    Borrowed(&'a str),
    /// Stack-formatted IPv4 or IPv6 peer address (zero heap allocation).
    Ip(StackIp),
    /// Owned string key (for dynamically formatted composite keys).
    Owned(String),
}

impl<'a> GrpcExtractedKey<'a> {
    /// Returns the extracted key as a `&str`.
    #[inline]
    pub fn as_str(&self) -> &str {
        match self {
            Self::Borrowed(s) => s,
            Self::Ip(ip) => ip.as_str(),
            Self::Owned(s) => s.as_str(),
        }
    }
}

impl Deref for GrpcExtractedKey<'_> {
    type Target = str;

    #[inline]
    fn deref(&self) -> &str {
        self.as_str()
    }
}

impl PartialEq<&str> for GrpcExtractedKey<'_> {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl PartialEq<String> for GrpcExtractedKey<'_> {
    fn eq(&self, other: &String) -> bool {
        self.as_str() == other.as_str()
    }
}

impl<'a> From<&'a str> for GrpcExtractedKey<'a> {
    #[inline]
    fn from(s: &'a str) -> Self {
        Self::Borrowed(s)
    }
}

impl From<String> for GrpcExtractedKey<'static> {
    #[inline]
    fn from(s: String) -> Self {
        Self::Owned(s)
    }
}

impl<'a> From<Cow<'a, str>> for GrpcExtractedKey<'a> {
    #[inline]
    fn from(c: Cow<'a, str>) -> Self {
        match c {
            Cow::Borrowed(s) => Self::Borrowed(s),
            Cow::Owned(s) => Self::Owned(s),
        }
    }
}

impl From<IpAddr> for GrpcExtractedKey<'static> {
    #[inline]
    fn from(ip: IpAddr) -> Self {
        Self::Ip(StackIp::from_ip(ip))
    }
}

/// Bridges an extracted gRPC key representation to a borrowed lookup query on `K`.
pub trait GrpcLookupKey<K: RateLimitKey> {
    /// Borrowed query type (`str` for `String`-keyed limiters, `K` for typed limiters).
    type Query: Hash + Ord + fmt::Debug + ToKey<K> + ?Sized;

    /// Borrows `self` as `&Self::Query`.
    fn as_query(&self) -> &Self::Query;
}

impl<K: RateLimitKey> GrpcLookupKey<K> for K {
    type Query = K;

    #[inline]
    fn as_query(&self) -> &K {
        self
    }
}

impl GrpcLookupKey<String> for &str {
    type Query = str;

    #[inline]
    fn as_query(&self) -> &str {
        self
    }
}

impl GrpcLookupKey<String> for StackIp {
    type Query = str;

    #[inline]
    fn as_query(&self) -> &str {
        self.as_str()
    }
}

impl GrpcLookupKey<String> for GrpcExtractedKey<'_> {
    type Query = str;

    #[inline]
    fn as_query(&self) -> &str {
        self.as_str()
    }
}

/// Trait for extracting rate limit keys from incoming gRPC requests.
pub trait GrpcKeyExtractor<K: RateLimitKey = String>: Send + Sync + 'static {
    /// Extracted key type, potentially borrowing from the request for `'a`.
    type Key<'a>: GrpcLookupKey<K>;

    /// Extracts a rate limit key from the gRPC HTTP/2 request, or `None` if unavailable.
    fn extract<'a>(&'a self, req: &'a http::Request<Body>) -> Option<Self::Key<'a>>;
}

/// Default key extractor that borrows the gRPC method path (`{package}.{Service}/{Method}`)
/// directly from the request URI with zero heap allocation.
#[derive(Debug, Clone, Copy, Default)]
pub struct MethodKeyExtractor;

impl GrpcKeyExtractor<String> for MethodKeyExtractor {
    type Key<'a> = &'a str;

    #[inline]
    fn extract<'a>(&'a self, req: &'a http::Request<Body>) -> Option<&'a str> {
        let path = req.uri().path();
        Some(path.strip_prefix('/').unwrap_or(path))
    }
}

/// Key extractor that uses the client IP address for gRPC rate limiting.
///
/// Prefers a `SocketAddr` in request extensions (peer address), falling back to
/// the *last* hop of `x-forwarded-for` and then `x-real-ip` with zero heap allocation.
#[derive(Debug, Clone, Copy, Default)]
pub struct IpKeyExtractor;

impl IpKeyExtractor {
    /// Extracts the client IP address from peer `SocketAddr` extensions, `x-forwarded-for`,
    /// or `x-real-ip` with zero heap allocation.
    #[inline]
    pub fn extract<'a>(&'a self, req: &'a http::Request<Body>) -> Option<GrpcExtractedKey<'a>> {
        if let Some(addr) = req.extensions().get::<SocketAddr>() {
            return Some(GrpcExtractedKey::Ip(StackIp::from_ip(addr.ip())));
        }

        req.headers()
            .get("x-forwarded-for")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.split(',').next_back().map(str::trim))
            .filter(|s| !s.is_empty())
            .or_else(|| {
                req.headers()
                    .get("x-real-ip")
                    .and_then(|v| v.to_str().ok())
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
            })
            .map(GrpcExtractedKey::Borrowed)
    }
}

impl GrpcKeyExtractor<String> for IpKeyExtractor {
    type Key<'a> = GrpcExtractedKey<'a>;

    #[inline]
    fn extract<'a>(&'a self, req: &'a http::Request<Body>) -> Option<GrpcExtractedKey<'a>> {
        Self::extract(self, req)
    }
}

impl GrpcKeyExtractor<IpAddr> for IpKeyExtractor {
    type Key<'a> = IpAddr;

    #[inline]
    fn extract<'a>(&'a self, req: &'a http::Request<Body>) -> Option<IpAddr> {
        if let Some(addr) = req.extensions().get::<SocketAddr>() {
            return Some(addr.ip());
        }

        req.headers()
            .get("x-forwarded-for")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.split(',').next_back().map(str::trim))
            .and_then(|s| s.parse::<IpAddr>().ok())
            .or_else(|| {
                req.headers()
                    .get("x-real-ip")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.trim().parse::<IpAddr>().ok())
            })
    }
}

/// Zero-allocation gRPC metadata header key extractor.
///
/// # Example
///
/// ```no_run
/// use tokio_rate_limit::tonic_middleware::MetadataKeyExtractor;
///
/// let extractor = MetadataKeyExtractor::new("user-id");
/// ```
#[derive(Debug, Clone)]
pub struct MetadataKeyExtractor {
    header_name: HeaderName,
}

impl MetadataKeyExtractor {
    /// Creates a new metadata key extractor for `header_name`.
    ///
    /// # Panics
    ///
    /// Panics if `header_name` is not a valid HTTP/2 metadata header name.
    pub fn new(header_name: impl AsRef<str>) -> Self {
        let name = HeaderName::from_bytes(header_name.as_ref().as_bytes())
            .expect("valid gRPC metadata header name");
        Self { header_name: name }
    }

    /// Creates a metadata key extractor from a lowercase `'static` header name.
    pub const fn from_static(header_name: &'static str) -> Self {
        Self {
            header_name: HeaderName::from_static(header_name),
        }
    }
}

impl GrpcKeyExtractor<String> for MetadataKeyExtractor {
    type Key<'a> = &'a str;

    #[inline]
    fn extract<'a>(&'a self, req: &'a http::Request<Body>) -> Option<&'a str> {
        req.headers()
            .get(&self.header_name)
            .and_then(|v| v.to_str().ok())
            .filter(|s| !s.is_empty())
    }
}

/// Custom gRPC key extractor backed by a closure.
///
/// # Example
///
/// ```no_run
/// use tokio_rate_limit::tonic_middleware::CustomGrpcKeyExtractor;
///
/// let extractor = CustomGrpcKeyExtractor::new(|req| {
///     let method = req.uri().path().trim_start_matches('/');
///     let user = req.headers()
///         .get("user-id")
///         .and_then(|v| v.to_str().ok())?;
///     Some(format!("{}:{}", method, user))
/// });
/// ```
#[derive(Clone)]
pub struct CustomGrpcKeyExtractor<F> {
    extractor: F,
}

impl<F> fmt::Debug for CustomGrpcKeyExtractor<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CustomGrpcKeyExtractor")
            .finish_non_exhaustive()
    }
}

impl<F> CustomGrpcKeyExtractor<F> {
    /// Creates a custom gRPC key extractor from a closure returning `Option<K>`
    /// (for example `Option<String>`, `Option<IpAddr>`, or `Option<u64>`).
    pub fn new<K>(extractor: F) -> Self
    where
        K: RateLimitKey,
        F: Fn(&http::Request<Body>) -> Option<K> + Send + Sync + Clone + 'static,
    {
        Self { extractor }
    }

    /// Creates a zero-allocation custom gRPC key extractor borrowing `&str` directly from `&Request<Body>`.
    pub fn borrowed(extractor: F) -> BorrowedGrpcKeyExtractor<F>
    where
        F: for<'a> Fn(&'a http::Request<Body>) -> Option<&'a str> + Send + Sync + Clone + 'static,
    {
        BorrowedGrpcKeyExtractor { extractor }
    }
}

impl<K, F> GrpcKeyExtractor<K> for CustomGrpcKeyExtractor<F>
where
    K: RateLimitKey,
    F: Fn(&http::Request<Body>) -> Option<K> + Send + Sync + Clone + 'static,
{
    type Key<'a> = K;

    #[inline]
    fn extract<'a>(&'a self, req: &'a http::Request<Body>) -> Option<K> {
        (self.extractor)(req)
    }
}

/// Zero-allocation custom gRPC key extractor borrowing `&str` from `&http::Request<Body>`.
#[derive(Clone)]
pub struct BorrowedGrpcKeyExtractor<F> {
    extractor: F,
}

impl<F> fmt::Debug for BorrowedGrpcKeyExtractor<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BorrowedGrpcKeyExtractor")
            .finish_non_exhaustive()
    }
}

impl<F> GrpcKeyExtractor<String> for BorrowedGrpcKeyExtractor<F>
where
    F: for<'a> Fn(&'a http::Request<Body>) -> Option<&'a str> + Send + Sync + Clone + 'static,
{
    type Key<'a> = &'a str;

    #[inline]
    fn extract<'a>(&'a self, req: &'a http::Request<Body>) -> Option<&'a str> {
        (self.extractor)(req)
    }
}

/// Per-RPC token cost function used by [`GrpcRateLimitLayer`].
pub trait GrpcCostExtractor: Send + Sync + 'static {
    /// Computes the token cost for `req`.
    fn cost(&self, req: &http::Request<Body>) -> u64;
}

/// Default unit cost (`1` token per RPC).
#[derive(Debug, Clone, Copy, Default)]
pub struct GrpcUnitCost;

impl GrpcCostExtractor for GrpcUnitCost {
    #[inline]
    fn cost(&self, _req: &http::Request<Body>) -> u64 {
        1
    }
}

/// Closure-backed gRPC cost extractor created via [`GrpcRateLimitLayer::with_cost`].
#[derive(Clone)]
pub struct CustomGrpcCostExtractor<F> {
    cost_fn: F,
}

impl<F> fmt::Debug for CustomGrpcCostExtractor<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CustomGrpcCostExtractor")
            .finish_non_exhaustive()
    }
}

impl<F> GrpcCostExtractor for CustomGrpcCostExtractor<F>
where
    F: Fn(&http::Request<Body>) -> u64 + Send + Sync + Clone + 'static,
{
    #[inline]
    fn cost(&self, req: &http::Request<Body>) -> u64 {
        (self.cost_fn)(req)
    }
}

/// Customizes the gRPC [`Status`] returned when a request is rate limited.
pub trait GrpcRejectionHandler: Send + Sync + 'static {
    /// Produces an HTTP/2 gRPC error response for a denied [`RateLimitDecision`].
    fn reject(&self, decision: &RateLimitDecision) -> http::Response<Body>;
}

/// Default `RESOURCE_EXHAUSTED` gRPC status handler.
#[derive(Debug, Clone, Copy, Default)]
pub struct DefaultGrpcRejectionHandler;

impl GrpcRejectionHandler for DefaultGrpcRejectionHandler {
    #[inline]
    fn reject(&self, decision: &RateLimitDecision) -> http::Response<Body> {
        rate_limit_error_response(Status::resource_exhausted("Rate limit exceeded"), decision)
    }
}

/// Closure-backed gRPC rejection handler created via [`GrpcRateLimitLayer::on_rejected`].
#[derive(Clone)]
pub struct CustomGrpcRejectionHandler<F> {
    handler: F,
}

impl<F> fmt::Debug for CustomGrpcRejectionHandler<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CustomGrpcRejectionHandler")
            .finish_non_exhaustive()
    }
}

impl<F> GrpcRejectionHandler for CustomGrpcRejectionHandler<F>
where
    F: Fn(&RateLimitDecision) -> Status + Send + Sync + Clone + 'static,
{
    #[inline]
    fn reject(&self, decision: &RateLimitDecision) -> http::Response<Body> {
        let status = (self.handler)(decision);
        rate_limit_error_response(status, decision)
    }
}

/// Tower layer for adding rate limiting to a Tonic gRPC service.
///
/// Accepts either a [`RateLimiter`] directly or an `Arc<RateLimiter>`.
pub struct GrpcRateLimitLayer<
    A = TokenBucket,
    E = MethodKeyExtractor,
    C = GrpcUnitCost,
    R = DefaultGrpcRejectionHandler,
> where
    A: Algorithm,
{
    limiter: RateLimiter<A>,
    extractor: E,
    cost_extractor: C,
    rejection_handler: R,
    fail_open: bool,
}

impl<A, E, C, R> Clone for GrpcRateLimitLayer<A, E, C, R>
where
    A: Algorithm,
    E: Clone,
    C: Clone,
    R: Clone,
{
    fn clone(&self) -> Self {
        Self {
            limiter: self.limiter.clone(),
            extractor: self.extractor.clone(),
            cost_extractor: self.cost_extractor.clone(),
            rejection_handler: self.rejection_handler.clone(),
            fail_open: self.fail_open,
        }
    }
}

impl<A, E, C, R> fmt::Debug for GrpcRateLimitLayer<A, E, C, R>
where
    A: Algorithm,
    E: fmt::Debug,
    C: fmt::Debug,
    R: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GrpcRateLimitLayer")
            .field("limiter", &self.limiter)
            .field("extractor", &self.extractor)
            .field("cost_extractor", &self.cost_extractor)
            .field("rejection_handler", &self.rejection_handler)
            .field("fail_open", &self.fail_open)
            .finish()
    }
}

impl<A: Algorithm>
    GrpcRateLimitLayer<A, MethodKeyExtractor, GrpcUnitCost, DefaultGrpcRejectionHandler>
{
    /// Creates a new gRPC rate limit layer with default method-path key extraction.
    pub fn new(limiter: impl IntoRateLimiter<A>) -> Self {
        Self {
            limiter: limiter.into_rate_limiter(),
            extractor: MethodKeyExtractor,
            cost_extractor: GrpcUnitCost,
            rejection_handler: DefaultGrpcRejectionHandler,
            fail_open: false,
        }
    }
}

impl<A: Algorithm, E> GrpcRateLimitLayer<A, E, GrpcUnitCost, DefaultGrpcRejectionHandler> {
    /// Creates a new gRPC rate limit layer with a custom [`GrpcKeyExtractor`].
    pub fn with_extractor(limiter: impl IntoRateLimiter<A>, extractor: E) -> Self {
        Self {
            limiter: limiter.into_rate_limiter(),
            extractor,
            cost_extractor: GrpcUnitCost,
            rejection_handler: DefaultGrpcRejectionHandler,
            fail_open: false,
        }
    }
}

impl<A, E, C, R> GrpcRateLimitLayer<A, E, C, R>
where
    A: Algorithm,
{
    /// Passes through requests when the key extractor returns `None` (default is fail-closed).
    pub fn fail_open(mut self) -> Self {
        self.fail_open = true;
        self
    }

    /// Configures a per-RPC token cost function (for example charging higher cost on
    /// expensive RPC methods).
    pub fn with_cost<F>(self, cost_fn: F) -> GrpcRateLimitLayer<A, E, CustomGrpcCostExtractor<F>, R>
    where
        F: Fn(&http::Request<Body>) -> u64 + Send + Sync + Clone + 'static,
    {
        GrpcRateLimitLayer {
            limiter: self.limiter,
            extractor: self.extractor,
            cost_extractor: CustomGrpcCostExtractor { cost_fn },
            rejection_handler: self.rejection_handler,
            fail_open: self.fail_open,
        }
    }

    /// Customizes the [`tonic::Status`] returned when an RPC is rate limited.
    ///
    /// Rate limit metadata (`x-ratelimit-limit`, `x-ratelimit-remaining`, `retry-after`,
    /// `x-ratelimit-reset`) is automatically attached if not already present on `Status`.
    pub fn on_rejected<F>(
        self,
        handler: F,
    ) -> GrpcRateLimitLayer<A, E, C, CustomGrpcRejectionHandler<F>>
    where
        F: Fn(&RateLimitDecision) -> Status + Send + Sync + Clone + 'static,
    {
        GrpcRateLimitLayer {
            limiter: self.limiter,
            extractor: self.extractor,
            cost_extractor: self.cost_extractor,
            rejection_handler: CustomGrpcRejectionHandler { handler },
            fail_open: self.fail_open,
        }
    }
}

impl<S, A, E, C, R> Layer<S> for GrpcRateLimitLayer<A, E, C, R>
where
    A: Algorithm,
    E: Clone,
    C: Clone,
    R: Clone,
{
    type Service = GrpcRateLimitService<S, A, E, C, R>;

    fn layer(&self, inner: S) -> Self::Service {
        GrpcRateLimitService {
            inner,
            limiter: self.limiter.clone(),
            extractor: self.extractor.clone(),
            cost_extractor: self.cost_extractor.clone(),
            rejection_handler: self.rejection_handler.clone(),
            fail_open: self.fail_open,
        }
    }
}

/// The gRPC rate limiting Tower [`Service`].
pub struct GrpcRateLimitService<
    S,
    A = TokenBucket,
    E = MethodKeyExtractor,
    C = GrpcUnitCost,
    R = DefaultGrpcRejectionHandler,
> where
    A: Algorithm,
{
    inner: S,
    limiter: RateLimiter<A>,
    extractor: E,
    cost_extractor: C,
    rejection_handler: R,
    fail_open: bool,
}

impl<S, A, E, C, R> Clone for GrpcRateLimitService<S, A, E, C, R>
where
    S: Clone,
    A: Algorithm,
    E: Clone,
    C: Clone,
    R: Clone,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            limiter: self.limiter.clone(),
            extractor: self.extractor.clone(),
            cost_extractor: self.cost_extractor.clone(),
            rejection_handler: self.rejection_handler.clone(),
            fail_open: self.fail_open,
        }
    }
}

impl<S, A, E, C, R> fmt::Debug for GrpcRateLimitService<S, A, E, C, R>
where
    S: fmt::Debug,
    A: Algorithm,
    E: fmt::Debug,
    C: fmt::Debug,
    R: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GrpcRateLimitService")
            .field("inner", &self.inner)
            .field("limiter", &self.limiter)
            .field("extractor", &self.extractor)
            .field("cost_extractor", &self.cost_extractor)
            .field("rejection_handler", &self.rejection_handler)
            .field("fail_open", &self.fail_open)
            .finish()
    }
}

pin_project! {
    /// Unboxed response future returned by [`GrpcRateLimitService`].
    pub struct GrpcRateLimitFuture<F> {
        #[pin]
        state: GrpcFutureState<F>,
    }
}

pin_project! {
    #[project = GrpcFutureStateProj]
    enum GrpcFutureState<F> {
        PassThrough {
            #[pin]
            future: F,
            decision: Option<RateLimitDecision>,
        },
        Immediate {
            response: Option<http::Response<Body>>,
        },
    }
}

impl<F, Err> Future for GrpcRateLimitFuture<F>
where
    F: Future<Output = Result<http::Response<Body>, Err>>,
    Err: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    type Output = Result<http::Response<Body>, Box<dyn std::error::Error + Send + Sync>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.project().state.project() {
            GrpcFutureStateProj::PassThrough { future, decision } => match future.poll(cx) {
                Poll::Ready(Ok(response)) => {
                    let response = match decision.take() {
                        Some(dec) => add_rate_limit_metadata(response, &dec),
                        None => response,
                    };
                    Poll::Ready(Ok(response))
                }
                Poll::Ready(Err(err)) => Poll::Ready(Err(err.into())),
                Poll::Pending => Poll::Pending,
            },
            GrpcFutureStateProj::Immediate { response } => Poll::Ready(Ok(response
                .take()
                .expect("GrpcRateLimitFuture polled after completion"))),
        }
    }
}

impl<S, A, E, C, R> Service<http::Request<Body>> for GrpcRateLimitService<S, A, E, C, R>
where
    S: Service<http::Request<Body>, Response = http::Response<Body>> + Clone,
    S::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    A: Algorithm + 'static,
    E: GrpcKeyExtractor<A::Key>,
    for<'a> A::Key: Borrow<<E::Key<'a> as GrpcLookupKey<A::Key>>::Query>,
    C: GrpcCostExtractor,
    R: GrpcRejectionHandler,
{
    type Response = http::Response<Body>;
    type Error = Box<dyn std::error::Error + Send + Sync>;
    type Future = GrpcRateLimitFuture<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, req: http::Request<Body>) -> Self::Future {
        let maybe_decision =
            <E as GrpcKeyExtractor<A::Key>>::extract(&self.extractor, &req).map(|extracted| {
                let cost = self.cost_extractor.cost(&req);
                self.limiter.check_with_cost(extracted.as_query(), cost)
            });

        let Some(decision) = maybe_decision else {
            if self.fail_open {
                let clone = self.inner.clone();
                let mut inner = std::mem::replace(&mut self.inner, clone);
                return GrpcRateLimitFuture {
                    state: GrpcFutureState::PassThrough {
                        future: inner.call(req),
                        decision: None,
                    },
                };
            }

            #[cfg(feature = "metrics-support")]
            {
                metrics::counter!("tokio_rate_limit.requests.denied").increment(1);
                metrics::counter!("tokio_rate_limit.grpc.requests.denied").increment(1);
            }

            let missing_decision = RateLimitDecision {
                permitted: false,
                retry_after: Some(std::time::Duration::from_secs(1)),
                remaining: Some(0),
                limit: 0,
                reset: None,
            };
            return GrpcRateLimitFuture {
                state: GrpcFutureState::Immediate {
                    response: Some(self.rejection_handler.reject(&missing_decision)),
                },
            };
        };

        if decision.permitted {
            #[cfg(feature = "metrics-support")]
            {
                metrics::counter!("tokio_rate_limit.grpc.requests.allowed").increment(1);
                if let Some(remaining) = decision.remaining {
                    metrics::histogram!("tokio_rate_limit.grpc.remaining_tokens")
                        .record(remaining as f64);
                }
            }

            let clone = self.inner.clone();
            let mut inner = std::mem::replace(&mut self.inner, clone);
            GrpcRateLimitFuture {
                state: GrpcFutureState::PassThrough {
                    future: inner.call(req),
                    decision: Some(decision),
                },
            }
        } else {
            #[cfg(feature = "metrics-support")]
            {
                metrics::counter!("tokio_rate_limit.grpc.requests.denied").increment(1);
                if let Some(remaining) = decision.remaining {
                    metrics::histogram!("tokio_rate_limit.grpc.remaining_tokens")
                        .record(remaining as f64);
                }
            }

            GrpcRateLimitFuture {
                state: GrpcFutureState::Immediate {
                    response: Some(self.rejection_handler.reject(&decision)),
                },
            }
        }
    }
}

/// Adds rate limit metadata to initial HTTP/2 response headers (accessible on Tonic
/// clients via `response.metadata()`).
fn add_rate_limit_metadata(
    mut response: http::Response<Body>,
    decision: &RateLimitDecision,
) -> http::Response<Body> {
    let headers = response.headers_mut();

    headers.insert(HDR_X_RATELIMIT_LIMIT, HeaderValue::from(decision.limit));

    if let Some(remaining) = decision.remaining {
        headers.insert(HDR_X_RATELIMIT_REMAINING, HeaderValue::from(remaining));
    }

    if let Some(reset) = decision.reset {
        headers.insert(
            HDR_X_RATELIMIT_RESET,
            HeaderValue::from(http_seconds_ceil(reset)),
        );
    }

    response
}

/// Converts a [`tonic::Status`] into a spec-compliant gRPC over HTTP/2 response.
///
/// Per the gRPC over HTTP/2 wire specification, gRPC responses MUST carry HTTP `:status 200`
/// alongside `content-type: application/grpc` and `grpc-status: 8` (`RESOURCE_EXHAUSTED`).
/// Returning HTTP `429` causes compliant gRPC clients and proxies to synthesize
/// `Code::Unavailable` (14) instead of `Code::ResourceExhausted` (8).
fn rate_limit_error_response(status: Status, decision: &RateLimitDecision) -> http::Response<Body> {
    let mut response = status.into_http::<Body>();
    let headers = response.headers_mut();

    headers
        .entry(HDR_X_RATELIMIT_LIMIT)
        .or_insert_with(|| HeaderValue::from(decision.limit));

    if let Some(remaining) = decision.remaining {
        headers
            .entry(HDR_X_RATELIMIT_REMAINING)
            .or_insert_with(|| HeaderValue::from(remaining));
    }

    if let Some(retry_after) = decision.retry_after {
        headers
            .entry(HDR_RETRY_AFTER)
            .or_insert_with(|| HeaderValue::from(http_seconds_ceil(retry_after)));
    }

    if let Some(reset) = decision.reset {
        headers
            .entry(HDR_X_RATELIMIT_RESET)
            .or_insert_with(|| HeaderValue::from(http_seconds_ceil(reset)));
    }

    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::task::{Context, Poll};
    use tonic::Code;

    fn assert_grpc_resource_exhausted(response: &http::Response<Body>) {
        // gRPC over HTTP/2 wire spec requires HTTP 200 OK with grpc-status = 8
        assert_eq!(response.status(), http::StatusCode::OK);
        let status = Status::from_header_map(response.headers())
            .expect("headers-only gRPC status must decode");
        assert_eq!(status.code(), Code::ResourceExhausted);
    }

    #[test]
    fn test_method_key_extractor() {
        let extractor = MethodKeyExtractor;
        let req = http::Request::builder()
            .uri("http://example.com/helloworld.Greeter/SayHello")
            .body(Body::default())
            .unwrap();

        let key = extractor.extract(&req);
        assert_eq!(key, Some("helloworld.Greeter/SayHello"));
    }

    #[test]
    fn test_metadata_key_extractor() {
        let extractor = MetadataKeyExtractor::new("user-id");
        let req = http::Request::builder()
            .uri("http://example.com/test")
            .header("user-id", "user-123")
            .body(Body::default())
            .unwrap();

        let key = extractor.extract(&req);
        assert_eq!(key, Some("user-123"));
    }

    #[test]
    fn test_metadata_key_extractor_missing_header() {
        let extractor = MetadataKeyExtractor::new("user-id");
        let req = http::Request::builder()
            .uri("http://example.com/test")
            .body(Body::default())
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
            .body(Body::default())
            .unwrap();

        let key = GrpcKeyExtractor::<String>::extract(&extractor, &req);
        assert_eq!(key.as_deref(), Some("10.0.0.1"));
    }

    #[test]
    fn test_ip_key_extractor_x_real_ip() {
        let extractor = IpKeyExtractor;
        let req = http::Request::builder()
            .uri("http://example.com/test")
            .header("x-real-ip", "10.0.0.1")
            .body(Body::default())
            .unwrap();

        let key = GrpcKeyExtractor::<String>::extract(&extractor, &req);
        assert_eq!(key.as_deref(), Some("10.0.0.1"));
    }

    #[test]
    fn test_ip_key_extractor_no_headers() {
        let extractor = IpKeyExtractor;
        let req = http::Request::builder()
            .uri("http://example.com/test")
            .body(Body::default())
            .unwrap();

        let key = GrpcKeyExtractor::<String>::extract(&extractor, &req);
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
            .body(Body::default())
            .unwrap();

        let key = extractor.extract(&req);
        assert_eq!(key, Some("custom:test.Service/Method".to_string()));
    }

    #[test]
    fn test_rate_limit_error_response_grpc_wire_compliant() {
        let decision = crate::RateLimitDecision {
            permitted: false,
            limit: 100,
            remaining: Some(0),
            reset: Some(std::time::Duration::from_secs(60)),
            retry_after: Some(std::time::Duration::from_secs(1)),
        };

        let response =
            rate_limit_error_response(Status::resource_exhausted("Rate limit exceeded"), &decision);

        assert_grpc_resource_exhausted(&response);
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
    fn test_add_rate_limit_metadata() {
        let decision = crate::RateLimitDecision {
            permitted: true,
            limit: 100,
            remaining: Some(75),
            reset: Some(std::time::Duration::from_secs(60)),
            retry_after: None,
        };

        let response = http::Response::new(Body::default());
        let response = add_rate_limit_metadata(response, &decision);

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

    #[derive(Clone)]
    struct MockService;

    impl Service<http::Request<Body>> for MockService {
        type Response = http::Response<Body>;
        type Error = Box<dyn std::error::Error + Send + Sync>;
        type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: http::Request<Body>) -> Self::Future {
            Box::pin(async move { Ok(http::Response::new(Body::default())) })
        }
    }

    #[tokio::test]
    async fn test_rate_limit_service_denies_with_grpc_resource_exhausted() {
        let limiter = Arc::new(
            RateLimiter::builder()
                .requests_per_second(1)
                .burst(1)
                .build()
                .unwrap(),
        );

        let layer = GrpcRateLimitLayer::new(limiter);
        let mut service = layer.layer(MockService);

        let req = http::Request::builder()
            .uri("http://example.com/test.Service/Method")
            .body(Body::default())
            .unwrap();
        let response = service.call(req).await.unwrap();
        assert_eq!(response.status(), http::StatusCode::OK);
        assert!(response.headers().get("grpc-status").is_none());

        let req = http::Request::builder()
            .uri("http://example.com/test.Service/Method")
            .body(Body::default())
            .unwrap();
        let response = service.call(req).await.unwrap();
        assert_grpc_resource_exhausted(&response);
        assert!(response.headers().get("x-ratelimit-limit").is_some());
        assert!(response.headers().get("retry-after").is_some());
    }

    #[tokio::test]
    async fn test_rate_limit_service_no_key_extracted() {
        let limiter = RateLimiter::builder()
            .requests_per_second(1)
            .burst(1)
            .build()
            .unwrap();

        let extractor = CustomGrpcKeyExtractor::new(|_req| None::<String>);
        let layer = GrpcRateLimitLayer::with_extractor(limiter, extractor);
        let mut service = layer.layer(MockService);

        let req = http::Request::builder()
            .uri("http://example.com/test")
            .body(Body::default())
            .unwrap();

        let response = service.call(req).await.unwrap();
        assert_grpc_resource_exhausted(&response);
    }

    #[tokio::test]
    async fn test_rate_limit_service_no_key_fail_open() {
        let limiter = RateLimiter::builder()
            .requests_per_second(1)
            .burst(1)
            .build()
            .unwrap();

        let extractor = CustomGrpcKeyExtractor::new(|_req| None::<String>);
        let layer = GrpcRateLimitLayer::with_extractor(limiter, extractor).fail_open();
        let mut service = layer.layer(MockService);

        let req = http::Request::builder()
            .uri("http://example.com/test")
            .body(Body::default())
            .unwrap();

        let response = service.call(req).await.unwrap();
        assert_eq!(response.status(), http::StatusCode::OK);
        assert!(response.headers().get("grpc-status").is_none());
    }
}
