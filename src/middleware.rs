//! Axum / Tower HTTP middleware integration for rate limiting.
//!
//! Provides zero-allocation request key extraction, RFC-compliant IETF and legacy
//! `X-RateLimit-*` response headers, per-request cost hooks, custom rejection
//! responses, and proper Tower `Service::poll_ready` readiness tracking.
//!
//! # Example
//!
//! ```no_run
//! use axum::{Router, routing::get};
//! use tokio_rate_limit::{RateLimiter, middleware::RateLimitLayer};
//!
//! # async fn example() {
//! let limiter = RateLimiter::builder()
//!     .requests_per_second(100)
//!     .burst(200)
//!     .build()
//!     .unwrap();
//!
//! let app: Router = Router::new()
//!     .route("/", get(|| async { "Hello, World!" }))
//!     .layer(RateLimitLayer::new(limiter));
//! # }
//! ```

use crate::algorithm::internal::http_seconds_ceil;
pub use crate::algorithm::internal::StackIp;
use crate::algorithm::{Algorithm, RateLimitKey, ToKey, TokenBucket};
use crate::{IntoRateLimiter, RateLimitDecision, RateLimiter};
use axum::{
    body::Body,
    extract::ConnectInfo,
    http::{
        header::{HeaderName, InvalidHeaderName},
        HeaderValue, Request, Response, StatusCode,
    },
    response::IntoResponse,
};
use pin_project_lite::pin_project;
use std::borrow::{Borrow, Cow};
use std::fmt;
use std::future::Future;
use std::hash::Hash;
use std::net::{IpAddr, SocketAddr};
use std::ops::Deref;
use std::pin::Pin;
use std::task::{Context, Poll};
use tower::{Layer, Service};

const HDR_RATELIMIT_LIMIT: HeaderName = HeaderName::from_static("ratelimit-limit");
const HDR_RATELIMIT_REMAINING: HeaderName = HeaderName::from_static("ratelimit-remaining");
const HDR_RATELIMIT_RESET: HeaderName = HeaderName::from_static("ratelimit-reset");
const HDR_X_RATELIMIT_LIMIT: HeaderName = HeaderName::from_static("x-ratelimit-limit");
const HDR_X_RATELIMIT_REMAINING: HeaderName = HeaderName::from_static("x-ratelimit-remaining");
const HDR_X_RATELIMIT_RESET: HeaderName = HeaderName::from_static("x-ratelimit-reset");
const HDR_RETRY_AFTER: HeaderName = HeaderName::from_static("retry-after");

/// Borrowed, stack-formatted, or owned string key extracted from an HTTP request.
///
/// Using [`ExtractedKey::Borrowed`] or [`ExtractedKey::Ip`] avoids heap allocations
/// on the middleware hot path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExtractedKey<'a> {
    /// Borrowed string slice from request headers, URI, or extensions.
    Borrowed(&'a str),
    /// Stack-formatted IPv4 or IPv6 address.
    Ip(StackIp),
    /// Heap-allocated string (for dynamically formatted composite keys).
    Owned(String),
}

impl<'a> ExtractedKey<'a> {
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

impl Deref for ExtractedKey<'_> {
    type Target = str;

    #[inline]
    fn deref(&self) -> &str {
        self.as_str()
    }
}

impl<'a> From<&'a str> for ExtractedKey<'a> {
    #[inline]
    fn from(s: &'a str) -> Self {
        Self::Borrowed(s)
    }
}

impl From<String> for ExtractedKey<'static> {
    #[inline]
    fn from(s: String) -> Self {
        Self::Owned(s)
    }
}

impl<'a> From<Cow<'a, str>> for ExtractedKey<'a> {
    #[inline]
    fn from(c: Cow<'a, str>) -> Self {
        match c {
            Cow::Borrowed(s) => Self::Borrowed(s),
            Cow::Owned(s) => Self::Owned(s),
        }
    }
}

impl From<IpAddr> for ExtractedKey<'static> {
    #[inline]
    fn from(ip: IpAddr) -> Self {
        Self::Ip(StackIp::from_ip(ip))
    }
}

impl From<StackIp> for ExtractedKey<'static> {
    #[inline]
    fn from(ip: StackIp) -> Self {
        Self::Ip(ip)
    }
}

/// Trait bridging an extracted key representation to a borrowed lookup query on `K`.
pub trait LookupKey<K: RateLimitKey> {
    /// Borrowed query type (`str` for `String`-keyed limiters, `K` for typed limiters).
    type Query: Hash + Ord + fmt::Debug + ToKey<K> + ?Sized;

    /// Borrows `self` as `&Self::Query`.
    fn as_query(&self) -> &Self::Query;
}

impl<K: RateLimitKey> LookupKey<K> for K {
    type Query = K;

    #[inline]
    fn as_query(&self) -> &K {
        self
    }
}

impl LookupKey<String> for &str {
    type Query = str;

    #[inline]
    fn as_query(&self) -> &str {
        self
    }
}

impl LookupKey<String> for Cow<'_, str> {
    type Query = str;

    #[inline]
    fn as_query(&self) -> &str {
        self.as_ref()
    }
}

impl LookupKey<String> for StackIp {
    type Query = str;

    #[inline]
    fn as_query(&self) -> &str {
        self.as_str()
    }
}

impl LookupKey<String> for ExtractedKey<'_> {
    type Query = str;

    #[inline]
    fn as_query(&self) -> &str {
        self.as_str()
    }
}

/// A trait for extracting rate limit keys from HTTP requests.
///
/// Generic over request body type `B` (defaulting to [`axum::body::Body`]) and
/// limiter key type `K` (defaulting to `String`).
pub trait KeyExtractor<B = Body, K: RateLimitKey = String>: Send + Sync + 'static {
    /// Extracted key type, potentially borrowing from the request for `'a`.
    type Key<'a>: LookupKey<K>
    where
        B: 'a;

    /// Extracts a rate limit key from the incoming request, or `None` if unavailable.
    ///
    /// When `None` is returned, [`RateLimitLayer`] denies the request (`429`) unless
    /// [`RateLimitLayer::fail_open`] is enabled.
    fn extract<'a>(&'a self, req: &'a Request<B>) -> Option<Self::Key<'a>>;
}

/// Default key extractor using the peer client's IP address from `ConnectInfo<SocketAddr>`.
///
/// Supports both `RateLimiter<TokenBucket<String>>` (stack-formatted IP with zero heap
/// allocation) and `RateLimiter<TokenBucket<IpAddr>>` (raw `IpAddr` with zero formatting).
#[derive(Debug, Clone, Copy, Default)]
pub struct IpKeyExtractor;

impl IpKeyExtractor {
    /// Extracts the peer IP address from `ConnectInfo<SocketAddr>` onto a stack buffer.
    #[inline]
    pub fn extract<B>(&self, req: &Request<B>) -> Option<StackIp> {
        req.extensions()
            .get::<ConnectInfo<SocketAddr>>()
            .map(|ConnectInfo(addr)| StackIp::from_ip(addr.ip()))
    }
}

impl<B> KeyExtractor<B, String> for IpKeyExtractor {
    type Key<'a>
        = StackIp
    where
        B: 'a;

    #[inline]
    fn extract<'a>(&'a self, req: &'a Request<B>) -> Option<StackIp> {
        Self::extract(self, req)
    }
}

impl<B> KeyExtractor<B, IpAddr> for IpKeyExtractor {
    type Key<'a>
        = IpAddr
    where
        B: 'a;

    #[inline]
    fn extract<'a>(&'a self, req: &'a Request<B>) -> Option<IpAddr> {
        req.extensions()
            .get::<ConnectInfo<SocketAddr>>()
            .map(|ConnectInfo(addr)| addr.ip())
    }
}

/// Zero-allocation HTTP header key extractor.
///
/// Borrows the header's UTF-8 value directly as `&str` without allocating a `String`.
///
/// # Example
///
/// ```
/// use tokio_rate_limit::middleware::HeaderKeyExtractor;
///
/// let extractor = HeaderKeyExtractor::new("x-api-key");
/// ```
#[derive(Debug, Clone)]
pub struct HeaderKeyExtractor {
    header_name: HeaderName,
}

impl HeaderKeyExtractor {
    /// Creates a new header key extractor for `header_name`.
    ///
    /// # Panics
    ///
    /// Panics if `header_name` is not a valid HTTP header name.
    pub fn new(header_name: impl AsRef<str>) -> Self {
        Self::try_new(header_name).expect("valid HTTP header name")
    }

    /// Creates a new header key extractor from a lowercase `'static` header name.
    ///
    /// # Panics
    ///
    /// Panics if `header_name` contains uppercase characters or invalid header bytes.
    pub const fn from_static(header_name: &'static str) -> Self {
        Self {
            header_name: HeaderName::from_static(header_name),
        }
    }

    /// Fallibly creates a new header key extractor.
    pub fn try_new(header_name: impl AsRef<str>) -> Result<Self, InvalidHeaderName> {
        HeaderName::from_bytes(header_name.as_ref().as_bytes())
            .map(|header_name| Self { header_name })
    }
}

impl<B> KeyExtractor<B, String> for HeaderKeyExtractor {
    type Key<'a>
        = &'a str
    where
        B: 'a;

    #[inline]
    fn extract<'a>(&'a self, req: &'a Request<B>) -> Option<&'a str> {
        req.headers()
            .get(&self.header_name)
            .and_then(|v| v.to_str().ok())
            .filter(|s| !s.is_empty())
    }
}

/// A custom key extractor backed by a user-supplied closure.
///
/// # Example
///
/// ```no_run
/// use tokio_rate_limit::middleware::CustomKeyExtractor;
/// use axum::http::Request;
/// use axum::body::Body;
///
/// let extractor = CustomKeyExtractor::new(|req: &Request<Body>| {
///     req.headers()
///         .get("X-User-Id")
///         .and_then(|v| v.to_str().ok())
///         .map(|s| s.to_string())
/// });
/// ```
#[derive(Clone)]
pub struct CustomKeyExtractor<F> {
    extractor: F,
}

impl<F> fmt::Debug for CustomKeyExtractor<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CustomKeyExtractor").finish_non_exhaustive()
    }
}

impl<F> CustomKeyExtractor<F> {
    /// Creates a new custom key extractor from a closure returning `Option<K>`
    /// (for example `Option<String>`, `Option<IpAddr>`, or `Option<u64>`).
    pub fn new<B, K>(extractor: F) -> Self
    where
        K: RateLimitKey,
        F: Fn(&Request<B>) -> Option<K> + Send + Sync + Clone + 'static,
    {
        Self { extractor }
    }

    /// Creates a zero-allocation custom key extractor from a closure that borrows `&str`
    /// directly from the request.
    ///
    /// # Example
    ///
    /// ```
    /// use tokio_rate_limit::middleware::CustomKeyExtractor;
    /// use axum::http::Request;
    /// use axum::body::Body;
    ///
    /// let extractor = CustomKeyExtractor::borrowed(|req: &Request<Body>| {
    ///     req.headers().get("x-user-id")?.to_str().ok()
    /// });
    /// ```
    pub fn borrowed<B>(extractor: F) -> BorrowedKeyExtractor<F>
    where
        F: for<'a> Fn(&'a Request<B>) -> Option<&'a str> + Send + Sync + Clone + 'static,
    {
        BorrowedKeyExtractor { extractor }
    }
}

impl<B, K, F> KeyExtractor<B, K> for CustomKeyExtractor<F>
where
    K: RateLimitKey,
    F: Fn(&Request<B>) -> Option<K> + Send + Sync + Clone + 'static,
{
    type Key<'a>
        = K
    where
        B: 'a;

    #[inline]
    fn extract<'a>(&'a self, req: &'a Request<B>) -> Option<K> {
        (self.extractor)(req)
    }
}

/// Zero-allocation custom key extractor borrowing `&str` directly from `&Request<B>`.
#[derive(Clone)]
pub struct BorrowedKeyExtractor<F> {
    extractor: F,
}

impl<F> fmt::Debug for BorrowedKeyExtractor<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BorrowedKeyExtractor")
            .finish_non_exhaustive()
    }
}

impl<B, F> KeyExtractor<B, String> for BorrowedKeyExtractor<F>
where
    F: for<'a> Fn(&'a Request<B>) -> Option<&'a str> + Send + Sync + Clone + 'static,
{
    type Key<'a>
        = &'a str
    where
        B: 'a;

    #[inline]
    fn extract<'a>(&'a self, req: &'a Request<B>) -> Option<&'a str> {
        (self.extractor)(req)
    }
}

/// Per-request token cost function used by [`RateLimitLayer`].
pub trait CostExtractor<B = Body>: Send + Sync + 'static {
    /// Computes the token cost for `req`.
    fn cost(&self, req: &Request<B>) -> u64;
}

/// Default unit cost (`1` token per request).
#[derive(Debug, Clone, Copy, Default)]
pub struct UnitCost;

impl<B> CostExtractor<B> for UnitCost {
    #[inline]
    fn cost(&self, _req: &Request<B>) -> u64 {
        1
    }
}

/// Closure-backed cost extractor created via [`RateLimitLayer::with_cost`].
#[derive(Clone)]
pub struct CustomCostExtractor<F> {
    cost_fn: F,
}

impl<F> fmt::Debug for CustomCostExtractor<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CustomCostExtractor")
            .finish_non_exhaustive()
    }
}

impl<B, F> CostExtractor<B> for CustomCostExtractor<F>
where
    F: Fn(&Request<B>) -> u64 + Send + Sync + Clone + 'static,
{
    #[inline]
    fn cost(&self, req: &Request<B>) -> u64 {
        (self.cost_fn)(req)
    }
}

/// Customizes the HTTP response returned when a request is rate limited.
pub trait RejectionHandler<RespBody = Body>: Send + Sync + 'static {
    /// Produces an HTTP response for a denied [`RateLimitDecision`].
    fn reject(&self, decision: &RateLimitDecision) -> Response<RespBody>;

    /// Produces an HTTP response when key extraction returns `None` in fail-closed mode.
    fn missing_key(&self) -> Response<RespBody>;
}

/// Default `429 Too Many Requests` plain-text response with standard rate limit headers.
#[derive(Debug, Clone, Copy, Default)]
pub struct DefaultRejectionHandler;

impl RejectionHandler<Body> for DefaultRejectionHandler {
    #[inline]
    fn reject(&self, decision: &RateLimitDecision) -> Response<Body> {
        rate_limit_response(decision)
    }

    #[inline]
    fn missing_key(&self) -> Response<Body> {
        missing_key_response()
    }
}

/// Closure-backed rejection handler created via [`RateLimitLayer::on_rejected`].
#[derive(Clone)]
pub struct CustomRejectionHandler<F> {
    handler: F,
}

impl<F> fmt::Debug for CustomRejectionHandler<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CustomRejectionHandler")
            .finish_non_exhaustive()
    }
}

impl<F> RejectionHandler<Body> for CustomRejectionHandler<F>
where
    F: Fn(&RateLimitDecision) -> Response<Body> + Send + Sync + Clone + 'static,
{
    #[inline]
    fn reject(&self, decision: &RateLimitDecision) -> Response<Body> {
        let mut resp = (self.handler)(decision);
        apply_rejection_headers(&mut resp, decision);
        resp
    }

    #[inline]
    fn missing_key(&self) -> Response<Body> {
        let decision = RateLimitDecision {
            permitted: false,
            retry_after: Some(std::time::Duration::from_secs(1)),
            remaining: None,
            limit: 0,
            reset: None,
        };
        let mut resp = (self.handler)(&decision);
        resp.headers_mut()
            .entry(HDR_RETRY_AFTER)
            .or_insert_with(|| HeaderValue::from(1u64));
        resp
    }
}

/// Tower layer for adding rate limiting to an Axum or Tower HTTP service.
///
/// Accepts either a [`RateLimiter`] directly or an `Arc<RateLimiter>`.
pub struct RateLimitLayer<
    A = TokenBucket,
    E = IpKeyExtractor,
    C = UnitCost,
    R = DefaultRejectionHandler,
> where
    A: Algorithm,
{
    limiter: RateLimiter<A>,
    extractor: E,
    cost_extractor: C,
    rejection_handler: R,
    fail_open: bool,
}

impl<A, E, C, R> Clone for RateLimitLayer<A, E, C, R>
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

impl<A, E, C, R> fmt::Debug for RateLimitLayer<A, E, C, R>
where
    A: Algorithm,
    E: fmt::Debug,
    C: fmt::Debug,
    R: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RateLimitLayer")
            .field("limiter", &self.limiter)
            .field("extractor", &self.extractor)
            .field("cost_extractor", &self.cost_extractor)
            .field("rejection_handler", &self.rejection_handler)
            .field("fail_open", &self.fail_open)
            .finish()
    }
}

impl<A: Algorithm> RateLimitLayer<A, IpKeyExtractor, UnitCost, DefaultRejectionHandler> {
    /// Creates a new rate limit layer with default IP-based key extraction.
    ///
    /// Missing keys deny the request (`429`) by default; call [`fail_open`](Self::fail_open)
    /// to pass through requests when no key can be extracted.
    pub fn new(limiter: impl IntoRateLimiter<A>) -> Self {
        Self {
            limiter: limiter.into_rate_limiter(),
            extractor: IpKeyExtractor,
            cost_extractor: UnitCost,
            rejection_handler: DefaultRejectionHandler,
            fail_open: false,
        }
    }
}

impl<A: Algorithm, E> RateLimitLayer<A, E, UnitCost, DefaultRejectionHandler> {
    /// Creates a new rate limit layer with a custom [`KeyExtractor`].
    pub fn with_extractor(limiter: impl IntoRateLimiter<A>, extractor: E) -> Self {
        Self {
            limiter: limiter.into_rate_limiter(),
            extractor,
            cost_extractor: UnitCost,
            rejection_handler: DefaultRejectionHandler,
            fail_open: false,
        }
    }
}

impl<A, E, C, R> RateLimitLayer<A, E, C, R>
where
    A: Algorithm,
{
    /// Passes through requests when the key extractor returns `None` (default is fail-closed `429`).
    pub fn fail_open(mut self) -> Self {
        self.fail_open = true;
        self
    }

    /// Configures a per-request token cost function (enabling weighted HTTP endpoints).
    ///
    /// # Example
    ///
    /// ```
    /// use axum::http::{Method, Request};
    /// use axum::body::Body;
    /// use tokio_rate_limit::{RateLimiter, middleware::RateLimitLayer};
    ///
    /// let limiter = RateLimiter::builder().requests_per_second(100).burst(200).build().unwrap();
    /// let layer = RateLimitLayer::new(limiter).with_cost(|req: &Request<Body>| {
    ///     if req.method() == Method::POST { 5 } else { 1 }
    /// });
    /// ```
    pub fn with_cost<F, B>(self, cost_fn: F) -> RateLimitLayer<A, E, CustomCostExtractor<F>, R>
    where
        F: Fn(&Request<B>) -> u64 + Send + Sync + Clone + 'static,
    {
        RateLimitLayer {
            limiter: self.limiter,
            extractor: self.extractor,
            cost_extractor: CustomCostExtractor { cost_fn },
            rejection_handler: self.rejection_handler,
            fail_open: self.fail_open,
        }
    }

    /// Customizes the HTTP response returned when a request is rate limited (e.g. JSON error body).
    ///
    /// Standard rate limit headers (`RateLimit-*`, `X-RateLimit-*`, `Retry-After`) are
    /// automatically populated if not already present on the returned response.
    pub fn on_rejected<F>(self, handler: F) -> RateLimitLayer<A, E, C, CustomRejectionHandler<F>>
    where
        F: Fn(&RateLimitDecision) -> Response<Body> + Send + Sync + Clone + 'static,
    {
        RateLimitLayer {
            limiter: self.limiter,
            extractor: self.extractor,
            cost_extractor: self.cost_extractor,
            rejection_handler: CustomRejectionHandler { handler },
            fail_open: self.fail_open,
        }
    }
}

impl<S, A, E, C, R> Layer<S> for RateLimitLayer<A, E, C, R>
where
    A: Algorithm,
    E: Clone,
    C: Clone,
    R: Clone,
{
    type Service = RateLimitService<S, A, E, C, R>;

    fn layer(&self, inner: S) -> Self::Service {
        RateLimitService {
            inner,
            limiter: self.limiter.clone(),
            extractor: self.extractor.clone(),
            cost_extractor: self.cost_extractor.clone(),
            rejection_handler: self.rejection_handler.clone(),
            fail_open: self.fail_open,
        }
    }
}

/// The rate limiting Tower [`Service`].
pub struct RateLimitService<
    S,
    A = TokenBucket,
    E = IpKeyExtractor,
    C = UnitCost,
    R = DefaultRejectionHandler,
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

impl<S, A, E, C, R> Clone for RateLimitService<S, A, E, C, R>
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

impl<S, A, E, C, R> fmt::Debug for RateLimitService<S, A, E, C, R>
where
    S: fmt::Debug,
    A: Algorithm,
    E: fmt::Debug,
    C: fmt::Debug,
    R: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RateLimitService")
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
    /// Unboxed response future returned by [`RateLimitService`].
    pub struct RateLimitFuture<F, RespBody = Body> {
        #[pin]
        state: FutureState<F, RespBody>,
    }
}

pin_project! {
    #[project = FutureStateProj]
    enum FutureState<F, RespBody> {
        PassThrough {
            #[pin]
            future: F,
            decision: Option<RateLimitDecision>,
        },
        Immediate {
            response: Option<Response<RespBody>>,
        },
    }
}

impl<F, RespBody, Err> Future for RateLimitFuture<F, RespBody>
where
    F: Future<Output = Result<Response<RespBody>, Err>>,
{
    type Output = Result<Response<RespBody>, Err>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.project().state.project() {
            FutureStateProj::PassThrough { future, decision } => match future.poll(cx) {
                Poll::Ready(Ok(mut response)) => {
                    if let Some(dec) = decision.take() {
                        add_rate_limit_headers(&mut response, &dec);
                    }
                    Poll::Ready(Ok(response))
                }
                Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
                Poll::Pending => Poll::Pending,
            },
            FutureStateProj::Immediate { response } => Poll::Ready(Ok(response
                .take()
                .expect("RateLimitFuture polled after completion"))),
        }
    }
}

impl<S, ReqBody, RespBody, A, E, C, R> Service<Request<ReqBody>> for RateLimitService<S, A, E, C, R>
where
    S: Service<Request<ReqBody>, Response = Response<RespBody>> + Clone,
    A: Algorithm + 'static,
    E: KeyExtractor<ReqBody, A::Key>,
    for<'a> A::Key: Borrow<<E::Key<'a> as LookupKey<A::Key>>::Query>,
    C: CostExtractor<ReqBody>,
    R: RejectionHandler<RespBody>,
{
    type Response = Response<RespBody>;
    type Error = S::Error;
    type Future = RateLimitFuture<S::Future, RespBody>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<ReqBody>) -> Self::Future {
        let maybe_decision = <E as KeyExtractor<ReqBody, A::Key>>::extract(&self.extractor, &req)
            .map(|extracted| {
                let cost = self.cost_extractor.cost(&req);
                self.limiter.check_with_cost(extracted.as_query(), cost)
            });

        let Some(decision) = maybe_decision else {
            if self.fail_open {
                let clone = self.inner.clone();
                let mut inner = std::mem::replace(&mut self.inner, clone);
                return RateLimitFuture {
                    state: FutureState::PassThrough {
                        future: inner.call(req),
                        decision: None,
                    },
                };
            }
            #[cfg(feature = "metrics-support")]
            metrics::counter!("tokio_rate_limit.requests.denied").increment(1);

            return RateLimitFuture {
                state: FutureState::Immediate {
                    response: Some(self.rejection_handler.missing_key()),
                },
            };
        };

        if decision.permitted {
            // Swap out the readied `self.inner` instance per the Tower Service contract
            // so stateful inner layers (Buffer, ConcurrencyLimit, etc.) never panic.
            let clone = self.inner.clone();
            let mut inner = std::mem::replace(&mut self.inner, clone);
            RateLimitFuture {
                state: FutureState::PassThrough {
                    future: inner.call(req),
                    decision: Some(decision),
                },
            }
        } else {
            RateLimitFuture {
                state: FutureState::Immediate {
                    response: Some(self.rejection_handler.reject(&decision)),
                },
            }
        }
    }
}

/// Adds IETF standard and legacy `X-RateLimit-*` headers without heap string formatting.
fn add_rate_limit_headers<B>(response: &mut Response<B>, decision: &RateLimitDecision) {
    let headers = response.headers_mut();

    let limit_val = HeaderValue::from(decision.limit);
    headers.insert(HDR_RATELIMIT_LIMIT, limit_val.clone());
    headers.insert(HDR_X_RATELIMIT_LIMIT, limit_val);

    if let Some(remaining) = decision.remaining {
        let rem_val = HeaderValue::from(remaining);
        headers.insert(HDR_RATELIMIT_REMAINING, rem_val.clone());
        headers.insert(HDR_X_RATELIMIT_REMAINING, rem_val);
    }

    if let Some(reset) = decision.reset {
        let reset_val = HeaderValue::from(http_seconds_ceil(reset));
        headers.insert(HDR_RATELIMIT_RESET, reset_val.clone());
        headers.insert(HDR_X_RATELIMIT_RESET, reset_val);
    }
}

fn apply_rejection_headers<B>(response: &mut Response<B>, decision: &RateLimitDecision) {
    let headers = response.headers_mut();

    let limit_val = HeaderValue::from(decision.limit);
    headers
        .entry(HDR_RATELIMIT_LIMIT)
        .or_insert_with(|| limit_val.clone());
    headers.entry(HDR_X_RATELIMIT_LIMIT).or_insert(limit_val);

    if let Some(remaining) = decision.remaining {
        let rem_val = HeaderValue::from(remaining);
        headers
            .entry(HDR_RATELIMIT_REMAINING)
            .or_insert_with(|| rem_val.clone());
        headers.entry(HDR_X_RATELIMIT_REMAINING).or_insert(rem_val);
    }

    if let Some(reset) = decision.reset {
        let reset_val = HeaderValue::from(http_seconds_ceil(reset));
        headers
            .entry(HDR_RATELIMIT_RESET)
            .or_insert_with(|| reset_val.clone());
        headers.entry(HDR_X_RATELIMIT_RESET).or_insert(reset_val);
    }

    if let Some(retry_after) = decision.retry_after {
        let seconds = http_seconds_ceil(retry_after);
        headers
            .entry(HDR_RETRY_AFTER)
            .or_insert_with(|| HeaderValue::from(seconds));
    }
}

fn rate_limit_response(decision: &RateLimitDecision) -> Response<Body> {
    let mut response = (StatusCode::TOO_MANY_REQUESTS, "Rate limit exceeded").into_response();
    apply_rejection_headers(&mut response, decision);
    response
}

fn missing_key_response() -> Response<Body> {
    let mut response = (StatusCode::TOO_MANY_REQUESTS, "Rate limit exceeded").into_response();
    response
        .headers_mut()
        .insert(HDR_RETRY_AFTER, HeaderValue::from(1u64));
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Body,
        http::{Method, Request, StatusCode},
        routing::{get, post},
        Router,
    };
    use std::sync::Arc;
    use tower::limit::ConcurrencyLimitLayer;
    use tower::ServiceExt;

    #[tokio::test]
    async fn test_rate_limit_allows_within_limit() {
        let limiter = RateLimiter::builder()
            .requests_per_second(10)
            .burst(10)
            .build()
            .unwrap();

        let app = Router::new().route("/", get(|| async { "Hello" })).layer(
            RateLimitLayer::with_extractor(
                limiter,
                CustomKeyExtractor::new(|_| Some("test-key".to_string())),
            ),
        );

        let response = app
            .clone()
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.headers().contains_key("X-RateLimit-Limit"));
        assert!(response.headers().contains_key("X-RateLimit-Remaining"));
        assert!(response.headers().contains_key("X-RateLimit-Reset"));
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

        for _ in 0..2 {
            let response = app
                .clone()
                .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
        }

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
    async fn test_header_key_extractor_zero_alloc() {
        let limiter = RateLimiter::builder()
            .requests_per_second(1)
            .burst(1)
            .build()
            .unwrap();

        let app = Router::new().route("/", get(|| async { "Hello" })).layer(
            RateLimitLayer::with_extractor(limiter, HeaderKeyExtractor::from_static("x-user-id")),
        );

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

    #[tokio::test]
    async fn test_typed_ip_key_limiter_with_ip_extractor() {
        let limiter = RateLimiter::builder()
            .requests_per_second(1)
            .burst(1)
            .build_with_key::<IpAddr>()
            .unwrap();

        let app = Router::new()
            .route("/", get(|| async { "Hello" }))
            .layer(RateLimitLayer::new(limiter));

        let addr: SocketAddr = "192.0.2.10:12345".parse().unwrap();
        let mut req1 = Request::builder().uri("/").body(Body::empty()).unwrap();
        req1.extensions_mut().insert(ConnectInfo(addr));
        assert_eq!(
            app.clone().oneshot(req1).await.unwrap().status(),
            StatusCode::OK
        );

        let mut req2 = Request::builder().uri("/").body(Body::empty()).unwrap();
        req2.extensions_mut().insert(ConnectInfo(addr));
        assert_eq!(
            app.clone().oneshot(req2).await.unwrap().status(),
            StatusCode::TOO_MANY_REQUESTS
        );
    }

    #[tokio::test]
    async fn test_with_cost_and_on_rejected_hooks() {
        let limiter = RateLimiter::builder()
            .requests_per_second(10)
            .burst(5)
            .build()
            .unwrap();

        let layer =
            RateLimitLayer::with_extractor(limiter, HeaderKeyExtractor::from_static("x-api-key"))
                .with_cost(|req: &Request<Body>| if req.method() == Method::POST { 5 } else { 1 })
                .on_rejected(|decision| {
                    (
                        StatusCode::TOO_MANY_REQUESTS,
                        format!("custom-reject:limit={}", decision.limit),
                    )
                        .into_response()
                });

        let app = Router::new()
            .route("/", get(|| async { "ok" }).post(|| async { "created" }))
            .layer(layer);

        // POST consumes all 5 tokens at once.
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/")
                    .header("x-api-key", "k1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(res.headers().get("ratelimit-remaining").unwrap(), "0");

        // Next GET is rejected with custom body + standard headers.
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/")
                    .header("x-api-key", "k1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::TOO_MANY_REQUESTS);
        assert!(res.headers().contains_key("retry-after"));
        let body_bytes = http_body_util::BodyExt::collect(res.into_body())
            .await
            .unwrap()
            .to_bytes();
        assert_eq!(&body_bytes[..], b"custom-reject:limit=5");
    }

    #[tokio::test]
    async fn test_composes_with_stateful_concurrency_limit_without_panic() {
        // Regression test for Tower `poll_ready` clone panic:
        // `ConcurrencyLimit` panics if an un-readied clone is called instead of the readied instance!
        let limiter = RateLimiter::builder()
            .requests_per_second(10)
            .burst(2)
            .build()
            .unwrap();

        let inner = tower::ServiceBuilder::new()
            .layer(ConcurrencyLimitLayer::new(4))
            .service(Router::new().route("/", post(|| async { "ok" })));

        let mut svc = RateLimitLayer::with_extractor(
            limiter,
            CustomKeyExtractor::borrowed(|_: &Request<Body>| Some("k")),
        )
        .layer(inner);

        for expected in [
            StatusCode::OK,
            StatusCode::OK,
            StatusCode::TOO_MANY_REQUESTS,
        ] {
            let resp = svc
                .ready()
                .await
                .unwrap()
                .call(
                    Request::builder()
                        .method(Method::POST)
                        .uri("/")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(resp.status(), expected);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn retry_after_header_is_at_least_one_second() {
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

        for _ in 0..10 {
            let response = app
                .clone()
                .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
        }

        let response = app
            .clone()
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        let retry_after = response
            .headers()
            .get("Retry-After")
            .unwrap()
            .to_str()
            .unwrap();
        assert_eq!(
            retry_after, "1",
            "sub-second wait must round up to 1s, not 0"
        );
    }

    #[tokio::test]
    async fn missing_key_fails_closed() {
        let limiter = RateLimiter::builder()
            .requests_per_second(100)
            .burst(100)
            .build()
            .unwrap();

        let app = Router::new()
            .route("/", get(|| async { "Hello" }))
            .layer(RateLimitLayer::new(limiter));

        let response = app
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    }

    #[tokio::test]
    async fn missing_key_fail_open_opt_in() {
        let limiter = RateLimiter::builder()
            .requests_per_second(100)
            .burst(100)
            .build()
            .unwrap();

        let app = Router::new()
            .route("/", get(|| async { "Hello" }))
            .layer(RateLimitLayer::new(limiter).fail_open());

        let response = app
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }
}
