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
use crate::{IntoRateLimiter, Ipv6Prefix, RateLimitDecision, RateLimiter};
use http::{header::HeaderName, HeaderValue};
use pin_project_lite::pin_project;
use std::borrow::{Borrow, Cow};
use std::fmt;
use std::future::Future;
use std::hash::Hash;
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::ops::Deref;
use std::pin::Pin;
use std::task::{Context, Poll};
use tonic::body::Body;
use tonic::transport::server::TcpConnectInfo;
use tonic::Status;
use tower::{Layer, Service};

const HDR_X_RATELIMIT_LIMIT: HeaderName = HeaderName::from_static("x-ratelimit-limit");
const HDR_X_RATELIMIT_REMAINING: HeaderName = HeaderName::from_static("x-ratelimit-remaining");
const HDR_X_RATELIMIT_RESET: HeaderName = HeaderName::from_static("x-ratelimit-reset");
const HDR_RETRY_AFTER: HeaderName = HeaderName::from_static("retry-after");
const HDR_X_FORWARDED_FOR: HeaderName = HeaderName::from_static("x-forwarded-for");
const HDR_X_REAL_IP: HeaderName = HeaderName::from_static("x-real-ip");

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

#[inline]
fn peer_ip(req: &http::Request<Body>) -> Option<IpAddr> {
    // Check a bare `SocketAddr` extension first so server-side middleware
    // (such as a PROXY protocol layer) can supply an explicit peer address
    // override, then fall back to Tonic's `TcpConnectInfo`.
    if let Some(addr) = req.extensions().get::<SocketAddr>() {
        return Some(addr.ip());
    }

    // `tonic::transport::Server` attaches `TcpConnectInfo` on every TCP request,
    // including connections accepted via `Server::tls_config`: tonic's
    // `ConnectInfo` layer (tonic 0.14.6 `src/transport/server/service/io.rs`,
    // `ServerIoConnectInfo::TlsIo`) inserts both the unwrapped inner
    // `TcpConnectInfo` and `TlsConnectInfo<TcpConnectInfo>`.
    // Custom `serve_with_incoming` TLS streams that attach only
    // `TlsConnectInfo<TcpConnectInfo>` return `None` here.
    req.extensions()
        .get::<TcpConnectInfo>()
        .and_then(|info| info.remote_addr())
        .map(|addr| addr.ip())
}

#[inline]
fn parse_forwarded_ip(raw_header: &[u8]) -> Option<IpAddr> {
    let last = raw_header.rsplit(|&b| b == b',').next()?;
    let token = std::str::from_utf8(last).ok()?.trim_ascii();
    parse_ip_token(token)
}

#[inline]
fn parse_ip_token(token: &str) -> Option<IpAddr> {
    if let Ok(ip) = token.parse::<IpAddr>() {
        return Some(ip);
    }
    if let Ok(addr) = token.parse::<SocketAddr>() {
        return Some(addr.ip());
    }
    if let Some(inner) = token
        .strip_prefix('[')
        .and_then(|rest| rest.strip_suffix(']'))
    {
        return inner.parse::<Ipv6Addr>().ok().map(IpAddr::V6);
    }
    None
}

/// Key extractor that uses the connection's peer IP address for gRPC rate limiting.
///
/// Checks a bare [`SocketAddr`] request extension first (allowing server-side layers such as
/// a PROXY protocol handler to supply an explicit peer override), then falls back to
/// [`tonic::transport::server::TcpConnectInfo::remote_addr`]. Forwarding headers
/// (`x-forwarded-for`, `x-real-ip`) are never read; deployments behind a trusted reverse
/// proxy should use [`ForwardedIpKeyExtractor`].
///
/// Supports both `RateLimiter<TokenBucket<String>>` (stack-formatted IP with zero heap
/// allocation) and `RateLimiter<TokenBucket<IpAddr>>` (raw `IpAddr` with zero formatting).
///
/// # TLS and non-TCP transports
///
/// Works with both plaintext TCP and Tonic's built-in TLS (`Server::tls_config`), because
/// Tonic inserts the underlying [`TcpConnectInfo`] extension alongside
/// `TlsConnectInfo<TcpConnectInfo>`. Transports without [`TcpConnectInfo`] — such as Unix
/// domain sockets or custom `serve_with_incoming` TLS streams that attach only
/// `TlsConnectInfo<TcpConnectInfo>` — return `None` (failing closed unless
/// [`GrpcRateLimitLayer::fail_open`] is enabled); those servers should insert a bare
/// [`SocketAddr`] or [`TcpConnectInfo`] in an outer layer or use [`CustomGrpcKeyExtractor`].
///
/// # IPv6 considerations
///
/// Keys use the full IP address by default. Opt in to prefix masking with
/// [`Self::with_ipv6_prefix`] so users sharing a prefix share a bucket. Use `/64` for a
/// typical end-site subnet or `/56` or `/48` to group a whole customer allocation.
/// With masking enabled, IPv4-mapped IPv6 addresses become IPv4 keys.
#[derive(Debug, Clone, Copy, Default)]
pub struct IpKeyExtractor;

impl IpKeyExtractor {
    /// Returns a peer IP extractor that groups IPv6 addresses by the given prefix.
    #[must_use]
    pub const fn with_ipv6_prefix(self, ipv6_prefix: Ipv6Prefix) -> PrefixIpKeyExtractor {
        PrefixIpKeyExtractor::new(ipv6_prefix)
    }

    /// Extracts the connection's peer IP address from a bare [`SocketAddr`] extension or
    /// [`TcpConnectInfo`] with zero heap allocation.
    #[inline]
    pub fn extract<'a>(&'a self, req: &'a http::Request<Body>) -> Option<GrpcExtractedKey<'a>> {
        peer_ip(req).map(|ip| GrpcExtractedKey::Ip(StackIp::from_ip(ip)))
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
        peer_ip(req)
    }
}

/// Peer IP key extractor with opt-in IPv6 prefix masking for gRPC.
///
/// Reads a bare [`SocketAddr`] extension or falls back to [`TcpConnectInfo`], then applies
/// [`Ipv6Prefix::mask`]. Like [`IpKeyExtractor`], it only uses connection information.
/// A limiter can use the resulting address directly as an [`IpAddr`] key or stack-format
/// it as a `String` key with zero heap allocation during extraction.
///
/// ```
/// use std::net::IpAddr;
/// use tokio_rate_limit::{Ipv6Prefix, RateLimiter};
/// use tokio_rate_limit::tonic_middleware::{GrpcRateLimitLayer, PrefixIpKeyExtractor};
///
/// let extractor = PrefixIpKeyExtractor::new(Ipv6Prefix::SLASH_64);
/// let config = RateLimiter::builder().per_minute(60).burst(10);
/// let string_layer = GrpcRateLimitLayer::with_extractor(config.build().unwrap(), extractor);
/// let ip_layer =
///     GrpcRateLimitLayer::with_extractor(config.build_with_key::<IpAddr>().unwrap(), extractor);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrefixIpKeyExtractor {
    ipv6_prefix: Ipv6Prefix,
}

impl PrefixIpKeyExtractor {
    /// Creates an extractor that masks IPv6 peer addresses to the given prefix.
    pub const fn new(ipv6_prefix: Ipv6Prefix) -> Self {
        Self { ipv6_prefix }
    }

    /// Returns the configured IPv6 prefix.
    pub const fn ipv6_prefix(&self) -> Ipv6Prefix {
        self.ipv6_prefix
    }

    /// Extracts and masks the connection's peer IP address with zero heap allocation.
    #[inline]
    pub fn extract<'a>(&'a self, req: &'a http::Request<Body>) -> Option<GrpcExtractedKey<'a>> {
        self.extract_ip(req)
            .map(|ip| GrpcExtractedKey::Ip(StackIp::from_ip(ip)))
    }

    #[inline]
    fn extract_ip(&self, req: &http::Request<Body>) -> Option<IpAddr> {
        peer_ip(req).map(|ip| self.ipv6_prefix.mask(ip))
    }
}

impl GrpcKeyExtractor<String> for PrefixIpKeyExtractor {
    type Key<'a> = GrpcExtractedKey<'a>;

    #[inline]
    fn extract<'a>(&'a self, req: &'a http::Request<Body>) -> Option<GrpcExtractedKey<'a>> {
        Self::extract(self, req)
    }
}

impl GrpcKeyExtractor<IpAddr> for PrefixIpKeyExtractor {
    type Key<'a> = IpAddr;

    #[inline]
    fn extract<'a>(&'a self, req: &'a http::Request<Body>) -> Option<IpAddr> {
        self.extract_ip(req)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ForwardedHeaderSource {
    XForwardedFor,
    XRealIp,
}

/// Opt-in key extractor that reads the client IP address from a single configured
/// reverse-proxy forwarding header for gRPC rate limiting.
///
/// Choose the header that your nearest trusted reverse proxy manages:
/// - [`ForwardedIpKeyExtractor::x_forwarded_for`] (also [`ForwardedIpKeyExtractor::new`] and
///   [`Default`]): reads **only** `x-forwarded-for`.
/// - [`ForwardedIpKeyExtractor::x_real_ip`]: reads **only** `x-real-ip`.
///
/// There is no cascade or fallback between `x-forwarded-for` and `x-real-ip`. For the
/// configured header, the extractor reads the **last** header field and, within it, the
/// **last** comma-separated element (scanning raw bytes so invalid bytes earlier in the line
/// cannot invalidate the proxy's hop), trims ASCII whitespace, and parses `ip`, `ip:port`,
/// `[v6]`, or `[v6]:port` into an [`IpAddr`].
///
/// - If the configured header is **present** but its last element is empty, non-UTF-8, or
///   not a valid IP address, extraction returns `None` so the layer fails closed unless
///   [`GrpcRateLimitLayer::fail_open`] is enabled.
/// - If the configured header is **absent**, extraction falls back to the connection's peer
///   address (bare [`SocketAddr`] extension first, then [`TcpConnectInfo::remote_addr`]).
///
/// Both `GrpcKeyExtractor<String>` (stack-formatted via [`StackIp::from_ip`], zero heap
/// allocation) and `GrpcKeyExtractor<IpAddr>` use the same client address after extraction
/// and any configured masking.
///
/// # Proxy contract
///
/// Only use `ForwardedIpKeyExtractor` when the gRPC server is not directly reachable by
/// untrusted clients and **every** request passes through a trusted proxy directly in front
/// of this server:
/// - For [`ForwardedIpKeyExtractor::x_forwarded_for`], the proxy directly in front of this
///   server must either overwrite `x-forwarded-for` with the client IP, or append the client
///   IP as the final element of the final `x-forwarded-for` field.
/// - For [`ForwardedIpKeyExtractor::x_real_ip`], the proxy must overwrite `x-real-ip` on
///   every request.
/// - In multi-proxy chains, configure the nearest proxy so the final element of the
///   configured header is the real client IP.
/// - The value the proxy writes must be a bare IP address, or `v4:port` / `[v6]:port` when
///   a port is included (an unbracketed IPv6 address with a port is ambiguous); IPv6 zone IDs
///   are not supported.
///
/// Directly reachable servers must use [`IpKeyExtractor`] instead.
///
/// # IPv6 considerations
///
/// Keys use the full IP address by default. Opt in to prefix masking with
/// [`Self::with_ipv6_prefix`] so users sharing a prefix share a bucket. Use `/64` for a
/// typical end-site subnet or `/56` or `/48` to group a whole customer allocation.
/// With masking enabled, IPv4-mapped IPv6 addresses become IPv4 keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ForwardedIpKeyExtractor {
    source: ForwardedHeaderSource,
    ipv6_prefix: Option<Ipv6Prefix>,
}

impl ForwardedIpKeyExtractor {
    /// Creates an extractor that reads only the `x-forwarded-for` header.
    ///
    /// Equivalent to [`ForwardedIpKeyExtractor::x_forwarded_for`].
    pub const fn new() -> Self {
        Self::x_forwarded_for()
    }

    /// Creates an extractor that reads only the `x-forwarded-for` header.
    pub const fn x_forwarded_for() -> Self {
        Self {
            source: ForwardedHeaderSource::XForwardedFor,
            ipv6_prefix: None,
        }
    }

    /// Creates an extractor that reads only the `x-real-ip` header.
    pub const fn x_real_ip() -> Self {
        Self {
            source: ForwardedHeaderSource::XRealIp,
            ipv6_prefix: None,
        }
    }

    /// Applies [`Ipv6Prefix::mask`] to the address from the configured header or peer fallback.
    #[must_use]
    pub const fn with_ipv6_prefix(mut self, ipv6_prefix: Ipv6Prefix) -> Self {
        self.ipv6_prefix = Some(ipv6_prefix);
        self
    }

    /// Returns the configured IPv6 prefix, or `None` when masking is disabled.
    pub const fn ipv6_prefix(&self) -> Option<Ipv6Prefix> {
        self.ipv6_prefix
    }

    #[inline]
    const fn header_name(&self) -> HeaderName {
        match self.source {
            ForwardedHeaderSource::XForwardedFor => HDR_X_FORWARDED_FOR,
            ForwardedHeaderSource::XRealIp => HDR_X_REAL_IP,
        }
    }

    #[inline]
    fn extract_ip(&self, req: &http::Request<Body>) -> Option<IpAddr> {
        let ip = match req.headers().get_all(self.header_name()).iter().next_back() {
            Some(last_field) => parse_forwarded_ip(last_field.as_bytes()),
            None => peer_ip(req),
        }?;
        Some(match self.ipv6_prefix {
            Some(prefix) => prefix.mask(ip),
            None => ip,
        })
    }

    /// Extracts the client IP address from the configured forwarding header, falling back
    /// to the connection's peer address when that header is absent. Applies the IPv6 prefix
    /// mask when configured.
    ///
    /// Returns `None` when the configured header is present but its last comma-separated
    /// element cannot be parsed as an IP address.
    #[inline]
    pub fn extract<'a>(&'a self, req: &'a http::Request<Body>) -> Option<GrpcExtractedKey<'a>> {
        self.extract_ip(req)
            .map(|ip| GrpcExtractedKey::Ip(StackIp::from_ip(ip)))
    }
}

impl Default for ForwardedIpKeyExtractor {
    fn default() -> Self {
        Self::x_forwarded_for()
    }
}

impl GrpcKeyExtractor<String> for ForwardedIpKeyExtractor {
    type Key<'a> = GrpcExtractedKey<'a>;

    #[inline]
    fn extract<'a>(&'a self, req: &'a http::Request<Body>) -> Option<GrpcExtractedKey<'a>> {
        Self::extract(self, req)
    }
}

impl GrpcKeyExtractor<IpAddr> for ForwardedIpKeyExtractor {
    type Key<'a> = IpAddr;

    #[inline]
    fn extract<'a>(&'a self, req: &'a http::Request<Body>) -> Option<IpAddr> {
        self.extract_ip(req)
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
    ///
    /// Note that transports without [`TcpConnectInfo`] (such as Unix domain sockets or custom
    /// `serve_with_incoming` TLS streams that do not attach `TcpConnectInfo` or a bare
    /// [`SocketAddr`] extension) have no peer address, so combining [`IpKeyExtractor`] with
    /// `fail_open()` applies no rate limit to those requests. Similarly, combining
    /// [`ForwardedIpKeyExtractor`] with `fail_open()` passes through requests whose configured
    /// forwarding header is malformed.
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

    fn assert_ip_extract_eq<E>(extractor: &E, req: &http::Request<Body>, expected: Option<IpAddr>)
    where
        E: for<'a> GrpcKeyExtractor<String, Key<'a> = GrpcExtractedKey<'a>>
            + for<'a> GrpcKeyExtractor<IpAddr, Key<'a> = IpAddr>,
    {
        let ip_key = GrpcKeyExtractor::<IpAddr>::extract(extractor, req);
        let str_key = GrpcKeyExtractor::<String>::extract(extractor, req);
        assert_eq!(ip_key, expected);
        assert_eq!(
            str_key.as_deref(),
            expected.map(|ip| ip.to_string()).as_deref()
        );
    }

    fn assert_forwarded_extract_eq(
        extractor: &ForwardedIpKeyExtractor,
        req: &http::Request<Body>,
        expected: Option<IpAddr>,
    ) {
        assert_ip_extract_eq(extractor, req, expected);
    }

    #[test]
    fn prefix_ip_key_extractor_groups_peers_from_both_extensions() {
        let slash_64 = IpKeyExtractor.with_ipv6_prefix(Ipv6Prefix::SLASH_64);
        let slash_56 = PrefixIpKeyExtractor::new(Ipv6Prefix::SLASH_56);
        for (extractor, peer, expected) in [
            (slash_64, "[2001:db8:1:203::1]:5000", "2001:db8:1:203::"),
            (slash_64, "[2001:db8:1:203::2]:5001", "2001:db8:1:203::"),
            (slash_64, "[2001:db8:1:204::1]:5000", "2001:db8:1:204::"),
            (slash_56, "[2001:db8:1:203::1]:5000", "2001:db8:1:200::"),
            (slash_56, "[2001:db8:1:204::1]:5000", "2001:db8:1:200::"),
            (slash_64, "192.0.2.7:5000", "192.0.2.7"),
            (slash_64, "[::ffff:192.0.2.7]:5000", "192.0.2.7"),
        ] {
            let peer: SocketAddr = peer.parse().unwrap();
            let expected = Some(expected.parse().unwrap());
            let mut req = http::Request::builder()
                .header("x-forwarded-for", "203.0.113.1")
                .header("x-real-ip", "203.0.113.2")
                .body(Body::default())
                .unwrap();
            assert_ip_extract_eq(&extractor, &req, None);

            req.extensions_mut().insert(TcpConnectInfo {
                local_addr: None,
                remote_addr: Some(peer),
            });
            assert_ip_extract_eq(&extractor, &req, expected);

            req.extensions_mut().insert(TcpConnectInfo {
                local_addr: None,
                remote_addr: Some("203.0.113.3:5000".parse().unwrap()),
            });
            req.extensions_mut().insert(peer);
            assert_ip_extract_eq(&extractor, &req, expected);
        }
    }

    #[test]
    fn forwarded_ip_key_extractor_masks_header_or_peer_fallback() {
        for (extractor, header, value) in [
            (
                ForwardedIpKeyExtractor::x_forwarded_for(),
                "x-forwarded-for",
                "192.0.2.7, 2001:db8:1:2::1234",
            ),
            (
                ForwardedIpKeyExtractor::x_real_ip(),
                "x-real-ip",
                "[2001:db8:1:2::1234]:5000",
            ),
        ] {
            assert_eq!(extractor.ipv6_prefix(), None);
            let masked = extractor.with_ipv6_prefix(Ipv6Prefix::SLASH_64);
            assert_eq!(masked.ipv6_prefix(), Some(Ipv6Prefix::SLASH_64));

            let peer: SocketAddr = "[2001:db8:1:3::5678]:5001".parse().unwrap();
            let mut req = http::Request::builder()
                .extension(peer)
                .header(header, value)
                .body(Body::default())
                .unwrap();
            assert_forwarded_extract_eq(
                &extractor,
                &req,
                Some("2001:db8:1:2::1234".parse().unwrap()),
            );
            assert_forwarded_extract_eq(&masked, &req, Some("2001:db8:1:2::".parse().unwrap()));

            req.headers_mut()
                .insert(header, HeaderValue::from_static("[::ffff:192.0.2.7]:5000"));
            assert_forwarded_extract_eq(&masked, &req, Some("192.0.2.7".parse().unwrap()));

            req.headers_mut()
                .insert(header, HeaderValue::from_static("invalid"));
            assert_forwarded_extract_eq(&masked, &req, None);

            req.headers_mut().remove(header);
            assert_forwarded_extract_eq(&extractor, &req, Some(peer.ip()));
            assert_forwarded_extract_eq(&masked, &req, Some("2001:db8:1:3::".parse().unwrap()));
        }
    }

    #[test]
    fn test_ip_key_extractor_from_tcp_connect_info() {
        let extractor = IpKeyExtractor;
        let peer: SocketAddr = "198.51.100.42:54321".parse().unwrap();
        let mut req = http::Request::builder()
            .uri("http://example.com/test")
            .header("x-forwarded-for", "192.168.1.1, 10.0.0.1")
            .header("x-real-ip", "10.0.0.2")
            .body(Body::default())
            .unwrap();
        req.extensions_mut().insert(TcpConnectInfo {
            local_addr: Some("127.0.0.1:50051".parse().unwrap()),
            remote_addr: Some(peer),
        });

        assert_ip_extract_eq(&extractor, &req, Some(peer.ip()));
    }

    #[test]
    fn test_ip_key_extractor_from_socket_addr_extension() {
        let extractor = IpKeyExtractor;
        let peer: SocketAddr = "203.0.113.9:40000".parse().unwrap();
        let mut req = http::Request::builder()
            .uri("http://example.com/test")
            .body(Body::default())
            .unwrap();
        req.extensions_mut().insert(peer);

        assert_ip_extract_eq(&extractor, &req, Some(peer.ip()));
    }

    #[test]
    fn test_ip_key_extractor_prefers_socket_addr_over_tcp_connect_info() {
        let extractor = IpKeyExtractor;
        let tcp_peer: SocketAddr = "198.51.100.10:50000".parse().unwrap();
        let raw_peer: SocketAddr = "203.0.113.20:50001".parse().unwrap();
        let mut req = http::Request::builder()
            .uri("http://example.com/test")
            .body(Body::default())
            .unwrap();
        req.extensions_mut().insert(TcpConnectInfo {
            local_addr: None,
            remote_addr: Some(tcp_peer),
        });
        req.extensions_mut().insert(raw_peer);

        // Bare SocketAddr extension takes precedence over TcpConnectInfo (e.g. PROXY protocol).
        assert_ip_extract_eq(&extractor, &req, Some(raw_peer.ip()));

        // Without bare SocketAddr, falls back to TcpConnectInfo::remote_addr().
        req.extensions_mut().remove::<SocketAddr>();
        assert_ip_extract_eq(&extractor, &req, Some(tcp_peer.ip()));

        // If TcpConnectInfo has no remote_addr and no bare SocketAddr is set, returns None.
        req.extensions_mut().insert(TcpConnectInfo {
            local_addr: None,
            remote_addr: None,
        });
        assert_ip_extract_eq(&extractor, &req, None);
    }

    #[test]
    fn test_ip_key_extractor_ipv6_and_v4_mapped_peers() {
        let ip_extractor = IpKeyExtractor;
        let fwd_extractor = ForwardedIpKeyExtractor::x_forwarded_for();

        let v6_peer: SocketAddr = "[2001:db8::42]:50000".parse().unwrap();
        let mut req_v6 = http::Request::builder()
            .uri("http://example.com/test")
            .body(Body::default())
            .unwrap();
        req_v6.extensions_mut().insert(TcpConnectInfo {
            local_addr: None,
            remote_addr: Some(v6_peer),
        });
        assert_ip_extract_eq(&ip_extractor, &req_v6, Some(v6_peer.ip()));
        assert_forwarded_extract_eq(&fwd_extractor, &req_v6, Some(v6_peer.ip()));

        let mapped_peer: SocketAddr = "[::ffff:192.0.2.15]:50001".parse().unwrap();
        let mut req_mapped = http::Request::builder()
            .uri("http://example.com/test")
            .body(Body::default())
            .unwrap();
        req_mapped.extensions_mut().insert(mapped_peer);
        assert_ip_extract_eq(&ip_extractor, &req_mapped, Some(mapped_peer.ip()));
        assert_forwarded_extract_eq(&fwd_extractor, &req_mapped, Some(mapped_peer.ip()));
    }

    #[test]
    fn test_ip_key_extractor_ignores_forwarding_headers() {
        let extractor = IpKeyExtractor;
        let req = http::Request::builder()
            .uri("http://example.com/test")
            .header("x-forwarded-for", "192.168.1.1, 10.0.0.1")
            .header("x-real-ip", "10.0.0.2")
            .body(Body::default())
            .unwrap();

        assert_ip_extract_eq(&extractor, &req, None);
    }

    #[test]
    fn test_ip_key_extractor_no_connection_info() {
        let extractor = IpKeyExtractor;
        let req = http::Request::builder()
            .uri("http://example.com/test")
            .body(Body::default())
            .unwrap();

        assert_ip_extract_eq(&extractor, &req, None);
    }

    #[test]
    fn test_forwarded_ip_key_extractor_constructors_and_default() {
        assert_eq!(
            ForwardedIpKeyExtractor::new(),
            ForwardedIpKeyExtractor::x_forwarded_for()
        );
        assert_eq!(
            ForwardedIpKeyExtractor::default(),
            ForwardedIpKeyExtractor::x_forwarded_for()
        );
        assert_ne!(
            ForwardedIpKeyExtractor::x_forwarded_for(),
            ForwardedIpKeyExtractor::x_real_ip()
        );
    }

    #[test]
    fn test_forwarded_ip_key_extractor_xff_last_hop() {
        let extractor = ForwardedIpKeyExtractor::x_forwarded_for();
        let peer: SocketAddr = "127.0.0.1:54321".parse().unwrap();
        let mut req = http::Request::builder()
            .uri("http://example.com/test")
            .header("x-forwarded-for", "192.168.1.1, \t 10.0.0.1 \t")
            .header("x-real-ip", "10.0.0.2")
            .body(Body::default())
            .unwrap();
        req.extensions_mut().insert(TcpConnectInfo {
            local_addr: None,
            remote_addr: Some(peer),
        });

        assert_forwarded_extract_eq(&extractor, &req, Some("10.0.0.1".parse().unwrap()));

        // Multiple x-forwarded-for headers: uses the last header's last element.
        req.headers_mut()
            .append("x-forwarded-for", HeaderValue::from_static("10.0.0.9"));
        assert_forwarded_extract_eq(&extractor, &req, Some("10.0.0.9".parse().unwrap()));
    }

    #[test]
    fn test_forwarded_ip_key_extractor_obs_text_in_earlier_element() {
        let extractor = ForwardedIpKeyExtractor::x_forwarded_for();
        let peer: SocketAddr = "127.0.0.1:54321".parse().unwrap();

        let mut req = http::Request::builder()
            .uri("http://example.com/test")
            .header("x-real-ip", "10.0.0.99")
            .body(Body::default())
            .unwrap();
        req.headers_mut().insert(
            HDR_X_FORWARDED_FOR,
            HeaderValue::from_bytes(b"\xff, 203.0.113.5").unwrap(),
        );
        req.extensions_mut().insert(TcpConnectInfo {
            local_addr: None,
            remote_addr: Some(peer),
        });

        assert_forwarded_extract_eq(&extractor, &req, Some("203.0.113.5".parse().unwrap()));

        // obs-text inside the last element itself is unusable -> None (fails closed).
        req.headers_mut().insert(
            HDR_X_FORWARDED_FOR,
            HeaderValue::from_bytes(b"203.0.113.5, \xff").unwrap(),
        );
        assert_forwarded_extract_eq(&extractor, &req, None);
    }

    #[test]
    fn test_forwarded_ip_key_extractor_empty_last_element_returns_none() {
        let extractor = ForwardedIpKeyExtractor::x_forwarded_for();
        let peer: SocketAddr = "127.0.0.1:54321".parse().unwrap();

        for raw in [b"203.0.113.1, ".as_slice(), b"203.0.113.1,", b"", b"   "] {
            let mut req = http::Request::builder()
                .uri("http://example.com/test")
                .header("x-real-ip", "10.0.0.2")
                .body(Body::default())
                .unwrap();
            req.headers_mut()
                .insert(HDR_X_FORWARDED_FOR, HeaderValue::from_bytes(raw).unwrap());
            req.extensions_mut().insert(TcpConnectInfo {
                local_addr: None,
                remote_addr: Some(peer),
            });

            assert_forwarded_extract_eq(&extractor, &req, None);
        }
    }

    #[test]
    fn test_forwarded_ip_key_extractor_ip_port_and_bracketed_v6() {
        let xff = ForwardedIpKeyExtractor::x_forwarded_for();
        let real_ip = ForwardedIpKeyExtractor::x_real_ip();

        let cases = [
            ("198.51.100.7:54321", Some("198.51.100.7")),
            ("192.0.2.1, 198.51.100.8:8080", Some("198.51.100.8")),
            ("[2001:db8::1]:8443", Some("2001:db8::1")),
            ("[2001:db8::1]", Some("2001:db8::1")),
            ("2001:db8::2", Some("2001:db8::2")),
            ("[::ffff:192.0.2.9]:443", Some("::ffff:192.0.2.9")),
            ("[198.51.100.7]", None),
            ("[198.51.100.7]:80", None),
        ];

        for (header_val, expected_str) in cases {
            let expected = expected_str.map(|s| s.parse::<IpAddr>().unwrap());

            let req_xff = http::Request::builder()
                .uri("http://example.com/test")
                .header("x-forwarded-for", header_val)
                .body(Body::default())
                .unwrap();
            assert_forwarded_extract_eq(&xff, &req_xff, expected);

            let req_real = http::Request::builder()
                .uri("http://example.com/test")
                .header("x-real-ip", header_val)
                .body(Body::default())
                .unwrap();
            assert_forwarded_extract_eq(&real_ip, &req_real, expected);
        }
    }

    #[test]
    fn test_forwarded_ip_key_extractor_unknown_returns_none() {
        let peer: SocketAddr = "127.0.0.1:54321".parse().unwrap();

        let mut req_xff = http::Request::builder()
            .uri("http://example.com/test")
            .header("x-forwarded-for", "203.0.113.1, unknown")
            .header("x-real-ip", "10.0.0.2")
            .body(Body::default())
            .unwrap();
        req_xff.extensions_mut().insert(TcpConnectInfo {
            local_addr: None,
            remote_addr: Some(peer),
        });
        assert_forwarded_extract_eq(&ForwardedIpKeyExtractor::x_forwarded_for(), &req_xff, None);

        let mut req_real = http::Request::builder()
            .uri("http://example.com/test")
            .header("x-forwarded-for", "203.0.113.1")
            .header("x-real-ip", "unknown")
            .body(Body::default())
            .unwrap();
        req_real.extensions_mut().insert(TcpConnectInfo {
            local_addr: None,
            remote_addr: Some(peer),
        });
        assert_forwarded_extract_eq(&ForwardedIpKeyExtractor::x_real_ip(), &req_real, None);
    }

    #[test]
    fn test_forwarded_ip_key_extractor_invalid_xff_never_cascades_to_x_real_ip() {
        let extractor = ForwardedIpKeyExtractor::x_forwarded_for();
        let peer: SocketAddr = "198.51.100.77:54321".parse().unwrap();

        // Invalid XFF + valid x-real-ip + valid peer -> None.
        let mut req = http::Request::builder()
            .uri("http://example.com/test")
            .header("x-forwarded-for", "not-an-ip")
            .header("x-real-ip", "10.0.0.2")
            .body(Body::default())
            .unwrap();
        req.extensions_mut().insert(TcpConnectInfo {
            local_addr: None,
            remote_addr: Some(peer),
        });
        assert_forwarded_extract_eq(&extractor, &req, None);

        // Absent XFF + valid x-real-ip + valid peer -> ignores x-real-ip, falls back to peer.
        let mut req_absent = http::Request::builder()
            .uri("http://example.com/test")
            .header("x-real-ip", "10.0.0.2")
            .body(Body::default())
            .unwrap();
        req_absent.extensions_mut().insert(TcpConnectInfo {
            local_addr: None,
            remote_addr: Some(peer),
        });
        assert_forwarded_extract_eq(&extractor, &req_absent, Some(peer.ip()));
    }

    #[test]
    fn test_forwarded_ip_key_extractor_x_real_ip_ignores_xff_and_uses_last_field() {
        let extractor = ForwardedIpKeyExtractor::x_real_ip();
        let peer: SocketAddr = "127.0.0.1:54321".parse().unwrap();

        let mut req = http::Request::builder()
            .uri("http://example.com/test")
            .header("x-forwarded-for", "203.0.113.1")
            .header("x-real-ip", "10.0.0.1")
            .body(Body::default())
            .unwrap();
        req.extensions_mut().insert(TcpConnectInfo {
            local_addr: None,
            remote_addr: Some(peer),
        });
        assert_forwarded_extract_eq(&extractor, &req, Some("10.0.0.1".parse().unwrap()));

        // Multiple x-real-ip fields: uses the last field.
        req.headers_mut()
            .append(HDR_X_REAL_IP, HeaderValue::from_static("10.0.0.5"));
        assert_forwarded_extract_eq(&extractor, &req, Some("10.0.0.5".parse().unwrap()));

        // Absent x-real-ip with XFF present: ignores XFF, falls back to peer.
        let mut req_no_real = http::Request::builder()
            .uri("http://example.com/test")
            .header("x-forwarded-for", "203.0.113.1")
            .body(Body::default())
            .unwrap();
        req_no_real.extensions_mut().insert(TcpConnectInfo {
            local_addr: None,
            remote_addr: Some(peer),
        });
        assert_forwarded_extract_eq(&extractor, &req_no_real, Some(peer.ip()));
    }

    #[test]
    fn test_forwarded_ip_key_extractor_string_and_ip_addr_agree_on_canonical_form() {
        let extractor = ForwardedIpKeyExtractor::x_forwarded_for();
        let canonical_v6: IpAddr = "2001:db8::1".parse().unwrap();

        for raw in [
            "2001:0db8:0000:0000::0001",
            "[2001:0db8::1]",
            "[2001:0db8::1]:443",
        ] {
            let req = http::Request::builder()
                .uri("http://example.com/test")
                .header("x-forwarded-for", raw)
                .body(Body::default())
                .unwrap();
            assert_forwarded_extract_eq(&extractor, &req, Some(canonical_v6));
            assert_eq!(
                GrpcKeyExtractor::<String>::extract(&extractor, &req).as_deref(),
                Some("2001:db8::1")
            );
        }
    }

    #[test]
    fn test_forwarded_ip_key_extractor_falls_back_to_peer_addr() {
        let extractor = ForwardedIpKeyExtractor::x_forwarded_for();
        let tcp_peer: SocketAddr = "198.51.100.77:54321".parse().unwrap();
        let override_peer: SocketAddr = "203.0.113.88:54322".parse().unwrap();
        let mut req = http::Request::builder()
            .uri("http://example.com/test")
            .body(Body::default())
            .unwrap();
        req.extensions_mut().insert(TcpConnectInfo {
            local_addr: None,
            remote_addr: Some(tcp_peer),
        });

        assert_forwarded_extract_eq(&extractor, &req, Some(tcp_peer.ip()));

        // Bare SocketAddr extension takes precedence over TcpConnectInfo in peer fallback too.
        req.extensions_mut().insert(override_peer);
        assert_forwarded_extract_eq(&extractor, &req, Some(override_peer.ip()));
    }

    #[test]
    fn test_forwarded_ip_key_extractor_no_headers_or_peer() {
        let req = http::Request::builder()
            .uri("http://example.com/test")
            .body(Body::default())
            .unwrap();

        assert_forwarded_extract_eq(&ForwardedIpKeyExtractor::x_forwarded_for(), &req, None);
        assert_forwarded_extract_eq(&ForwardedIpKeyExtractor::x_real_ip(), &req, None);
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

    #[tokio::test]
    async fn test_ip_key_extractor_fail_open_with_no_extensions_and_xff() {
        let limiter = RateLimiter::builder()
            .per_minute(1)
            .burst(1)
            .build()
            .unwrap();

        let layer = GrpcRateLimitLayer::with_extractor(limiter, IpKeyExtractor).fail_open();
        let mut service = layer.layer(MockService);

        for _ in 0..5 {
            let req = http::Request::builder()
                .uri("http://example.com/test.Service/Method")
                .header("x-forwarded-for", "203.0.113.1")
                .header("x-real-ip", "203.0.113.1")
                .body(Body::default())
                .unwrap();

            let response = service.call(req).await.unwrap();
            assert_eq!(response.status(), http::StatusCode::OK);
            assert!(response.headers().get("grpc-status").is_none());
        }
    }
}
