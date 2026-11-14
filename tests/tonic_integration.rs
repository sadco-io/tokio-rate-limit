//! Integration tests for Tonic gRPC rate limiting middleware.
//!
//! These tests verify wire-compliant gRPC over HTTP/2 rate limiting (`:status 200` +
//! `grpc-status: 8` `RESOURCE_EXHAUSTED`), key extractors, per-RPC costs, custom rejection
//! handlers, stateful Tower layer composition, and real Tonic client/server transport.

#![cfg(feature = "tonic-support")]

use bytes::{Buf, BufMut, Bytes};
use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::time::sleep;
use tokio_rate_limit::tonic_middleware::{
    CustomGrpcKeyExtractor, GrpcRateLimitLayer, IpKeyExtractor, MetadataKeyExtractor,
};
use tokio_rate_limit::RateLimiter;
use tonic::body::Body;
use tonic::codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};
use tonic::server::NamedService;
use tonic::{Code, Request, Response, Status};
use tower::limit::ConcurrencyLimitLayer;
use tower::{Layer, Service, ServiceExt};

// Mock gRPC service implementation returning a valid empty gRPC response (`200 OK` + `grpc-status: 0`)
#[derive(Clone)]
struct TestService;

impl tower::Service<http::Request<Body>> for TestService {
    type Response = http::Response<Body>;
    type Error = Box<dyn std::error::Error + Send + Sync>;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _req: http::Request<Body>) -> Self::Future {
        Box::pin(async move {
            let response = http::Response::builder()
                .status(http::StatusCode::OK)
                .header("content-type", "application/grpc")
                .header("grpc-status", "0")
                .body(Body::default())
                .unwrap();
            Ok(response)
        })
    }
}

fn is_grpc_ok(response: &http::Response<Body>) -> bool {
    response.status() == http::StatusCode::OK
        && response
            .headers()
            .get("grpc-status")
            .and_then(|v| v.to_str().ok())
            == Some("0")
}

fn assert_grpc_ok(response: &http::Response<Body>) {
    assert_eq!(response.status(), http::StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get("grpc-status")
            .and_then(|v| v.to_str().ok()),
        Some("0")
    );
}

fn assert_grpc_rate_limited(response: &http::Response<Body>) {
    // Per the gRPC over HTTP/2 wire spec, Trailers-Only status responses carry HTTP 200 OK
    // alongside grpc-status = 8 (RESOURCE_EXHAUSTED).
    assert_eq!(response.status(), http::StatusCode::OK);
    let status = Status::from_header_map(response.headers())
        .expect("rate-limited gRPC response must decode via Status::from_header_map");
    assert_eq!(status.code(), Code::ResourceExhausted);
}

#[tokio::test]
async fn test_basic_rate_limiting() {
    let limiter = RateLimiter::builder()
        .requests_per_second(2)
        .burst(2)
        .build()
        .unwrap();

    let layer = GrpcRateLimitLayer::new(limiter);
    let mut service = layer.layer(TestService);

    for _ in 1..=2 {
        let request = http::Request::builder()
            .uri("http://localhost/test.Service/Method")
            .body(Body::default())
            .unwrap();

        let response = service.ready().await.unwrap().call(request).await.unwrap();
        assert_grpc_ok(&response);
    }

    let request = http::Request::builder()
        .uri("http://localhost/test.Service/Method")
        .body(Body::default())
        .unwrap();

    let response = service.ready().await.unwrap().call(request).await.unwrap();
    assert_grpc_rate_limited(&response);
    assert!(response.headers().get("x-ratelimit-limit").is_some());
    assert!(response.headers().get("retry-after").is_some());
}

#[tokio::test]
async fn test_rate_limit_recovery() {
    let limiter = Arc::new(
        RateLimiter::builder()
            .requests_per_second(10)
            .burst(10)
            .build()
            .unwrap(),
    );

    let layer = GrpcRateLimitLayer::new(limiter);
    let mut service = layer.layer(TestService);

    for _ in 0..10 {
        let request = http::Request::builder()
            .uri("http://localhost/test.Service/Method")
            .body(Body::default())
            .unwrap();

        let response = service.ready().await.unwrap().call(request).await.unwrap();
        assert_grpc_ok(&response);
    }

    let request = http::Request::builder()
        .uri("http://localhost/test.Service/Method")
        .body(Body::default())
        .unwrap();

    let response = service.ready().await.unwrap().call(request).await.unwrap();
    assert_grpc_rate_limited(&response);

    sleep(Duration::from_millis(150)).await;

    let request = http::Request::builder()
        .uri("http://localhost/test.Service/Method")
        .body(Body::default())
        .unwrap();

    let response = service.ready().await.unwrap().call(request).await.unwrap();
    assert_grpc_ok(&response);
}

#[tokio::test]
async fn test_per_method_rate_limiting() {
    let limiter = RateLimiter::builder()
        .requests_per_second(1)
        .burst(1)
        .build()
        .unwrap();

    let layer = GrpcRateLimitLayer::new(limiter);
    let mut service = layer.layer(TestService);

    let request = http::Request::builder()
        .uri("http://localhost/test.Service/Method1")
        .body(Body::default())
        .unwrap();
    assert_grpc_ok(&service.ready().await.unwrap().call(request).await.unwrap());

    let request = http::Request::builder()
        .uri("http://localhost/test.Service/Method1")
        .body(Body::default())
        .unwrap();
    assert_grpc_rate_limited(&service.ready().await.unwrap().call(request).await.unwrap());

    let request = http::Request::builder()
        .uri("http://localhost/test.Service/Method2")
        .body(Body::default())
        .unwrap();
    assert_grpc_ok(&service.ready().await.unwrap().call(request).await.unwrap());

    let request = http::Request::builder()
        .uri("http://localhost/test.Service/Method2")
        .body(Body::default())
        .unwrap();
    assert_grpc_rate_limited(&service.ready().await.unwrap().call(request).await.unwrap());
}

#[tokio::test]
async fn test_ip_based_rate_limiting() {
    let limiter = RateLimiter::builder()
        .requests_per_second(1)
        .burst(1)
        .build()
        .unwrap();

    let layer = GrpcRateLimitLayer::with_extractor(limiter, IpKeyExtractor);
    let mut service = layer.layer(TestService);

    let request = http::Request::builder()
        .uri("http://localhost/test.Service/Method")
        .header("x-forwarded-for", "192.168.1.1")
        .body(Body::default())
        .unwrap();
    assert_grpc_ok(&service.ready().await.unwrap().call(request).await.unwrap());

    let request = http::Request::builder()
        .uri("http://localhost/test.Service/Method")
        .header("x-forwarded-for", "192.168.1.1")
        .body(Body::default())
        .unwrap();
    assert_grpc_rate_limited(&service.ready().await.unwrap().call(request).await.unwrap());

    let request = http::Request::builder()
        .uri("http://localhost/test.Service/Method")
        .header("x-forwarded-for", "192.168.1.2")
        .body(Body::default())
        .unwrap();
    assert_grpc_ok(&service.ready().await.unwrap().call(request).await.unwrap());
}

#[tokio::test]
async fn test_metadata_based_rate_limiting() {
    let limiter = RateLimiter::builder()
        .requests_per_second(1)
        .burst(1)
        .build()
        .unwrap();

    let extractor = MetadataKeyExtractor::new("x-user-id");
    let layer = GrpcRateLimitLayer::with_extractor(limiter, extractor);
    let mut service = layer.layer(TestService);

    let request = http::Request::builder()
        .uri("http://localhost/test.Service/Method")
        .header("x-user-id", "user-123")
        .body(Body::default())
        .unwrap();
    assert_grpc_ok(&service.ready().await.unwrap().call(request).await.unwrap());

    let request = http::Request::builder()
        .uri("http://localhost/test.Service/Method")
        .header("x-user-id", "user-123")
        .body(Body::default())
        .unwrap();
    assert_grpc_rate_limited(&service.ready().await.unwrap().call(request).await.unwrap());

    let request = http::Request::builder()
        .uri("http://localhost/test.Service/Method")
        .header("x-user-id", "user-456")
        .body(Body::default())
        .unwrap();
    assert_grpc_ok(&service.ready().await.unwrap().call(request).await.unwrap());
}

#[tokio::test]
async fn test_custom_extractor_combining_method_and_user() {
    let limiter = RateLimiter::builder()
        .requests_per_second(1)
        .burst(1)
        .build()
        .unwrap();

    let extractor = CustomGrpcKeyExtractor::new(|req| {
        let method = req.uri().path().trim_start_matches('/');
        let user = req.headers().get("x-user-id")?.to_str().ok()?;
        Some(format!("{}:{}", method, user))
    });

    let layer = GrpcRateLimitLayer::with_extractor(limiter, extractor);
    let mut service = layer.layer(TestService);

    let request = http::Request::builder()
        .uri("http://localhost/test.Service/Method1")
        .header("x-user-id", "user-1")
        .body(Body::default())
        .unwrap();
    assert_grpc_ok(&service.ready().await.unwrap().call(request).await.unwrap());

    let request = http::Request::builder()
        .uri("http://localhost/test.Service/Method1")
        .header("x-user-id", "user-1")
        .body(Body::default())
        .unwrap();
    assert_grpc_rate_limited(&service.ready().await.unwrap().call(request).await.unwrap());

    let request = http::Request::builder()
        .uri("http://localhost/test.Service/Method1")
        .header("x-user-id", "user-2")
        .body(Body::default())
        .unwrap();
    assert_grpc_ok(&service.ready().await.unwrap().call(request).await.unwrap());

    let request = http::Request::builder()
        .uri("http://localhost/test.Service/Method2")
        .header("x-user-id", "user-1")
        .body(Body::default())
        .unwrap();
    assert_grpc_ok(&service.ready().await.unwrap().call(request).await.unwrap());
}

#[tokio::test]
async fn test_concurrent_requests_different_keys() {
    let limiter = RateLimiter::builder()
        .requests_per_second(100)
        .burst(100)
        .build()
        .unwrap();

    let layer = GrpcRateLimitLayer::new(limiter);
    let mut handles = vec![];

    for i in 0..10 {
        let mut service = layer.layer(TestService);
        let handle = tokio::spawn(async move {
            let request = http::Request::builder()
                .uri(format!("http://localhost/test.Service/Method{}", i))
                .body(Body::default())
                .unwrap();

            service.ready().await.unwrap().call(request).await
        });
        handles.push(handle);
    }

    for handle in handles {
        let response = handle.await.unwrap().unwrap();
        assert_grpc_ok(&response);
    }
}

#[tokio::test]
async fn test_concurrent_requests_same_key() {
    let limiter = RateLimiter::builder()
        .requests_per_second(10)
        .burst(10)
        .build()
        .unwrap();

    let layer = GrpcRateLimitLayer::new(limiter);
    let mut handles = vec![];

    for _ in 0..10 {
        let mut service = layer.layer(TestService);
        let handle = tokio::spawn(async move {
            let request = http::Request::builder()
                .uri("http://localhost/test.Service/Method")
                .body(Body::default())
                .unwrap();

            service.ready().await.unwrap().call(request).await
        });
        handles.push(handle);
    }

    let mut success_count = 0;
    let mut rate_limited_count = 0;

    for handle in handles {
        let response = handle.await.unwrap().unwrap();
        if is_grpc_ok(&response) {
            success_count += 1;
        } else {
            assert_grpc_rate_limited(&response);
            rate_limited_count += 1;
        }
    }

    assert_eq!(success_count, 10);
    assert_eq!(rate_limited_count, 0);
}

#[tokio::test]
async fn test_rate_limit_headers_in_response() {
    let limiter = RateLimiter::builder()
        .requests_per_second(10)
        .burst(10)
        .build()
        .unwrap();

    let layer = GrpcRateLimitLayer::new(limiter);
    let mut service = layer.layer(TestService);

    let request = http::Request::builder()
        .uri("http://localhost/test.Service/Method")
        .body(Body::default())
        .unwrap();

    let response = service.ready().await.unwrap().call(request).await.unwrap();
    assert_grpc_ok(&response);

    let limit = response
        .headers()
        .get("x-ratelimit-limit")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok());
    assert_eq!(limit, Some(10));

    let remaining = response
        .headers()
        .get("x-ratelimit-remaining")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok());
    assert_eq!(remaining, Some(9));

    let reset = response
        .headers()
        .get("x-ratelimit-reset")
        .and_then(|v| v.to_str().ok());
    assert!(reset.is_some());
}

#[tokio::test]
async fn test_rate_limit_error_headers() {
    let limiter = RateLimiter::builder()
        .requests_per_second(1)
        .burst(1)
        .build()
        .unwrap();

    let layer = GrpcRateLimitLayer::new(limiter);
    let mut service = layer.layer(TestService);

    let request = http::Request::builder()
        .uri("http://localhost/test.Service/Method")
        .body(Body::default())
        .unwrap();
    service.ready().await.unwrap().call(request).await.unwrap();

    let request = http::Request::builder()
        .uri("http://localhost/test.Service/Method")
        .body(Body::default())
        .unwrap();

    let response = service.ready().await.unwrap().call(request).await.unwrap();
    assert_grpc_rate_limited(&response);

    let grpc_message = response
        .headers()
        .get("grpc-message")
        .and_then(|v| v.to_str().ok());
    assert!(grpc_message.is_some());

    let retry_after = response
        .headers()
        .get("retry-after")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok());
    assert_eq!(retry_after, Some(1));
}

#[tokio::test]
async fn test_missing_key_fails_closed() {
    let limiter = RateLimiter::builder()
        .requests_per_second(1)
        .burst(1)
        .build()
        .unwrap();

    let extractor = CustomGrpcKeyExtractor::new(|_req| None);
    let layer = GrpcRateLimitLayer::with_extractor(limiter, extractor);
    let mut service = layer.layer(TestService);

    let request = http::Request::builder()
        .uri("http://localhost/test.Service/Method")
        .body(Body::default())
        .unwrap();

    let response = service.ready().await.unwrap().call(request).await.unwrap();
    assert_grpc_rate_limited(&response);
}

#[tokio::test]
async fn test_missing_key_fail_open() {
    let limiter = RateLimiter::builder()
        .requests_per_second(1)
        .burst(1)
        .build()
        .unwrap();

    let extractor = CustomGrpcKeyExtractor::new(|_req| None);
    let layer = GrpcRateLimitLayer::with_extractor(limiter, extractor).fail_open();
    let mut service = layer.layer(TestService);

    for _ in 0..10 {
        let request = http::Request::builder()
            .uri("http://localhost/test.Service/Method")
            .body(Body::default())
            .unwrap();

        let response = service.ready().await.unwrap().call(request).await.unwrap();
        assert_grpc_ok(&response);
    }
}

#[tokio::test]
async fn test_weighted_cost_and_custom_rejection_and_stateful_inner_layer() {
    let limiter = RateLimiter::builder()
        .requests_per_second(1)
        .burst(5)
        .build()
        .unwrap();

    let layer = GrpcRateLimitLayer::new(limiter)
        .with_cost(|req| {
            if req.uri().path().ends_with("/Batch") {
                4
            } else {
                1
            }
        })
        .on_rejected(|_decision| Status::unavailable("Backoff requested"));

    // Compose over a stateful ConcurrencyLimitLayer to prove poll_ready readiness handoff.
    let inner = ConcurrencyLimitLayer::new(4).layer(TestService);
    let mut service = layer.layer(inner);

    let batch_req = http::Request::builder()
        .uri("http://localhost/test.Service/Batch")
        .body(Body::default())
        .unwrap();
    let ok_resp = service
        .ready()
        .await
        .unwrap()
        .call(batch_req)
        .await
        .unwrap();
    assert_grpc_ok(&ok_resp);
    assert_eq!(
        ok_resp
            .headers()
            .get("x-ratelimit-remaining")
            .and_then(|v| v.to_str().ok()),
        Some("1")
    );

    let batch_req2 = http::Request::builder()
        .uri("http://localhost/test.Service/Batch")
        .body(Body::default())
        .unwrap();
    let denied = service
        .ready()
        .await
        .unwrap()
        .call(batch_req2)
        .await
        .unwrap();
    assert_eq!(denied.status(), http::StatusCode::OK);
    let status = Status::from_header_map(denied.headers()).unwrap();
    assert_eq!(status.code(), Code::Unavailable);
    assert_eq!(status.message(), "Backoff requested");
    assert!(denied.headers().contains_key("retry-after"));
}

#[tokio::test]
async fn test_high_throughput() {
    let limiter = RateLimiter::builder()
        .requests_per_second(1000)
        .burst(1000)
        .build()
        .unwrap();

    let layer = GrpcRateLimitLayer::new(limiter);
    let mut handles = vec![];

    for i in 0..100 {
        let mut service = layer.layer(TestService);
        let handle = tokio::spawn(async move {
            let request = http::Request::builder()
                .uri(format!("http://localhost/test.Service/Method{}", i % 10))
                .body(Body::default())
                .unwrap();

            service.ready().await.unwrap().call(request).await
        });
        handles.push(handle);
    }

    for handle in handles {
        let response = handle.await.unwrap().unwrap();
        assert_grpc_ok(&response);
    }
}

// --- Real end-to-end Tonic HTTP/2 Client + Server wire compliance test (no protoc needed) ---

#[derive(Debug, Clone, Copy, Default)]
struct RawBytesCodec;

#[derive(Debug, Clone, Copy, Default)]
struct RawBytesEncoder;

#[derive(Debug, Clone, Copy, Default)]
struct RawBytesDecoder;

impl Codec for RawBytesCodec {
    type Encode = Bytes;
    type Decode = Bytes;
    type Encoder = RawBytesEncoder;
    type Decoder = RawBytesDecoder;

    fn encoder(&mut self) -> Self::Encoder {
        RawBytesEncoder
    }

    fn decoder(&mut self) -> Self::Decoder {
        RawBytesDecoder
    }
}

impl Encoder for RawBytesEncoder {
    type Item = Bytes;
    type Error = Status;

    fn encode(&mut self, item: Self::Item, dst: &mut EncodeBuf<'_>) -> Result<(), Self::Error> {
        dst.put(item);
        Ok(())
    }
}

impl Decoder for RawBytesDecoder {
    type Item = Bytes;
    type Error = Status;

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<Self::Item>, Self::Error> {
        let remaining = src.remaining();
        if remaining == 0 {
            return Ok(None);
        }
        Ok(Some(src.copy_to_bytes(remaining)))
    }
}

#[derive(Clone, Default)]
struct RawEchoGrpcService;

impl NamedService for RawEchoGrpcService {
    const NAME: &'static str = "test.Echo";
}

impl tonic::server::UnaryService<Bytes> for RawEchoGrpcService {
    type Response = Bytes;
    type Future = Pin<Box<dyn Future<Output = Result<Response<Bytes>, Status>> + Send>>;

    fn call(&mut self, request: Request<Bytes>) -> Self::Future {
        Box::pin(async move { Ok(Response::new(request.into_inner())) })
    }
}

impl Service<http::Request<Body>> for RawEchoGrpcService {
    type Response = http::Response<Body>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: http::Request<Body>) -> Self::Future {
        let svc = self.clone();
        Box::pin(async move {
            let mut grpc = tonic::server::Grpc::new(RawBytesCodec);
            Ok(grpc.unary(svc, req).await)
        })
    }
}

#[tokio::test]
async fn test_real_tonic_client_receives_resource_exhausted_over_http2() {
    let limiter = RateLimiter::builder()
        .requests_per_second(1)
        .burst(2)
        .build()
        .unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let incoming = ListenerStream(listener);
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    let server_task = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .layer(GrpcRateLimitLayer::new(limiter))
            .add_service(RawEchoGrpcService)
            .serve_with_incoming_shutdown(incoming, async move {
                let _ = shutdown_rx.await;
            })
            .await
            .unwrap();
    });

    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();

    let mut client = tonic::client::Grpc::new(channel);
    let path = http::uri::PathAndQuery::from_static("/test.Echo/Ping");

    // First two RPCs succeed and expose rate limit metadata on tonic::Response::metadata()
    for expected_remaining in ["1", "0"] {
        client.ready().await.unwrap();
        let resp = client
            .unary(
                Request::new(Bytes::from_static(b"hello")),
                path.clone(),
                RawBytesCodec,
            )
            .await
            .expect("within burst should succeed");
        assert_eq!(resp.get_ref().as_ref(), b"hello");
        assert_eq!(
            resp.metadata()
                .get("x-ratelimit-remaining")
                .and_then(|v| v.to_str().ok()),
            Some(expected_remaining)
        );
    }

    // Third RPC exceeds burst -> Tonic client MUST decode Code::ResourceExhausted (not Unavailable!)
    client.ready().await.unwrap();
    let err = client
        .unary(
            Request::new(Bytes::from_static(b"hello")),
            path,
            RawBytesCodec,
        )
        .await
        .expect_err("third RPC must be rate limited");

    assert_eq!(err.code(), Code::ResourceExhausted);
    assert_eq!(err.message(), "Rate limit exceeded");
    assert_eq!(
        err.metadata()
            .get("retry-after")
            .and_then(|v| v.to_str().ok()),
        Some("1")
    );
    assert_eq!(
        err.metadata()
            .get("x-ratelimit-limit")
            .and_then(|v| v.to_str().ok()),
        Some("2")
    );

    let _ = shutdown_tx.send(());
    server_task.await.unwrap();
}

struct ListenerStream(TcpListener);

impl tokio_stream::Stream for ListenerStream {
    type Item = std::io::Result<tokio::net::TcpStream>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut()
            .0
            .poll_accept(cx)
            .map(|res| Some(res.map(|(stream, _)| stream)))
    }
}
