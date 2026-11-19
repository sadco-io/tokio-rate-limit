//! Integration tests for Tonic gRPC rate limiting middleware.
//!
//! These tests verify wire-compliant gRPC over HTTP/2 rate limiting (`:status 200` +
//! `grpc-status: 8` `RESOURCE_EXHAUSTED`), key extractors, per-RPC costs, custom rejection
//! handlers, stateful Tower layer composition, and real Tonic client/server transport.

#![cfg(feature = "tonic-support")]

use bytes::{Buf, BufMut, Bytes};
use std::convert::Infallible;
use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::time::sleep;
use tokio_rate_limit::tonic_middleware::{
    CustomGrpcKeyExtractor, ForwardedIpKeyExtractor, GrpcKeyExtractor, GrpcLookupKey,
    GrpcRateLimitLayer, IpKeyExtractor, MetadataKeyExtractor,
};
use tokio_rate_limit::RateLimiter;
use tonic::body::Body;
use tonic::codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};
use tonic::metadata::MetadataValue;
use tonic::server::NamedService;
use tonic::transport::server::TcpConnectInfo;
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
        .per_minute(1)
        .burst(1)
        .build()
        .unwrap();

    let layer = GrpcRateLimitLayer::with_extractor(limiter, IpKeyExtractor);
    let mut service = layer.layer(TestService);

    let peer1: SocketAddr = "192.168.1.1:40001".parse().unwrap();
    let peer2: SocketAddr = "192.168.1.2:40002".parse().unwrap();

    // First request from peer1 via TcpConnectInfo succeeds.
    let mut request = http::Request::builder()
        .uri("http://localhost/test.Service/Method")
        .header("x-forwarded-for", "203.0.113.1")
        .body(Body::default())
        .unwrap();
    request.extensions_mut().insert(TcpConnectInfo {
        local_addr: None,
        remote_addr: Some(peer1),
    });
    assert_grpc_ok(&service.ready().await.unwrap().call(request).await.unwrap());

    // Second request from peer1 with a different x-forwarded-for header still shares peer1's key.
    let mut request = http::Request::builder()
        .uri("http://localhost/test.Service/Method")
        .header("x-forwarded-for", "203.0.113.2")
        .body(Body::default())
        .unwrap();
    request.extensions_mut().insert(TcpConnectInfo {
        local_addr: None,
        remote_addr: Some(peer1),
    });
    assert_grpc_rate_limited(&service.ready().await.unwrap().call(request).await.unwrap());

    // Request from peer2 via bare SocketAddr extension uses peer2's independent quota.
    let mut request = http::Request::builder()
        .uri("http://localhost/test.Service/Method")
        .body(Body::default())
        .unwrap();
    request.extensions_mut().insert(peer2);
    assert_grpc_ok(&service.ready().await.unwrap().call(request).await.unwrap());

    // Request with only forwarding headers and no connection info fails closed.
    let request = http::Request::builder()
        .uri("http://localhost/test.Service/Method")
        .header("x-forwarded-for", "203.0.113.3")
        .body(Body::default())
        .unwrap();
    assert_grpc_rate_limited(&service.ready().await.unwrap().call(request).await.unwrap());
}

#[tokio::test]
async fn test_forwarded_ip_based_rate_limiting() {
    let limiter = RateLimiter::builder()
        .per_minute(1)
        .burst(1)
        .build()
        .unwrap();

    let layer =
        GrpcRateLimitLayer::with_extractor(limiter, ForwardedIpKeyExtractor::x_forwarded_for());
    let mut service = layer.layer(TestService);

    let proxy_peer: SocketAddr = "10.0.0.10:50000".parse().unwrap();

    let mut request = http::Request::builder()
        .uri("http://localhost/test.Service/Method")
        .header("x-forwarded-for", "198.51.100.1, 192.168.1.1")
        .body(Body::default())
        .unwrap();
    request.extensions_mut().insert(TcpConnectInfo {
        local_addr: None,
        remote_addr: Some(proxy_peer),
    });
    assert_grpc_ok(&service.ready().await.unwrap().call(request).await.unwrap());

    // Same last hop (192.168.1.1) is rate limited even if earlier hops differ.
    let mut request = http::Request::builder()
        .uri("http://localhost/test.Service/Method")
        .header("x-forwarded-for", "198.51.100.99, 192.168.1.1")
        .body(Body::default())
        .unwrap();
    request.extensions_mut().insert(TcpConnectInfo {
        local_addr: None,
        remote_addr: Some(proxy_peer),
    });
    assert_grpc_rate_limited(&service.ready().await.unwrap().call(request).await.unwrap());

    // Different last hop gets its own quota.
    let mut request = http::Request::builder()
        .uri("http://localhost/test.Service/Method")
        .header("x-forwarded-for", "192.168.1.2")
        .body(Body::default())
        .unwrap();
    request.extensions_mut().insert(TcpConnectInfo {
        local_addr: None,
        remote_addr: Some(proxy_peer),
    });
    assert_grpc_ok(&service.ready().await.unwrap().call(request).await.unwrap());

    // Present but invalid x-forwarded-for fails closed even if x-real-ip and peer are present.
    let mut request = http::Request::builder()
        .uri("http://localhost/test.Service/Method")
        .header("x-forwarded-for", "198.51.100.1, ")
        .header("x-real-ip", "192.168.1.3")
        .body(Body::default())
        .unwrap();
    request.extensions_mut().insert(TcpConnectInfo {
        local_addr: None,
        remote_addr: Some(proxy_peer),
    });
    assert_grpc_rate_limited(&service.ready().await.unwrap().call(request).await.unwrap());

    // Without x-forwarded-for, falls back to the peer address from TcpConnectInfo
    // (ignoring x-real-ip).
    let mut request = http::Request::builder()
        .uri("http://localhost/test.Service/Method")
        .header("x-real-ip", "192.168.1.4")
        .body(Body::default())
        .unwrap();
    request.extensions_mut().insert(TcpConnectInfo {
        local_addr: None,
        remote_addr: Some(proxy_peer),
    });
    assert_grpc_ok(&service.ready().await.unwrap().call(request).await.unwrap());

    let mut request = http::Request::builder()
        .uri("http://localhost/test.Service/Method")
        .header("x-real-ip", "192.168.1.5")
        .body(Body::default())
        .unwrap();
    request.extensions_mut().insert(TcpConnectInfo {
        local_addr: None,
        remote_addr: Some(proxy_peer),
    });
    assert_grpc_rate_limited(&service.ready().await.unwrap().call(request).await.unwrap());
}

#[tokio::test]
async fn test_forwarded_x_real_ip_based_rate_limiting() {
    let limiter = RateLimiter::builder()
        .per_minute(1)
        .burst(1)
        .build()
        .unwrap();

    let layer = GrpcRateLimitLayer::with_extractor(limiter, ForwardedIpKeyExtractor::x_real_ip());
    let mut service = layer.layer(TestService);
    let proxy_peer: SocketAddr = "10.0.0.10:50000".parse().unwrap();

    // Reads x-real-ip and ignores x-forwarded-for.
    let mut request = http::Request::builder()
        .uri("http://localhost/test.Service/Method")
        .header("x-forwarded-for", "203.0.113.1")
        .header("x-real-ip", "192.0.2.1")
        .body(Body::default())
        .unwrap();
    request.extensions_mut().insert(TcpConnectInfo {
        local_addr: None,
        remote_addr: Some(proxy_peer),
    });
    assert_grpc_ok(&service.ready().await.unwrap().call(request).await.unwrap());

    // Different x-forwarded-for with the same x-real-ip shares the same bucket.
    let mut request = http::Request::builder()
        .uri("http://localhost/test.Service/Method")
        .header("x-forwarded-for", "203.0.113.2")
        .header("x-real-ip", "192.0.2.1")
        .body(Body::default())
        .unwrap();
    request.extensions_mut().insert(TcpConnectInfo {
        local_addr: None,
        remote_addr: Some(proxy_peer),
    });
    assert_grpc_rate_limited(&service.ready().await.unwrap().call(request).await.unwrap());

    // Invalid x-real-ip fails closed even when x-forwarded-for is valid.
    let mut request = http::Request::builder()
        .uri("http://localhost/test.Service/Method")
        .header("x-forwarded-for", "203.0.113.3")
        .header("x-real-ip", "unknown")
        .body(Body::default())
        .unwrap();
    request.extensions_mut().insert(TcpConnectInfo {
        local_addr: None,
        remote_addr: Some(proxy_peer),
    });
    assert_grpc_rate_limited(&service.ready().await.unwrap().call(request).await.unwrap());
}

#[tokio::test]
async fn test_ip_key_extractor_fail_open_with_no_extensions_and_xff() {
    let limiter = RateLimiter::builder()
        .per_minute(1)
        .burst(1)
        .build()
        .unwrap();

    let layer = GrpcRateLimitLayer::with_extractor(limiter, IpKeyExtractor).fail_open();
    let mut service = layer.layer(TestService);

    for _ in 0..5 {
        let request = http::Request::builder()
            .uri("http://localhost/test.Service/Method")
            .header("x-forwarded-for", "203.0.113.1")
            .header("x-real-ip", "203.0.113.1")
            .body(Body::default())
            .unwrap();
        assert_grpc_ok(&service.ready().await.unwrap().call(request).await.unwrap());
    }
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

async fn run_real_server_ip_scenario<K, E>(
    limiter: RateLimiter<tokio_rate_limit::algorithm::TokenBucket<K>>,
    extractor: E,
    total_requests: usize,
    xff_for_index: impl Fn(usize) -> Option<String>,
) -> (usize, Vec<Code>)
where
    K: tokio_rate_limit::RateLimitKey,
    E: GrpcKeyExtractor<K> + Clone,
    for<'a> K: std::borrow::Borrow<<E::Key<'a> as GrpcLookupKey<K>>::Query>,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    let server_task = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .layer(GrpcRateLimitLayer::with_extractor(limiter, extractor))
            .add_service(RawEchoGrpcService)
            .serve_with_incoming_shutdown(ListenerStream(listener), async move {
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
    let mut permitted = 0usize;
    let mut rejected_codes = Vec::new();

    for i in 0..total_requests {
        let mut req = Request::new(Bytes::from_static(b"ping"));
        if let Some(xff) = xff_for_index(i) {
            req.metadata_mut().insert(
                "x-forwarded-for",
                MetadataValue::try_from(xff.as_str()).unwrap(),
            );
        }
        client.ready().await.unwrap();
        match client.unary(req, path.clone(), RawBytesCodec).await {
            Ok(_) => permitted += 1,
            Err(status) => rejected_codes.push(status.code()),
        }
    }

    drop(client);
    let _ = shutdown_tx.send(());
    server_task.await.unwrap();
    (permitted, rejected_codes)
}

#[tokio::test(flavor = "multi_thread")]
async fn test_real_tonic_server_peer_address_and_forwarded_extractors() {
    let total = 10usize;

    // (a) IpKeyExtractor with rotating x-forwarded-for from a single TCP client:
    //     keys on the connection peer address (127.0.0.1), so exactly burst (3) are permitted.
    let limiter = RateLimiter::builder()
        .per_minute(1)
        .burst(3)
        .build()
        .unwrap();
    let (ok_rot, codes_rot) = run_real_server_ip_scenario(limiter, IpKeyExtractor, total, |i| {
        Some(format!("203.0.113.{}", i + 1))
    })
    .await;
    assert_eq!(ok_rot, 3);
    assert_eq!(codes_rot.len(), total - 3);
    assert!(codes_rot.iter().all(|&c| c == Code::ResourceExhausted));

    // (b) IpKeyExtractor with no forwarding headers:
    //     keys on the connection peer address, so exactly burst (3) are permitted.
    let limiter = RateLimiter::builder()
        .per_minute(1)
        .burst(3)
        .build()
        .unwrap();
    let (ok_none, codes_none) =
        run_real_server_ip_scenario(limiter, IpKeyExtractor, total, |_| None).await;
    assert_eq!(ok_none, 3);
    assert_eq!(codes_none.len(), total - 3);
    assert!(codes_none.iter().all(|&c| c == Code::ResourceExhausted));

    // (b2) IpKeyExtractor with a typed IpAddr limiter over a real Tonic server.
    let limiter = RateLimiter::builder()
        .per_minute(1)
        .burst(3)
        .build_with_key::<IpAddr>()
        .unwrap();
    let (ok_typed, codes_typed) =
        run_real_server_ip_scenario(limiter, IpKeyExtractor, total, |i| {
            Some(format!("203.0.113.{}", i + 1))
        })
        .await;
    assert_eq!(ok_typed, 3);
    assert_eq!(codes_typed.len(), total - 3);
    assert!(codes_typed.iter().all(|&c| c == Code::ResourceExhausted));

    // (c) ForwardedIpKeyExtractor::x_forwarded_for() with rotating x-forwarded-for:
    //     keys on the last x-forwarded-for hop, so all distinct client IPs are permitted.
    let limiter = RateLimiter::builder()
        .per_minute(1)
        .burst(3)
        .build()
        .unwrap();
    let (ok_fwd_rot, codes_fwd_rot) = run_real_server_ip_scenario(
        limiter,
        ForwardedIpKeyExtractor::x_forwarded_for(),
        total,
        |i| Some(format!("203.0.113.{}", i + 1)),
    )
    .await;
    assert_eq!(ok_fwd_rot, total);
    assert!(codes_fwd_rot.is_empty());

    // (d) ForwardedIpKeyExtractor::x_forwarded_for() with no forwarding headers:
    //     falls back to the connection peer address, so exactly burst (3) are permitted.
    let limiter = RateLimiter::builder()
        .per_minute(1)
        .burst(3)
        .build()
        .unwrap();
    let (ok_fwd_none, codes_fwd_none) = run_real_server_ip_scenario(
        limiter,
        ForwardedIpKeyExtractor::x_forwarded_for(),
        total,
        |_| None,
    )
    .await;
    assert_eq!(ok_fwd_none, 3);
    assert_eq!(codes_fwd_none.len(), total - 3);
    assert!(codes_fwd_none.iter().all(|&c| c == Code::ResourceExhausted));

    // (e) ForwardedIpKeyExtractor::x_real_ip() ignores x-forwarded-for and falls back
    //     to the connection peer address when x-real-ip is absent.
    let limiter = RateLimiter::builder()
        .per_minute(1)
        .burst(3)
        .build()
        .unwrap();
    let (ok_real_fallback, codes_real_fallback) =
        run_real_server_ip_scenario(limiter, ForwardedIpKeyExtractor::x_real_ip(), total, |i| {
            Some(format!("203.0.113.{}", i + 1))
        })
        .await;
    assert_eq!(ok_real_fallback, 3);
    assert_eq!(codes_real_fallback.len(), total - 3);
    assert!(codes_real_fallback
        .iter()
        .all(|&c| c == Code::ResourceExhausted));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_real_tonic_tls_server_ip_key_extractor() {
    use tonic::transport::{Certificate, ClientTlsConfig, Identity, ServerTlsConfig};

    let limiter = RateLimiter::builder()
        .per_minute(1)
        .burst(3)
        .build()
        .unwrap();

    let server_identity = Identity::from_pem(
        include_bytes!("fixtures/tls/server.pem"),
        include_bytes!("fixtures/tls/server.key"),
    );
    let server_tls = ServerTlsConfig::new().identity(server_identity);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    let server_task = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .tls_config(server_tls)
            .unwrap()
            .layer(GrpcRateLimitLayer::with_extractor(limiter, IpKeyExtractor))
            .add_service(RawEchoGrpcService)
            .serve_with_incoming_shutdown(ListenerStream(listener), async move {
                let _ = shutdown_rx.await;
            })
            .await
            .unwrap();
    });

    let ca = Certificate::from_pem(include_bytes!("fixtures/tls/ca.pem"));
    let client_tls = ClientTlsConfig::new()
        .ca_certificate(ca)
        .domain_name("localhost");

    let channel =
        tonic::transport::Endpoint::from_shared(format!("https://localhost:{}", addr.port()))
            .unwrap()
            .tls_config(client_tls)
            .unwrap()
            .connect()
            .await
            .unwrap();

    let mut client = tonic::client::Grpc::new(channel);
    let path = http::uri::PathAndQuery::from_static("/test.Echo/Ping");
    let total = 10usize;
    let mut permitted = 0usize;
    let mut rejected_codes = Vec::new();

    for i in 0..total {
        let mut req = Request::new(Bytes::from_static(b"ping"));
        req.metadata_mut().insert(
            "x-forwarded-for",
            MetadataValue::try_from(format!("203.0.113.{}", i + 1).as_str()).unwrap(),
        );
        client.ready().await.unwrap();
        match client.unary(req, path.clone(), RawBytesCodec).await {
            Ok(_) => permitted += 1,
            Err(status) => rejected_codes.push(status.code()),
        }
    }

    assert_eq!(permitted, 3);
    assert_eq!(rejected_codes.len(), total - 3);
    assert!(rejected_codes.iter().all(|&c| c == Code::ResourceExhausted));

    drop(client);
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
