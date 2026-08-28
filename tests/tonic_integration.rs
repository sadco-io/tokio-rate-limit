//! Integration tests for Tonic gRPC rate limiting middleware.
//!
//! These tests verify the complete behavior of the rate limiting middleware
//! in realistic gRPC server scenarios.

#![cfg(feature = "tonic-support")]

use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tokio_rate_limit::tonic_middleware::{
    CustomGrpcKeyExtractor, GrpcRateLimitLayer, IpKeyExtractor, MetadataKeyExtractor,
};
use tokio_rate_limit::RateLimiter;
use tonic::body::Body;
use tonic::Code;
use tower::{Layer, Service, ServiceExt};

// Mock gRPC service implementation
#[derive(Clone)]
struct TestService;

impl tower::Service<http::Request<Body>> for TestService {
    type Response = http::Response<Body>;
    type Error = Box<dyn std::error::Error + Send + Sync>;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, _req: http::Request<Body>) -> Self::Future {
        Box::pin(async move {
            let response = http::Response::builder()
                .status(200)
                .body(Body::default())
                .unwrap();
            Ok(response)
        })
    }
}

#[tokio::test]
async fn test_basic_rate_limiting() {
    let limiter = Arc::new(
        RateLimiter::builder()
            .requests_per_second(2)
            .burst(2)
            .build()
            .unwrap(),
    );

    let layer = GrpcRateLimitLayer::new(limiter);
    let mut service = layer.layer(TestService);

    // First two requests should succeed
    for i in 1..=2 {
        let request = http::Request::builder()
            .uri("http://localhost/test.Service/Method")
            .body(Body::default())
            .unwrap();

        let response = service.ready().await.unwrap().call(request).await.unwrap();
        assert_eq!(response.status(), 200, "Request {} should succeed", i);
    }

    // Third immediate request should be rate limited
    let request = http::Request::builder()
        .uri("http://localhost/test.Service/Method")
        .body(Body::default())
        .unwrap();

    let response = service.ready().await.unwrap().call(request).await.unwrap();
    assert_eq!(
        response.status(),
        429,
        "Third request should be rate limited"
    );

    // Check for rate limit headers
    assert!(response.headers().get("x-ratelimit-limit").is_some());
    assert!(response.headers().get("retry-after").is_some());
}

#[tokio::test]
async fn test_rate_limit_recovery() {
    // Use a low burst to test recovery
    let limiter = Arc::new(
        RateLimiter::builder()
            .requests_per_second(10)
            .burst(10)
            .build()
            .unwrap(),
    );

    let layer = GrpcRateLimitLayer::new(limiter);
    let mut service = layer.layer(TestService);

    // Exhaust the burst limit with 10 requests
    for _ in 0..10 {
        let request = http::Request::builder()
            .uri("http://localhost/test.Service/Method")
            .body(Body::default())
            .unwrap();

        let response = service.ready().await.unwrap().call(request).await.unwrap();
        assert_eq!(response.status(), 200);
    }

    // 11th immediate request should be rate limited
    let request = http::Request::builder()
        .uri("http://localhost/test.Service/Method")
        .body(Body::default())
        .unwrap();

    let response = service.ready().await.unwrap().call(request).await.unwrap();
    assert_eq!(response.status(), 429);

    // Wait for tokens to refill (10 req/s = 100ms per token)
    sleep(Duration::from_millis(150)).await;

    // Request should succeed after waiting
    let request = http::Request::builder()
        .uri("http://localhost/test.Service/Method")
        .body(Body::default())
        .unwrap();

    let response = service.ready().await.unwrap().call(request).await.unwrap();
    assert_eq!(response.status(), 200);
}

#[tokio::test]
async fn test_per_method_rate_limiting() {
    let limiter = Arc::new(
        RateLimiter::builder()
            .requests_per_second(1)
            .burst(1)
            .build()
            .unwrap(),
    );

    let layer = GrpcRateLimitLayer::new(limiter);
    let mut service = layer.layer(TestService);

    // Request to Method1 succeeds
    let request = http::Request::builder()
        .uri("http://localhost/test.Service/Method1")
        .body(Body::default())
        .unwrap();

    let response = service.ready().await.unwrap().call(request).await.unwrap();
    assert_eq!(response.status(), 200);

    // Second request to Method1 is rate limited
    let request = http::Request::builder()
        .uri("http://localhost/test.Service/Method1")
        .body(Body::default())
        .unwrap();

    let response = service.ready().await.unwrap().call(request).await.unwrap();
    assert_eq!(response.status(), 429);

    // Request to Method2 succeeds (different rate limit bucket)
    let request = http::Request::builder()
        .uri("http://localhost/test.Service/Method2")
        .body(Body::default())
        .unwrap();

    let response = service.ready().await.unwrap().call(request).await.unwrap();
    assert_eq!(response.status(), 200);

    // Second request to Method2 is also rate limited
    let request = http::Request::builder()
        .uri("http://localhost/test.Service/Method2")
        .body(Body::default())
        .unwrap();

    let response = service.ready().await.unwrap().call(request).await.unwrap();
    assert_eq!(response.status(), 429);
}

#[tokio::test]
async fn test_ip_based_rate_limiting() {
    let limiter = Arc::new(
        RateLimiter::builder()
            .requests_per_second(1)
            .burst(1)
            .build()
            .unwrap(),
    );

    let layer = GrpcRateLimitLayer::with_extractor(limiter, IpKeyExtractor);
    let mut service = layer.layer(TestService);

    // Request from IP1 succeeds
    let request = http::Request::builder()
        .uri("http://localhost/test.Service/Method")
        .header("x-forwarded-for", "192.168.1.1")
        .body(Body::default())
        .unwrap();

    let response = service.ready().await.unwrap().call(request).await.unwrap();
    assert_eq!(response.status(), 200);

    // Second request from IP1 is rate limited
    let request = http::Request::builder()
        .uri("http://localhost/test.Service/Method")
        .header("x-forwarded-for", "192.168.1.1")
        .body(Body::default())
        .unwrap();

    let response = service.ready().await.unwrap().call(request).await.unwrap();
    assert_eq!(response.status(), 429);

    // Request from IP2 succeeds (different rate limit bucket)
    let request = http::Request::builder()
        .uri("http://localhost/test.Service/Method")
        .header("x-forwarded-for", "192.168.1.2")
        .body(Body::default())
        .unwrap();

    let response = service.ready().await.unwrap().call(request).await.unwrap();
    assert_eq!(response.status(), 200);
}

#[tokio::test]
async fn test_metadata_based_rate_limiting() {
    let limiter = Arc::new(
        RateLimiter::builder()
            .requests_per_second(1)
            .burst(1)
            .build()
            .unwrap(),
    );

    let extractor = MetadataKeyExtractor::new("x-user-id");
    let layer = GrpcRateLimitLayer::with_extractor(limiter, extractor);
    let mut service = layer.layer(TestService);

    // Request from user1 succeeds
    let request = http::Request::builder()
        .uri("http://localhost/test.Service/Method")
        .header("x-user-id", "user-123")
        .body(Body::default())
        .unwrap();

    let response = service.ready().await.unwrap().call(request).await.unwrap();
    assert_eq!(response.status(), 200);

    // Second request from user1 is rate limited
    let request = http::Request::builder()
        .uri("http://localhost/test.Service/Method")
        .header("x-user-id", "user-123")
        .body(Body::default())
        .unwrap();

    let response = service.ready().await.unwrap().call(request).await.unwrap();
    assert_eq!(response.status(), 429);

    // Request from user2 succeeds (different rate limit bucket)
    let request = http::Request::builder()
        .uri("http://localhost/test.Service/Method")
        .header("x-user-id", "user-456")
        .body(Body::default())
        .unwrap();

    let response = service.ready().await.unwrap().call(request).await.unwrap();
    assert_eq!(response.status(), 200);
}

#[tokio::test]
async fn test_custom_extractor_combining_method_and_user() {
    let limiter = Arc::new(
        RateLimiter::builder()
            .requests_per_second(1)
            .burst(1)
            .build()
            .unwrap(),
    );

    let extractor = CustomGrpcKeyExtractor::new(|req| {
        let method = req.uri().path().trim_start_matches('/');
        let user = req.headers().get("x-user-id")?.to_str().ok()?;
        Some(format!("{}:{}", method, user))
    });

    let layer = GrpcRateLimitLayer::with_extractor(limiter, extractor);
    let mut service = layer.layer(TestService);

    // Request to Method1 from user1 succeeds
    let request = http::Request::builder()
        .uri("http://localhost/test.Service/Method1")
        .header("x-user-id", "user-1")
        .body(Body::default())
        .unwrap();

    let response = service.ready().await.unwrap().call(request).await.unwrap();
    assert_eq!(response.status(), 200);

    // Second request to Method1 from user1 is rate limited
    let request = http::Request::builder()
        .uri("http://localhost/test.Service/Method1")
        .header("x-user-id", "user-1")
        .body(Body::default())
        .unwrap();

    let response = service.ready().await.unwrap().call(request).await.unwrap();
    assert_eq!(response.status(), 429);

    // Request to Method1 from user2 succeeds (different user)
    let request = http::Request::builder()
        .uri("http://localhost/test.Service/Method1")
        .header("x-user-id", "user-2")
        .body(Body::default())
        .unwrap();

    let response = service.ready().await.unwrap().call(request).await.unwrap();
    assert_eq!(response.status(), 200);

    // Request to Method2 from user1 succeeds (different method)
    let request = http::Request::builder()
        .uri("http://localhost/test.Service/Method2")
        .header("x-user-id", "user-1")
        .body(Body::default())
        .unwrap();

    let response = service.ready().await.unwrap().call(request).await.unwrap();
    assert_eq!(response.status(), 200);
}

#[tokio::test]
async fn test_concurrent_requests_different_keys() {
    let limiter = Arc::new(
        RateLimiter::builder()
            .requests_per_second(100)
            .burst(100)
            .build()
            .unwrap(),
    );

    let layer = GrpcRateLimitLayer::new(limiter);

    // Spawn multiple concurrent requests to different methods
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

    // All requests should succeed (different keys)
    for handle in handles {
        let response = handle.await.unwrap().unwrap();
        assert_eq!(response.status(), 200);
    }
}

#[tokio::test]
async fn test_concurrent_requests_same_key() {
    let limiter = Arc::new(
        RateLimiter::builder()
            .requests_per_second(10)
            .burst(10)
            .build()
            .unwrap(),
    );

    let layer = GrpcRateLimitLayer::new(limiter);

    // Spawn 10 concurrent requests to the same method
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

    // Collect results
    let mut success_count = 0;
    let mut rate_limited_count = 0;

    for handle in handles {
        let response = handle.await.unwrap().unwrap();
        if response.status() == 200 {
            success_count += 1;
        } else if response.status() == 429 {
            rate_limited_count += 1;
        }
    }

    // With burst of 10 and 10 concurrent requests, all should succeed
    assert_eq!(
        success_count, 10,
        "Expected 10 successful requests, got {}",
        success_count
    );
    assert_eq!(
        rate_limited_count, 0,
        "Expected 0 rate limited requests, got {}",
        rate_limited_count
    );
}

#[tokio::test]
async fn test_rate_limit_headers_in_response() {
    let limiter = Arc::new(
        RateLimiter::builder()
            .requests_per_second(10)
            .burst(10)
            .build()
            .unwrap(),
    );

    let layer = GrpcRateLimitLayer::new(limiter);
    let mut service = layer.layer(TestService);

    let request = http::Request::builder()
        .uri("http://localhost/test.Service/Method")
        .body(Body::default())
        .unwrap();

    let response = service.ready().await.unwrap().call(request).await.unwrap();
    assert_eq!(response.status(), 200);

    // Check for rate limit headers
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
    assert!(remaining.is_some());
    assert!(remaining.unwrap() < 10);

    let reset = response
        .headers()
        .get("x-ratelimit-reset")
        .and_then(|v| v.to_str().ok());
    assert!(reset.is_some());
}

#[tokio::test]
async fn test_rate_limit_error_headers() {
    let limiter = Arc::new(
        RateLimiter::builder()
            .requests_per_second(1)
            .burst(1)
            .build()
            .unwrap(),
    );

    let layer = GrpcRateLimitLayer::new(limiter);
    let mut service = layer.layer(TestService);

    // First request succeeds
    let request = http::Request::builder()
        .uri("http://localhost/test.Service/Method")
        .body(Body::default())
        .unwrap();

    service.ready().await.unwrap().call(request).await.unwrap();

    // Second request is rate limited
    let request = http::Request::builder()
        .uri("http://localhost/test.Service/Method")
        .body(Body::default())
        .unwrap();

    let response = service.ready().await.unwrap().call(request).await.unwrap();
    assert_eq!(response.status(), 429);

    // Check error headers
    let grpc_status = response
        .headers()
        .get("grpc-status")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<i32>().ok());
    assert_eq!(grpc_status, Some(Code::ResourceExhausted as i32));

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
    assert!(retry_after.is_some());
}

#[tokio::test]
async fn test_missing_key_fails_closed() {
    let limiter = Arc::new(
        RateLimiter::builder()
            .requests_per_second(1)
            .burst(1)
            .build()
            .unwrap(),
    );

    let extractor = CustomGrpcKeyExtractor::new(|_req| None);
    let layer = GrpcRateLimitLayer::with_extractor(limiter, extractor);
    let mut service = layer.layer(TestService);

    let request = http::Request::builder()
        .uri("http://localhost/test.Service/Method")
        .body(Body::default())
        .unwrap();

    let response = service.ready().await.unwrap().call(request).await.unwrap();
    assert_eq!(response.status(), 429);
}

#[tokio::test]
async fn test_missing_key_fail_open() {
    let limiter = Arc::new(
        RateLimiter::builder()
            .requests_per_second(1)
            .burst(1)
            .build()
            .unwrap(),
    );

    let extractor = CustomGrpcKeyExtractor::new(|_req| None);
    let layer = GrpcRateLimitLayer::with_extractor(limiter, extractor).fail_open();
    let mut service = layer.layer(TestService);

    for _ in 0..10 {
        let request = http::Request::builder()
            .uri("http://localhost/test.Service/Method")
            .body(Body::default())
            .unwrap();

        let response = service.ready().await.unwrap().call(request).await.unwrap();
        assert_eq!(response.status(), 200);
    }
}

#[tokio::test]
async fn test_high_throughput() {
    let limiter = Arc::new(
        RateLimiter::builder()
            .requests_per_second(1000)
            .burst(1000)
            .build()
            .unwrap(),
    );

    let layer = GrpcRateLimitLayer::new(limiter);

    // Create 100 concurrent requests
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

    // All should succeed
    for handle in handles {
        let response = handle.await.unwrap().unwrap();
        assert_eq!(response.status(), 200);
    }
}
