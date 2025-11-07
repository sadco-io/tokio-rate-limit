//! Benchmarks for Tonic gRPC middleware rate limiting performance.
//!
//! These benchmarks measure the overhead of rate limiting in gRPC scenarios
//! and compare with Axum middleware performance.

#![cfg(feature = "tonic-support")]

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use std::sync::Arc;
use tokio_rate_limit::tonic_middleware::{
    CustomGrpcKeyExtractor, GrpcKeyExtractor, GrpcRateLimitLayer, IpKeyExtractor,
    MetadataKeyExtractor, MethodKeyExtractor,
};
use tokio_rate_limit::RateLimiter;
use tonic::body::BoxBody;
use tower::{Layer, Service, ServiceExt};

// Mock gRPC service for benchmarking
#[derive(Clone)]
struct BenchService;

impl tower::Service<http::Request<BoxBody>> for BenchService {
    type Response = http::Response<BoxBody>;
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

    fn call(&mut self, _req: http::Request<BoxBody>) -> Self::Future {
        Box::pin(async move {
            let response = http::Response::builder()
                .status(200)
                .body(BoxBody::default())
                .unwrap();
            Ok(response)
        })
    }
}

fn bench_key_extractors(c: &mut Criterion) {
    let mut group = c.benchmark_group("tonic_key_extractors");

    // Benchmark MethodKeyExtractor
    group.bench_function("method_key_extractor", |b| {
        let extractor = MethodKeyExtractor;
        let req = http::Request::builder()
            .uri("http://example.com/helloworld.Greeter/SayHello")
            .body(BoxBody::default())
            .unwrap();

        b.iter(|| {
            let key = extractor.extract(black_box(&req));
            black_box(key);
        });
    });

    // Benchmark IpKeyExtractor
    group.bench_function("ip_key_extractor", |b| {
        let extractor = IpKeyExtractor;
        let req = http::Request::builder()
            .uri("http://example.com/test")
            .header("x-forwarded-for", "192.168.1.1, 10.0.0.1")
            .body(BoxBody::default())
            .unwrap();

        b.iter(|| {
            let key = extractor.extract(black_box(&req));
            black_box(key);
        });
    });

    // Benchmark MetadataKeyExtractor
    group.bench_function("metadata_key_extractor", |b| {
        let extractor = MetadataKeyExtractor::new("user-id");
        let req = http::Request::builder()
            .uri("http://example.com/test")
            .header("user-id", "user-123")
            .body(BoxBody::default())
            .unwrap();

        b.iter(|| {
            let key = extractor.extract(black_box(&req));
            black_box(key);
        });
    });

    // Benchmark CustomGrpcKeyExtractor
    group.bench_function("custom_key_extractor", |b| {
        let extractor = CustomGrpcKeyExtractor::new(|req| {
            let method = req.uri().path().trim_start_matches('/');
            Some(format!("custom:{}", method))
        });
        let req = http::Request::builder()
            .uri("http://example.com/test.Service/Method")
            .body(BoxBody::default())
            .unwrap();

        b.iter(|| {
            let key = extractor.extract(black_box(&req));
            black_box(key);
        });
    });

    group.finish();
}

fn bench_rate_limit_check_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("tonic_rate_limit_overhead");
    group.throughput(Throughput::Elements(1));

    let rt = tokio::runtime::Runtime::new().unwrap();

    // Benchmark allowed request (no rate limiting)
    group.bench_function("allowed_request", |b| {
        let limiter = Arc::new(
            RateLimiter::builder()
                .requests_per_second(1_000_000)
                .burst(1_000_000)
                .build()
                .unwrap(),
        );

        let layer = GrpcRateLimitLayer::new(limiter);

        b.to_async(&rt).iter(|| async {
            let mut service = layer.layer(BenchService);
            let req = http::Request::builder()
                .uri("http://example.com/test.Service/Method")
                .body(BoxBody::default())
                .unwrap();

            let response = service.ready().await.unwrap().call(req).await.unwrap();
            black_box(response);
        });
    });

    // Benchmark denied request (rate limited)
    group.bench_function("denied_request", |b| {
        let limiter = Arc::new(
            RateLimiter::builder()
                .requests_per_second(1)
                .burst(1)
                .build()
                .unwrap(),
        );

        let layer = GrpcRateLimitLayer::new(limiter);

        b.to_async(&rt).iter(|| async {
            let mut service = layer.layer(BenchService);

            // First request to exhaust limit
            let req = http::Request::builder()
                .uri("http://example.com/test.Service/BenchMethod")
                .body(BoxBody::default())
                .unwrap();
            let _ = service.ready().await.unwrap().call(req).await;

            // Subsequent request will be denied
            let req = http::Request::builder()
                .uri("http://example.com/test.Service/BenchMethod")
                .body(BoxBody::default())
                .unwrap();

            let response = service.ready().await.unwrap().call(req).await.unwrap();
            black_box(response);
        });
    });

    // Benchmark without rate limiting (baseline)
    group.bench_function("no_rate_limiting_baseline", |b| {
        b.to_async(&rt).iter(|| async {
            let mut service = BenchService;
            let req = http::Request::builder()
                .uri("http://example.com/test.Service/Method")
                .body(BoxBody::default())
                .unwrap();

            let response = service.ready().await.unwrap().call(req).await.unwrap();
            black_box(response);
        });
    });

    group.finish();
}

fn bench_different_key_extractors_performance(c: &mut Criterion) {
    let mut group = c.benchmark_group("tonic_extractor_performance");
    group.throughput(Throughput::Elements(1));

    let rt = tokio::runtime::Runtime::new().unwrap();

    let limiter = Arc::new(
        RateLimiter::builder()
            .requests_per_second(1_000_000)
            .burst(1_000_000)
            .build()
            .unwrap(),
    );

    // Method-based extraction
    group.bench_function("method_based", |b| {
        let layer = GrpcRateLimitLayer::new(limiter.clone());

        b.to_async(&rt).iter(|| async {
            let mut service = layer.layer(BenchService);
            let req = http::Request::builder()
                .uri("http://example.com/test.Service/Method")
                .body(BoxBody::default())
                .unwrap();

            let response = service.ready().await.unwrap().call(req).await.unwrap();
            black_box(response);
        });
    });

    // IP-based extraction
    group.bench_function("ip_based", |b| {
        let layer = GrpcRateLimitLayer::with_extractor(limiter.clone(), IpKeyExtractor);

        b.to_async(&rt).iter(|| async {
            let mut service = layer.layer(BenchService);
            let req = http::Request::builder()
                .uri("http://example.com/test.Service/Method")
                .header("x-forwarded-for", "192.168.1.1")
                .body(BoxBody::default())
                .unwrap();

            let response = service.ready().await.unwrap().call(req).await.unwrap();
            black_box(response);
        });
    });

    // Metadata-based extraction
    group.bench_function("metadata_based", |b| {
        let extractor = MetadataKeyExtractor::new("user-id");
        let layer = GrpcRateLimitLayer::with_extractor(limiter.clone(), extractor);

        b.to_async(&rt).iter(|| async {
            let mut service = layer.layer(BenchService);
            let req = http::Request::builder()
                .uri("http://example.com/test.Service/Method")
                .header("user-id", "user-123")
                .body(BoxBody::default())
                .unwrap();

            let response = service.ready().await.unwrap().call(req).await.unwrap();
            black_box(response);
        });
    });

    // Custom extraction with complex logic
    group.bench_function("custom_complex", |b| {
        let extractor = CustomGrpcKeyExtractor::new(|req| {
            let method = req.uri().path().trim_start_matches('/');
            let user = req.headers().get("user-id")?.to_str().ok()?;
            Some(format!("{}:{}", method, user))
        });
        let layer = GrpcRateLimitLayer::with_extractor(limiter.clone(), extractor);

        b.to_async(&rt).iter(|| async {
            let mut service = layer.layer(BenchService);
            let req = http::Request::builder()
                .uri("http://example.com/test.Service/Method")
                .header("user-id", "user-123")
                .body(BoxBody::default())
                .unwrap();

            let response = service.ready().await.unwrap().call(req).await.unwrap();
            black_box(response);
        });
    });

    group.finish();
}

fn bench_concurrent_requests(c: &mut Criterion) {
    let mut group = c.benchmark_group("tonic_concurrent");
    group.throughput(Throughput::Elements(100));

    let rt = tokio::runtime::Runtime::new().unwrap();

    // High throughput scenario - 100 concurrent requests to different methods
    group.bench_function("100_concurrent_different_keys", |b| {
        let limiter = Arc::new(
            RateLimiter::builder()
                .requests_per_second(1_000_000)
                .burst(1_000_000)
                .build()
                .unwrap(),
        );

        let layer = GrpcRateLimitLayer::new(limiter);

        b.to_async(&rt).iter(|| async {
            let mut handles = vec![];

            for i in 0..100 {
                let mut service = layer.layer(BenchService);
                let handle = tokio::spawn(async move {
                    let req = http::Request::builder()
                        .uri(format!("http://example.com/test.Service/Method{}", i))
                        .body(BoxBody::default())
                        .unwrap();

                    service.ready().await.unwrap().call(req).await
                });
                handles.push(handle);
            }

            for handle in handles {
                let response = handle.await.unwrap().unwrap();
                black_box(response);
            }
        });
    });

    // Same key contention
    group.bench_function("100_concurrent_same_key", |b| {
        let limiter = Arc::new(
            RateLimiter::builder()
                .requests_per_second(1_000_000)
                .burst(1_000_000)
                .build()
                .unwrap(),
        );

        let layer = GrpcRateLimitLayer::new(limiter);

        b.to_async(&rt).iter(|| async {
            let mut handles = vec![];

            for _ in 0..100 {
                let mut service = layer.layer(BenchService);
                let handle = tokio::spawn(async move {
                    let req = http::Request::builder()
                        .uri("http://example.com/test.Service/Method")
                        .body(BoxBody::default())
                        .unwrap();

                    service.ready().await.unwrap().call(req).await
                });
                handles.push(handle);
            }

            for handle in handles {
                let response = handle.await.unwrap().unwrap();
                black_box(response);
            }
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_key_extractors,
    bench_rate_limit_check_overhead,
    bench_different_key_extractors_performance,
    bench_concurrent_requests,
);
criterion_main!(benches);
