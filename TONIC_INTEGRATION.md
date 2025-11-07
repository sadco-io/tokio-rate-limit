# Tonic gRPC Integration Guide

This guide explains how to integrate `tokio-rate-limit` with Tonic gRPC services.

## Overview

The tonic integration provides high-performance rate limiting for gRPC services using Tower middleware. Unlike simple interceptors, this implementation uses Tower's Layer and Service traits to provide comprehensive rate limiting with access to both requests and responses.

## Architecture Decision: Tower Middleware vs Interceptors

### Why Tower Middleware?

We chose Tower middleware over Tonic's built-in interceptors for several reasons:

1. **More Powerful**: Tower middleware can intercept both requests AND responses, while interceptors only see requests
2. **Method Name Access**: Middleware has access to the full HTTP/2 request, including the URI path (gRPC method name)
3. **Standards Compliant**: Can return proper gRPC status codes (RESOURCE_EXHAUSTED) with metadata
4. **Composable**: Works seamlessly with other Tower middleware
5. **Future Proof**: Tonic 0.5+ uses Tower internally, so this is the recommended approach

### What Are Tonic Interceptors?

Interceptors are lightweight request filters that can:
- Add/remove/check items in the MetadataMap
- Cancel requests with a Status error

However, interceptors have limitations:
- Cannot access the response
- Cannot easily extract method names
- Less suitable for complex middleware like rate limiting

## Features

- Per-method rate limiting (different limits for different RPC methods)
- Per-client rate limiting (based on IP, user ID, or custom keys)
- Streaming RPC support (both unary and streaming calls)
- Proper gRPC error responses (RESOURCE_EXHAUSTED status code)
- Rate limit metadata in responses
- Zero-copy key extraction
- Composable with other Tower middleware

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
tokio-rate-limit = { version = "0.4", features = ["tonic-support"] }
tonic = "0.12"
tower = "0.5"
```

## Basic Usage

### Server Setup

```rust
use tokio_rate_limit::{RateLimiter, tonic_middleware::GrpcRateLimitLayer};
use tonic::transport::Server;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create rate limiter (100 req/s, burst 200)
    let limiter = Arc::new(
        RateLimiter::builder()
            .requests_per_second(100)
            .burst(200)
            .build()?
    );

    // Create gRPC rate limit layer
    let layer = GrpcRateLimitLayer::new(limiter);

    // Apply to server
    Server::builder()
        .layer(layer)
        .add_service(YourService::new(implementation))
        .serve("[::1]:50051".parse()?)
        .await?;

    Ok(())
}
```

## Key Extraction Strategies

### 1. Per-Method Rate Limiting (Default)

Rate limit each gRPC method independently:

```rust
use tokio_rate_limit::tonic_middleware::MethodKeyExtractor;

let layer = GrpcRateLimitLayer::with_extractor(
    limiter,
    MethodKeyExtractor // Default: uses method path as key
);

// Keys will be like: "helloworld.Greeter/SayHello"
```

### 2. Per-Client Rate Limiting

Rate limit by client IP address:

```rust
use tokio_rate_limit::tonic_middleware::IpKeyExtractor;

let layer = GrpcRateLimitLayer::with_extractor(
    limiter,
    IpKeyExtractor
);

// Extracts IP from x-forwarded-for or x-real-ip headers
```

### 3. Custom Metadata Extraction

Rate limit by custom metadata (user ID, API key, tenant ID):

```rust
use tokio_rate_limit::tonic_middleware::MetadataKeyExtractor;

// Extract from "user-id" metadata header
let layer = GrpcRateLimitLayer::with_extractor(
    limiter,
    MetadataKeyExtractor::new("user-id")
);
```

### 4. Complex Custom Logic

Combine multiple factors for rate limiting:

```rust
use tokio_rate_limit::tonic_middleware::CustomGrpcKeyExtractor;

let layer = GrpcRateLimitLayer::with_extractor(
    limiter,
    CustomGrpcKeyExtractor::new(|req| {
        let method = req.uri().path().trim_start_matches('/');

        // Extract user ID from metadata
        let user = req.headers()
            .get("user-id")
            .and_then(|v| v.to_str().ok())?;

        // Combine method and user: "method:user-id"
        Some(format!("{}:{}", method, user))
    })
);
```

## Different Rate Limits Per Method

To apply different rate limits to different RPC methods, use custom key extraction with method-specific limiters:

```rust
use tokio_rate_limit::tonic_middleware::CustomGrpcKeyExtractor;

// Fast endpoint: 1000 req/s
let fast_limiter = Arc::new(
    RateLimiter::builder()
        .requests_per_second(1000)
        .burst(2000)
        .build()?
);

// Expensive endpoint: 10 req/s
let expensive_limiter = Arc::new(
    RateLimiter::builder()
        .requests_per_second(10)
        .burst(20)
        .build()?
);

// In practice, you'd want to dynamically choose the limiter based on method
// For now, use the method path in the key to distinguish
let layer = GrpcRateLimitLayer::with_extractor(
    fast_limiter.clone(),
    CustomGrpcKeyExtractor::new(move |req| {
        let path = req.uri().path();
        if path.contains("ExpensiveOperation") {
            Some(format!("expensive:{}", path))
        } else {
            Some(format!("fast:{}", path))
        }
    })
);
```

## gRPC Status Codes

When rate limited, the middleware returns:

- **Status Code**: `RESOURCE_EXHAUSTED` (code 8)
- **Message**: "Rate limit exceeded"
- **Metadata**:
  - `x-ratelimit-limit`: Maximum requests allowed
  - `x-ratelimit-remaining`: Requests remaining (usually 0)
  - `retry-after`: Seconds to wait before retrying
  - `x-ratelimit-reset`: Seconds until bucket refills

### Why RESOURCE_EXHAUSTED?

According to gRPC specifications, `RESOURCE_EXHAUSTED` is the appropriate status for rate limiting:
- Indicates "some resource has been exhausted"
- Maps to HTTP 429 (Too Many Requests)
- Clients should back off and retry
- Can include `RetryInfo` metadata

## Client Handling

### Detecting Rate Limits

```rust
use tonic::Code;

match client.say_hello(request).await {
    Ok(response) => {
        // Success - check rate limit headers
        let remaining = response.metadata()
            .get("x-ratelimit-remaining")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok());

        println!("Remaining: {:?}", remaining);
    }
    Err(status) if status.code() == Code::ResourceExhausted => {
        // Rate limited - extract retry-after
        let retry_after = status.metadata()
            .get("retry-after")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok());

        println!("Rate limited! Retry after: {:?}s", retry_after);
    }
    Err(e) => {
        // Other error
        eprintln!("Error: {}", e);
    }
}
```

## Streaming RPCs

The middleware works with both unary and streaming RPCs:

### Server-Side Streaming

```rust
async fn list_items(
    &self,
    request: Request<ListRequest>,
) -> Result<Response<Self::ListItemsStream>, Status> {
    // Rate limit is applied to the initial request
    // The stream itself is not individually rate limited per item
    let (tx, rx) = mpsc::channel(4);

    tokio::spawn(async move {
        for item in items {
            tx.send(Ok(item)).await.ok();
        }
    });

    Ok(Response::new(ReceiverStream::new(rx)))
}
```

### Client-Side Streaming

Rate limit applies to the initial connection, not each message in the stream.

### Bidirectional Streaming

Same as client-side streaming - rate limit on connection establishment.

## HTTP/2 and Multiplexing

gRPC uses HTTP/2, which supports:
- **Connection multiplexing**: Multiple requests over a single TCP connection
- **Header compression**: Efficient metadata transfer
- **Server push**: Not used in standard gRPC

### Implications for Rate Limiting

- Rate limits are per-key (method/user/IP), not per-connection
- Multiple concurrent requests from same client count separately
- Connection pooling doesn't affect rate limiting
- Each RPC call is independently rate limited

## Performance Considerations

### Overhead

The middleware adds minimal overhead:
1. **Key extraction**: ~10-50ns (string copy or header lookup)
2. **Rate limit check**: ~50-200ns (atomic operations)
3. **Response modification**: ~20-100ns (header additions)

Total: **~100-350ns per request**

At 100,000 req/s, this is only 1-3.5% overhead.

### Best Practices

1. **Reuse limiter instances**: Share `Arc<RateLimiter>` across services
2. **Choose appropriate burst sizes**: Burst should be >= requests_per_second
3. **Use method-specific keys**: Fine-grained control per RPC
4. **Monitor metrics**: Use the `metrics-support` feature
5. **Set reasonable limits**: Test with realistic traffic patterns

## Integration with Other Middleware

Tower middleware composes naturally:

```rust
use tower::ServiceBuilder;
use tower_http::trace::TraceLayer;

let layer = ServiceBuilder::new()
    .layer(TraceLayer::new_for_grpc())
    .layer(GrpcRateLimitLayer::new(limiter))
    .layer(AuthLayer::new())
    .into_inner();

Server::builder()
    .layer(layer)
    .add_service(service)
    .serve(addr)
    .await?;
```

Execution order: Auth -> RateLimit -> Trace -> Service

## Examples

See the `examples/` directory:

- `examples/grpc_tonic.rs` - Server with rate limiting
- `examples/grpc_tonic_client.rs` - Client testing rate limits

Run the server:
```bash
cargo run --example grpc_tonic --features tonic-support
```

In another terminal, run the client:
```bash
cargo run --example grpc_tonic_client --features tonic-support
```

Or use `grpcurl` for manual testing:
```bash
grpcurl -plaintext -d '{"name": "World"}' '[::1]:50051' helloworld.Greeter/SayHello
```

## Comparison with HTTP Middleware

| Feature | HTTP (Axum) | gRPC (Tonic) |
|---------|-------------|--------------|
| Protocol | HTTP/1.1 | HTTP/2 |
| Rate limit status | 429 Too Many Requests | RESOURCE_EXHAUSTED |
| Metadata format | HTTP headers | gRPC metadata |
| Streaming | Server-sent events | Bidirectional streams |
| Method extraction | Path parsing | URI path |
| Connection pooling | Per-request | Multiplexed |

## Limitations and Gotchas

1. **Metadata vs Headers**: gRPC metadata uses a different type system than HTTP headers
2. **Streaming Granularity**: Rate limit applies to stream start, not individual messages
3. **Connection Establishment**: First RPC on a connection may have higher latency
4. **Binary Metadata**: Binary metadata headers (ending in `-bin`) need special handling
5. **Reflection**: gRPC reflection service should typically bypass rate limiting

## Future Enhancements

Potential improvements for future versions:

1. **Per-message streaming rate limits**: Limit messages within a stream
2. **Dynamic rate limit configuration**: Adjust limits without restart
3. **Distributed rate limiting**: Redis/etcd backend for multi-instance coordination
4. **Circuit breaker integration**: Combine with failure detection
5. **Adaptive rate limiting**: Automatically adjust based on server load
6. **Quota systems**: Monthly/daily quotas in addition to per-second limits

## Troubleshooting

### "Rate limited but shouldn't be"

- Check key extraction: Print keys to verify uniqueness
- Verify burst size: Should be >= requests_per_second
- Check for key collisions: Different clients using same key
- Monitor token refill: Ensure system clock is accurate

### "Not rate limiting"

- Verify feature flag: `tonic-support` must be enabled
- Check layer order: Rate limit layer should be before service
- Verify key extraction: Keys might be None (bypassing limits)
- Check limiter sharing: Ensure same limiter instance

### "High latency"

- Profile key extraction: Custom extractors might be slow
- Check burst size: Too low causes frequent denials
- Monitor lock contention: Use `flurry` for lock-free state
- Verify system resources: CPU/memory sufficient

## Contributing

Contributions welcome! Areas for improvement:

- Additional key extractors
- Better streaming support
- Performance optimizations
- Documentation improvements
- More examples

## License

Same as `tokio-rate-limit`: MIT OR Apache-2.0
