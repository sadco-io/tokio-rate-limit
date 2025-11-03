# tokio-rate-limit

High-performance rate limiting library for Rust with lock-free token accounting, lock-free concurrent hashmap for per-key state, pluggable algorithms, and Axum middleware support.

[![Crates.io](https://img.shields.io/crates/v/tokio-rate-limit)](https://crates.io/crates/tokio-rate-limit)
[![Documentation](https://docs.rs/tokio-rate-limit/badge.svg)](https://docs.rs/tokio-rate-limit)
[![License](https://img.shields.io/crates/l/tokio-rate-limit)](LICENSE-MIT)
[![Build Status](https://img.shields.io/github/workflow/status/danielrcurtis/tokio-rate-limit/CI)](https://github.com/danielrcurtis/tokio-rate-limit/actions)

## Features

- **Blazing Fast**: 17M+ operations/second with lock-free token accounting and lock-free concurrent hashmap
- **Per-Key Rate Limiting**: Independent limits per client/IP/user/API key
- **Memory Safe**: Optional TTL-based eviction for high-cardinality keys
- **Overflow Protected**: Saturating arithmetic with explicit bounds prevents panics
- **Pluggable Algorithms**: Token bucket included, extensible for custom algorithms
- **Axum Middleware**: Drop-in middleware for Axum web applications with proper headers
- **Custom Key Extraction**: Rate limit by IP, user ID, API key, or any custom logic
- **Deterministic Testing**: Uses tokio::time for testable time controls
- **Zero Allocations**: In the hot path for maximum performance
- **Production Ready**: Comprehensive tests, benchmarks, and documentation

## Performance

Benchmarks on an Apple M1 Pro (darwin) using flurry's lock-free HashMap:

| Configuration | Latency (P50) | Throughput | vs DashMap |
|--------------|---------------|------------|------------|
| Single-threaded | 56ns | 17.7M ops/sec | +19% |
| 2 threads | 64ns | 15.5M ops/sec | +66% |
| 4 threads | 74ns | 13.5M ops/sec | +69% |
| 8 threads | 141ns | 7.1M ops/sec | +117% |
| 16 threads | 406ns | 2.5M ops/sec | +40% |

**Key Insight**: Our library excels at per-key rate limiting (separate limits per client), while libraries like `governor` are optimized for global rate limiting (single limit for all requests). Both have their use cases, and this library fills the per-key niche with excellent performance.

## Quick Start

Add to your `Cargo.toml`:

```toml
[dependencies]
tokio-rate-limit = "0.1"

# For Axum middleware support
tokio-rate-limit = { version = "0.1", features = ["middleware"] }
```

### Basic Usage

```rust
use tokio_rate_limit::RateLimiter;

#[tokio::main]
async fn main() {
    // Create a rate limiter: 100 requests/second, burst of 200
    let limiter = RateLimiter::builder()
        .requests_per_second(100)
        .burst(200)
        .build()
        .unwrap();

    // Check if a request should be allowed
    let decision = limiter.check("client-123").await.unwrap();

    if decision.permitted {
        // Process request
        println!("Request allowed! Remaining: {}", decision.remaining.unwrap());
    } else {
        // Rate limit exceeded
        println!("Rate limited! Retry after: {:?}", decision.retry_after.unwrap());
    }
}
```

### Axum Middleware

```rust
use axum::{Router, routing::get};
use tokio_rate_limit::{RateLimiter, middleware::RateLimitLayer};
use std::sync::Arc;

#[tokio::main]
async fn main() {
    let limiter = Arc::new(
        RateLimiter::builder()
            .requests_per_second(100)
            .burst(200)
            .build()
            .unwrap()
    );

    let app: Router = Router::new()
        .route("/api/data", get(handler))
        // Apply rate limiting to all routes (IP-based by default)
        .layer(RateLimitLayer::new(limiter));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000")
        .await
        .unwrap();

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .await
    .unwrap();
}

async fn handler() -> &'static str {
    "Hello, World!"
}
```

### Custom Key Extraction

Rate limit by user ID, API key, or any custom logic:

```rust
use tokio_rate_limit::middleware::{RateLimitLayer, CustomKeyExtractor};
use axum::{body::Body, extract::Request};

let limiter = Arc::new(
    RateLimiter::builder()
        .requests_per_second(50)
        .burst(100)
        .build()
        .unwrap()
);

// Extract user ID from header
let app: Router = Router::new()
    .route("/api/user-data", get(handler))
    .layer(RateLimitLayer::with_extractor(
        limiter,
        CustomKeyExtractor::new(|req: &Request<Body>| {
            req.headers()
                .get("X-User-Id")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string())
        }),
    ));
```

## Examples

See the `examples/` directory for complete working examples:

- [`basic.rs`](examples/basic.rs) - Direct usage without middleware
- [`axum_middleware.rs`](examples/axum_middleware.rs) - IP-based rate limiting with Axum
- [`custom_key_extraction.rs`](examples/custom_key_extraction.rs) - User ID and API key rate limiting

Run examples:

```bash
# Basic usage
cargo run --example basic

# Axum middleware with IP-based rate limiting
cargo run --example axum_middleware --features middleware

# Custom key extraction (user ID, API key)
cargo run --example custom_key_extraction --features middleware
```

## How It Works

### Token Bucket Algorithm

The library uses a token bucket algorithm for rate limiting:

- **Bucket Capacity**: Maximum burst size (e.g., 200 tokens)
- **Refill Rate**: Tokens added per second (e.g., 100 tokens/sec)
- **Per-Key Buckets**: Each client/user/key has an independent bucket
- **Lock-Free Token Accounting**: Uses atomic operations for token updates without locks
- **Lock-Free State Management**: flurry provides lock-free concurrent hashmap for key lookup

When a request arrives:
1. Calculate tokens to refill based on elapsed time
2. Attempt to consume one token via compare-and-swap (lock-free)
3. If successful, allow the request
4. If bucket is empty, deny and return retry-after duration

### Architectural Highlights

- **flurry**: Lock-free concurrent hashmap (Java ConcurrentHashMap port) for per-key token buckets
- **Lock-Free Token Updates**: Atomic compare-and-swap operations on token counts
- **Auto-Tuning**: flurry automatically tunes internal parameters for optimal performance
- **Precision**: 1000x scaling factor for sub-token precision
- **Zero Allocations**: Hot path avoids heap allocations

The entire hot path is lock-free, using atomic operations for both token accounting and key access.

## Performance Tuning

As of v0.2.0, the library uses flurry's lock-free concurrent hashmap which automatically tunes its internal parameters for optimal performance across different workloads and thread counts. No manual tuning is required.

**Performance improvements in v0.2.0:**
- **Single-threaded:** +19% improvement over DashMap
- **2 threads:** +66% improvement over DashMap
- **4 threads:** +69% improvement over DashMap
- **8 threads:** +117% improvement over DashMap
- **16 threads:** +40% improvement over DashMap

The `with_shard_count()` method is now deprecated and internally calls the standard constructor, as flurry does not expose shard configuration.

## Comparison with Governor

| Feature | tokio-rate-limit | governor |
|---------|------------------|----------|
| **Use Case** | Per-key rate limiting | Global rate limiting |
| **Performance** | 17.7M ops/sec (single-threaded) | 357M ops/sec (global) |
| **Key Management** | Built-in per-key tracking | Manual key management |
| **Middleware** | Axum integration included | DIY middleware |
| **Algorithm** | Pluggable (token bucket default) | GCRA algorithm |

**When to use tokio-rate-limit:**
- You need per-client/per-user/per-IP rate limits
- You want drop-in Axum middleware
- You need custom key extraction logic
- You want pluggable algorithms

**When to use governor:**
- You need a single global rate limit
- You want maximum single-limiter performance
- You prefer the GCRA algorithm

Both libraries are excellent choices depending on your use case!

## Feature Flags

- `middleware` - Enables Axum middleware support (adds `axum` and `tower` dependencies)

## API Documentation

Full API documentation is available at [docs.rs/tokio-rate-limit](https://docs.rs/tokio-rate-limit).

## Minimum Supported Rust Version (MSRV)

This crate requires Rust 1.75.0 or later.

## Performance Tips

1. **Reuse RateLimiter instances**: Create once, use many times (wrap in `Arc`)
2. **Choose appropriate burst sizes**: Burst should be ≥ requests_per_second
3. **Key length**: Shorter keys perform better (IP addresses are fine)
4. **Cleanup**: Token buckets are created on-demand and never removed (consider periodic cleanup for high-cardinality keys)

## Testing

```bash
# Run all tests
cargo test

# Run tests with middleware feature
cargo test --features middleware

# Run benchmarks
cargo bench

# Run examples
cargo run --example basic
cargo run --example axum_middleware --features middleware
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

## Acknowledgments

- Inspired by [governor](https://github.com/benwis/governor) for Rust rate limiting
- Uses [flurry](https://github.com/jonhoo/flurry) for lock-free concurrent hashmap (Java ConcurrentHashMap port)
- Built with [Tokio](https://tokio.rs) for async runtime
- Axum middleware support via [Tower](https://github.com/tower-rs/tower)

## See Also

- [governor](https://github.com/benwis/governor) - GCRA-based rate limiting
- [tower-governor](https://github.com/benwis/tower-governor) - Tower/Axum integration for governor
- [leaky-bucket](https://github.com/udoprog/leaky-bucket) - Leaky bucket algorithm
