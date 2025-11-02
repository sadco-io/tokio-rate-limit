# tokio-rate-limit

High-performance rate limiting library for Rust with lock-free token accounting, sharded map with fine-grained locking for per-key state, pluggable algorithms, and Axum middleware support.

[![Crates.io](https://img.shields.io/crates/v/tokio-rate-limit)](https://crates.io/crates/tokio-rate-limit)
[![Documentation](https://docs.rs/tokio-rate-limit/badge.svg)](https://docs.rs/tokio-rate-limit)
[![License](https://img.shields.io/crates/l/tokio-rate-limit)](LICENSE-MIT)
[![Build Status](https://img.shields.io/github/workflow/status/danielrcurtis/tokio-rate-limit/CI)](https://github.com/danielrcurtis/tokio-rate-limit/actions)

## Features

- **Blazing Fast**: 17M+ operations/second with lock-free token accounting and sharded concurrent access
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

Benchmarks on an Apple M1 Pro (darwin):

| Configuration | Latency (P50) | Throughput |
|--------------|---------------|------------|
| Single-threaded | 57ns | 17.6M ops/sec |
| 2 threads | 124ns | 8.1M ops/sec |
| 4 threads | 132ns | 7.6M ops/sec |
| 8 threads | 286ns | 3.5M ops/sec |
| 16 threads | 568ns | 1.76M ops/sec |

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
- **Sharded State Management**: DashMap provides fine-grained per-shard locking for key lookup

When a request arrives:
1. Calculate tokens to refill based on elapsed time
2. Attempt to consume one token via compare-and-swap (lock-free)
3. If successful, allow the request
4. If bucket is empty, deny and return retry-after duration

### Architectural Highlights

- **DashMap**: Concurrent hashmap with sharded locking for per-key token buckets (default 16 shards)
- **Lock-Free Token Updates**: Atomic compare-and-swap operations on token counts
- **Precision**: 1000x scaling factor for sub-token precision
- **Zero Allocations**: Hot path avoids heap allocations

**Note on "Lock-Free"**: The token accounting itself uses true lock-free atomic operations. However, accessing per-key state in the DashMap uses fine-grained sharded locking (each shard has its own lock). This provides excellent concurrency while maintaining per-key isolation. For truly lock-free key access, alternatives like `evmap` could be considered, though they have different consistency trade-offs.

## Performance Tuning

The library includes CPU-aware auto-tuning of DashMap's shard count for optimal multi-threaded performance. By default, the shard count is calculated as `(num_cpus * 4).next_power_of_two().max(32)`.

### Auto-Tuning Behavior

| CPU Cores | Shards | Best For |
|-----------|--------|----------|
| 4-8       | 32     | Low-medium contention workloads |
| 12-16     | 64     | Balanced production systems |
| 32+       | 128+   | High-contention scenarios |

### Manual Tuning (Advanced)

For specialized workloads, you can manually specify the shard count:

```rust
use tokio_rate_limit::algorithm::TokenBucket;

// For 2-4 threads
let bucket = TokenBucket::with_shard_count(200, 100, 32);

// For 4-8 threads
let bucket = TokenBucket::with_shard_count(200, 100, 64);

// For 8-16 threads
let bucket = TokenBucket::with_shard_count(200, 100, 128);

// For 16+ threads or high contention
let bucket = TokenBucket::with_shard_count(200, 100, 256);
```

**Performance Impact:**
- **8 threads:** Up to 11% improvement with tuned shards
- **16 threads:** Up to 15% improvement with tuned shards
- **Memory overhead:** ~16KB for 256 shards (negligible)

See [SHARD_TUNING_RESULTS.md](SHARD_TUNING_RESULTS.md) for detailed benchmarks.

## Comparison with Governor

| Feature | tokio-rate-limit | governor |
|---------|------------------|----------|
| **Use Case** | Per-key rate limiting | Global rate limiting |
| **Performance** | 14M ops/sec (single-threaded) | 357M ops/sec (global) |
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
- Uses [DashMap](https://github.com/xacrimon/dashmap) for concurrent key-value storage
- Built with [Tokio](https://tokio.rs) for async runtime
- Axum middleware support via [Tower](https://github.com/tower-rs/tower)

## See Also

- [governor](https://github.com/benwis/governor) - GCRA-based rate limiting
- [tower-governor](https://github.com/benwis/tower-governor) - Tower/Axum integration for governor
- [leaky-bucket](https://github.com/udoprog/leaky-bucket) - Leaky bucket algorithm
