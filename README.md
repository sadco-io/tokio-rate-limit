# tokio-rate-limit

High-performance rate limiting library for Rust with lock-free token accounting, lock-free concurrent hashmap for per-key state, pluggable algorithms, and Axum middleware support.

[![Crates.io](https://img.shields.io/crates/v/tokio-rate-limit)](https://crates.io/crates/tokio-rate-limit)
[![Documentation](https://docs.rs/tokio-rate-limit/badge.svg)](https://docs.rs/tokio-rate-limit)
[![License](https://img.shields.io/crates/l/tokio-rate-limit)](LICENSE-MIT)

**Performance:** 15.2M ops/sec single-threaded | 8.0M ops/sec on 4 cores | Sub-microsecond P99 latency

## Why Another Rate Limiter?

Most Rust rate limiting libraries (like `governor`) are optimized for **global rate limiting** - applying a single limit across all requests. This works great for simple "API allows 1000 requests/sec total" scenarios.

But what if you need **per-client rate limits**? Different limits for each user, IP address, or API key?

That's where `tokio-rate-limit` shines:

- ✅ **Built-in per-key tracking** - Independent buckets for each client/user/IP
- ✅ **Drop-in Axum middleware** - Zero boilerplate, automatic 429 responses with RFC-compliant headers
- ✅ **Cost-based limiting** - Different costs for different operations (NEW in v0.2.0)
- ✅ **Production observability** - Optional tracing & metrics with zero overhead when disabled (NEW in v0.2.0)
- ✅ **15M+ ops/sec performance** - Lock-free design scales to thousands of keys
- ✅ **Memory safe** - TTL-based eviction prevents unbounded growth

**Use Cases:**
- Rate limiting per user account in a multi-tenant SaaS
- Per-IP rate limiting for public APIs
- Per-API-key rate limiting for developer platforms
- Weighted rate limiting (heavy operations consume more tokens)
- Any scenario where you need independent limits for different entities

## Design Goals

1. **Per-Key Performance**: Optimize for thousands of independent rate limit keys, not just a single global limit
2. **Lock-Free**: Zero locks in the hot path - atomic operations only
3. **Production Ready**: Comprehensive testing, observability, standards compliance (IETF headers)
4. **Ergonomic**: Drop-in Axum middleware with sensible defaults
5. **Flexible**: Custom key extraction, cost-based limiting, pluggable algorithms
6. **Safe**: Memory-safe with TTL eviction, overflow protection, deterministic testing

## Features

- **Blazing Fast**: 15M+ operations/second with lock-free token accounting and lock-free concurrent hashmap
- **Per-Key Rate Limiting**: Independent limits per client/IP/user/API key
- **Memory Safe**: Optional TTL-based eviction for high-cardinality keys
- **Overflow Protected**: Saturating arithmetic with explicit bounds prevents panics
- **Standards Compliant**: IETF RateLimit headers (`RateLimit-Limit`, `RateLimit-Remaining`, `RateLimit-Reset`) *(NEW in v0.2.0)*
- **Cost-Based Limiting**: Different token costs for different operations *(NEW in v0.2.0)*
- **Blocking Acquire**: Wait for tokens with `acquire()` and `acquire_timeout()` *(NEW in v0.2.0)*
- **Observability**: Optional tracing and metrics with zero overhead when disabled *(NEW in v0.2.0)*
- **Pluggable Algorithms**: Token bucket and leaky bucket algorithms, sealed for API stability *(NEW in v0.3.0)*
- **Axum Middleware**: Drop-in middleware for Axum web applications with proper headers
- **Custom Key Extraction**: Rate limit by IP, user ID, API key, or any custom logic
- **Deterministic Testing**: Uses tokio::time for testable time controls
- **Zero Allocations**: In the hot path for maximum performance
- **Production Ready**: Comprehensive tests, benchmarks, and documentation

## Performance

Benchmarks on an Apple M1 Pro (darwin) using flurry's lock-free HashMap:

| Configuration | Latency (P50) | Throughput | vs DashMap |
|--------------|---------------|------------|------------|
| Single-threaded | 65ns | 15.2M ops/sec | +19% |
| 2 threads | 117ns | 8.6M ops/sec | +66% |
| 4 threads | 125ns | 8.0M ops/sec | +69% |
| 8 threads | 221ns | 4.5M ops/sec | +117% |
| 16 threads | 384ns | 2.6M ops/sec | +40% |

**Observability Overhead (Optional Features):**
- Baseline (no features): 15.2M ops/sec
- With tracing: 12.8M ops/sec (-16%, but <0.001% in real HTTP workloads)
- With metrics: 12.9M ops/sec (-15%, negligible in production)

See [ENHANCED_API_BENCHMARKS.md](ENHANCED_API_BENCHMARKS.md) for detailed performance analysis.

**Key Insight**: Our library excels at **per-key rate limiting** (separate limits per client), while libraries like `governor` are optimized for **global rate limiting** (single limit for all requests). Both have their use cases, and this library fills the per-key niche with excellent performance.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     tokio-rate-limit                         │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌───────────────────────────────────────────────────────┐  │
│  │                 RateLimiter API                       │  │
│  │  check() | check_with_cost() | acquire()             │  │
│  └────────────────────┬──────────────────────────────────┘  │
│                       │                                      │
│  ┌────────────────────▼──────────────────────────────────┐  │
│  │              Algorithm Trait                          │  │
│  │         (Pluggable, Token Bucket default)             │  │
│  └────────────────────┬──────────────────────────────────┘  │
│                       │                                      │
│  ┌────────────────────▼──────────────────────────────────┐  │
│  │         flurry::HashMap<Key, TokenBucket>             │  │
│  │         (Lock-free concurrent hashmap)                │  │
│  │                                                        │  │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐            │  │
│  │  │ Bucket   │  │ Bucket   │  │ Bucket   │   ...      │  │
│  │  │ "ip1"    │  │ "user2"  │  │ "key3"   │            │  │
│  │  │ tokens:  │  │ tokens:  │  │ tokens:  │            │  │
│  │  │ AtomicU64│  │ AtomicU64│  │ AtomicU64│            │  │
│  │  └──────────┘  └──────────┘  └──────────┘            │  │
│  │                                                        │  │
│  │         Each bucket: atomic CAS operations            │  │
│  │         Zero locks, zero contention                   │  │
│  └────────────────────────────────────────────────────────┘  │
│                                                               │
│  Optional: TTL-based eviction (1% probabilistic cleanup)     │
│  Optional: Tracing spans & metrics                           │
└───────────────────────────────────────────────────────────────┘
```

**Request Flow (Sub-microsecond):**
1. Extract key (e.g., IP address) from request - ~5ns
2. Hash lookup in flurry HashMap (lock-free) - ~20ns
3. Atomic CAS to consume token - ~10ns
4. Calculate remaining tokens & reset time - ~5ns
5. Return decision with IETF headers - ~5ns

**Total: ~45-65ns** for in-memory permission check.

## Quick Start

Add to your `Cargo.toml`:

```toml
[dependencies]
tokio-rate-limit = "0.2"

# For Axum middleware support
tokio-rate-limit = { version = "0.2", features = ["middleware"] }

# For observability (tracing + metrics)
tokio-rate-limit = { version = "0.2", features = ["middleware", "observability"] }
tokio-rate-limit = { version = "0.2", features = ["middleware", "metrics-support"] }
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
        println!("Reset in: {:?}", decision.reset.unwrap());
    } else {
        // Rate limit exceeded
        println!("Rate limited! Retry after: {:?}", decision.retry_after.unwrap());
    }
}
```

### Cost-Based Rate Limiting *(NEW in v0.2.0)*

Assign different costs to different operations:

```rust
let limiter = RateLimiter::builder()
    .requests_per_second(100)
    .burst(200)
    .build()
    .unwrap();

// Light operation - costs 1 token
limiter.check_with_cost("user-123", 1).await?;

// Heavy operation - costs 50 tokens
limiter.check_with_cost("user-123", 50).await?;

// Use cases:
// - Simple queries: cost=1, Complex queries: cost=10
// - Small uploads: cost=1, Large uploads: cost=100
// - Fast API calls: cost=1, Expensive AI inference: cost=50
```

### Blocking Acquire *(NEW in v0.2.0)*

Wait for tokens to become available:

```rust
// Block indefinitely until tokens available
let decision = limiter.acquire("user-123").await?;

// Block with timeout
use std::time::Duration;
let decision = limiter.acquire_timeout("user-123", Duration::from_secs(5)).await?;
if !decision.permitted {
    println!("Timed out waiting for tokens");
}

// Non-blocking (original behavior)
let decision = limiter.try_acquire("user-123").await?;
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

**Response Headers (IETF RFC Standards):**

When rate limit is applied, responses include:

```http
RateLimit-Limit: 100
RateLimit-Remaining: 42
RateLimit-Reset: 3
X-RateLimit-Limit: 100          # Legacy header (backward compat)
X-RateLimit-Remaining: 42       # Legacy header (backward compat)
```

When rate limit is exceeded (HTTP 429):

```http
HTTP/1.1 429 Too Many Requests
RateLimit-Limit: 100
RateLimit-Remaining: 0
RateLimit-Reset: 8
Retry-After: 8
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

## Algorithms *(NEW in v0.3.0)*

This library provides two rate limiting algorithms with different characteristics:

### Token Bucket (Default)

The token bucket algorithm allows bursts up to capacity and refills at a constant rate.

**Characteristics:**
- Allows bursts up to bucket capacity
- Refills at constant rate (tokens/second)
- Good for accommodating bursty traffic
- Permits temporary spikes in usage

**Best For:**
- Public APIs and user-facing services
- Mobile applications with intermittent connectivity
- Scenarios where users expect burst capability
- General-purpose rate limiting

**Example:**
```rust
use tokio_rate_limit::RateLimiter;

// Token bucket: 100/sec rate, burst of 200
let limiter = RateLimiter::builder()
    .requests_per_second(100)
    .burst(200)
    .build()
    .unwrap();
```

### Leaky Bucket *(NEW in v0.3.0)*

The leaky bucket algorithm enforces a steady rate by "leaking" tokens at a constant rate.

**Characteristics:**
- Enforces strict steady rate, no bursts
- Smooths traffic into consistent flow
- Requests add tokens to bucket; overflow = deny
- More predictable load on downstream services

**Best For:**
- Backend protection and rate smoothing
- Strict QPS enforcement requirements
- Preventing overwhelming downstream services
- Fair queuing scenarios

**Example:**
```rust
use tokio_rate_limit::RateLimiter;
use tokio_rate_limit::algorithm::LeakyBucket;

// Leaky bucket: capacity 50, leak rate 100/sec
let algorithm = LeakyBucket::new(50, 100);
let limiter = RateLimiter::from_algorithm(algorithm);
```

### Comparison

| Feature | Token Bucket | Leaky Bucket |
|---------|--------------|--------------|
| **Bursts** | ✅ Allowed (up to capacity) | ❌ Not allowed |
| **Rate Enforcement** | Average over time | Strict steady rate |
| **Traffic Pattern** | Bursty | Smooth |
| **Best For** | Public APIs, users | Backend protection |
| **Predictability** | Moderate | High |

**When to Choose:**

- **Token Bucket**: When users expect burst capability (e.g., uploading multiple files, batch operations)
- **Leaky Bucket**: When protecting backends from overload (e.g., database query limiting, API gateway)

See `examples/leaky_bucket.rs` for a detailed comparison with examples.

## Memory Safety and TTL Eviction

By default, token buckets are created on-demand and persist indefinitely. For high-cardinality keys (e.g., per-IP limits with millions of IPs), use TTL-based eviction:

```rust
use std::time::Duration;
use tokio_rate_limit::algorithm::TokenBucket;

// Evict idle buckets after 1 hour
let algorithm = TokenBucket::with_ttl(
    200,                          // capacity
    100,                          // refill rate per second
    Duration::from_secs(3600)     // TTL
);

let limiter = RateLimiter::from_algorithm(algorithm);
```

**How it works:**
- Each token bucket tracks last access time
- 1% probabilistic cleanup check on each access
- Idle buckets are removed after TTL expires
- Prevents unbounded memory growth
- Minimal performance impact (<1%)

**Guidance:**
- **Low cardinality** (hundreds of keys): No TTL needed
- **Medium cardinality** (thousands of keys): TTL = 1-24 hours
- **High cardinality** (millions of keys): TTL = 15-60 minutes

## Observability *(NEW in v0.2.0)*

Enable distributed tracing and metrics for production debugging:

```toml
# Cargo.toml
tokio-rate-limit = { version = "0.2", features = ["middleware", "observability"] }

# For metrics collection
tokio-rate-limit = { version = "0.2", features = ["middleware", "metrics-support"] }
```

### Distributed Tracing

When `observability` feature is enabled, all rate limit checks create trace spans:

```rust
use tracing_subscriber;

// Configure tracing subscriber (once at startup)
tracing_subscriber::fmt::init();

// All rate limit checks now emit spans
let decision = limiter.check("user-123").await?;

// Span includes:
// - key: "user-123"
// - permitted: true/false
// - remaining: token count
// - latency: nanoseconds
```

**Trace Output Example:**
```
DEBUG tokio_rate_limit::limiter: Rate limit check: PERMITTED key="user-123" remaining=199
```

### Metrics

When `metrics-support` feature is enabled:

```rust
// Metrics automatically recorded:
// - tokio_rate_limit.requests.allowed (counter)
// - tokio_rate_limit.requests.denied (counter)
// - tokio_rate_limit.remaining_tokens (histogram)

// Use any metrics backend (Prometheus, StatsD, etc.)
use metrics_exporter_prometheus::PrometheusBuilder;

PrometheusBuilder::new()
    .install()
    .expect("Failed to install Prometheus exporter");
```

**Performance Impact:**
- **No features (default):** Zero overhead
- **observability:** ~8-19% in microbenchmarks, <0.001% in real HTTP workloads
- **metrics-support:** ~18-34% in microbenchmarks, <0.001% in real HTTP workloads

See [OBSERVABILITY.md](OBSERVABILITY.md) for comprehensive integration guide with OpenTelemetry, Jaeger, Prometheus, and production best practices.

## Examples

See the `examples/` directory for complete working examples:

- [`basic.rs`](examples/basic.rs) - Direct usage without middleware
- [`axum_middleware.rs`](examples/axum_middleware.rs) - IP-based rate limiting with Axum
- [`custom_key_extraction.rs`](examples/custom_key_extraction.rs) - User ID and API key rate limiting
- [`cost_based_limiting.rs`](examples/cost_based_limiting.rs) - Weighted operations *(NEW in v0.2.0)*
- [`blocking_acquire.rs`](examples/blocking_acquire.rs) - Wait patterns *(NEW in v0.2.0)*
- [`leaky_bucket.rs`](examples/leaky_bucket.rs) - Algorithm comparison: token vs leaky bucket *(NEW in v0.3.0)*

Run examples:

```bash
# Basic usage
cargo run --example basic

# Axum middleware with IP-based rate limiting
cargo run --example axum_middleware --features middleware

# Custom key extraction (user ID, API key)
cargo run --example custom_key_extraction --features middleware

# Cost-based limiting
cargo run --example cost_based_limiting

# Blocking acquire patterns
cargo run --example blocking_acquire

# Leaky bucket algorithm comparison
cargo run --example leaky_bucket
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
2. Attempt to consume `cost` tokens via compare-and-swap (lock-free)
3. If successful, allow the request
4. If bucket is empty, deny and return retry-after duration
5. Calculate reset time (when bucket will be full)

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
| **Performance** | 15.2M ops/sec (single-threaded) | 357M ops/sec (global) |
| **Key Management** | Built-in per-key tracking | Manual key management |
| **Middleware** | Axum integration included | DIY middleware |
| **Algorithm** | Pluggable (token bucket default) | GCRA algorithm |
| **Standards** | IETF RateLimit headers | Custom headers |
| **Cost-Based** | ✅ Built-in | ❌ Not supported |
| **Observability** | ✅ Optional tracing/metrics | ❌ Manual |

**When to use tokio-rate-limit:**
- You need per-client/per-user/per-IP rate limits
- You want drop-in Axum middleware
- You need custom key extraction logic
- You want cost-based/weighted rate limiting
- You need IETF-compliant headers
- You want optional observability

**When to use governor:**
- You need a single global rate limit
- You want maximum single-limiter performance
- You prefer the GCRA algorithm

Both libraries are excellent choices depending on your use case!

## Feature Flags

- `middleware` - Enables Axum middleware support (adds `axum` and `tower` dependencies)
- `observability` - Enables distributed tracing via `tracing` crate *(NEW in v0.2.0)*
- `metrics-support` - Enables metrics collection via `metrics` crate (implies `observability`) *(NEW in v0.2.0)*

## API Documentation

Full API documentation is available at [docs.rs/tokio-rate-limit](https://docs.rs/tokio-rate-limit).

## What's New in v0.3.0

- **Leaky Bucket Algorithm**: New algorithm for enforcing steady rate without bursts
- **Sealed Algorithm Trait**: Trait is now sealed for API stability, allows non-breaking changes
- **from_algorithm()**: Create RateLimiter with custom algorithms (TokenBucket or LeakyBucket)
- **Algorithm Comparison**: New example demonstrating differences between algorithms

See [CHANGELOG.md](CHANGELOG.md) for complete release notes and v0.2.0 features.

## Minimum Supported Rust Version (MSRV)

This crate requires Rust 1.75.0 or later.

## Performance Tips

1. **Reuse RateLimiter instances**: Create once, use many times (wrap in `Arc`)
2. **Choose appropriate burst sizes**: Burst should be ≥ requests_per_second
3. **Key length**: Shorter keys perform better (IP addresses are fine)
4. **TTL for high-cardinality keys**: Use `TokenBucket::with_ttl()` when you have millions of unique keys
5. **Observability**: Enable only in production where the operational benefits outweigh the minimal overhead

## Testing

```bash
# Run all tests
cargo test

# Run tests with all features
cargo test --all-features

# Run benchmarks
cargo bench

# Run specific benchmark
cargo bench --bench rate_limit_performance

# Run examples
cargo run --example basic
cargo run --example axum_middleware --features middleware
cargo run --example cost_based_limiting
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
