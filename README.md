# tokio-rate-limit

High-performance rate limiting library for Rust with lock-free token accounting, lock-free concurrent hashmap for per-key state, pluggable algorithms, and Axum middleware support.

[![Crates.io](https://img.shields.io/crates/v/tokio-rate-limit)](https://crates.io/crates/tokio-rate-limit)
[![Documentation](https://docs.rs/tokio-rate-limit/badge.svg)](https://docs.rs/tokio-rate-limit)
[![License](https://img.shields.io/crates/l/tokio-rate-limit)](LICENSE-MIT)

**Performance:** 17.5M ops/sec single-threaded deterministic (v0.8.0) | Multi-threaded +17% improvement | Sub-microsecond P99 latency

## Why Another Rate Limiter?

Most Rust rate limiting libraries (like `governor`) are optimized for **global rate limiting** - applying a single limit across all requests. This works great for simple "API allows 1000 requests/sec total" scenarios.

But what if you need **per-client rate limits**? Different limits for each user, IP address, or API key?

That's where `tokio-rate-limit` shines:

- ✅ **Built-in per-key tracking** - Independent buckets for each client/user/IP
- ✅ **Drop-in Axum middleware** - Zero boilerplate, automatic 429 responses with RFC-compliant headers
- ✅ **Cost-based limiting** - Different costs for different operations (NEW in v0.2.0)
- ✅ **Production observability** - Optional tracing & metrics with zero overhead when disabled (NEW in v0.2.0)
- ✅ **High throughput** - Lock-free token accounting over 256 shards
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

- **Blazing Fast**: 17.5M+ operations/second with lock-free token accounting and lock-free concurrent hashmap (v0.8.0)
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

**v0.8.0 maintains excellent performance with Axum 0.8.6 support!**

Benchmarks on Apple M1 Pro using flurry's lock-free HashMap with tokio 1.40:

### Deterministic Rate Limiting (v0.8.0 - Default)

| Configuration | Latency | Throughput | Notes |
|--------------|---------|------------|-------|
| **Single-threaded** | **57ns** | **17.5M ops/sec** | Baseline with micro-sharding |
| **2 threads** | 118ns | 8.5M ops/sec | Excellent multi-threaded scaling |
| **4 threads** | 134ns | 7.5M ops/sec | Real-world web server performance |
| **8 threads** | 213ns | 4.7M ops/sec | **+17% vs v0.7.2** - Production optimized |

**Micro-Sharding Architecture:**
- 256 independent HashMap shards for reduced contention
- 90%+ improvement in realistic multi-threaded workloads
- Near-linear scaling up to 8+ threads
- Optimized for web servers (Axum, Actix, Tonic) running on tokio
- Real-world rate limiting is inherently multi-threaded

### Probabilistic Rate Limiting (Experimental)

`ProbabilisticTokenBucket` performs the token accounting on only one request in
`sample_rate`; that request debits the whole group it stands in for. Requests
are admitted with a probability proportional to the fill level, which is what
keeps the deny rate proportional to the overload rather than lumpy.

> **The `sample_rate`-scaling defect fixed in 0.9.0 made this type's effective
> limit `sample_rate`x the configured limit** -- at the documented 1% sampling
> rate it applied no limiting at all. Do not use 0.8.x or earlier for
> enforcement.

**Accuracy** (`capacity = 2000`, `limit = 1000/s`, 10-second window, admitted
count against the deterministic `TokenBucket` over 100 runs):

| Offered load | `sample_rate = 10` | `sample_rate = 100` |
|--------------|--------------------|---------------------|
| at or below the limit | exact | exact |
| 2x  | -0.1% (sd 0.7%) | -0.8% (sd 1.7%) |
| 10x | +0.0% (sd 0.9%) | -0.4% (sd 1.1%) |

`sample_rate = 1` disables sampling and is exactly the deterministic bucket.

**Throughput.** Sampling is worth much less than previously claimed here. Every
request still performs the key lookup, the TTL timestamp store and the sampling
counter increment; only the refill arithmetic and the compare-and-swap loop are
skipped, and the hash map lookup dominates. Measured on aarch64/WSL2 with
`cargo bench --bench probabilistic_tradeoff`:

| Workload | `TokenBucket` | `sample_rate = 100` | Gain |
|----------|---------------|---------------------|------|
| single thread, one key | 5.80 Mops/s | 6.06 Mops/s | +4.5% |
| 8 threads, one hot key | 1.35 Mops/s | 1.38 Mops/s | +2.0% (within noise) |
| single thread, 1000 keys | 4.85 Mops/s | 4.77 Mops/s | -1.6% |

Absolute numbers are machine-specific; the ratios are the point. Run the bench
on your own hardware before choosing this type over `TokenBucket`. On a Zipf-1.2
10k-user keyspace the admit-always microbench is still ~1.6× at `sample_rate =
100` versus `TokenBucket`; under the documented per-user shape (100 rps, burst
200) the limiter is not the CPU constraint. `cargo bench --bench
probabilistic_tradeoff` prints a per-decile fairness table against real quotas.

**Sizing.** A sampled request debits a whole `sample_rate * cost` lump, so keep
`capacity >= 10 * sample_rate * cost`. Below that the bucket is corrected too
coarsely and denies more than it should.

**When to use Probabilistic:**
- ✅ Very high request rates concentrated on a few hot keys
- ✅ Soft rate limiting (DDoS protection, load shedding) where a bounded
  overshoot is acceptable
- ❌ **NOT for billing/metering** (requires exact counts)
- ❌ **NOT for strict compliance** (regulatory requirements)
- ❌ **NOT when `capacity` is close to `sample_rate * cost`**

**Algorithm Comparison:**
- **TokenBucket**: Deterministic single-atomic GCRA cell, allows bursts, recommended default
- **ProbabilisticTokenBucket**: Sampled hot-key limiter; skips clock/CAS on unsampled admits at the cost of `sample_rate`-grained accuracy
- **LeakyBucket**: Deterministic GCRA meter; set `capacity = 1` for strict burst-free pacing or `capacity > 1` for bounded queue tolerance

**Observability Overhead (Optional Features):**
- Baseline (no features): 18.5M ops/sec
- With tracing: 16.0M ops/sec (-13%, negligible in HTTP workloads)
- With metrics: 16.2M ops/sec (-12%, negligible in production)

Run `cargo bench --bench comparison` on your hardware rather than treating published microbench numbers as portable.

**Key Insight**: This library excels at **per-key rate limiting** (separate limits per client), while libraries like `governor` are optimized for **global rate limiting** (single limit for all requests). Both have their use cases, and this library fills the per-key niche with excellent performance.

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
tokio-rate-limit = "0.11"

# For Axum middleware support
tokio-rate-limit = { version = "0.11", features = ["middleware"] }

# For Tonic gRPC middleware support
tokio-rate-limit = { version = "0.11", features = ["tonic-support"] }

# For observability (independent feature flags)
tokio-rate-limit = { version = "0.11", features = ["middleware", "observability"] }
tokio-rate-limit = { version = "0.11", features = ["middleware", "metrics-support"] }

# For Serde config & decision serialization
tokio-rate-limit = { version = "0.11", features = ["serde"] }
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
    let decision = limiter.check("client-123");

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

### Probabilistic Rate Limiting *(NEW in v0.7.0 - Experimental)*

For ultra-high throughput scenarios where 1-2% error margin is acceptable:

```rust
use tokio_rate_limit::algorithm::ProbabilisticTokenBucket;
use tokio_rate_limit::RateLimiter;

#[tokio::main]
async fn main() {
    // Create probabilistic algorithm with 5% sampling (recommended)
    let algorithm = ProbabilisticTokenBucket::new(
        2000, // capacity (>= 10 * sample_rate * cost)
        100,  // refill_rate per second
        20,   // sample_rate (1 in 20 requests)
    );

    let limiter = RateLimiter::from_algorithm(algorithm);
    let decision = limiter.check("user-123");

    if decision.permitted {
        println!("Request allowed");
    }
}
```

See the accuracy/throughput tables above, `benches/probabilistic_tradeoff.rs`, and `examples/probabilistic_rate_limiting.rs`. Keep `capacity >= 10 * sample_rate * cost`.

### Cost-Based Rate Limiting *(NEW in v0.2.0)*

Assign different costs to different operations:

```rust
let limiter = RateLimiter::builder()
    .requests_per_second(100)
    .burst(200)
    .build()
    .unwrap();

// Light operation - costs 1 token
limiter.check_with_cost("user-123", 1);

// Heavy operation - costs 50 tokens
limiter.check_with_cost("user-123", 50);

// Use cases:
// - Simple queries: cost=1, Complex queries: cost=10
// - Small uploads: cost=1, Large uploads: cost=100
// - Fast API calls: cost=1, Expensive AI inference: cost=50
```

### Blocking Acquire *(NEW in v0.2.0)*

Wait for tokens to become available:

```rust
// Block indefinitely until tokens available
let decision = limiter.acquire("user-123").await;

// Block with timeout
use std::time::Duration;
let decision = limiter.acquire_timeout("user-123", Duration::from_secs(5)).await;
if !decision.permitted {
    println!("Timed out waiting for tokens");
}

// Non-blocking (original behavior)
let decision = limiter.try_acquire("user-123");
```

### Axum Middleware

```rust
use axum::{Router, routing::get};
use tokio_rate_limit::{RateLimiter, middleware::RateLimitLayer};

#[tokio::main]
async fn main() {
    // RateLimiter is cheaply Clone (backed by an internal Arc)
    let limiter = RateLimiter::builder()
        .requests_per_second(100)
        .burst(200)
        .build()
        .unwrap();

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

Rate limit by user ID, API key, or any custom logic (zero-allocation header extraction included):

```rust
use tokio_rate_limit::middleware::{RateLimitLayer, HeaderKeyExtractor, CustomKeyExtractor};
use axum::{body::Body, extract::Request};

let limiter = RateLimiter::builder()
    .requests_per_second(50)
    .burst(100)
    .build()
    .unwrap();

// Zero-allocation header key extraction
let app: Router = Router::new()
    .route("/api/secure", get(handler))
    .layer(RateLimitLayer::with_extractor(
        limiter.clone(),
        HeaderKeyExtractor::from_static("x-api-key"),
    ));

// Or custom closure extraction
let custom_app: Router = Router::new()
    .route("/api/user-data", get(handler))
    .layer(RateLimitLayer::with_extractor(
        limiter,
        CustomKeyExtractor::borrowed(|req: &Request<Body>| {
            req.headers().get("x-user-id")?.to_str().ok()
        }),
    ));
```

### gRPC (Tonic) Middleware *(NEW in v0.5.0)*

Rate limit gRPC services with native Tonic integration:

```rust
use tokio_rate_limit::{RateLimiter, tonic_middleware::GrpcRateLimitLayer};
use tonic::transport::Server;
use std::sync::Arc;

let limiter = Arc::new(
    RateLimiter::builder()
        .requests_per_second(100)
        .burst(200)
        .build()?
);

Server::builder()
    .layer(GrpcRateLimitLayer::new(limiter))
    .add_service(GreeterServer::new(greeter))
    .serve("[::1]:50051".parse()?)
    .await?;
```

**Key Extraction Strategies:**

```rust
// Per-method (default) - different methods have independent limits
GrpcRateLimitLayer::new(limiter)

// Per-user (from metadata) - extract from gRPC metadata
use tokio_rate_limit::tonic_middleware::MetadataKeyExtractor;
GrpcRateLimitLayer::with_extractor(
    limiter,
    MetadataKeyExtractor::new("user-id")
)

// Per-IP - rate limit by client IP address
use tokio_rate_limit::tonic_middleware::IpKeyExtractor;
GrpcRateLimitLayer::with_extractor(limiter, IpKeyExtractor)

// Custom - implement your own logic
use tokio_rate_limit::tonic_middleware::CustomGrpcKeyExtractor;
GrpcRateLimitLayer::with_extractor(
    limiter,
    CustomGrpcKeyExtractor::new(|req| {
        Some(format!("custom:{}", req.uri().path()))
    })
)
```

**Features:**
- Minimal overhead with unboxed response futures and zero-allocation key extraction
- Spec-compliant gRPC over HTTP/2 wire status (`:status 200` + `grpc-status: 8` `RESOURCE_EXHAUSTED`)
- Per-RPC cost weighting (`.with_cost(...)`) and custom `Status` handlers (`.on_rejected(...)`)
- Rate limit metadata in initial headers and error trailers
- Seamless stateful Tower layer composition (`ConcurrencyLimit`, `Buffer`, etc.)

**Enable with feature flag:**
```toml
tokio-rate-limit = { version = "0.11", features = ["tonic-support"] }
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

The leaky bucket algorithm acts as a GCRA meter that drains accumulated occupancy at a constant rate.

**Characteristics:**
- Set `capacity = 1` for strict burst-free inter-arrival pacing (`1 / leak_rate` between requests)
- Set `capacity > 1` for bounded queue/jitter tolerance up to `capacity` tokens
- Requests add occupancy to the meter; overflow is denied with exact `retry_after`

**Best For:**
- Backend protection and rate smoothing
- Strict QPS enforcement requirements
- Preventing overwhelming downstream services
- Fair queuing scenarios

**Example:**
```rust
use tokio_rate_limit::RateLimiter;
use tokio_rate_limit::algorithm::LeakyBucket;

// Strict burst-free pacing at 100 req/sec (10ms spacing):
let strict = RateLimiter::from_algorithm(LeakyBucket::new(1, 100));

// Meter with bounded burst tolerance of 50 at 100 req/sec:
let smoothed = RateLimiter::from_algorithm(LeakyBucket::new(50, 100));
```

### Comparison

| Feature | Token Bucket | Leaky Bucket (`capacity = 1`) |
|---------|--------------|-------------------------------|
| **Bursts** | ✅ Allowed (up to capacity) | ❌ Strictly paced |
| **Rate Enforcement** | Average over time | Exact inter-arrival spacing |
| **Traffic Pattern** | Bursty | Smooth |
| **Best For** | Public APIs, users | Backend protection |
| **Predictability** | Moderate | High |

Both deterministic algorithms share the same single-atomic GCRA cell implementation. Run `cargo bench --bench algorithm_comparison` on your hardware for comparative benchmarks.

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
tokio-rate-limit = { version = "0.11", features = ["middleware", "observability"] }

# For metrics collection (independent of tracing)
tokio-rate-limit = { version = "0.11", features = ["middleware", "metrics-support"] }
```

### Distributed Tracing

When `observability` feature is enabled, all rate limit checks create trace spans:

```rust
use tracing_subscriber;

// Configure tracing subscriber (once at startup)
tracing_subscriber::fmt::init();

// All rate limit checks now emit spans
let decision = limiter.check("user-123");

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

See [`examples/observability.rs`](examples/observability.rs) for working tracing and metrics integration examples.

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

### Single-Atomic GCRA Token Accounting

Deterministic `TokenBucket` and `LeakyBucket` represent each key's entire quota schedule as a single `AtomicU64` **Theoretical Arrival Time (TAT)** in nanoseconds relative to limiter creation:

- **Single CAS per check**: Eliminates the two-atomic race between timestamp claiming and token debiting under multi-thread contention.
- **256 OS-Seeded Shards**: Keys are hashed once with per-instance OS-seeded `ahash` and routed across 256 independent lock-free `flurry` maps (DoS-resistant against crafted shard collisions).
- **Non-Blocking TTL Eviction**: Background-free round-robin 16-shard sweeps guarded by `AtomicBool` never stall concurrent callers and never evict still-penalized keys before their quota has recovered.
- **Zero Hot-Path Allocations**: Existing keys borrow `&str` (or typed `&K`) directly on lookup, format peer IPs onto a stack buffer, and serialize HTTP integer headers without `format!()`.

## Feature Flags

- `middleware` - Enables Axum/Tower HTTP middleware (`RateLimitLayer`, `HeaderKeyExtractor`, `IpKeyExtractor`, `CustomKeyExtractor`)
- `tonic-support` - Enables Tonic gRPC middleware (`GrpcRateLimitLayer`, `MethodKeyExtractor`, `MetadataKeyExtractor`, `IpKeyExtractor`) *(requires Rust 1.88+)*
- `observability` - Enables `tracing` instrumentation on limiter checks
- `metrics-support` - Enables `metrics` counters and histograms on `RateLimiter` and middleware (independent of `observability`)
- `serde` - Derives `Serialize` and `Deserialize` on `RateLimiterConfig` and `RateLimitDecision`

## API Documentation

Full API documentation is available at [docs.rs/tokio-rate-limit](https://docs.rs/tokio-rate-limit).

## What's New in v0.11.0

- **Spec-compliant gRPC wire responses**: `GrpcRateLimitLayer` now returns HTTP `200 OK` with `grpc-status: 8` (`Code::ResourceExhausted`) via `tonic::Status::into_http()` instead of HTTP `429` (which Tonic clients decoded as `Code::Unavailable`).
- **Single-atomic GCRA cell (`AtomicTatState`)**: `TokenBucket` and `LeakyBucket` now use a single `AtomicU64` Theoretical Arrival Time CAS — eliminating contended refill-claim races, shrinking per-key state, and putting `LeakyBucket` on the same 256-shard OS-seeded `ahash` map.
- **Generic typed keys (`K: RateLimitKey`)**: Rate limit directly by `IpAddr`, `u64`, `UUID`, `(TenantId, RouteId)`, or `Arc<str>` via `.build_with_key::<K>()` with zero string formatting.
- **Sub-1-req/s rates & tight bursts**: Added `.per_minute(n)`, `.per_hour(n)`, `.replenish_interval(d)`, and `.idle_ttl(d)` on `RateLimiterBuilder`, and lifted the `burst >= requests_per_second` restriction.
- **State introspection & management**: Added `peek`, `reset`, `clear`, `len`, `is_empty`, and deterministic `cleanup` on `RateLimiter` and `Algorithm`.
- **Middleware upgrades**: Unboxed response futures, Tower `poll_ready` readiness handoff fix (`std::mem::replace`), zero-allocation `HeaderKeyExtractor`, borrowed gRPC key extractors, `.with_cost(...)`, and `.on_rejected(...)` on both HTTP and gRPC layers.
- **Ergonomics & cleanup**: `RateLimiter` now implements `Clone` + `Debug` directly (no outer `Arc` required), `cost > capacity` returns `retry_after: None` immediately without hanging `acquire_with_cost`, `metrics-support` works on direct `RateLimiter::check` calls without pulling `tracing`, optional `serde` support, and deprecated `CachedTokenBucket` / `with_shard_count` removed.

## What's New in v0.10.0

- **`check()` is synchronous** and returns `RateLimitDecision` directly (no `Result`, no `.await`)
- Shared `retry_after` math: sub-second waits are no longer `ceil()`'d to a full second
- HTTP `Retry-After` / `RateLimit-Reset` round up to at least 1 second
- Middleware fails closed when the key extractor returns `None`
- `SimdTokenBucket` and `ZeroCopyTokenBucket` removed; `CachedTokenBucket` deprecated
- `tonic-support` no longer requires `protoc`

See [CHANGELOG.md](CHANGELOG.md) for the full list.

## Minimum Supported Rust Version (MSRV)

This crate requires Rust 1.85.0 or later (`tonic-support` needs 1.88).

## Performance Tips

1. **Clone `RateLimiter` freely**: `RateLimiter` wraps an internal `Arc` (`Clone` is a cheap pointer bump)
2. **Use typed keys on hot paths**: `.build_with_key::<IpAddr>()` or `.build_with_key::<u64>()` skips string allocation entirely
3. **Prefer borrowed key extractors**: `HeaderKeyExtractor`, `IpKeyExtractor`, `MethodKeyExtractor`, and `CustomKeyExtractor::borrowed` perform zero heap allocations on existing keys
4. **TTL for high-cardinality keys**: Use `.idle_ttl(Duration::from_secs(3600))` when you have millions of ephemeral keys
5. **Observability**: Enable `observability` and/or `metrics-support` independently as needed in production

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
