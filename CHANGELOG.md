# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.5.0] - 2025-01-07

### Added

- **Tonic gRPC Middleware Support**
  - `GrpcRateLimitLayer` for Tower-based gRPC rate limiting
  - 4 key extraction strategies:
    - `MethodKeyExtractor`: Per-method rate limiting (default)
    - `IpKeyExtractor`: Per-IP rate limiting from connection info
    - `MetadataKeyExtractor`: Extract from gRPC metadata headers
    - `CustomGrpcKeyExtractor`: Custom extraction logic
  - Proper gRPC status codes (`RESOURCE_EXHAUSTED` on limit exceeded)
  - Rate limit metadata in response trailers
  - Feature flag: `tonic-support`
  - **54 comprehensive tests** covering all key scenarios
  - Performance: <300ns overhead per request

- **Documentation**
  - `TONIC_INTEGRATION.md`: Complete integration guide with examples
  - `TONIC_RESEARCH_SUMMARY.md`: Design decisions and architecture
  - `TONIC_TEST_REPORT.md`: Test coverage and validation
  - `FUTURE_PLANS.md`: Project roadmap and priorities
  - `BENCHMARK_COMPARISON_v0.5.0.md`: Performance analysis

### Performance

Benchmarks on Apple M1 Pro with tokio 1.40, flurry 0.5:

- **Single-threaded**: 18.5M ops/sec (54ns latency)
- **Multi-threaded**:
  - 2 threads: 9.5M ops/sec (105ns)
  - 4 threads: 7.9M ops/sec (126ns)
  - 8 threads: 4.9M ops/sec (205ns)
  - 16 threads: 2.7M ops/sec (371ns)
- **Algorithm Comparison**:
  - TokenBucket: 56ns per operation (fastest, allows bursts)
  - LeakyBucket: 67ns per operation (stricter rate enforcement)
- **Tonic Middleware Overhead**: <1% (<300ns per request)
- **Key Distribution**:
  - Hot key (worst case): 18.5M ops/sec single-threaded
  - Distributed keys (realistic): 18.6M ops/sec single-threaded
  - Key contention impact: <1%

### Dependencies

- **Core dependencies** (unchanged from v0.4.0):
  - `tokio = "1.40"` - Kept at stable version for optimal performance
  - `flurry = "0.5"` - Lock-free concurrent HashMap
  - `parking_lot = "0.12"` - Fast synchronization primitives
  - `axum = "0.7"` (optional) - Web framework middleware
  - `tower = "0.5"` (optional) - Service middleware

- **Added** (optional, with `tonic-support` flag):
  - `tonic = "0.14.2"` - gRPC framework
  - `tonic-prost = "0.14.2"` - Protocol buffers support
  - `http = "1.3.1"` - HTTP types
  - `tonic-prost-build = "0.14.2"` (build-time only)

### Testing

- **54 new tests** for Tonic gRPC middleware:
  - Method-based key extraction (7 tests)
  - IP-based key extraction (7 tests)
  - Metadata-based key extraction (11 tests)
  - Custom key extraction (8 tests)
  - Tower Service integration (9 tests)
  - Layer configuration and edge cases (12 tests)
- All tests passing with comprehensive coverage

### Migration Guide

**Backward Compatible** - No breaking changes from v0.4.0.

**Adding Tonic gRPC Support:**

```toml
# Add to Cargo.toml
tokio-rate-limit = { version = "0.5", features = ["tonic-support"] }
```

```rust
use tokio_rate_limit::tonic_middleware::GrpcRateLimitLayer;
use std::sync::Arc;

let limiter = Arc::new(
    RateLimiter::builder()
        .requests_per_second(100)
        .burst(200)
        .build()?
);

// Default: Per-method rate limiting
Server::builder()
    .layer(GrpcRateLimitLayer::new(limiter.clone()))
    .add_service(GreeterServer::new(greeter))
    .serve(addr)
    .await?;

// Per-IP rate limiting
use tokio_rate_limit::tonic_middleware::IpKeyExtractor;
Server::builder()
    .layer(GrpcRateLimitLayer::with_extractor(limiter.clone(), IpKeyExtractor))
    .add_service(service)
    .serve(addr)
    .await?;

// Per-user from metadata
use tokio_rate_limit::tonic_middleware::MetadataKeyExtractor;
Server::builder()
    .layer(GrpcRateLimitLayer::with_extractor(
        limiter,
        MetadataKeyExtractor::new("user-id")
    ))
    .add_service(service)
    .serve(addr)
    .await?;
```

**Features Summary:**
- Minimal overhead (<300ns per request)
- Proper gRPC status codes and metadata
- Multiple key extraction strategies
- Seamless Tower integration
- Compatible with all Tonic services

## [0.4.0] - 2025-01-06

### Performance Improvements

- **Zero-Copy Optimization (Automatic)**
  - Integrated zero-copy key handling into baseline TokenBucket
  - Eliminates string allocations on HashMap lookups (~90% reduction in allocations)
  - **Performance:** +10-19% improvement across all workloads
  - No API changes - users automatically get the performance boost
  - Works on all platforms, no unsafe code

- **Thread-Local Caching (Opt-In)**
  - New `CachedTokenBucket` algorithm for hot-key workloads
  - **Performance:** +20-26% for low-cardinality hot-key scenarios
  - Best for per-IP or per-user rate limiting (<1000 unique keys)
  - Slight regression (-1.4%) for high-cardinality uniform distribution
  - Opt-in via `CachedTokenBucket::new()`

### New Features

- **CachedTokenBucket Algorithm**
  - Thread-local cached token bucket implementation
  - Adaptive caching strategy (only caches frequently accessed keys)
  - RefCell-based interior mutability (safe Rust)
  - Ideal for workloads with hot keys (80/20 distribution)

### Documentation

- **V0_6_OPTIMIZATION_ANALYSIS.md**: Comprehensive performance analysis
- **V0_6_QUICK_REFERENCE.md**: Quick decision guide for algorithm selection
- Updated README with performance improvements
- Added benchmark results for all optimization techniques

### Performance Summary

**TokenBucket (with zero-copy):**
- Single-threaded: 20.2M ops/sec (was 16.3M) - **+19%**
- Multi-threaded (2T): 9.9M ops/sec (was 8.7M) - **+12%**
- Multi-threaded (4T): 9.0M ops/sec (was 8.0M) - **+12%**

**CachedTokenBucket (hot keys):**
- Single-threaded: 21.7M ops/sec - **+25% vs baseline**
- Best for: Per-IP, per-user, low-cardinality scenarios

### Experimental (Not Recommended)

- `ZeroCopyTokenBucket`: Zero-copy prototype (now integrated into TokenBucket)
- `SimdTokenBucket`: SIMD prototype (deferred - no performance benefit)

### Migration Guide

**Automatic Performance Boost:**
No changes required! Existing code gets +10-19% faster automatically.

**Optional Caching for Hot-Key Workloads:**
```rust
use tokio_rate_limit::algorithm::CachedTokenBucket;

// For per-IP or per-user rate limiting
let algorithm = CachedTokenBucket::new(200, 100);
let limiter = RateLimiter::from_algorithm(algorithm);
// 25% faster for hot-key workloads!
```

## [0.3.0] - 2025-01-06

### Added

- **Leaky Bucket Algorithm**
  - New `LeakyBucket` algorithm for enforcing steady rate without bursts
  - Smooths traffic into consistent flow
  - Better for backend protection and strict QPS enforcement
  - Similar performance characteristics to TokenBucket
  - Supports TTL-based eviction like TokenBucket
  - Full support for cost-based limiting

- **Sealed Algorithm Trait**
  - Algorithm trait is now sealed using the sealed trait pattern
  - Prevents external implementations while maintaining internal flexibility
  - Allows future trait changes without semver major bump
  - Improves API stability guarantees

- **from_algorithm() Constructor**
  - New `RateLimiter::from_algorithm()` method
  - Create RateLimiter with custom algorithms (TokenBucket or LeakyBucket)
  - Enables algorithm selection at runtime

### Documentation

- **Algorithm Comparison Section** in README
  - Detailed comparison of TokenBucket vs LeakyBucket
  - Use case guidance for each algorithm
  - Performance characteristics
  - Example code for both algorithms

- **New Example**: `leaky_bucket.rs`
  - Demonstrates differences between token and leaky bucket algorithms
  - Shows burst behavior vs steady rate enforcement
  - Includes cost-based limiting examples
  - Real-world use case guidance

### Changed

- Algorithm trait is now sealed (breaking change for external implementations)
  - No user-visible impact if not implementing custom algorithms
  - Custom algorithms were never officially supported

### Performance

- LeakyBucket expected to match TokenBucket performance (15M+ ops/sec)
- Minimal overhead for algorithm selection

## [0.2.0] - 2025-11-03

Initial release of tokio-rate-limit, a high-performance, lock-free rate limiting library for Rust.

### Features

- **Lock-Free Per-Key Rate Limiting**
  - Independent token buckets for each client/IP/user/API key
  - Lock-free token accounting using atomic operations
  - Lock-free concurrent hashmap (flurry) for per-key state
  - 15.2M ops/sec single-threaded, 8.0M ops/sec at 4 threads
  - Sub-microsecond P99 latency

- **IETF Standard Headers** ([RFC Draft](https://datatracker.ietf.org/doc/html/draft-ietf-httpapi-ratelimit-headers))
  - `RateLimit-Limit`: Maximum requests allowed
  - `RateLimit-Remaining`: Requests remaining in current window
  - `RateLimit-Reset`: Seconds until bucket is full
  - Legacy `X-RateLimit-*` headers for backward compatibility

- **Cost-Based Rate Limiting**
  - `check_with_cost(key, cost)`: Weighted operations (different token costs)
  - `try_acquire_n(key, cost)`: Alias for cost-based checking
  - Use cases: Simple queries (cost=1), complex operations (cost=10-100)

- **Blocking Acquire Methods**
  - `acquire(key)`: Block indefinitely until tokens available
  - `acquire_timeout(key, timeout)`: Block with timeout
  - `try_acquire(key)`: Non-blocking check (immediate return)
  - Efficient polling with adaptive sleep intervals

- **Optional Observability** (zero overhead when disabled)
  - `observability` feature: Distributed tracing via `tracing` crate
  - `metrics-support` feature: Metrics collection via `metrics` crate
  - Instrumentation on all rate limit checks
  - Metrics: requests.allowed, requests.denied, remaining_tokens
  - ~1-3% overhead when enabled, negligible in production HTTP workloads

- **Axum Middleware** (optional `middleware` feature)
  - Drop-in `RateLimitLayer` for Axum applications
  - IP-based rate limiting by default
  - Custom key extraction (user ID, API key, etc.)
  - Automatic 429 responses with proper headers
  - Graceful error handling (fail-open on errors)

- **Memory Safety**
  - TTL-based eviction for high-cardinality keys
  - Overflow protection with saturating arithmetic
  - Deterministic testing with tokio::time
  - No unbounded memory growth

- **Pluggable Algorithms**
  - `Algorithm` trait for custom rate limiting strategies
  - Token bucket implementation included
  - Extensible for future algorithms (leaky bucket, sliding window, etc.)

### Performance

Benchmarked on Apple M1 Pro (darwin):

| Configuration | Latency (P50) | Throughput | Scaling Efficiency |
|--------------|---------------|------------|-------------------|
| Single-threaded | 65ns | 15.2M ops/sec | 100% (baseline) |
| 2 threads | 117ns | 8.6M ops/sec | 87% |
| 4 threads | 125ns | 8.0M ops/sec | 81% |
| 8 threads | 221ns | 4.5M ops/sec | 69% |
| 16 threads | 384ns | 2.6M ops/sec | 50% |

**Observability overhead (when enabled):**
- With tracing: 12.8M ops/sec (-16% in microbenchmarks, <0.001% in production)
- With metrics: 12.9M ops/sec (-15% in microbenchmarks, <0.001% in production)

See [ENHANCED_API_BENCHMARKS.md](ENHANCED_API_BENCHMARKS.md) for detailed performance analysis.

### Architecture

- **flurry::HashMap**: Lock-free concurrent hashmap (Java ConcurrentHashMap port)
- **Atomic operations**: Compare-and-swap for token updates
- **Auto-tuning**: No manual shard configuration required
- **Zero allocations**: Hot path avoids heap allocations
- **Sub-token precision**: 1000x scaling factor for accurate refills

### Documentation

- **README.md**: Comprehensive guide with examples
- **OBSERVABILITY.md**: Production observability integration guide
  - OpenTelemetry, Jaeger, Prometheus, Honeycomb examples
  - Best practices and troubleshooting
- **ENHANCED_API_BENCHMARKS.md**: Detailed performance analysis
- **API Documentation**: Complete rustdoc coverage with examples

### Examples

- `basic.rs`: Direct usage without middleware
- `axum_middleware.rs`: IP-based rate limiting with Axum
- `custom_key_extraction.rs`: User ID and API key rate limiting
- `cost_based_limiting.rs`: Weighted operations
- `blocking_acquire.rs`: Blocking wait patterns
- `observability.rs`: Tracing and metrics integration

### Dependencies

Core:
- tokio = "1.40" (async runtime)
- flurry = "0.5" (lock-free concurrent hashmap)
- parking_lot = "0.12" (synchronization primitives)
- async-trait = "0.1" (async trait support)
- thiserror = "2.0" (error handling)

Optional:
- axum = "0.7" (`middleware` feature)
- tower = "0.5" (`middleware` feature)
- tracing = "0.1" (`observability` feature)
- metrics = "0.24" (`metrics-support` feature)

### Quality Assurance

- ✅ 30+ tests passing (14 unit tests + 16 doc tests)
- ✅ Zero clippy warnings
- ✅ All examples verified working
- ✅ Comprehensive documentation
- ✅ MSRV: Rust 1.75.0

### Comparison with Alternatives

**vs governor:**
- tokio-rate-limit: Per-key rate limiting (built-in multi-tenant)
- governor: Global rate limiting (single shared limit)
- tokio-rate-limit: 15.2M ops/sec per-key performance
- governor: 357M ops/sec global performance

Both libraries excel at different use cases. Use tokio-rate-limit for per-client/per-user limits, governor for global API limits.

[0.2.0]: https://github.com/danielrcurtis/tokio-rate-limit/releases/tag/v0.2.0
