# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
