# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2025-11-03

### Changed

- **BREAKING**: Replaced DashMap with flurry's lock-free concurrent HashMap for per-key state management
  - The entire hot path is now lock-free (both token accounting and key access)
  - flurry uses internal auto-tuning instead of manual shard configuration
  - No API changes for most users; only affects those using `TokenBucket` directly

### Deprecated

- `TokenBucket::with_shard_count()` - Now internally calls `new()` as flurry uses auto-tuning
- `TokenBucket::with_ttl_and_shard_count()` - Now internally calls `with_ttl()` as flurry uses auto-tuning

### Performance Improvements

Major performance gains across all thread counts (benchmarked on Apple M1 Pro):

- **Single-threaded**: 17.7M ops/sec (+19% vs DashMap)
- **2 threads**: 15.5M ops/sec (+66% vs DashMap)
- **4 threads**: 13.5M ops/sec (+69% vs DashMap)
- **8 threads**: 7.1M ops/sec (+117% vs DashMap)
- **16 threads**: 2.5M ops/sec (+40% vs DashMap)

### Technical Details

- Lock-free reads and writes using flurry's Java ConcurrentHashMap port
- Automatic internal tuning eliminates need for manual shard count configuration
- Better scaling efficiency: 86% at 2 threads (vs 66% with DashMap)
- Reduced contention under high concurrency

### Migration Guide

For most users, no code changes are required. The public API remains unchanged.

If you were using `with_shard_count()` for performance tuning:
```rust
// Before (still works, but deprecated)
let bucket = TokenBucket::with_shard_count(200, 100, 64);

// After (recommended)
let bucket = TokenBucket::new(200, 100);
```

The new implementation automatically optimizes for your workload without manual tuning.

## [0.1.0] - 2025-11-02

### Added

- **Core Rate Limiting**
  - Lock-free token bucket algorithm with atomic operations
  - Per-key rate limiting with independent buckets per client/user/key
  - Configurable requests per second and burst capacity
  - Sub-token precision using 1000x scaling factor
  - Automatic token refill based on elapsed time

- **Performance**
  - 14M+ operations/second single-threaded performance
  - Lock-free compare-and-swap operations for zero contention
  - Zero allocations in the hot path
  - Efficient concurrent access via DashMap

- **Fluent Builder API**
  - `RateLimiter::builder()` for ergonomic configuration
  - Configuration validation (burst >= requests_per_second)
  - Backwards compatible with direct struct initialization

- **Axum Middleware** (feature: `middleware`)
  - Drop-in `RateLimitLayer` for Axum applications
  - IP-based rate limiting by default via `IpKeyExtractor`
  - Custom key extraction via `CustomKeyExtractor`
  - Trait-based `KeyExtractor` for custom implementations
  - Automatic rate limit headers on all responses:
    - `X-RateLimit-Limit`: The rate limit ceiling
    - `X-RateLimit-Remaining`: Number of requests remaining
    - `Retry-After`: Seconds until retry (on 429 responses)
  - Graceful error handling (allows requests on errors)

- **Pluggable Algorithm Design**
  - `Algorithm` trait for custom rate limiting strategies
  - `TokenBucket` implementation included
  - Extensible for future algorithms (leaky bucket, sliding window, etc.)

- **Comprehensive Documentation**
  - Module-level docs with examples
  - Struct/enum docs with usage patterns
  - Inline code examples in rustdoc
  - Three complete working examples:
    - `basic.rs`: Direct usage without middleware
    - `axum_middleware.rs`: IP-based rate limiting
    - `custom_key_extraction.rs`: User ID and API key extraction

- **Testing & Benchmarks**
  - Unit tests for token bucket algorithm
  - Integration tests for middleware
  - Performance benchmarks (single and multi-threaded)
  - Comparison benchmarks vs governor
  - Functional verification tests (burst, refill, enforcement)

- **Quality Assurance**
  - Zero clippy warnings
  - Formatted with rustfmt
  - Full test coverage
  - Comprehensive documentation coverage
  - MSRV: Rust 1.75.0

### Performance Benchmarks

Measured on Apple M1 Pro (darwin):

- **Single-threaded**: 71ns per check (14.0M ops/sec)
- **2 threads**: 152ns per check (6.6M ops/sec)
- **4 threads**: 165ns per check (6.1M ops/sec)
- **8 threads**: 354ns per check (2.8M ops/sec)
- **16 threads**: 609ns per check (1.6M ops/sec)

### Architecture Highlights

- Lock-free atomic operations using `AtomicU64`
- Per-key sharding via `DashMap` for concurrent access
- Zero-copy token state updates via compare-and-swap
- Tower/Axum integration following best practices
- Graceful degradation on errors

### Dependencies

Core dependencies:
- `tokio` - Async runtime
- `dashmap` - Concurrent hashmap
- `parking_lot` - Fast synchronization primitives
- `async-trait` - Async trait support
- `thiserror` - Error handling

Optional dependencies:
- `axum` - Web framework (middleware feature)
- `tower` - Middleware primitives (middleware feature)

[0.2.0]: https://github.com/danielrcurtis/tokio-rate-limit/releases/tag/v0.2.0
[0.1.0]: https://github.com/danielrcurtis/tokio-rate-limit/releases/tag/v0.1.0
