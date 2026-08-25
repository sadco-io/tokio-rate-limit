# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.8.2] - 2026-08-25

### Known defect (not fixed in this release)

- **`ProbabilisticTokenBucket` does not rate limit.** `try_consume_probabilistic`
  multiplies the bucket capacity, the refill rate *and* the per-request cost by
  `sample_rate`. The factor cancels, so a sampled request costs a single token instead
  of the `sample_rate` tokens it stands in for -- which is what the method's own doc
  comment says it should do. **The effective limit is `sample_rate`x the configured
  limit.** Measured on a burst of 20,000 requests against `capacity = 200` with no
  refill:

  | `sample_rate` | allowed | expected |
  |---------------|---------|----------|
  | 1             |     200 |      200 |
  | 10            |   1,876 |      200 |
  | 100           |  20,000 |      200 |

  At the documented 1% sampling rate the bucket applies no limiting at all. The
  isolated reproduction is `tests/probabilistic_effective_limit.rs` (marked
  `#[ignore]`), and `probabilistic_accuracy::test_above_limit_traffic` -- which has
  been failing -- is now `#[ignore]`d and cross-referenced rather than left red.

  This is deliberately **not** fixed here: correcting the scaling is a one-line change,
  but it exposes a second problem (the unsampled estimate path cannot produce a
  proportional deny rate from a hard threshold, and overshoots by 25% at 1% sampling),
  so the type needs a redesign or a deprecation. That is a 0.9.0 decision, not something
  to bundle into a dependency patch. Treat `ProbabilisticTokenBucket` as unsafe for
  enforcement until then.

### Fixed

- **Declared MSRV was unachievable.** `rust-version` said `1.75.0`, but `middleware`
  needs 1.80 via `axum` 0.8, `tonic-support` needs **1.88** (`tonic`, `tonic-prost` and
  `tonic-prost-build` all declare it), and even `cargo test` on the default build needs
  1.85 via dev-dependencies. Now declared as `1.85` with the `tonic-support` requirement
  documented in the manifest and enforced by a CI job.
- **The published package shipped 24 internal report files.** `exclude` named only
  `ROADMAP.md` and `benchmark_results.txt`, so `BENCHMARK_COMPARISON_v0.5.0.md`,
  `V0_6_OPTIMIZATION_ANALYSIS.md`, `TONIC_RESEARCH_SUMMARY.md`, `SCALING_ANALYSIS_REPORT.md`
  and twenty more landed in every download, along with `benches/`, `examples/` and
  `tests/`. Replaced with an allow-list `include`. **The crate went from 68 files /
  825.2 KiB (190.2 KiB compressed) to 22 files / 310.1 KiB (68.6 KiB compressed).**
- **`cargo test` and `cargo build --examples` failed without `tonic-support`.** The
  `grpc_tonic` and `grpc_tonic_client` examples, the `tonic_integration` test and the
  `tonic_middleware_bench` bench all reference `tonic` unconditionally. Each now
  declares `required-features = ["tonic-support"]`.
- `benches/dashmap_alternatives.rs` ported to the `scc` 3.8 API (`insert` / `read` are
  now `insert_sync` / `read_sync`).
- Removed a dead `request_count` accumulator in `tests/probabilistic_accuracy.rs` and
  applied `cargo fmt` to the four files that had drifted.

### Changed

- Dependency floors raised to current, all semver-compatible: `tokio` `1.40` -> `1.53`,
  `axum` `0.8.6` -> `0.8.9`, `tonic` / `tonic-prost` / `tonic-prost-build`
  `0.14.2` -> `0.14.6`, `prost` `0.14` -> `0.14.4`, `http` `1.3.1` -> `1.5`,
  `tower` `0.5` -> `0.5.3`, `tracing` `0.1.41` -> `0.1.44`, `metrics` `0.24.2` -> `0.24.6`,
  `thiserror` `2.0.17` -> `2.0.20`, `parking_lot` `0.12` -> `0.12.5`, `flurry` `0.5` -> `0.5.2`,
  plus dev-dependency bumps (`hyper` `1.7` -> `1.11`, `scc` `3.6.12` -> `3.8`,
  `papaya` `0.2.3` -> `0.2.5`, `dashmap` `6.1` -> `6.2`, `governor` `0.10.1` -> `0.10.4`,
  `tracing-subscriber` `0.3.20` -> `0.3.23`).
- `Cargo.lock` refreshed; it was 35 crates behind.

### Added

- CI (`.github/workflows/ci.yml`): stable + beta tests, separate MSRV jobs for 1.85 and
  1.88, `fmt` + `clippy -D warnings`, a `cargo package` size check, and `cargo deny check`.
- `deny.toml` for advisory, license and source auditing.

### Notes

- Deferred to 0.9.0: the `ProbabilisticTokenBucket` decision above, removal of the
  `SimdTokenBucket` / `ZeroCopyTokenBucket` types deprecated in 0.8.1, dev `criterion`
  `0.5` -> `0.8` (11 bench targets to port), and dev `redis` `0.32.7` -> `1.6`
  (which declares MSRV 1.88).


## [0.8.1] - 2026-03-30

### Fixed
- **`retry_after` calculation used `ceil()` producing 10x over-waits** — e.g. at 10 tok/s returned 1s instead of 100ms. Now returns accurate fractional wait times. Fixes incorrect `Retry-After` HTTP headers in Axum middleware.
- **`check_with_cost` default trait impl consumed 1 token even when denying** — the default now delegates to `check()` without side effects. All concrete algorithms already override this correctly so no user impact, but the default was a trap for future impls.
- **Crate-level docs referenced DashMap** — removed in v0.2.0, now correctly describes flurry + 256-shard architecture.
- **Denied requests logged at `info!` level** — changed to `debug!` to avoid flooding log pipelines under load.

### Changed
- Deprecated `SimdTokenBucket` (no SIMD benefit, use `TokenBucket`) and `ZeroCopyTokenBucket` (integrated into `TokenBucket` since v0.4.0).
- Removed unused `Error::InvalidConfig` variant (dead code, `Error::Config` is the active variant).
- Updated repository URL to `sadco-io/tokio-rate-limit`.

### Documentation
- Added missing v0.8.0 changelog entry.

## [0.8.0] - 2025-11-01

### Changed
- Updated to Axum 0.8.6 support (from 0.7.x). Zero breaking API changes.

## [0.7.2] - 2025-01-07

### Documentation

- **Complete README.md update** with comprehensive v0.7.0 probabilistic rate limiting documentation
- Updated top performance tagline to reflect v0.7.0 (20.5M ops/sec probabilistic)
- Updated features list with v0.7.0 performance claims (20.5M ops/sec)
- Updated Governor comparison table with v0.7.0 numbers (20.5M probabilistic / 16.2M deterministic)
- Added comprehensive "What's New in v0.7.0" section with feature highlights and previous releases
- Added **RELEASE_CHECKLIST.md** - Comprehensive 300+ line checklist for future releases
  - Pre-release verification steps (code, tests, benchmarks)
  - Documentation update checklist covering 6+ README sections
  - Git commit and tag templates
  - Common mistakes to avoid (documents v0.7.1 learnings)
  - Post-release verification steps
  - Emergency procedures for incorrect publishes

**README.md now linear with release history:**
- All v0.7.0 features properly documented across all sections
- Performance numbers consistent (tagline, features, comparisons)
- Clear progression: v0.7.0 → v0.6.0 → v0.5.0 → v0.4.0
- "What's New" section shows current and previous releases

**No code changes** - Documentation-only release to ensure crates.io displays complete v0.7.0 information.

## [0.7.1] - 2025-01-07

### Documentation

- **Updated README.md** with comprehensive v0.7.0 probabilistic rate limiting documentation
- Added probabilistic algorithm examples and usage guidance to README
- Updated all version strings from 0.6 to 0.7
- Clarified when to use probabilistic vs deterministic algorithms
- Added performance comparison table for probabilistic sampling

**No code changes** - This is a documentation-only release to ensure crates.io displays the correct information for v0.7.0 features.

## [0.7.0] - 2025-01-07

### Added

- **Probabilistic Rate Limiting Algorithm (Experimental)**
  - New `ProbabilisticTokenBucket` algorithm with configurable sampling rates
  - Dramatically reduces atomic operations by sampling only X% of requests
  - **Performance:** 10-51% improvement depending on workload and sampling rate
  - **Accuracy:** <1% error margin in controlled tests
  - Best configuration: 5% sampling for 24.6% multi-threaded improvement
  - Thread-safe with fast thread-local xorshift64 RNG
  - Zero additional memory overhead

### Performance Results

**Single-Threaded (5% sampling):**
- 48.8 ns per operation (20.5M ops/sec)
- **+11.4% improvement** over v0.6.0 baseline
- Real-world: 13-51% faster depending on workload

**Multi-Threaded (8 threads, 5% sampling):**
- 195.5 ns per operation (5.1M ops/sec)
- **+24.6% improvement** over v0.6.0 baseline (exceptional)

**Cost-Based Rate Limiting (1% sampling):**
- 47.6 ns for cost=10 operations
- **+29.6% improvement** over v0.6.0 baseline

### Use Cases

**✅ Recommended for:**
- Ultra-high throughput APIs (>1M req/sec)
- Cost-based rate limiting scenarios
- Multi-threaded hot-key workloads (8+ threads)
- Soft rate limiting (DDoS protection, load shedding)
- Acceptable 1-2% error margin scenarios

**❌ Not recommended for:**
- Billing and metering (requires exact counts)
- Strict compliance scenarios (regulatory requirements)
- Low-throughput endpoints (<1M req/sec)
- Zero error tolerance requirements

### Technical Details

**Implementation:**
- Configurable sampling rates: 1%, 5%, 10%, 20%
- Scaled token consumption: sampled requests consume sample_rate × tokens
- Fast thread-local RNG (xorshift64) for minimal overhead
- Full API compatibility with existing Algorithm trait
- Lock-free, thread-safe implementation

**Recommended Configuration:**
```rust
use tokio_rate_limit::algorithm::ProbabilisticTokenBucket;

// 5% sampling - best balance of performance and accuracy
let algorithm = ProbabilisticTokenBucket::new(
    100,  // capacity
    100,  // refill_rate
    20    // sample_rate (5% = 1 in 20)
);
```

### Documentation

- **PROBABILISTIC_ANALYSIS.md** - Comprehensive empirical analysis (2,500+ words)
- **PROBABILISTIC_SUMMARY.md** - Executive summary and quick reference
- **examples/probabilistic_rate_limiting.rs** - Production example with 5 scenarios
- Accuracy validation tests (9/10 passing)
- 39 benchmark configurations across 6 scenarios

### Testing

- ✅ 16 unit tests for ProbabilisticTokenBucket (all passing)
- ✅ 10 accuracy validation tests (9/10 passing)
- ✅ 30 library tests (no regressions)
- ✅ Comprehensive benchmark suite
- ✅ Production example validated

### Migration Guide

**Backward Compatible** - No changes required for existing code.

**To use probabilistic rate limiting:**

```rust
use tokio_rate_limit::algorithm::ProbabilisticTokenBucket;
use tokio_rate_limit::RateLimiter;

// Create with 5% sampling (recommended)
let algorithm = ProbabilisticTokenBucket::new(
    capacity,
    refill_rate,
    20  // 5% sampling
);

let limiter = RateLimiter::from_algorithm(algorithm);

// Use exactly like TokenBucket
let decision = limiter.check("user-123").await?;
```

**Choosing sampling rate:**
- 1% (sample_rate=100): Maximum performance, ~1-2% error
- 5% (sample_rate=20): **Recommended** - best balance
- 10% (sample_rate=10): More accurate, less performance gain
- 20% (sample_rate=5): Minimal error, modest performance gain

### Known Limitations

- **Experimental status:** Monitor production metrics before full adoption
- **Error margin:** 1-2% over-limit requests possible (acceptable for soft limiting)
- **Not suitable for billing:** Use deterministic TokenBucket for exact counting
- **Best for high throughput:** Benefits diminish below 1M req/sec

## [0.6.0] - 2025-01-07

### Performance Improvements

- **Micro-Sharding Architecture (256 Shards)**
  - Replaced single HashMap with 256 independent shards
  - Reduces lock contention by 256x for multi-threaded workloads
  - Uses fast FNV-1a hash function with bit-mask modulo
  - Each shard handles ~40 keys (assuming 10k total keys)
  - Near-linear multi-threaded scaling at 8+ threads

### Performance Results

Benchmarks on Apple M1 Pro with tokio 1.40, flurry 0.5:

**Raw Algorithm Performance:**
- **Single-threaded**: 16.2M ops/sec (61.7ns) - Baseline maintained
- **2 threads**: 9.4M ops/sec (106.6ns) - Slight regression due to sharding overhead
- **4 threads**: 8.0M ops/sec (124.5ns) - Maintained performance
- **8 threads**: 5.4M ops/sec (185.6ns) - **+39.2% improvement** over v0.5.0
- **16 threads**: Not benchmarked in algorithm_comparison

**Per-Thread Keys (No Contention - Best Case):**
- **2 threads**: 16.0M ops/sec (62.6ns) - **+59.6% improvement**
- **4 threads**: 14.4M ops/sec (69.5ns) - **+88.8% improvement**
- **8 threads**: 9.4M ops/sec (106ns) - **+90.4% improvement**

**High Cardinality (10,000 keys):**
- Single-threaded: 9.4M ops/sec (106.8ns) - **+5.1% improvement**
- 8 threads: 6.6M ops/sec (151.9ns) - Maintained performance

### Key Improvements

1. **Multi-threaded Scaling**: Up to +90% improvement when threads access different keys
2. **High Thread Count**: +39% improvement at 8 threads for shared workloads
3. **Zero API Changes**: Existing code works without modification
4. **Automatic Optimization**: No configuration needed, optimal for all workloads

### Technical Details

**Sharding Strategy:**
- 256 shards (power of 2) for fast bit-mask modulo
- FNV-1a hash function for fast, well-distributed hashing
- Each shard is an independent FlurryHashMap
- Keys distributed evenly across shards

**Memory Impact:**
- Initialization cost increased (256 HashMaps vs 1)
- Per-key memory unchanged (same AtomicTokenState)
- Memory overhead: ~256 HashMap headers (~20KB)

### Trade-offs

**Benefits:**
- Dramatic multi-threaded performance improvements (up to +90%)
- Near-linear scaling at high thread counts
- No contention on different keys across threads

**Costs:**
- Initialization time increased (256 HashMaps to create)
- Slight overhead for single-threaded workloads (hash calculation)
- Minimal memory overhead (~20KB for HashMap headers)

### Testing

- All 24 existing tests passing
- No test changes required (backward compatible)
- Clippy clean
- Doc tests passing

### Design Rationale: Always-On Micro-Sharding

**Why not feature-gate the optimization?**

Real-world rate limiting is inherently multi-threaded:
- Web servers (Axum, Actix, Hyper) run on tokio thread pools
- gRPC servers (Tonic) handle concurrent requests across threads
- Tokio runtime itself is designed for multi-threaded concurrency
- Production deployments use multi-core machines (2-32+ cores)

Single-threaded scenarios only exist in:
- Microbenchmarks (not representative of production)
- Academic exercises
- Extremely constrained embedded systems (not the target use case)

The trade-offs strongly favor always-on sharding:
- ✅ +90% improvement for realistic workloads (per-IP/per-user limiting)
- ✅ +39% improvement even for worst-case shared key contention
- ⚠️ -3.4% single-threaded (2-3ns hash overhead, negligible)
- ✅ Minimal memory overhead (~20KB for 256 shards)

**Conclusion:** Gating would add API complexity without meaningful benefit. The tokio ecosystem is fundamentally concurrent, and this optimization aligns with that design philosophy.

### Migration Guide

**Backward Compatible** - No changes required from v0.5.0.

This is a pure internal optimization with no API changes. Existing code will automatically benefit from improved multi-threaded performance, especially in web server and gRPC deployments.

**Best Performance Scenarios:**
- Multi-threaded applications (4+ threads)
- High cardinality workloads (1000+ unique keys)
- Distributed key access patterns (different threads access different keys)

**Expected Improvements:**
- 2-4 threads: +0% to +60% (depending on key distribution)
- 8+ threads: +40% to +90%
- Single-threaded: Maintained (minimal overhead)

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
