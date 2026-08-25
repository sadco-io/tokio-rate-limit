# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.9.0] - 2026-08-25
### Performance


- **`ProbabilisticTokenBucket` unsampled fast path.** Profiling on a host
  without a working `clock_gettime` vDSO (WSL2) attributed ~60% of the
  per-request cost to the monotonic clock read -- not, as previously assumed,
  to the hash-map lookup. The clock is now read lazily: an unsampled request
  whose credited token level already covers a whole `sample_rate * cost` lump
  admits without a timestamp (the decision is provably identical, since
  accrued refill can only raise a level that is already at admission
  probability 1), the per-key state is used through the flurry guard instead
  of cloning the `Arc` out of the map (two contended reference-count updates
  per request removed), the TTL `last_access` store is skipped entirely when
  no TTL is configured, and the per-request sampler tick is isolated on its
  own cache line so it no longer invalidates the token level for concurrent
  readers. Measured with `benches/probabilistic_tradeoff.rs` run identically
  on the parent commit and this one, same host, with the deterministic
  `TokenBucket` as an unchanged control (5.749 -> 5.784 us/op/thread at 8
  threads): `sample_rate = 100` went from **1.05x to 4.9x** the deterministic
  bucket single-threaded (155.8 -> 33.4 ns/op), and from **parity to 23.5x**
  under 8-thread hot-key contention (5.838 us -> 246 ns/op/thread).
  Decisions are unchanged;
  the only observable difference is that `RateLimitDecision::remaining` on
  fast-path admits omits refill accrued since the last sampled request. With
  an idle TTL configured the clock is still read on every request.
- New `benches/component_breakdown.rs` attributes the per-request cost
  component by component, and `benches/probabilistic_tradeoff.rs` gained a
  deny-path group (where the fast path cannot skip the clock) and an
  idle-TTL row.
- New `tests/probabilistic_statistics.rs` characterizes the admitted-count
  distribution over 100 independent runs per load/sampling-rate cell
  (`cargo test --release --test probabilistic_statistics -- --ignored`).


### Versioning

This release was prepared as `0.8.2`, a dependency and packaging patch. It is
released as **0.9.0** because the `ProbabilisticTokenBucket` fix below is a
behaviour change, not a bug fix that users can take blindly:

- The effective limit drops by a factor of `sample_rate`. At the documented 1%
  sampling rate that is **100x**. Traffic that previously passed will now be
  denied, which is the point of the fix, but it is not something to ship in a
  patch release that a user might take automatically.
- `RateLimitDecision::remaining` changes meaning for this algorithm. It was
  divided by `SCALE * sample_rate` and is now a real token count.
- Users must review their sizing. A sampled request debits a whole
  `sample_rate * cost` lump, so `capacity` needs to be comfortably larger than
  one lump -- a rule of thumb of `capacity >= 10 * sample_rate * cost` is now
  documented on `ProbabilisticTokenBucket::new`.

The public Rust API is unchanged: `ProbabilisticTokenBucket::new`, `with_ttl`,
`sample_rate` and the `Algorithm` impl all keep their signatures. Semver for a
rate limiter is not only about the type signatures, though -- "limits to N per
second" is the contract, and this release changes what the library actually
does with a request. Releasing it as 0.8.2 would also mean a user could not take
the packaging and MSRV fixes without also taking the enforcement change.

### Fixed

- **`ProbabilisticTokenBucket` did not rate limit.** `try_consume_probabilistic`
  multiplied the bucket capacity, the refill rate *and* the per-request cost by
  `sample_rate`. The factor cancelled, so a sampled request cost a single token
  instead of the `sample_rate` tokens it stands in for -- which is what the
  method's own doc comment said it should do. **The effective limit was
  `sample_rate`x the configured limit.** Measured on a burst of 20,000 requests
  against `capacity = 200` with no refill:

  | `sample_rate` | allowed before | allowed after | expected |
  |---------------|----------------|---------------|----------|
  | 1             |            200 |           200 |      200 |
  | 10            |          1,876 |       191-200 |      200 |
  | 100           |         20,000 |       101-200 |      200 |

  At the documented 1% sampling rate the bucket applied no limiting at all.

  Correcting the scaling is a one-line change, but on its own it produces a
  limiter that cannot track the deterministic bucket, because debits then arrive
  in lumps of `sample_rate * cost` tokens while the unsampled path has to answer
  from a single stale read. Two variants were measured and rejected: comparing
  the unsampled request against its own cost caps the deny rate near the
  sampling rate (1% denied at 2x the limit, where 30% is correct), and mirroring
  the sampled charge over-denies by 25% at exactly the limit rate. **A hard
  threshold on the unsampled path cannot produce a proportional deny rate.**
  The algorithm's internals were therefore redesigned around four properties:

  1. **Sampling is systematic and counted per key.** Each key carries its own
     request counter from a random phase, so the group a sampled request
     represents is exactly `sample_rate` requests however the traffic is
     interleaved. A per-*thread* counter was tried first and aliases
     catastrophically against periodic key patterns: with 100 keys visited
     round-robin and `sample_rate = 100`, one key is sampled on every request
     and the other 99 never (90,109 admitted against a baseline of 11,900). An
     independent coin flip per request avoids the aliasing but makes the group
     size geometric -- measured +26% over-admission at 2x on a single key and a
     -33%..+38% swing across 100 keys.
  2. **Admission is probabilistic near empty**, with probability
     `min(1, tokens / lump)` rather than a threshold. This is what makes the
     deny rate proportional: the bucket settles at the fill level where
     `offered_rate * admit_probability == refill_rate`, so the long-run admitted
     rate converges to `min(offered_rate, refill_rate)` -- exactly what the
     deterministic bucket does.
  3. **A sampled request observes the bucket at a uniformly random point inside
     the refill window it represents.** It necessarily *arrives* at the end of
     that window, so the level it sees is systematically fuller than what its
     group saw; since that observation also gates the debit, using it directly
     made the bucket debit faster than it admits (347 admitted where the
     deterministic bucket admits 700, at 2x with 1% sampling).
  4. **The debit is `lump * admit_probability`, charged deterministically**,
     rather than a whole lump gated on a coin flip. Both are unbiased, but the
     coin-flip form makes the token level a random walk with steps of one lump.
     Measured over 25 runs at 1% sampling and 10x overload it spread the
     admitted count over -10.9%..+15.4% of the deterministic baseline; charging
     `lump * p` holds it inside 1%.

  Token accounting was also tightened while the code was open: the level is held
  in an `i64` so an overdraft is carried rather than silently forgiven, refill is
  computed in integer arithmetic with the sub-token remainder carried forward
  instead of `f64` seconds, and the elapsed interval is claimed with a CAS before
  it is credited so two concurrent samplers cannot credit the same interval
  twice. `sample_rate = 1` now short-circuits to an exact, fully deterministic
  path.

  **Measured accuracy after the fix.** Admitted count against the deterministic
  `TokenBucket` on the same virtual timeline, `capacity = 2000`,
  `limit = 1000/s`, 10-second window, 100 independent runs
  (`tests/probabilistic_effective_limit.rs`):

  | offered load | `sample_rate` | mean error | sd | observed range |
  |--------------|---------------|-----------|-----|----------------|
  | 0.25x        | 1 / 10 / 100  | exact | exact | 0 |
  | 1x           | 1 / 10 / 100  | exact | exact | 0 |
  | 2x           | 1             | exact | exact | 0 |
  | 2x           | 10            |  -8.4 req |  80.9 | -210 .. +170 |
  | 2x           | 100           | -92.5 req | 208.0 | -546 .. +387 |
  | 10x          | 1             | exact | exact | 0 |
  | 10x          | 10            |  +3.2 req | 103.5 | -241 .. +316 |
  | 10x          | 100           | -48.8 req | 129.0 | -293 .. +292 |

  against a baseline of 11,999 admitted -- i.e. within 0.8% on average and 4.6%
  worst case, where the defect was 100x. A no-refill burst is now capped at the
  configured capacity for every sampling rate.

- **`RateLimitDecision::remaining` under-reported for `ProbabilisticTokenBucket`.**
  It was divided by `SCALE * sample_rate`; it is now a real token count.
- **`deny.toml` did not pass `cargo deny check licenses`.** `tiny-keccak`
  (via `flurry` -> `ahash` -> `const-random`) is CC0-1.0, which was not in the
  allow list. CC0-1.0 is a public domain dedication with no obligations; added
  with a comment naming the crate that needs it. `cargo deny check` is now clean
  on all four checks.

### Changed

- **`tests/probabilistic_accuracy.rs::test_above_limit_traffic` threshold
  lowered from `deny_rate >= 30%` to `>= 20%`, and the test now compares against
  a deterministic `TokenBucket` run on the same timeline.** The 30% figure is
  not attainable by *any* approximation of the deterministic bucket, including a
  perfect one: with `capacity = 200`, `limit = 100` and 1000 requests offered
  over 5 virtual seconds the deterministic bucket admits exactly
  200 (burst) + 500 (refill) = 700 and denies exactly 300, i.e. 30.00%. It is
  the target value, not a lower bound -- an unbiased estimator sits on it and
  crosses below half the time. Measured over 100 runs after the fix: admitted
  471-692 (mean 591.4, sd 42.7) against a baseline of 699, deny rate 30.8%-52.9%
  (mean 40.9%), never exceeding the baseline. A 20% floor is 4.9 standard
  deviations below the mean. The added assertion that the probabilistic bucket
  admits no more than 1.10x the deterministic one is the one that would catch a
  regression of the original defect -- pre-fix this configuration admitted all
  1000 requests. This configuration is also the pathological corner for
  sampling: one lump is half the entire bucket.

- **The performance claim for `ProbabilisticTokenBucket` was overstated.** The
  module documented "50-100x faster" at 1% sampling. Sampling never removed all
  the shared-state traffic: every request still performs the key lookup, the TTL
  timestamp store, and (now) the sampling counter increment. What it removes is
  the refill arithmetic and the compare-and-swap loop -- which is not the
  dominant cost. Measured on this machine (aarch64, WSL2, `cargo bench`,
  uncontended single thread, buckets sized so nothing is denied), the
  *pre-fix* code was already only 7% faster than the deterministic
  `TokenBucket` at 1% sampling: 6.29 Mops/s against 5.86. The docs now describe
  the trade-off honestly and point at `benches/probabilistic_tradeoff.rs`.

### Added

- **`benches/probabilistic_tradeoff.rs`** (new `[[bench]]` target). Reports both
  halves of the trade-off, because throughput alone is not meaningful for a
  sampling limiter -- a bucket that admits everything is infinitely fast. It
  prints a deterministic, virtual-clock accuracy table (admitted count and deny
  rate against the deterministic baseline, across sampling rates, offered loads,
  key interleaving, and bucket sizing) before running criterion groups for the
  uncontended path, contention on one hot key at 1/2/4/8 threads, and key
  cardinality.

  Throughput on this machine (aarch64, WSL2; median of criterion's estimate;
  `Mops/s`, higher is better). The fix costs 1-6%, within about 3x of the
  run-to-run noise on the unchanged baseline (1.3%):

  | benchmark | before | after | delta |
  |-----------|--------|-------|-------|
  | single thread, `TokenBucket` baseline | 5.86 | 5.80 | -1.1% (noise) |
  | single thread, `sample_rate = 1`      | 5.84 | 5.51 | -5.7% |
  | single thread, `sample_rate = 10`     | 6.09 | 6.01 | -1.3% |
  | single thread, `sample_rate = 20`     | 6.37 | 6.18 | -3.0% |
  | single thread, `sample_rate = 100`    | 6.29 | 6.06 | -3.7% |
  | 8 threads on one key, baseline        | 1.41 | 1.35 | -4.1% |
  | 8 threads on one key, `sample_rate = 10`  | 1.41 | 1.40 | -0.4% |
  | 8 threads on one key, `sample_rate = 100` | 1.43 | 1.38 | -4.0% |
  | 1000 keys, baseline                   | 4.89 | 4.85 | -0.8% |
  | 1000 keys, `sample_rate = 100`        | 5.01 | 4.77 | -4.7% |

  The headline is not the delta, it is the level: sampling buys **3-8%** over
  the deterministic bucket on this workload, not 50-100x, because the per-key
  hash map lookup dominates. Under contention on a single hot key the advantage
  is inside the noise at every thread count.

- **`tests/probabilistic_effective_limit.rs`** now asserts, rather than
  documents, the effective limit: a no-refill burst is capped at capacity for
  every sampling rate, and the long-run admitted count tracks the deterministic
  `TokenBucket` across four offered loads and three sampling rates. Both this
  file and `probabilistic_accuracy::test_above_limit_traffic` were `#[ignore]`d
  for the defect and now run by default. Stability: 1,150 release-mode runs of
  the two statistical test binaries and 25 debug-mode runs of the full suite,
  with one unexplained failure that did not reproduce in 1,000 subsequent runs
  and produced no captured assertion text.

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
- **`benches/tonic_middleware_bench.rs` had never compiled.** tonic 0.14 replaced
  `tonic::body::BoxBody` (`UnsyncBoxBody<Bytes, Status>`) with `tonic::body::Body`;
  the library moved with it, the bench did not. Nothing caught this because the crate
  had no CI and building the `tonic-support` feature needs `protoc`. Now a type alias
  swap, verified against a real protoc build.
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

- `cargo update` moved one crate: `combine` `4.6.7` -> `4.6.8` (transitive, dev-only
  via `redis`). `cargo deny check advisories` is clean across all 232 crates with
  all features enabled, and no crate in the graph is yanked. Three major bumps are
  available and were **not** taken, none of them for a security reason: dev
  `criterion` `0.5.1` -> `0.8.2` (11 bench targets to port), dev `redis`
  `0.32.7` -> `1.6.0` (declares MSRV 1.88, above the crate's 1.85 floor), and
  `matchit` `0.8.4` -> `0.8.6`, which is not ours to take -- `axum` 0.8.9 pins it
  as `=0.8.4`.
- Deferred: removal of the `SimdTokenBucket` / `ZeroCopyTokenBucket` types
  deprecated in 0.8.1.


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
