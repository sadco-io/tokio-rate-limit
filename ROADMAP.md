# tokio-rate-limit Roadmap

This document tracks planned features and enhancements for future releases.

## v0.3.0 - Algorithm Improvements

### Priority 4: Sealed Algorithm Trait
**Status:** Planned
**Effort:** Low
**Impact:** API stability

**Description:**
Seal the `Algorithm` trait to prevent external implementations while maintaining extensibility within the crate.

**Rationale:**
- Allows future breaking changes to the trait without semver major bump
- Maintains control over algorithm implementations
- Still allows users to request new algorithms via issues

**Implementation:**
```rust
mod private {
    pub trait Sealed {}
}

pub trait Algorithm: private::Sealed {
    async fn check(&self, key: &str) -> Result<RateLimitDecision>;
    async fn check_with_cost(&self, key: &str, cost: u64) -> Result<RateLimitDecision>;
}

impl private::Sealed for TokenBucket {}
impl private::Sealed for LeakyBucket {}  // future
```

**Files:**
- `src/algorithm/mod.rs`: Add sealed trait pattern
- Update documentation with rationale

---

### Priority 5: Leaky Bucket Algorithm
**Status:** Planned
**Effort:** Medium
**Impact:** Feature completeness

**Description:**
Add leaky bucket algorithm as an alternative to token bucket.

**Comparison:**
- **Token Bucket**: Allows bursts up to capacity, good for bursty traffic
- **Leaky Bucket**: Enforces steady rate, good for smoothing traffic

**Use Cases:**
- API rate limiting with strict QPS enforcement
- Backend protection requiring consistent load
- Scenarios where bursts are undesirable

**Implementation:**
```rust
pub struct LeakyBucket {
    capacity: u64,
    leak_rate: u64,  // tokens leaked per second
    buckets: Arc<FlurryHashMap<String, Arc<AtomicBucketState>>>,
}
```

**Files:**
- `src/algorithm/leaky_bucket.rs`: New algorithm implementation
- `examples/leaky_bucket.rs`: Demo comparing token vs leaky bucket
- Update README with algorithm comparison table

---

### Priority 6: Sliding Window Algorithm
**Status:** Research
**Effort:** High
**Impact:** Advanced use cases

**Description:**
Implement sliding window rate limiting for more precise rate enforcement.

**Advantages:**
- More accurate rate limiting (no "reset boundary" issue)
- Better for strict compliance scenarios
- Industry standard for many APIs (Redis, Cloudflare, etc.)

**Challenges:**
- Requires storing timestamps or request counts
- Higher memory overhead per key
- More complex eviction logic

**Research Questions:**
- Memory overhead acceptable for typical workloads?
- Can we maintain lock-free properties?
- Performance vs token bucket trade-off?

**Potential Implementation:**
- Fixed window counters (simpler, less precise)
- Sliding log (accurate, memory intensive)
- Sliding window counters (balanced approach)

**Decision:** Defer until user demand or specific use case identified.

---

## v0.4.0 - Distributed Rate Limiting

### Redis Backend Support
**Status:** Planned
**Effort:** High
**Impact:** Distributed systems

**Description:**
Add Redis backend for distributed rate limiting across multiple service instances.

**Use Cases:**
- Multi-instance deployments without sticky sessions
- Centralized rate limit enforcement
- Cross-region rate limiting
- Shared limits across heterogeneous services

**Design Considerations:**

1. **API Design:**
   ```rust
   pub enum Backend {
       Local(TokenBucket),
       Redis(RedisTokenBucket),
   }

   let limiter = RateLimiter::builder()
       .requests_per_second(100)
       .burst(200)
       .backend(Backend::redis("redis://localhost:6379"))
       .build()?;
   ```

2. **Performance Trade-offs:**
   - Local: 15M ops/sec, no network latency
   - Redis: ~10K-50K ops/sec, network latency overhead
   - Use local for high-throughput, Redis for consistency

3. **Lua Scripts:**
   Use EVAL for atomic token consumption:
   ```lua
   local tokens = redis.call('get', KEYS[1])
   local now = tonumber(ARGV[1])
   -- ... token bucket logic in Lua
   redis.call('set', KEYS[1], new_tokens)
   return {permitted, remaining}
   ```

4. **Connection Pooling:**
   - Use `redis` crate with connection manager
   - Configurable pool size
   - Automatic reconnection

5. **Feature Flag:**
   - `redis-backend` feature (optional)
   - Adds redis dependency (~0.25+)

**Benchmarking:**
- Compare local vs Redis performance
- Measure network latency impact
- Test connection pool efficiency

**Documentation:**
- When to use local vs Redis backend
- Redis deployment best practices
- Performance tuning guide

**Example:**
```rust
// examples/redis_backend.rs
let limiter = RateLimiter::builder()
    .requests_per_second(100)
    .burst(200)
    .backend(Backend::redis("redis://localhost:6379"))
    .build()?;
```

---

### DynamoDB Backend Support
**Status:** Research
**Effort:** High
**Impact:** AWS ecosystems

**Description:**
Add DynamoDB backend for AWS-native distributed rate limiting.

**Advantages:**
- Serverless, no Redis cluster to manage
- Global tables for multi-region
- AWS-native for Lambda, ECS deployments
- Built-in TTL for automatic cleanup

**Challenges:**
- Higher latency than Redis (10-50ms P99)
- Conditional writes for atomicity
- Cost considerations (read/write units)
- Throughput limits per partition

**Decision:** Defer until proven user demand for AWS-specific backend.

---

## v0.5.0 - Advanced Features

### Dynamic Rate Limit Configuration
**Status:** Planned
**Effort:** Medium
**Impact:** Production flexibility

**Description:**
Allow rate limits to be changed at runtime without recreating limiter.

**Use Cases:**
- Per-tenant rate limit customization
- Dynamic adjustment based on system load
- A/B testing different rate limit values
- Emergency rate limit reduction

**API Design:**
```rust
let limiter = RateLimiter::builder()
    .dynamic_limits()  // Enable dynamic configuration
    .build()?;

// Update limits for specific key
limiter.set_limit("user-123", 200, 400).await?;

// Update global default
limiter.set_default_limit(100, 200).await?;

// Get current limit for key
let (rate, burst) = limiter.get_limit("user-123").await?;
```

**Implementation:**
- Store per-key limits in concurrent hashmap
- Atomic updates to limit configuration
- Backwards compatible (static limits still work)

---

### Rate Limit Groups
**Status:** Planned
**Effort:** Medium
**Impact:** Multi-tenant SaaS

**Description:**
Support hierarchical rate limiting with groups/tiers.

**Use Cases:**
- SaaS pricing tiers (free: 100/s, pro: 1000/s, enterprise: 10000/s)
- Organization-level limits with per-user sub-limits
- Resource pool sharing across users

**API Design:**
```rust
let limiter = RateLimiter::builder()
    .requests_per_second(100)
    .burst(200)
    .with_groups(|groups| {
        groups
            .tier("free", 100, 200)
            .tier("pro", 1000, 2000)
            .tier("enterprise", 10000, 20000)
    })
    .build()?;

// Check with tier
let decision = limiter.check_with_tier("user-123", "pro").await?;
```

**Implementation:**
- Group configuration stored in concurrent hashmap
- Per-key group association
- Efficient group lookup

---

### Rate Limit Policies
**Status:** Research
**Effort:** High
**Impact:** Enterprise features

**Description:**
Advanced rate limiting policies beyond simple token bucket.

**Policy Types:**

1. **Composite Policies:**
   ```rust
   // Both per-IP and per-user limits
   let policy = Policy::all_of(vec![
       Policy::per_ip(100, 200),
       Policy::per_user(50, 100),
   ]);
   ```

2. **Time-Based Policies:**
   ```rust
   // Different limits by time of day
   let policy = Policy::schedule()
       .business_hours(1000, 2000)
       .off_hours(100, 200);
   ```

3. **Conditional Policies:**
   ```rust
   // Different limits based on request properties
   let policy = Policy::conditional()
       .when(|req| req.is_premium_endpoint())
       .then(50, 100)
       .otherwise(100, 200);
   ```

**Decision:** Defer until clear user requirements emerge.

---

## v0.6.0 - Performance Optimizations

### SIMD Token Accounting
**Status:** Research
**Effort:** High
**Impact:** Performance (potentially 2-5x)

**Description:**
Use SIMD instructions for parallel token bucket updates.

**Opportunities:**
- Batch process multiple keys in parallel
- Vectorized token refill calculations
- SIMD-optimized CAS operations

**Challenges:**
- Platform-specific (x86_64 AVX2, ARM NEON)
- Limited benefit for single-key operations
- Complexity vs performance trade-off

**Research Required:**
- Profile to identify SIMD-friendly operations
- Benchmark on target platforms
- Evaluate complexity cost

---

### Zero-Copy Key Handling
**Status:** Research
**Effort:** Medium
**Impact:** Memory efficiency

**Description:**
Eliminate string allocations in hot path using borrowed keys.

**Current:**
```rust
pub async fn check(&self, key: &str) -> Result<RateLimitDecision> {
    let key_string = key.to_string();  // Allocation!
    // ...
}
```

**Proposed:**
```rust
pub async fn check(&self, key: impl AsRef<str>) -> Result<RateLimitDecision> {
    // Use &str directly in HashMap lookup
    // Only allocate if inserting new key
}
```

**Challenges:**
- flurry's API requires owned keys
- Need to benchmark allocation cost
- May require upstream changes

---

### Thread-Local Caching (Revisited)
**Status:** Deferred from v0.1.0
**Effort:** Medium
**Impact:** Unknown (previously showed regression)

**Previous Research:**
- Tested in v0.1.0 development
- Showed -6.4% regression at 4 threads
- `RefCell::borrow_mut()` overhead
- LRU cache management cost

**Revisit Conditions:**
- New caching strategy identified
- Different workload profiles
- User reports cache-friendly workloads

**Alternative Approaches:**
- Lock-free thread-local caching
- Probabilistic caching (cache hit sampling)
- Adaptive caching based on key access patterns

---

## Community & Ecosystem

### Integration Examples
**Status:** Ongoing
**Effort:** Low-Medium
**Impact:** Adoption

**Planned Examples:**

1. **Actix-web Integration:**
   - Middleware for Actix-web 4.x
   - Similar API to Axum middleware

2. **Rocket Integration:**
   - Fairing for Rocket framework
   - Request guard pattern

3. **Tonic gRPC:**
   - Interceptor for gRPC services
   - Per-method rate limiting

4. **AWS Lambda:**
   - Lambda middleware integration
   - DynamoDB backend example

5. **Kubernetes:**
   - Deployment manifests
   - Redis cluster setup
   - Horizontal scaling guide

---

### Testing Tools
**Status:** Planned
**Effort:** Medium
**Impact:** Developer experience

**Mock Rate Limiter:**
```rust
#[cfg(test)]
pub struct MockRateLimiter {
    // Always permit or deny for testing
    behavior: Behavior,
}

impl MockRateLimiter {
    pub fn new_permissive() -> Self { /* ... */ }
    pub fn new_restrictive() -> Self { /* ... */ }
    pub fn with_sequence(permits: Vec<bool>) -> Self { /* ... */ }
}
```

**Test Utilities:**
```rust
// Test rate limit enforcement without real delays
pub struct RateLimitTestHarness {
    limiter: RateLimiter,
    clock: TestClock,
}

impl RateLimitTestHarness {
    pub async fn check_and_advance(&mut self, key: &str, duration: Duration) -> bool {
        let result = self.limiter.check(key).await;
        self.clock.advance(duration);
        result.permitted
    }
}
```

---

## Documentation Improvements

### Interactive Examples
**Status:** Planned
**Effort:** Low
**Impact:** Adoption

**Planned:**
- Interactive rate limit simulator (WASM demo)
- Visual token bucket animation
- Comparison tool (algorithms, backends)
- Performance calculator

---

### Cookbook
**Status:** Planned
**Effort:** Medium
**Impact:** Developer experience

**Recipes:**

1. **Multi-Tier SaaS Rate Limiting:**
   - Per-user and per-organization limits
   - Pricing tier enforcement
   - Quota tracking

2. **API Gateway Pattern:**
   - Per-endpoint limits
   - Global rate limits
   - IP reputation-based limiting

3. **Microservices Rate Limiting:**
   - Service-to-service rate limiting
   - Circuit breaker integration
   - Distributed tracing

4. **High-Cardinality Keys:**
   - TTL configuration
   - Memory management
   - Eviction strategies

5. **Cost-Based Limiting Patterns:**
   - GPU inference workloads
   - Database query complexity
   - Storage operations

---

## Non-Goals

These are explicitly **not** planned:

### 1. Built-in Storage Backends Beyond Redis/DynamoDB
**Rationale:**
- Can't support every database
- Users can implement custom backends via `Algorithm` trait
- Maintenance burden too high

**Alternative:**
- Document how to implement custom backends
- Provide reference implementations
- Community can contribute additional backends

---

### 2. Complex Policy Language
**Rationale:**
- Adds significant complexity
- Most users need simple rate limiting
- Can be built on top of library

**Alternative:**
- Focus on composable primitives
- Users can build policy engines externally

---

### 3. Rate Limit Analytics/Reporting
**Rationale:**
- Out of scope for rate limiting library
- Better handled by observability stack
- Metrics support already provides data

**Alternative:**
- Export metrics via `metrics` crate
- Users integrate with their analytics platform
- Provide example Grafana dashboards

---

### 4. GUI/Dashboard
**Rationale:**
- Not a library responsibility
- Numerous off-the-shelf solutions exist
- Maintenance burden

**Alternative:**
- Document Grafana integration
- Provide example dashboards
- Link to compatible tools

---

## Release Cadence

- **Minor releases (0.x.0):** Every 2-3 months with new features
- **Patch releases (0.x.y):** As needed for bug fixes
- **Major release (1.0.0):** When API is stable and battle-tested (6-12 months)

---

## Contributing

Community contributions welcome for any roadmap item! Please:

1. Open an issue to discuss before starting work
2. Reference roadmap item in PR description
3. Include benchmarks for performance-related changes
4. Add examples and documentation

---

## Feedback

Have ideas for the roadmap? Please open an issue with:
- Use case description
- Proposed API design
- Why existing features don't address the need
- Willingness to contribute implementation

---

**Last Updated:** 2025-11-03
**Current Version:** v0.2.0
