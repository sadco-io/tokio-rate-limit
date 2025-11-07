# Future Plans for tokio-rate-limit

**Last Updated:** 2025-01-07
**Current Version:** v0.5.0

This document outlines high-impact features and improvements for future releases.

---

## 🚀 Performance Optimizations (v0.6.0 - Top Priority)

### Deferred Locking (Read-Optimized Fast Path) ⭐⭐⭐
**Status:** Designed, Ready to Implement
**Effort:** Low (1-2 days)
**Impact:** Very High (2-3x throughput)
**Priority:** P0

**Why:** Most requests don't need token refill - optimize the hot path

**Current Bottleneck:**
- 2 CAS operations per request (tokens + time update)
- Always updates last_refill, even when not needed

**Optimization:**
```rust
// FAST PATH (90% of requests): Single CAS, no time update
if tokens >= cost {
    return tokens.compare_exchange(...); // ✅ 1 CAS
}

// SLOW PATH (10%): Need refill (existing complex logic)
self.refill_and_consume()
```

**Expected Impact:**
- Single-threaded: 18.5M → 25M+ ops/sec (+35%)
- 8 threads: 4.9M → 12M+ ops/sec (+145%)

---

### Micro-Sharding (256 Shards) ⭐⭐
**Status:** Designed, Benchmarks Exist
**Effort:** Medium (1 week)
**Impact:** Very High (4-8x multi-threaded)
**Priority:** P1

**Why:** Single HashMap is bottleneck for multi-threaded scale-up

**Current Bottleneck:**
- All threads contend on single HashMap guard
- 10,000 keys in one structure

**Optimization:**
```rust
const SHARDS: usize = 256;
let shard_id = hash(key) & (SHARDS - 1); // Fast modulo
shards[shard_id].get(key) // 256x less contention
```

**Expected Impact:**
- 2 threads: 9.5M → 35M+ ops/sec (+268%)
- 8 threads: 4.9M → 100M+ ops/sec (+1,940%)

---

### Probabilistic Rate Limiting (Optional Algorithm) ⭐
**Status:** Prototype Ready
**Effort:** Medium (1 week)
**Impact:** Extreme (50-100x for specific use cases)
**Priority:** P2

**Why:** For ultra-high throughput where approximation is acceptable

**Approach:**
```rust
// Sample 1% of requests, scale result
if rand() % 100 == 0 {
    counter.fetch_add(100, Relaxed); // Only 1% do atomic op
}
return counter.load() < limit * 100;
```

**Expected Impact:**
- Single-threaded: 18.5M → 500M+ ops/sec (+2,600%)
- Near-linear multi-threaded scaling

**Trade-off:** ~1-2% error margin (acceptable for rate limiting)

---

## 🎯 Standard Features (Ship Next)

### 1. GitHub Actions CI/CD ⭐ (CRITICAL)
**Status:** Not Started
**Effort:** Low (1-2 hours)
**Impact:** High
**Priority:** P0

**Why:** No CI/CD setup detected - essential for any production crate

**Components:**
- Automated testing on push/PR
- Benchmark regression detection
- Multi-platform testing (Linux, macOS, Windows)
- Automated docs generation
- Cargo clippy + fmt checks
- Security audit (cargo-deny, cargo-audit)
- Coverage tracking (tarpaulin)

**Files to Create:**
- `.github/workflows/ci.yml`
- `.github/workflows/benchmarks.yml`
- `.github/workflows/security.yml`
- `.github/dependabot.yml`

---

### 2. Redis Backend 🔥 (HIGH VALUE)
**Status:** Planned (ROADMAP.md)
**Effort:** High (2-3 weeks)
**Impact:** Very High
**Priority:** P1

**Why:** #1 requested feature for distributed systems

**Use Cases:**
- Multi-instance deployments (Kubernetes, Docker Swarm)
- Centralized rate limit enforcement
- Cross-region rate limiting
- Shared limits across heterogeneous services

**Performance Targets:**
- Local: 20M ops/sec, no network latency
- Redis: 10K-50K ops/sec, network latency overhead
- Use local for high-throughput, Redis for consistency

**API Design:**
```rust
use tokio_rate_limit::backend::Backend;

let limiter = RateLimiter::builder()
    .requests_per_second(100)
    .burst(200)
    .backend(Backend::redis("redis://localhost:6379"))
    .build()?;
```

**Implementation Details:**
- Lua scripts for atomic token consumption
- Connection pooling with redis crate
- Configurable pool size
- Automatic reconnection
- Feature flag: `redis-backend`
- Benchmark local vs Redis performance

**Files to Create:**
- `src/backend/mod.rs`
- `src/backend/redis.rs`
- `examples/redis_backend.rs`
- `benches/redis_comparison.rs`

---

### 3. More Real-World Examples 📚
**Status:** Not Started
**Effort:** Low (4-6 hours)
**Impact:** Medium
**Priority:** P2

**Current:** 7 examples (957 lines)

**Missing Examples:**
- `examples/production_patterns.rs` - Best practices guide
- `examples/kubernetes_deployment.rs` - K8s integration with liveness probes
- `examples/actix_web.rs` - Actix framework middleware
- `examples/multi_tenant_saas.rs` - Pricing tiers implementation
- `examples/graphql_rate_limiting.rs` - GraphQL-specific patterns
- `examples/websocket_rate_limiting.rs` - WebSocket connections
- `examples/grpc_tonic.rs` - gRPC with tonic framework

**Goal:** 12-15 examples covering all major use cases

---

## 🚀 High Value Features (v0.5.0)

### 4. Dynamic Rate Limit Configuration
**Status:** Planned (ROADMAP.md)
**Effort:** Medium (1-2 weeks)
**Impact:** High for SaaS
**Priority:** P1

**Why:** Essential for multi-tenant SaaS platforms

**API Design:**
```rust
let limiter = RateLimiter::builder()
    .dynamic_limits()  // Enable runtime configuration
    .build()?;

// Update limits for specific key
limiter.set_limit("user-123", 200, 400).await?;

// Update global default
limiter.set_default_limit(100, 200).await?;

// Get current limit for key
let (rate, burst) = limiter.get_limit("user-123").await?;
```

**Use Cases:**
- Per-tenant rate limit customization
- A/B testing different rate limit values
- Emergency rate limit reduction
- Load-based dynamic scaling
- Promotional rate increases

**Implementation:**
- Store per-key limits in concurrent hashmap
- Atomic updates to limit configuration
- Backwards compatible (static limits still work)
- Optional feature flag: `dynamic-limits`

---

### 5. Rate Limit Groups/Tiers
**Status:** Planned (ROADMAP.md)
**Effort:** Medium (1-2 weeks)
**Impact:** High for SaaS
**Priority:** P1

**Why:** Multi-tenant SaaS billing and pricing tiers

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

// Update user tier
limiter.set_user_tier("user-123", "enterprise").await?;
```

**Use Cases:**
- SaaS pricing tiers (free/pro/enterprise)
- Organization-level limits with per-user sub-limits
- Resource pool sharing across users
- Hierarchical rate limiting

**Implementation:**
- Group configuration stored in concurrent hashmap
- Per-key group association
- Efficient group lookup
- Combine with dynamic configuration

---

### 6. Framework Integrations
**Status:** Partially Complete (Axum done)
**Effort:** Low per framework (2-4 hours each)
**Impact:** Medium
**Priority:** P2

**Completed:**
- ✅ Axum (v0.2.0)

**Planned:**
- **Actix-web** - 2nd most popular Rust web framework
- **Rocket** - Popular for simplicity
- **Warp** - Lightweight and fast
- **Tower** - Generic service middleware
- **Tonic** - gRPC framework (high priority)

**API Design:**
```rust
// Actix-web
use actix_web::middleware::from_fn;
app.wrap(RateLimitMiddleware::new(limiter));

// Tonic (gRPC)
let service = tonic::service::interceptor(
    server,
    RateLimitInterceptor::new(limiter)
);
```

---

## 📊 Community & Growth

### 7. Performance Dashboard
**Status:** Not Started
**Effort:** Medium (1 week)
**Impact:** High for visibility
**Priority:** P2

**Components:**
- Automated benchmark tracking over time
- GitHub Pages with interactive charts
- Regression detection alerts
- Compare against competitors (governor, tower-governor)
- Historical performance data

**Tools:**
- GitHub Actions + criterion
- github-pages for hosting
- Chart.js or D3.js for visualization
- Benchmark-action for automation

---

### 8. Blog Post / Case Study
**Status:** Not Started
**Effort:** Low (2-3 hours)
**Impact:** Very High for adoption
**Priority:** P1

**Topics:**
- "How we achieved 20M ops/sec rate limiting in Rust"
- "Zero-copy optimization: A deep dive"
- "Choosing the right rate limiting algorithm"
- "From 16M to 20M ops/sec: A performance journey"
- "Building a lock-free rate limiter"

**Distribution:**
- Hacker News, Reddit (r/rust)
- This Week in Rust newsletter
- Rust blog aggregators (Read Rust)
- Dev.to, Medium
- Company engineering blog

---

### 9. Documentation Enhancements
**Status:** Ongoing
**Effort:** Medium (ongoing)
**Impact:** Medium
**Priority:** P2

**Planned:**
- **Video tutorial** (YouTube/Twitch)
- **Interactive examples** (Rust Playground links)
- **Migration guides** from governor, tower-governor
- **Architecture diagrams** (mermaid.js in README)
- **Performance comparison matrix**
- **Cookbook** with common patterns
- **FAQ** section
- **Troubleshooting guide**

---

## 🛠️ Infrastructure

### 10. Automated Performance Benchmarking
**Status:** Not Started
**Effort:** Low (4 hours)
**Impact:** High
**Priority:** P1

**Components:**
- Run benchmarks on every PR
- Track performance over time
- Alert on >5% regressions
- Compare against baseline
- Store results in git (benchmark-action)
- Comment results on PRs

**Tools:**
- GitHub Actions
- benchmark-action
- criterion
- Custom alerting script

---

### 11. Security & Quality
**Status:** Not Started
**Effort:** Low (2 hours)
**Impact:** High
**Priority:** P1

**Components:**
- **Dependabot** - Automated dependency updates
- **cargo-deny** - License and security checks
- **cargo-audit** - Security vulnerability scanning
- **cargo-tarpaulin** - Code coverage tracking
- **SECURITY.md** - Security policy and disclosure
- **Minimum Supported Rust Version (MSRV) policy**

**Files to Create:**
- `.github/dependabot.yml`
- `deny.toml`
- `SECURITY.md`
- `.github/workflows/security.yml`

---

## 🎨 Nice to Have (v0.6.0+)

### 12. Sliding Window Algorithm
**Status:** Research (ROADMAP.md)
**Effort:** High (3-4 weeks)
**Impact:** Medium
**Priority:** P3

**Why:** More precise rate limiting

**Advantages:**
- More accurate rate limiting (no "reset boundary" issue)
- Better for strict compliance scenarios
- Industry standard for many APIs (Redis, Cloudflare)

**Challenges:**
- Requires storing timestamps or request counts
- Higher memory overhead per key
- More complex eviction logic
- Performance vs token bucket trade-off

**Research Questions:**
- Memory overhead acceptable for typical workloads?
- Can we maintain lock-free properties?
- Performance vs token bucket trade-off?

**Implementation Options:**
- Fixed window counters (simpler, less precise)
- Sliding log (accurate, memory intensive)
- Sliding window counters (balanced approach)

---

### 13. WebAssembly Support
**Status:** Not Started
**Effort:** Medium (1-2 weeks)
**Impact:** Low (niche)
**Priority:** P4

**Use Cases:**
- Client-side rate limiting
- Edge computing (Cloudflare Workers, Fastly Compute)
- Browser-based rate limiting
- WASM-compatible algorithms

**Challenges:**
- No tokio runtime in WASM
- Need wasm-bindgen compatible APIs
- Different time sources
- No filesystem access

---

### 14. Metrics & Observability
**Status:** Partially Complete (tracing in v0.2.0)
**Effort:** Medium (1-2 weeks)
**Impact:** Medium
**Priority:** P3

**Completed:**
- ✅ Tracing integration (v0.2.0)
- ✅ Metrics support (v0.2.0)

**Planned:**
- Prometheus metrics exporter
- Grafana dashboard template
- OpenTelemetry full integration
- Rate limit analytics
- Historical data tracking
- Rate limit health metrics

---

### 15. DynamoDB Backend
**Status:** Research (ROADMAP.md)
**Effort:** High (3-4 weeks)
**Impact:** Medium (AWS-specific)
**Priority:** P4

**Why:** AWS-native distributed rate limiting

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

**Decision:** Defer until proven user demand

---

## 🏆 Recommended Roadmap

### **Immediate (This Week)**
1. ✅ GitHub Actions CI/CD (P0)
2. ✅ Blog post announcing v0.4.0 (P1)
3. ✅ Security setup (SECURITY.md, cargo-audit) (P1)

### **v0.5.0 (Next Month)**
4. Redis Backend Support (P1)
5. Dynamic Configuration (P1)
6. Rate Limit Groups/Tiers (P1)
7. 3-4 more examples including tonic (P2)

### **v0.6.0 (Next Quarter)**
8. Framework integrations (Actix, Rocket, Tonic) (P2)
9. Performance dashboard (P2)
10. Documentation enhancements (P2)

### **Future (v0.7.0+)**
11. Sliding Window algorithm (P3)
12. DynamoDB backend (if demand exists) (P4)
13. WebAssembly support (P4)

---

## 💡 Quick Wins (High Impact, Low Effort)

If you want **maximum impact with minimal effort**, do these first:

1. **GitHub Actions CI** (1-2 hours) - Essential foundation
2. **Blog post** (2-3 hours) - Drive adoption
3. **3-4 more examples** (4-6 hours) - Help users succeed
4. **SECURITY.md + cargo-audit** (30 min) - Trust & safety
5. **Tonic example** (2-3 hours) - gRPC support showcase

**Total time:** ~1 day
**Impact:** Dramatically improves professionalism and adoption potential

---

## 📈 Success Metrics

**Current (v0.4.0):**
- ⭐ GitHub stars: ?
- 📦 Downloads: ?
- 🐛 Open issues: ?
- 📝 Documentation: Good
- 🧪 Test coverage: High
- 🔒 Security: Basic
- 🚀 CI/CD: None

**Target (v0.6.0):**
- ⭐ GitHub stars: 1000+
- 📦 Downloads: 10K+/month
- 🐛 Open issues: <10
- 📝 Documentation: Excellent
- 🧪 Test coverage: >90%
- 🔒 Security: Excellent
- 🚀 CI/CD: Full automation

---

## 🤝 Community Engagement

**Planned:**
- Regular blog posts (monthly)
- Conference talks (RustConf, RustLab)
- Podcast appearances (Rustacean Station)
- Community support (Discord, GitHub Discussions)
- Contributor guide
- Good first issues

---

## 📚 Resources

**Inspiration:**
- governor: https://github.com/benwis/governor
- tower-governor: https://github.com/benwis/tower-governor
- leaky-bucket: https://github.com/udoprog/leaky-bucket
- Redis rate limiting: https://redis.io/docs/latest/develop/reference/modules/redis-cell/

**Tools:**
- GitHub Actions
- criterion (benchmarking)
- tarpaulin (coverage)
- cargo-deny (security)
- cargo-audit (vulnerabilities)

---

**Next Review:** After v0.5.0 release
