//! Correctness matrix: each in-tree algorithm against the same shapes, plus v0.11 API shapes.
//!
//! Shapes:
//! - burst then deny
//! - retry_after is fractional (~100ms at 10 tok/s), not ceil()'d to 1s
//! - cost == 0 is rejected without consuming
//! - cost > capacity returns retry_after: None (permanently unfulfillable)
//! - keys are isolated
//! - refill after a paused-clock advance
//! - sub-1-req/s builders (per_minute, per_hour), burst < rate, typed keys, peek/reset/clear/cleanup

use std::net::{IpAddr, Ipv4Addr};
use std::time::Duration;
use tokio_rate_limit::algorithm::{Algorithm, LeakyBucket, ProbabilisticTokenBucket, TokenBucket};
use tokio_rate_limit::{RateLimitDecision, RateLimiter, RateLimiterConfig};

fn assert_retry_after_fractional(decision: &RateLimitDecision) {
    assert!(!decision.permitted, "expected deny");
    let wait = decision
        .retry_after
        .expect("denied request must carry retry_after");
    assert!(
        wait.as_millis() >= 50 && wait.as_millis() <= 150,
        "retry_after should be ~100ms at 10 tok/s, got {}ms",
        wait.as_millis()
    );
}

#[tokio::test(start_paused = true)]
async fn token_bucket_shapes() {
    let bucket = TokenBucket::new(1, 10);
    assert!(bucket.check("k").permitted);
    assert_retry_after_fractional(&bucket.check("k"));

    let bucket = TokenBucket::new(2, 10);
    assert!(bucket.check("a").permitted);
    assert!(bucket.check("a").permitted);
    assert!(!bucket.check("a").permitted);
    assert!(bucket.check("b").permitted);

    let bucket = TokenBucket::new(5, 10);
    for _ in 0..5 {
        assert!(bucket.check("r").permitted);
    }
    assert!(!bucket.check("r").permitted);
    tokio::time::advance(Duration::from_millis(100)).await;
    assert!(bucket.check("r").permitted);

    let bucket = TokenBucket::new(10, 10);
    let denied = bucket.check_with_cost("c", 0);
    assert!(!denied.permitted);
    assert!(bucket.check("c").permitted);

    let impossible = bucket.check_with_cost("c", 11);
    assert!(!impossible.permitted);
    assert!(impossible.retry_after.is_none());
}

#[tokio::test(start_paused = true)]
async fn leaky_bucket_shapes() {
    // Empty leaky bucket admits until occupancy hits capacity.
    let bucket = LeakyBucket::new(1, 10);
    assert!(bucket.check("k").permitted);
    assert_retry_after_fractional(&bucket.check("k"));

    let bucket = LeakyBucket::new(2, 10);
    assert!(bucket.check("a").permitted);
    assert!(bucket.check("a").permitted);
    assert!(!bucket.check("a").permitted);
    assert!(bucket.check("b").permitted);

    let bucket = LeakyBucket::new(5, 10);
    for _ in 0..5 {
        assert!(bucket.check("r").permitted);
    }
    assert!(!bucket.check("r").permitted);
    tokio::time::advance(Duration::from_millis(100)).await;
    assert!(bucket.check("r").permitted);

    let bucket = LeakyBucket::new(10, 10);
    let denied = bucket.check_with_cost("c", 0);
    assert!(!denied.permitted);
    assert!(bucket.check("c").permitted);

    let impossible = bucket.check_with_cost("c", 11);
    assert!(!impossible.permitted);
    assert!(impossible.retry_after.is_none());
}

#[tokio::test(start_paused = true)]
async fn probabilistic_exact_shapes() {
    // sample_rate = 1 is the deterministic bucket.
    let bucket = ProbabilisticTokenBucket::new(1, 10, 1);
    assert!(bucket.check("k").permitted);
    assert_retry_after_fractional(&bucket.check("k"));

    let bucket = ProbabilisticTokenBucket::new(2, 10, 1);
    assert!(bucket.check("a").permitted);
    assert!(bucket.check("a").permitted);
    assert!(!bucket.check("a").permitted);
    assert!(bucket.check("b").permitted);

    let bucket = ProbabilisticTokenBucket::new(5, 10, 1);
    for _ in 0..5 {
        assert!(bucket.check("r").permitted);
    }
    assert!(!bucket.check("r").permitted);
    tokio::time::advance(Duration::from_millis(100)).await;
    assert!(bucket.check("r").permitted);

    let bucket = ProbabilisticTokenBucket::new(10, 10, 1);
    let denied = bucket.check_with_cost("c", 0);
    assert!(!denied.permitted);
    assert!(bucket.check("c").permitted);

    let impossible = bucket.check_with_cost("c", 11);
    assert!(!impossible.permitted);
    assert!(impossible.retry_after.is_none());
}

#[tokio::test(start_paused = true)]
async fn probabilistic_hot_key_concurrent_bound() {
    let algorithm = ProbabilisticTokenBucket::new(1_000, 1_000, 10);
    let limiter = RateLimiter::from_algorithm(algorithm);
    let mut handles = Vec::new();
    for _ in 0..8 {
        let limiter = limiter.clone();
        handles.push(tokio::spawn(async move {
            let mut admitted = 0u64;
            for _ in 0..500 {
                if limiter.check("hot").permitted {
                    admitted += 1;
                }
            }
            admitted
        }));
    }
    let mut total = 0u64;
    for h in handles {
        total += h.await.unwrap();
    }
    // capacity 1000, no time passed, 8 tasks. Overdraft is O(threads * lump)
    // with lump = sample_rate * cost = 10. Bound generously.
    assert!(
        total <= 1_000 + 8 * 10,
        "concurrent admits {total} exceeded capacity + threads * lump"
    );
}

#[test]
fn config_validation_allows_tight_burst_and_rejects_zeros() {
    assert!(RateLimiter::new(RateLimiterConfig {
        requests_per_second: 0,
        burst: 10,
    })
    .is_err());
    assert!(RateLimiter::new(RateLimiterConfig {
        requests_per_second: 10,
        burst: 0,
    })
    .is_err());
    // Tight bursts (burst < requests_per_second) are valid in v0.11+.
    assert!(RateLimiter::new(RateLimiterConfig {
        requests_per_second: 10,
        burst: 5,
    })
    .is_ok());
    assert!(RateLimiter::new(RateLimiterConfig {
        requests_per_second: 10,
        burst: 10,
    })
    .is_ok());
}

#[tokio::test(start_paused = true)]
async fn per_minute_and_per_hour_builders() {
    // 6 per minute = 1 token every 10s, burst = 2
    let limiter = RateLimiter::builder()
        .per_minute(6)
        .burst(2)
        .build()
        .unwrap();

    assert!(limiter.check("user").permitted);
    assert!(limiter.check("user").permitted);
    let denied = limiter.check("user");
    assert!(!denied.permitted);
    assert_eq!(denied.retry_after, Some(Duration::from_secs(10)));

    tokio::time::advance(Duration::from_secs(10)).await;
    assert!(limiter.check("user").permitted);

    // Conflicting rate setters return an error.
    let conflict = RateLimiter::builder()
        .requests_per_second(10)
        .per_hour(60)
        .burst(1)
        .build();
    assert!(conflict.is_err());
}

#[tokio::test(start_paused = true)]
async fn acquire_with_unfulfillable_cost_returns_immediately() {
    let limiter = RateLimiter::builder()
        .requests_per_second(1)
        .burst(5)
        .build()
        .unwrap();

    let start = tokio::time::Instant::now();
    let decision = limiter.acquire_with_cost("k", 6).await;
    assert!(!decision.permitted);
    assert!(decision.retry_after.is_none());
    assert_eq!(start.elapsed(), Duration::ZERO);

    let timed = limiter
        .acquire_timeout_with_cost("k", 6, Duration::from_secs(30))
        .await;
    assert_eq!(timed, decision);
    assert_eq!(start.elapsed(), Duration::ZERO);
}

#[tokio::test(start_paused = true)]
async fn state_introspection_and_management() {
    let limiter = RateLimiter::builder()
        .requests_per_second(10)
        .burst(5)
        .idle_ttl(Duration::from_secs(60))
        .build()
        .unwrap();

    assert!(limiter.is_empty());
    assert_eq!(limiter.peek("alice").remaining, Some(5));
    assert!(limiter.is_empty(), "peek must not insert keys");

    let cloned = limiter.clone();
    assert!(format!("{cloned:?}").contains("RateLimiter"));

    assert_eq!(limiter.check_with_cost("alice", 3).remaining, Some(2));
    assert_eq!(cloned.peek("alice").remaining, Some(2));
    assert_eq!(limiter.len(), 1);

    assert!(limiter.reset("alice"));
    assert_eq!(limiter.len(), 0);
    assert!(!limiter.reset("alice"));

    assert!(limiter.check("bob").permitted);
    assert!(limiter.check("carol").permitted);
    assert_eq!(limiter.len(), 2);

    tokio::time::advance(Duration::from_secs(61)).await;
    assert_eq!(limiter.cleanup(), 2);
    assert!(limiter.is_empty());

    assert!(limiter.check("dave").permitted);
    limiter.clear();
    assert!(limiter.is_empty());
}

#[test]
fn generic_typed_keys_ip_and_u64() {
    let ip_limiter = RateLimiter::builder()
        .requests_per_second(5)
        .burst(2)
        .build_with_key::<IpAddr>()
        .unwrap();

    let ip = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 7));
    assert!(ip_limiter.check(&ip).permitted);
    assert!(ip_limiter.check(&ip).permitted);
    assert!(!ip_limiter.check(&ip).permitted);

    let id_limiter = RateLimiter::builder()
        .requests_per_second(5)
        .burst(1)
        .build_with_key::<u64>()
        .unwrap();

    assert!(id_limiter.check(&42u64).permitted);
    assert!(!id_limiter.check(&42u64).permitted);
    assert!(id_limiter.check(&99u64).permitted);
}

#[cfg(feature = "serde")]
#[test]
fn serde_round_trip_config_and_decision() {
    let cfg = RateLimiterConfig {
        requests_per_second: 50,
        burst: 25,
    };
    let json = serde_json::to_string(&cfg).unwrap();
    let decoded: RateLimiterConfig = serde_json::from_str(&json).unwrap();
    assert_eq!(cfg, decoded);

    let limiter = RateLimiter::new(decoded).unwrap();
    let decision = limiter.check("serde-key");
    let dec_json = serde_json::to_string(&decision).unwrap();
    let dec_decoded: RateLimitDecision = serde_json::from_str(&dec_json).unwrap();
    assert_eq!(decision, dec_decoded);
}
