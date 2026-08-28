//! Correctness matrix: each in-tree algorithm against the same shapes.
//!
//! Shapes:
//! - burst then deny
//! - retry_after is fractional (~100ms at 10 tok/s), not ceil()'d to 1s
//! - cost == 0 is rejected without consuming
//! - keys are isolated
//! - refill after a paused-clock advance

use std::time::Duration;
#[allow(deprecated)]
use tokio_rate_limit::algorithm::CachedTokenBucket;
use tokio_rate_limit::algorithm::{Algorithm, LeakyBucket, ProbabilisticTokenBucket, TokenBucket};

fn assert_retry_after_fractional(decision: &tokio_rate_limit::RateLimitDecision) {
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
}

#[tokio::test(start_paused = true)]
#[allow(deprecated)]
async fn cached_token_bucket_shapes() {
    let bucket = CachedTokenBucket::new(1, 10);
    assert!(bucket.check("k").permitted);
    assert_retry_after_fractional(&bucket.check("k"));

    let bucket = CachedTokenBucket::new(2, 10);
    assert!(bucket.check("a").permitted);
    assert!(bucket.check("a").permitted);
    assert!(!bucket.check("a").permitted);
    assert!(bucket.check("b").permitted);

    let bucket = CachedTokenBucket::new(5, 10);
    for _ in 0..5 {
        assert!(bucket.check("r").permitted);
    }
    assert!(!bucket.check("r").permitted);
    tokio::time::advance(Duration::from_millis(100)).await;
    assert!(bucket.check("r").permitted);

    let bucket = CachedTokenBucket::new(10, 10);
    let denied = bucket.check_with_cost("c", 0);
    assert!(!denied.permitted);
    assert!(bucket.check("c").permitted);
}

#[tokio::test(start_paused = true)]
async fn probabilistic_hot_key_concurrent_bound() {
    use std::sync::Arc;
    use tokio_rate_limit::RateLimiter;

    let algorithm = ProbabilisticTokenBucket::new(1_000, 1_000, 10);
    let limiter = Arc::new(RateLimiter::from_algorithm(algorithm));
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
fn new_rejects_invalid_config() {
    use tokio_rate_limit::{RateLimiter, RateLimiterConfig};

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
    assert!(RateLimiter::new(RateLimiterConfig {
        requests_per_second: 10,
        burst: 5,
    })
    .is_err());
    assert!(RateLimiter::new(RateLimiterConfig {
        requests_per_second: 10,
        burst: 10,
    })
    .is_ok());
}
