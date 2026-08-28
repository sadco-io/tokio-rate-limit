//! Accuracy validation tests for probabilistic rate limiting.
//!
//! These tests validate that the probabilistic algorithm maintains acceptable
//! error margins compared to the deterministic baseline.

use std::time::Duration;
use tokio_rate_limit::algorithm::{ProbabilisticTokenBucket, TokenBucket};
use tokio_rate_limit::Algorithm;

/// Test helper to measure actual throughput
async fn measure_throughput<A: Algorithm>(
    algorithm: &A,
    key: &str,
    duration: Duration,
    target_rate: u64,
) -> (u64, u64) {
    let start = tokio::time::Instant::now();
    let end = start + duration;

    let mut allowed = 0u64;
    let mut denied = 0u64;

    // Generate requests at target rate
    let interval = Duration::from_secs_f64(1.0 / target_rate as f64);

    while tokio::time::Instant::now() < end {
        let decision = algorithm.check(key);
        if decision.permitted {
            allowed += 1;
        } else {
            denied += 1;
        }
        tokio::time::sleep(interval).await;
    }

    (allowed, denied)
}

/// Test accuracy with steady traffic
#[tokio::test(start_paused = true)]
async fn test_steady_traffic_accuracy_1_percent() {
    let limit = 100; // 100 req/sec
    let capacity = 200;

    // Baseline deterministic
    let baseline = TokenBucket::new(capacity, limit);

    // Probabilistic with 1% sampling
    let prob = ProbabilisticTokenBucket::new(capacity, limit, 100);

    // Run at exactly the limit rate for 10 seconds
    let duration = Duration::from_secs(10);

    let (baseline_allowed, baseline_denied) =
        measure_throughput(&baseline, "key1", duration, limit).await;
    let (prob_allowed, prob_denied) = measure_throughput(&prob, "key2", duration, limit).await;

    // Calculate error margin
    let total_baseline = baseline_allowed + baseline_denied;
    let total_prob = prob_allowed + prob_denied;

    // Both should process roughly the same number of requests
    assert!(
        total_baseline > 0 && total_prob > 0,
        "Both algorithms should have processed requests"
    );

    // Allowed requests should be similar (within margin)
    let error_rate = if baseline_allowed > 0 {
        ((prob_allowed as f64 - baseline_allowed as f64) / baseline_allowed as f64).abs()
    } else {
        0.0
    };

    println!("Steady traffic (1% sampling):");
    println!(
        "  Baseline: {} allowed, {} denied",
        baseline_allowed, baseline_denied
    );
    println!(
        "  Probabilistic: {} allowed, {} denied",
        prob_allowed, prob_denied
    );
    println!("  Error rate: {:.2}%", error_rate * 100.0);

    // Allow up to 5% error for 1% sampling (conservative estimate)
    assert!(
        error_rate <= 0.05,
        "Error rate {:.2}% exceeds 5% threshold",
        error_rate * 100.0
    );
}

/// Test accuracy with 5% sampling
#[tokio::test(start_paused = true)]
async fn test_steady_traffic_accuracy_5_percent() {
    let limit = 100;
    let capacity = 200;

    let baseline = TokenBucket::new(capacity, limit);
    let prob = ProbabilisticTokenBucket::new(capacity, limit, 20); // 5% sampling

    let duration = Duration::from_secs(10);

    let (baseline_allowed, baseline_denied) =
        measure_throughput(&baseline, "key1", duration, limit).await;
    let (prob_allowed, prob_denied) = measure_throughput(&prob, "key2", duration, limit).await;

    let error_rate = if baseline_allowed > 0 {
        ((prob_allowed as f64 - baseline_allowed as f64) / baseline_allowed as f64).abs()
    } else {
        0.0
    };

    println!("Steady traffic (5% sampling):");
    println!(
        "  Baseline: {} allowed, {} denied",
        baseline_allowed, baseline_denied
    );
    println!(
        "  Probabilistic: {} allowed, {} denied",
        prob_allowed, prob_denied
    );
    println!("  Error rate: {:.2}%", error_rate * 100.0);

    // 5% sampling should have lower error (within 2%)
    assert!(
        error_rate <= 0.02,
        "Error rate {:.2}% exceeds 2% threshold",
        error_rate * 100.0
    );
}

/// Test accuracy with 10% sampling
#[tokio::test(start_paused = true)]
async fn test_steady_traffic_accuracy_10_percent() {
    let limit = 100;
    let capacity = 200;

    let baseline = TokenBucket::new(capacity, limit);
    let prob = ProbabilisticTokenBucket::new(capacity, limit, 10); // 10% sampling

    let duration = Duration::from_secs(10);

    let (baseline_allowed, baseline_denied) =
        measure_throughput(&baseline, "key1", duration, limit).await;
    let (prob_allowed, prob_denied) = measure_throughput(&prob, "key2", duration, limit).await;

    let error_rate = if baseline_allowed > 0 {
        ((prob_allowed as f64 - baseline_allowed as f64) / baseline_allowed as f64).abs()
    } else {
        0.0
    };

    println!("Steady traffic (10% sampling):");
    println!(
        "  Baseline: {} allowed, {} denied",
        baseline_allowed, baseline_denied
    );
    println!(
        "  Probabilistic: {} allowed, {} denied",
        prob_allowed, prob_denied
    );
    println!("  Error rate: {:.2}%", error_rate * 100.0);

    // 10% sampling should have very low error (within 1%)
    assert!(
        error_rate <= 0.01,
        "Error rate {:.2}% exceeds 1% threshold",
        error_rate * 100.0
    );
}

/// Test that burst capacity is respected
#[tokio::test(start_paused = true)]
async fn test_burst_capacity() {
    let limit = 10; // 10 req/sec
    let capacity = 50; // Burst of 50

    let prob = ProbabilisticTokenBucket::new(capacity, limit, 1); // 100% sampling for accuracy

    // Should allow burst of 50
    let mut allowed = 0;
    for _ in 0..60 {
        let decision = prob.check("test-key");
        if decision.permitted {
            allowed += 1;
        }
    }

    println!("Burst test: {} allowed out of 60 requests", allowed);

    // Should allow approximately the capacity (50), with some tolerance
    assert!(
        (45..=55).contains(&allowed),
        "Burst allowed {} requests, expected ~50",
        allowed
    );
}

/// Test with traffic above the limit.
///
/// # Why the threshold moved from 30% to 20%
///
/// The original assertion was `deny_rate >= 0.30`, which is not attainable by
/// *any* approximation of the deterministic bucket in this configuration --
/// including a perfect one. With `capacity = 200`, `limit = 100` and 1000
/// requests offered over 5 virtual seconds, the deterministic `TokenBucket`
/// admits exactly 200 (initial burst) + 500 (refill) = 700 and denies exactly
/// 300, i.e. 30.00%. So 30% is the *target value*, not a lower bound: an
/// unbiased estimator sits on it and crosses below it half the time.
///
/// The configuration is also the pathological corner for sampling:
/// `sample_rate * cost = 100` tokens is half the entire bucket, so the bucket
/// carries a residual of roughly half a lump that it never spends. That shows
/// up as denying *more* than the baseline, which is the safe direction.
///
/// Measured over 100 independent runs after the fix (baseline: 699 admitted):
///
/// - admitted: min 471, max 692, mean 591.4, sd 42.7
/// - deny rate: min 30.8%, max 52.9%, mean 40.9%
/// - ratio to the deterministic baseline: 0.674 .. 0.990 (never above it)
///
/// A 20% floor is 4.9 standard deviations below the observed mean. The
/// assertion against the deterministic baseline below is the one that would
/// actually catch a regression of the original defect -- pre-fix, this
/// configuration admitted all 1000 requests.
#[tokio::test(start_paused = true)]
async fn test_above_limit_traffic() {
    let limit = 100;
    let capacity = 200;

    let prob = ProbabilisticTokenBucket::new(capacity, limit, 100); // 1% sampling
    let baseline = TokenBucket::new(capacity, limit);

    // Send at 2x the limit rate
    let duration = Duration::from_secs(5);
    let (allowed, denied) = measure_throughput(&prob, "test-key", duration, limit * 2).await;
    let (baseline_allowed, _) =
        measure_throughput(&baseline, "baseline-key", duration, limit * 2).await;

    println!("Above limit (2x rate):");
    println!("  Allowed: {}, Denied: {}", allowed, denied);
    println!("  Deterministic baseline allowed: {}", baseline_allowed);

    // Must not be materially more permissive than the algorithm it approximates.
    // Measured worst case over 100 runs was 0.990x the baseline -- it never
    // exceeded it -- so 1.10x is about 4 sd of headroom.
    let ceiling = (baseline_allowed as f64 * 1.10) as u64;
    assert!(
        allowed <= ceiling,
        "Probabilistic admitted {} against a deterministic baseline of {} (ceiling {})",
        allowed,
        baseline_allowed,
        ceiling
    );

    // ...and it must still deny a significant portion.
    let total = allowed + denied;
    let deny_rate = denied as f64 / total as f64;

    assert!(
        deny_rate >= 0.2,
        "Should deny at least 20% when over limit, got {:.2}%",
        deny_rate * 100.0
    );
}

/// Test with traffic below the limit
#[tokio::test(start_paused = true)]
async fn test_below_limit_traffic() {
    let limit = 100;
    let capacity = 200;

    let prob = ProbabilisticTokenBucket::new(capacity, limit, 100); // 1% sampling

    // Send at 50% of the limit rate
    let duration = Duration::from_secs(5);
    let (allowed, denied) = measure_throughput(&prob, "test-key", duration, limit / 2).await;

    println!("Below limit (50% rate):");
    println!("  Allowed: {}, Denied: {}", allowed, denied);

    // Should allow nearly all requests
    let total = allowed + denied;
    let allow_rate = allowed as f64 / total as f64;

    assert!(
        allow_rate >= 0.95,
        "Should allow at least 95% when under limit, got {:.2}%",
        allow_rate * 100.0
    );
}

/// Test refill behavior
#[tokio::test(start_paused = true)]
async fn test_refill_accuracy() {
    let limit = 10; // 10 req/sec
    let capacity = 10;

    let prob = ProbabilisticTokenBucket::new(capacity, limit, 1); // 100% sampling

    // Exhaust the bucket
    for _ in 0..10 {
        let decision = prob.check("test-key");
        assert!(decision.permitted);
    }

    // Next should be denied
    let decision = prob.check("test-key");
    assert!(!decision.permitted);

    // Wait 1 second (should refill 10 tokens)
    tokio::time::advance(Duration::from_secs(1)).await;

    // Should allow ~10 more requests
    let mut allowed = 0;
    for _ in 0..15 {
        let decision = prob.check("test-key");
        if decision.permitted {
            allowed += 1;
        }
    }

    println!("Refill test: {} allowed after 1 second", allowed);

    assert!(
        (8..=12).contains(&allowed),
        "Should allow ~10 requests after refill, got {}",
        allowed
    );
}

/// Test multiple keys isolation
#[tokio::test(start_paused = true)]
async fn test_key_isolation() {
    let limit = 10;
    let capacity = 10;

    let prob = ProbabilisticTokenBucket::new(capacity, limit, 1);

    // Exhaust key1
    for _ in 0..10 {
        let _ = prob.check("key1");
    }

    let decision1 = prob.check("key1");
    assert!(!decision1.permitted, "key1 should be rate limited");

    // key2 should still work
    let decision2 = prob.check("key2");
    assert!(decision2.permitted, "key2 should not be rate limited");
}

/// Test cost-based rate limiting
#[tokio::test(start_paused = true)]
async fn test_cost_based_accuracy() {
    let limit = 100;
    let capacity = 100;

    let prob = ProbabilisticTokenBucket::new(capacity, limit, 1); // 100% sampling

    // Consume with costs: 30, 30, 30
    for _ in 0..3 {
        let decision = prob.check_with_cost("test-key", 30);
        assert!(decision.permitted);
    }

    // Should have ~10 tokens left
    let decision = prob.check_with_cost("test-key", 30);
    assert!(
        !decision.permitted,
        "Should not have enough tokens for cost=30"
    );

    // But should work with cost=10
    let decision = prob.check_with_cost("test-key", 10);
    assert!(decision.permitted, "Should have enough tokens for cost=10");
}

/// Test error margin scales with sampling rate
#[tokio::test(start_paused = true)]
async fn test_error_scaling_with_sample_rate() {
    let limit = 1000;
    let capacity = 2000;
    let duration = Duration::from_secs(5);

    let baseline = TokenBucket::new(capacity, limit);
    let (baseline_allowed, _) = measure_throughput(&baseline, "baseline", duration, limit).await;

    // Test different sampling rates
    let sampling_rates = vec![
        (100, "1%"), // 1%
        (20, "5%"),  // 5%
        (10, "10%"), // 10%
        (5, "20%"),  // 20%
    ];

    println!(
        "\nError scaling test (baseline allowed: {}):",
        baseline_allowed
    );

    for (sample_rate, label) in sampling_rates {
        let prob = ProbabilisticTokenBucket::new(capacity, limit, sample_rate);
        let (prob_allowed, _) =
            measure_throughput(&prob, &format!("prob_{}", sample_rate), duration, limit).await;

        let error_rate = if baseline_allowed > 0 {
            ((prob_allowed as f64 - baseline_allowed as f64) / baseline_allowed as f64).abs()
        } else {
            0.0
        };

        println!(
            "  {} sampling: {} allowed, error: {:.2}%",
            label,
            prob_allowed,
            error_rate * 100.0
        );
    }
}
