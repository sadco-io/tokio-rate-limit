//! Example demonstrating probabilistic rate limiting for ultra-high throughput scenarios.
//!
//! This example shows how to use ProbabilisticTokenBucket for maximum performance
//! with acceptable accuracy trade-offs.
//!
//! Run with: cargo run --example probabilistic_rate_limiting --release

use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use tokio_rate_limit::algorithm::{ProbabilisticTokenBucket, TokenBucket};
use tokio_rate_limit::Algorithm;

#[tokio::main]
async fn main() {
    println!("=== Probabilistic Rate Limiting Demo ===\n");

    // Scenario 1: Basic comparison
    println!("📊 Scenario 1: Performance Comparison");
    println!("Testing 100,000 requests with 1000 req/sec limit\n");

    let iterations = 100_000;

    // Baseline: Deterministic TokenBucket
    let baseline = Arc::new(TokenBucket::new(2000, 1000));
    let start = Instant::now();
    for i in 0..iterations {
        let key = format!("user-{}", i % 100);
        let _ = baseline.check(&key).await;
    }
    let baseline_time = start.elapsed();

    // Probabilistic: 1% sampling
    let prob_1pct = Arc::new(ProbabilisticTokenBucket::new(2000, 1000, 100));
    let start = Instant::now();
    for i in 0..iterations {
        let key = format!("user-{}", i % 100);
        let _ = prob_1pct.check(&key).await;
    }
    let prob_1pct_time = start.elapsed();

    // Probabilistic: 5% sampling
    let prob_5pct = Arc::new(ProbabilisticTokenBucket::new(2000, 1000, 20));
    let start = Instant::now();
    for i in 0..iterations {
        let key = format!("user-{}", i % 100);
        let _ = prob_5pct.check(&key).await;
    }
    let prob_5pct_time = start.elapsed();

    // Probabilistic: 10% sampling
    let prob_10pct = Arc::new(ProbabilisticTokenBucket::new(2000, 1000, 10));
    let start = Instant::now();
    for i in 0..iterations {
        let key = format!("user-{}", i % 100);
        let _ = prob_10pct.check(&key).await;
    }
    let prob_10pct_time = start.elapsed();

    println!("Results:");
    println!(
        "  Baseline (deterministic): {:?} ({:.2}M ops/sec)",
        baseline_time,
        iterations as f64 / baseline_time.as_secs_f64() / 1_000_000.0
    );
    println!(
        "  1% sampling:             {:?} ({:.2}M ops/sec) - {:.1}% faster",
        prob_1pct_time,
        iterations as f64 / prob_1pct_time.as_secs_f64() / 1_000_000.0,
        (baseline_time.as_secs_f64() - prob_1pct_time.as_secs_f64()) / baseline_time.as_secs_f64()
            * 100.0
    );
    println!(
        "  5% sampling:             {:?} ({:.2}M ops/sec) - {:.1}% faster",
        prob_5pct_time,
        iterations as f64 / prob_5pct_time.as_secs_f64() / 1_000_000.0,
        (baseline_time.as_secs_f64() - prob_5pct_time.as_secs_f64()) / baseline_time.as_secs_f64()
            * 100.0
    );
    println!(
        "  10% sampling:            {:?} ({:.2}M ops/sec) - {:.1}% faster",
        prob_10pct_time,
        iterations as f64 / prob_10pct_time.as_secs_f64() / 1_000_000.0,
        (baseline_time.as_secs_f64() - prob_10pct_time.as_secs_f64()) / baseline_time.as_secs_f64()
            * 100.0
    );

    println!("\n");

    // Scenario 2: Rate limiting enforcement
    println!("🚦 Scenario 2: Rate Limit Enforcement");
    println!("Testing with 100 req/sec limit, 500 capacity\n");

    // capacity (500) is 25x one lump (sample_rate * cost = 20), well inside the
    // "capacity >= 10 * sample_rate * cost" rule of thumb.
    let limiter = ProbabilisticTokenBucket::new(500, 100, 20); // 5% sampling

    // Burst test
    println!("  Burst test (60 requests immediately):");
    let mut allowed = 0;
    let mut denied = 0;
    for _ in 0..60 {
        let decision = limiter.check("burst-user").await.unwrap();
        if decision.permitted {
            allowed += 1;
        } else {
            denied += 1;
        }
    }
    println!("    Allowed: {}, Denied: {}", allowed, denied);
    println!("    (Expected: all 60 -- the burst capacity is 500)");

    // Wait for refill
    sleep(Duration::from_millis(1000)).await;

    // Sustained test
    println!("\n  Sustained test (100 req/sec for 1 second):");
    allowed = 0;
    denied = 0;
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(1) {
        let decision = limiter.check("sustained-user").await.unwrap();
        if decision.permitted {
            allowed += 1;
        } else {
            denied += 1;
        }
        sleep(Duration::from_millis(10)).await; // 100 req/sec
    }
    println!("    Allowed: {}, Denied: {}", allowed, denied);
    println!("    (Expected: all of them -- 500 burst + 100/sec refill)");

    println!("\n");

    // Scenario 3: Cost-based rate limiting
    println!("💰 Scenario 3: Cost-Based Rate Limiting");
    println!("GPU resource allocation with weighted costs\n");

    // Costs here go up to 100, so one lump at 1% sampling would be 10_000
    // tokens against a 1_000-token bucket. With cost-based limiting the lump is
    // `sample_rate * cost`, so the sampling rate has to come down accordingly.
    let gpu_limiter = ProbabilisticTokenBucket::new(1000, 100, 5); // 20% sampling

    // Simulate GPU requests with different costs
    let requests = vec![
        ("small-model", 10),  // 10 GPU units
        ("medium-model", 50), // 50 GPU units
        ("large-model", 100), // 100 GPU units
    ];

    for (model, cost) in &requests {
        let decision = gpu_limiter
            .check_with_cost("api-user", *cost)
            .await
            .unwrap();
        println!(
            "  {} (cost={}): {}",
            model,
            cost,
            if decision.permitted {
                "✅ ALLOWED"
            } else {
                "❌ DENIED"
            }
        );
        if let Some(remaining) = decision.remaining {
            println!("    Remaining budget: {} GPU units", remaining);
        }
    }

    println!("\n");

    // Scenario 4: Hot key workload
    println!("🔥 Scenario 4: Hot Key Workload (80/20 distribution)");
    println!("Simulating multi-tenant SaaS with popular users\n");

    let saas_limiter = Arc::new(ProbabilisticTokenBucket::new(100, 50, 20)); // 5% sampling

    let iterations = 10_000;
    let start = Instant::now();

    for i in 0..iterations {
        // 80% of requests go to 20% of users
        let user_id = if i % 100 < 80 {
            format!("hot-user-{}", i % 20) // Hot users
        } else {
            format!("cold-user-{}", i % 100) // Cold users
        };

        let _ = saas_limiter.check(&user_id).await;
    }

    let hot_key_time = start.elapsed();
    println!("  Processed {} requests in {:?}", iterations, hot_key_time);
    println!(
        "  Throughput: {:.2}M ops/sec",
        iterations as f64 / hot_key_time.as_secs_f64() / 1_000_000.0
    );
    println!("  (a hot key is the workload sampling helps most, and even");
    println!("   there the gain is single-digit percent -- see the benchmark)");

    println!("\n");

    // Scenario 5: Multi-tenant with different sampling rates
    println!("🏢 Scenario 5: Multi-Tenant Configuration");
    println!("Different sampling rates for different tiers\n");

    // Free tier: small bucket, so no aggressive sampling -- one lump at 1%
    // sampling would be twice the whole bucket. Sampling needs a bucket that
    // holds many lumps; a low-traffic tier does not have one.
    let free_tier = ProbabilisticTokenBucket::new(50, 10, 1);

    // Pro tier: 10% sampling -- capacity (500) holds 50 lumps.
    let pro_tier = ProbabilisticTokenBucket::new(500, 100, 10);

    // Enterprise: Deterministic (exact accuracy)
    let enterprise_tier = TokenBucket::new(5000, 1000);

    println!("  Free tier: no sampling (50 burst, 10/sec)");
    let decision = free_tier.check("free-user").await.unwrap();
    println!(
        "    Status: {}, Remaining: {}",
        if decision.permitted {
            "✅ ALLOWED"
        } else {
            "❌ DENIED"
        },
        decision.remaining.unwrap_or(0)
    );

    println!("\n  Pro tier: 10% sampling (500 burst, 100/sec)");
    let decision = pro_tier.check("pro-user").await.unwrap();
    println!(
        "    Status: {}, Remaining: {}",
        if decision.permitted {
            "✅ ALLOWED"
        } else {
            "❌ DENIED"
        },
        decision.remaining.unwrap_or(0)
    );

    println!("\n  Enterprise tier: Deterministic (5000 burst, 1000/sec)");
    let decision = enterprise_tier.check("enterprise-user").await.unwrap();
    println!(
        "    Status: {}, Remaining: {}",
        if decision.permitted {
            "✅ ALLOWED"
        } else {
            "❌ DENIED"
        },
        decision.remaining.unwrap_or(0)
    );

    println!("\n");
    println!("=== Demo Complete ===");
    println!("\n📚 Key Takeaways:");
    println!("  • Keep capacity >= 10 * sample_rate * cost -- a sampled request");
    println!("    debits a whole `sample_rate * cost` lump, so a bucket that is");
    println!("    not much larger than one lump is corrected too coarsely");
    println!("  • The speed-up is a few percent, not a few times: the per-key");
    println!("    lookup dominates, and sampling only removes the refill");
    println!("    arithmetic and the compare-and-swap. Benchmark before choosing");
    println!("    this over TokenBucket");
    println!("  • sample_rate = 1 is exactly the deterministic TokenBucket");
    println!("  • Use deterministic for billing/compliance scenarios");
    println!("\n📖 See benches/probabilistic_tradeoff.rs for the accuracy/throughput");
    println!("   benchmark this guidance comes from");
}
