//! Example demonstrating the leaky bucket algorithm.
//!
//! This example shows the difference between token bucket and leaky bucket algorithms:
//! - Token bucket allows bursts up to capacity
//! - Leaky bucket enforces steady rate, preventing bursts
//!
//! Run with:
//! ```bash
//! cargo run --example leaky_bucket
//! ```

use std::time::Duration;
use tokio_rate_limit::algorithm::{LeakyBucket, TokenBucket};
use tokio_rate_limit::RateLimiter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Leaky Bucket vs Token Bucket Comparison ===\n");

    // Configuration: 10 requests/sec, capacity of 20
    let rate_per_sec = 10;
    let capacity = 20;

    println!("Configuration:");
    println!("  Rate: {} requests/second", rate_per_sec);
    println!("  Capacity: {}", capacity);
    println!();

    // Test 1: Token Bucket - allows bursts
    println!("--- Test 1: Token Bucket (allows bursts) ---");
    let token_bucket = TokenBucket::new(capacity, rate_per_sec);
    let token_limiter = RateLimiter::from_algorithm(token_bucket);

    let mut allowed = 0;
    let mut denied = 0;

    // Try to send 30 requests rapidly
    for i in 1..=30 {
        let decision = token_limiter.check("client-1");
        if decision.permitted {
            allowed += 1;
            println!(
                "  Request {}: ✓ ALLOWED (remaining: {})",
                i,
                decision.remaining.unwrap()
            );
        } else {
            denied += 1;
            println!(
                "  Request {}: ✗ DENIED (retry after: {:?})",
                i,
                decision.retry_after.unwrap()
            );
        }
    }

    println!("\nToken Bucket Results:");
    println!("  Allowed: {} requests", allowed);
    println!("  Denied: {} requests", denied);
    println!(
        "  Note: Token bucket allowed burst of {} requests (up to capacity)",
        allowed
    );
    println!();

    // Test 2: Leaky Bucket - enforces steady rate
    println!("--- Test 2: Leaky Bucket (steady rate, no bursts) ---");
    let leaky_bucket = LeakyBucket::new(capacity, rate_per_sec);
    let leaky_limiter = RateLimiter::from_algorithm(leaky_bucket);

    allowed = 0;
    denied = 0;

    // Try to send 30 requests rapidly
    for i in 1..=30 {
        let decision = leaky_limiter.check("client-1");
        if decision.permitted {
            allowed += 1;
            println!(
                "  Request {}: ✓ ALLOWED (remaining capacity: {})",
                i,
                decision.remaining.unwrap()
            );
        } else {
            denied += 1;
            println!(
                "  Request {}: ✗ DENIED (retry after: {:?})",
                i,
                decision.retry_after.unwrap()
            );
        }
    }

    println!("\nLeaky Bucket Results:");
    println!("  Allowed: {} requests", allowed);
    println!("  Denied: {} requests", denied);
    println!("  Note: Leaky bucket filled up and started denying requests");
    println!();

    // Test 3: Leaky Bucket with pacing
    println!("--- Test 3: Leaky Bucket with Pacing ---");
    println!("Sending requests with 110ms delay (slightly faster than steady rate)...");

    let leaky_bucket = LeakyBucket::new(capacity, rate_per_sec);
    let leaky_limiter = RateLimiter::from_algorithm(leaky_bucket);

    allowed = 0;
    denied = 0;

    for i in 1..=20 {
        let decision = leaky_limiter.check("client-2");
        if decision.permitted {
            allowed += 1;
            println!(
                "  Request {}: ✓ ALLOWED (remaining capacity: {})",
                i,
                decision.remaining.unwrap()
            );
        } else {
            denied += 1;
            println!(
                "  Request {}: ✗ DENIED (retry after: {:?})",
                i,
                decision.retry_after.unwrap()
            );
        }

        // Wait 110ms between requests (at 10/sec, perfect spacing would be 100ms)
        tokio::time::sleep(Duration::from_millis(110)).await;
    }

    println!("\nPaced Leaky Bucket Results:");
    println!("  Allowed: {} requests", allowed);
    println!("  Denied: {} requests", denied);
    println!("  Note: With proper pacing, leaky bucket works smoothly");
    println!();

    // Test 4: Cost-based limiting with Leaky Bucket
    println!("--- Test 4: Cost-Based Limiting with Leaky Bucket ---");
    let leaky_bucket = LeakyBucket::new(100, 50); // 100 capacity, 50/sec leak rate
    let leaky_limiter = RateLimiter::from_algorithm(leaky_bucket);

    println!("Configuration: 100 capacity, 50 tokens/sec leak rate");
    println!();

    // Small requests (cost 10 each)
    println!("Sending 5 small requests (cost 10 each):");
    for i in 1..=5 {
        let decision = leaky_limiter.check_with_cost("client-3", 10);
        println!(
            "  Request {}: {} (remaining capacity: {})",
            i,
            if decision.permitted { "✓" } else { "✗" },
            decision.remaining.unwrap()
        );
    }

    // Large request (cost 60)
    println!("\nSending 1 large request (cost 60):");
    let decision = leaky_limiter.check_with_cost("client-3", 60);
    println!(
        "  Large request: {} (remaining capacity: {})",
        if decision.permitted { "✓" } else { "✗" },
        decision.remaining.unwrap()
    );
    if !decision.permitted {
        println!("  Note: Request would overflow bucket (50 used + 60 needed > 100 capacity)");
    }

    println!();

    // Summary
    println!("=== Summary ===");
    println!();
    println!("Token Bucket:");
    println!("  ✓ Allows bursts up to capacity");
    println!("  ✓ Good for: Public APIs, user-facing services");
    println!("  ✓ Accommodates bursty traffic patterns");
    println!();
    println!("Leaky Bucket:");
    println!("  ✓ Enforces steady rate, no bursts");
    println!("  ✓ Good for: Backend protection, strict QPS limits");
    println!("  ✓ Smooths traffic into consistent flow");
    println!();
    println!("Use Cases:");
    println!("  - Token Bucket: When users expect burst capability (e.g., mobile apps)");
    println!(
        "  - Leaky Bucket: When protecting backends from overload (e.g., database rate limiting)"
    );

    Ok(())
}
