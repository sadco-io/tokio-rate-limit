//! Example: Cost-Based Rate Limiting
//!
//! This example demonstrates weighted rate limiting where different operations
//! consume different amounts of quota.
//!
//! Run with:
//! ```bash
//! cargo run --example cost_based_limiting
//! ```

use tokio_rate_limit::{RateLimitDecision, RateLimiter, RateLimiterConfig};

fn report(decision: RateLimitDecision) {
    if decision.permitted {
        println!("   Permitted - Remaining: {}", decision.remaining.unwrap());
    } else {
        println!(
            "   Denied - Retry after: {:?}",
            decision.retry_after.unwrap()
        );
    }
}

#[tokio::main]
async fn main() {
    let limiter = RateLimiter::new(RateLimiterConfig {
        requests_per_second: 100,
        burst: 200,
    })
    .unwrap();

    println!("Cost-Based Rate Limiting Demo");
    println!("Limit: 100 tokens/sec, Burst: 200 tokens\n");

    let client = "client-123";

    println!("1. Light operation (cost=1):");
    report(limiter.check_with_cost(client, 1));

    println!("\n2. Medium operation (cost=10):");
    report(limiter.check_with_cost(client, 10));

    println!("\n3. Heavy operation (cost=50):");
    report(limiter.check_with_cost(client, 50));

    println!("\n4. Very heavy operation (cost=100):");
    report(limiter.check_with_cost(client, 100));

    println!("\n5. Another operation (cost=50):");
    report(limiter.check_with_cost(client, 50));

    println!("\n6. Using try_acquire_n (cost=1):");
    report(limiter.try_acquire_n(client, 1));
}
