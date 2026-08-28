//! Example: Blocking Acquire Methods
//!
//! This example demonstrates the blocking acquire methods that wait for
//! tokens to become available.
//!
//! Run with:
//! ```bash
//! cargo run --example blocking_acquire
//! ```

use std::time::Duration;
use tokio_rate_limit::{RateLimiter, RateLimiterConfig};

#[tokio::main]
async fn main() {
    let limiter = RateLimiter::new(RateLimiterConfig {
        requests_per_second: 5,
        burst: 10,
    })
    .unwrap();

    println!("Blocking Acquire Demo");
    println!("Limit: 5 tokens/sec, Burst: 10 tokens\n");

    let client = "client-123";

    println!("=== Example 1: try_acquire (non-blocking) ===");
    for i in 1..=12 {
        let decision = limiter.try_acquire(client);
        if decision.permitted {
            println!(
                "Request {}: Permitted (remaining: {})",
                i,
                decision.remaining.unwrap()
            );
        } else {
            println!(
                "Request {}: Denied (retry after: {:?})",
                i,
                decision.retry_after.unwrap()
            );
        }
    }

    println!("\nWaiting 2 seconds for refill...");
    tokio::time::sleep(Duration::from_secs(2)).await;

    println!("\n=== Example 2: acquire_timeout (blocking with timeout) ===");
    for i in 1..=10 {
        let _ = limiter.check(client);
        println!("Consumed token {}/10", i);
    }

    println!("\nAttempting acquire with 3-second timeout...");
    let start = std::time::Instant::now();
    let decision = limiter
        .acquire_timeout(client, Duration::from_secs(3))
        .await;
    let elapsed = start.elapsed();
    if decision.permitted {
        println!(
            "Acquired after {:?} (remaining: {})",
            elapsed,
            decision.remaining.unwrap()
        );
    } else {
        println!("Timeout expired after {:?}", elapsed);
    }

    println!("\n=== Example 3: acquire (blocking indefinitely) ===");
    for i in 1..=5 {
        let _ = limiter.check(client);
        println!("Consumed token {}/5", i);
    }

    println!("\nAttempting acquire (will wait for tokens)...");
    let start = std::time::Instant::now();
    let decision = limiter.acquire(client).await;
    let elapsed = start.elapsed();
    println!(
        "Acquired after {:?} (remaining: {})",
        elapsed,
        decision.remaining.unwrap()
    );
}
