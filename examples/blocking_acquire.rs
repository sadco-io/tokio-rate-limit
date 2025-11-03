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
    // Create a rate limiter: 5 tokens/second, burst of 10
    let limiter = RateLimiter::new(RateLimiterConfig {
        requests_per_second: 5,
        burst: 10,
    });

    println!("Blocking Acquire Demo");
    println!("Limit: 5 tokens/sec, Burst: 10 tokens\n");

    let client = "client-123";

    // Example 1: try_acquire (non-blocking)
    println!("=== Example 1: try_acquire (non-blocking) ===");
    println!("Attempting to acquire 10 tokens rapidly...\n");

    for i in 1..=12 {
        match limiter.try_acquire(client).await {
            Ok(decision) if decision.permitted => {
                println!("Request {}: ✓ Permitted (remaining: {})", i, decision.remaining.unwrap());
            }
            Ok(decision) => {
                println!(
                    "Request {}: ✗ Denied (retry after: {:?})",
                    i,
                    decision.retry_after.unwrap()
                );
            }
            Err(e) => eprintln!("Request {}: Error: {}", i, e),
        }
    }

    // Wait for refill
    println!("\nWaiting 2 seconds for refill...");
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Example 2: acquire_timeout (blocking with timeout)
    println!("\n=== Example 2: acquire_timeout (blocking with timeout) ===");
    println!("Exhausting tokens, then using acquire_timeout...\n");

    // Exhaust all tokens
    for i in 1..=10 {
        let _ = limiter.check(client).await;
        println!("Consumed token {}/10", i);
    }

    // Try to acquire with timeout
    println!("\nAttempting acquire with 3-second timeout...");
    let start = std::time::Instant::now();
    match limiter.acquire_timeout(client, Duration::from_secs(3)).await {
        Ok(decision) if decision.permitted => {
            let elapsed = start.elapsed();
            println!(
                "✓ Acquired after {:?} (remaining: {})",
                elapsed,
                decision.remaining.unwrap()
            );
        }
        Ok(_) => {
            let elapsed = start.elapsed();
            println!("✗ Timeout expired after {:?}", elapsed);
        }
        Err(e) => eprintln!("Error: {}", e),
    }

    // Example 3: acquire (blocking indefinitely)
    println!("\n=== Example 3: acquire (blocking indefinitely) ===");
    println!("Exhausting tokens, then using acquire (will wait)...\n");

    // Exhaust all tokens
    for i in 1..=5 {
        let _ = limiter.check(client).await;
        println!("Consumed token {}/5", i);
    }

    println!("\nAttempting acquire (will wait for tokens)...");
    let start = std::time::Instant::now();
    match limiter.acquire(client).await {
        Ok(decision) => {
            let elapsed = start.elapsed();
            println!(
                "✓ Acquired after {:?} (remaining: {})",
                elapsed,
                decision.remaining.unwrap()
            );
        }
        Err(e) => eprintln!("Error: {}", e),
    }

    // Example 4: Concurrent blocking acquires
    println!("\n=== Example 4: Concurrent blocking acquires ===");
    println!("Spawning 5 tasks that all try to acquire simultaneously...\n");

    let mut handles = vec![];
    for i in 1..=5 {
        let limiter = std::sync::Arc::new(RateLimiter::new(RateLimiterConfig {
            requests_per_second: 2,
            burst: 5,
        }));
        let handle = tokio::spawn(async move {
            let start = std::time::Instant::now();
            match limiter.acquire("shared-client").await {
                Ok(_) => {
                    let elapsed = start.elapsed();
                    println!("Task {} acquired after {:?}", i, elapsed);
                }
                Err(e) => eprintln!("Task {}: Error: {}", i, e),
            }
        });
        handles.push(handle);
    }

    // Wait for all tasks
    for handle in handles {
        let _ = handle.await;
    }

    println!("\n--- Usage Guidelines ---");
    println!("• try_acquire: Use for immediate feedback (non-blocking)");
    println!("• acquire_timeout: Use when you have a deadline");
    println!("• acquire: Use for guaranteed execution (caution: can block forever)");
    println!("\nCommon patterns:");
    println!("• Worker queues: Use acquire to process jobs at a controlled rate");
    println!("• API clients: Use acquire_timeout to respect rate limits with fallback");
    println!("• Batch processing: Use try_acquire to maximize throughput");
}
