//! Basic usage example without middleware.
//!
//! This example demonstrates how to use the rate limiter directly in your code
//! without using the Axum middleware integration.
//!
//! Run with:
//! ```bash
//! cargo run --example basic
//! ```

use tokio_rate_limit::{RateLimiter, RateLimiterConfig};

#[tokio::main]
async fn main() {
    println!("Basic Rate Limiter Example\n");

    // Create a rate limiter with 10 requests per second and burst of 15
    let limiter = RateLimiter::new(RateLimiterConfig {
        requests_per_second: 10,
        burst: 15,
    });

    println!("Configuration: 10 requests/second, burst capacity: 15\n");

    // Or use the builder API:
    // let limiter = RateLimiter::builder()
    //     .requests_per_second(10)
    //     .burst(15)
    //     .build()
    //     .unwrap();

    let client_id = "client-123";

    // Make a burst of requests
    println!("Making 20 requests rapidly...\n");
    for i in 1..=20 {
        match limiter.check(client_id).await {
            Ok(decision) => {
                if decision.permitted {
                    println!(
                        "Request {}: ✓ ALLOWED (remaining: {})",
                        i,
                        decision.remaining.unwrap_or(0)
                    );
                } else {
                    println!(
                        "Request {}: ✗ RATE LIMITED (retry after: {:?})",
                        i,
                        decision.retry_after.unwrap()
                    );
                }
            }
            Err(e) => {
                println!("Request {}: ERROR: {}", i, e);
            }
        }
    }

    // Wait a bit and try again
    println!("\nWaiting 500ms for token refill...\n");
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    println!("Making 5 more requests...\n");
    for i in 21..=25 {
        match limiter.check(client_id).await {
            Ok(decision) => {
                if decision.permitted {
                    println!(
                        "Request {}: ✓ ALLOWED (remaining: {})",
                        i,
                        decision.remaining.unwrap_or(0)
                    );
                } else {
                    println!(
                        "Request {}: ✗ RATE LIMITED (retry after: {:?})",
                        i,
                        decision.retry_after.unwrap()
                    );
                }
            }
            Err(e) => {
                println!("Request {}: ERROR: {}", i, e);
            }
        }
    }

    // Demonstrate per-key rate limiting
    println!("\n--- Per-Key Rate Limiting ---\n");

    let limiter = RateLimiter::builder()
        .requests_per_second(2)
        .burst(2)
        .build()
        .unwrap();

    println!("Configuration: 2 requests/second per client\n");

    // Client 1 exhausts their limit
    println!("Client 1:");
    for i in 1..=3 {
        let decision = limiter.check("client-1").await.unwrap();
        println!(
            "  Request {}: {}",
            i,
            if decision.permitted {
                "ALLOWED"
            } else {
                "RATE LIMITED"
            }
        );
    }

    // Client 2 has their own limit
    println!("\nClient 2:");
    for i in 1..=3 {
        let decision = limiter.check("client-2").await.unwrap();
        println!(
            "  Request {}: {}",
            i,
            if decision.permitted {
                "ALLOWED"
            } else {
                "RATE LIMITED"
            }
        );
    }

    println!("\nExample complete!");
}
