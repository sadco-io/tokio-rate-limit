//! Example: Cost-Based Rate Limiting
//!
//! This example demonstrates weighted rate limiting where different operations
//! consume different amounts of quota.
//!
//! Run with:
//! ```bash
//! cargo run --example cost_based_limiting
//! ```

use tokio_rate_limit::{RateLimiter, RateLimiterConfig};

#[tokio::main]
async fn main() {
    // Create a rate limiter: 100 tokens/second, burst of 200
    let limiter = RateLimiter::new(RateLimiterConfig {
        requests_per_second: 100,
        burst: 200,
    });

    println!("Cost-Based Rate Limiting Demo");
    println!("Limit: 100 tokens/sec, Burst: 200 tokens\n");

    let client = "client-123";

    // Light operation: costs 1 token
    println!("1. Light operation (cost=1):");
    match limiter.check_with_cost(client, 1).await {
        Ok(decision) if decision.permitted => {
            println!("   ✓ Permitted - Remaining: {}", decision.remaining.unwrap());
        }
        Ok(decision) => {
            println!("   ✗ Denied - Retry after: {:?}", decision.retry_after.unwrap());
        }
        Err(e) => eprintln!("   Error: {}", e),
    }

    // Medium operation: costs 10 tokens
    println!("\n2. Medium operation (cost=10):");
    match limiter.check_with_cost(client, 10).await {
        Ok(decision) if decision.permitted => {
            println!("   ✓ Permitted - Remaining: {}", decision.remaining.unwrap());
        }
        Ok(decision) => {
            println!("   ✗ Denied - Retry after: {:?}", decision.retry_after.unwrap());
        }
        Err(e) => eprintln!("   Error: {}", e),
    }

    // Heavy operation: costs 50 tokens
    println!("\n3. Heavy operation (cost=50):");
    match limiter.check_with_cost(client, 50).await {
        Ok(decision) if decision.permitted => {
            println!("   ✓ Permitted - Remaining: {}", decision.remaining.unwrap());
        }
        Ok(decision) => {
            println!("   ✗ Denied - Retry after: {:?}", decision.retry_after.unwrap());
        }
        Err(e) => eprintln!("   Error: {}", e),
    }

    // Very heavy operation: costs 100 tokens
    println!("\n4. Very heavy operation (cost=100):");
    match limiter.check_with_cost(client, 100).await {
        Ok(decision) if decision.permitted => {
            println!("   ✓ Permitted - Remaining: {}", decision.remaining.unwrap());
        }
        Ok(decision) => {
            println!("   ✗ Denied - Retry after: {:?}", decision.retry_after.unwrap());
        }
        Err(e) => eprintln!("   Error: {}", e),
    }

    // Try another operation - should be denied (insufficient tokens)
    println!("\n5. Another operation (cost=50):");
    match limiter.check_with_cost(client, 50).await {
        Ok(decision) if decision.permitted => {
            println!("   ✓ Permitted - Remaining: {}", decision.remaining.unwrap());
        }
        Ok(decision) => {
            println!(
                "   ✗ Denied - Remaining: {}, Retry after: {:?}",
                decision.remaining.unwrap(),
                decision.retry_after.unwrap()
            );
        }
        Err(e) => eprintln!("   Error: {}", e),
    }

    // Using the try_acquire_n alias
    println!("\n6. Using try_acquire_n (cost=1):");
    match limiter.try_acquire_n(client, 1).await {
        Ok(decision) if decision.permitted => {
            println!("   ✓ Permitted - Remaining: {}", decision.remaining.unwrap());
        }
        Ok(decision) => {
            println!(
                "   ✗ Denied - Remaining: {}, Retry after: {:?}",
                decision.remaining.unwrap(),
                decision.retry_after.unwrap()
            );
        }
        Err(e) => eprintln!("   Error: {}", e),
    }

    println!("\n--- Use Cases ---");
    println!("Cost-based rate limiting is useful for:");
    println!("• API endpoints with varying computational costs");
    println!("• Database queries (simple vs complex)");
    println!("• Storage operations (small vs large files)");
    println!("• AI model inference (different model sizes)");
    println!("• Video processing (resolution/length)");
}
