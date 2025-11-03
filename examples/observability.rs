//! Example: Observability with Tracing and Metrics
//!
//! This example demonstrates how to integrate tokio-rate-limit with tracing
//! and metrics for observability.
//!
//! Run with:
//! ```bash
//! cargo run --example observability --features observability,metrics-support
//! ```
//!
//! To see trace output:
//! ```bash
//! RUST_LOG=debug cargo run --example observability --features observability,metrics-support
//! ```

use tokio_rate_limit::{RateLimiter, RateLimiterConfig};

#[tokio::main]
async fn main() {
    // Initialize tracing subscriber to see logs
    #[cfg(feature = "observability")]
    {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .with_target(false)
            .init();
    }

    // In a real app, initialize metrics exporter (e.g., Prometheus, StatsD)
    #[cfg(feature = "metrics-support")]
    {
        println!("Metrics support enabled");
        println!("In production, integrate with:");
        println!("  - Prometheus: Use metrics-exporter-prometheus");
        println!("  - StatsD: Use metrics-exporter-statsd");
        println!("  - CloudWatch: Use metrics-exporter-cloudwatch\n");
    }

    #[cfg(not(feature = "metrics-support"))]
    {
        println!("Note: Run with --features metrics-support to see metrics");
        println!("Example: cargo run --example observability --features observability,metrics-support\n");
    }

    // Create a rate limiter
    let limiter = RateLimiter::new(RateLimiterConfig {
        requests_per_second: 10,
        burst: 20,
    });

    println!("Observability Demo");
    println!("Watch for trace spans and metrics as requests are processed\n");

    // Make some requests
    for i in 1..=25 {
        let client = format!("client-{}", i % 3); // 3 different clients

        match limiter.check(&client).await {
            Ok(decision) if decision.permitted => {
                #[cfg(feature = "observability")]
                tracing::info!(
                    client = %client,
                    remaining = decision.remaining,
                    "Request permitted"
                );
                println!("Request {} ({}): ✓ Permitted", i, client);
            }
            Ok(decision) => {
                #[cfg(feature = "observability")]
                tracing::warn!(
                    client = %client,
                    retry_after = ?decision.retry_after,
                    "Request denied"
                );
                println!("Request {} ({}): ✗ Denied", i, client);
            }
            Err(e) => {
                #[cfg(feature = "observability")]
                tracing::error!(error = %e, "Rate limit check failed");
                eprintln!("Request {}: Error: {}", i, e);
            }
        }

        // Small delay between requests
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    println!("\n--- Integration Examples ---");
    println!("\nTracing Integration:");
    println!("• OpenTelemetry: Use opentelemetry-tracing crate");
    println!("• Jaeger: Export spans to Jaeger for distributed tracing");
    println!("• Honeycomb: Send traces to Honeycomb.io");
    println!("• DataDog: Use tracing-datadog crate");
    println!("\nMetrics Integration:");
    println!("• Prometheus: Use metrics-exporter-prometheus");
    println!("• StatsD: Use metrics-exporter-statsd");
    println!("• CloudWatch: Use metrics-exporter-cloudwatch");
    println!("\nCommon Metrics:");
    println!("• tokio_rate_limit.requests.allowed - Counter");
    println!("• tokio_rate_limit.requests.denied - Counter");
    println!("• tokio_rate_limit.remaining_tokens - Histogram");
    println!("\nSee OBSERVABILITY.md for complete integration examples!");
}
