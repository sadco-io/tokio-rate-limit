//! Example client for testing gRPC rate limiting.
//!
//! This client makes multiple requests to demonstrate rate limiting behavior:
//! - Successful requests under the limit
//! - Rate limited requests (RESOURCE_EXHAUSTED errors)
//! - Extracting rate limit metadata from responses
//!
//! Before running this, start the server:
//!   cargo run --example grpc_tonic --features tonic-support
//!
//! Then run this client:
//!   cargo run --example grpc_tonic_client --features tonic-support

use std::time::Duration;
use tokio::time::sleep;

// Include the generated proto code
pub mod hello_world {
    tonic::include_proto!("helloworld");
}

use hello_world::greeter_client::GreeterClient;
use hello_world::{DataRequest, HelloRequest};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = GreeterClient::connect("http://[::1]:50051").await?;

    println!("=== Testing SayHello (10 req/s limit, burst 20) ===\n");

    // Test 1: Send burst of requests to trigger rate limit
    println!("Sending 25 rapid requests (should hit rate limit after ~20)...");
    let mut success_count = 0;
    let mut rate_limited_count = 0;

    for i in 1..=25 {
        let request = tonic::Request::new(HelloRequest {
            name: format!("Client-{}", i),
        });

        match client.say_hello(request).await {
            Ok(response) => {
                success_count += 1;
                let metadata = response.metadata();

                // Extract rate limit information
                let limit = metadata
                    .get("x-ratelimit-limit")
                    .and_then(|v| v.to_str().ok())
                    .map(|s| s.to_string());
                let remaining = metadata
                    .get("x-ratelimit-remaining")
                    .and_then(|v| v.to_str().ok())
                    .map(|s| s.to_string());

                let message = response.into_inner().message;

                println!(
                    "  Request {}: SUCCESS - {} (limit: {:?}, remaining: {:?})",
                    i, message, limit, remaining
                );
            }
            Err(status) => {
                rate_limited_count += 1;
                let metadata = status.metadata();

                // Extract rate limit information from error
                let retry_after = metadata
                    .get("retry-after")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.parse::<u64>().ok());

                println!(
                    "  Request {}: RATE LIMITED - {} (code: {}, retry after: {:?}s)",
                    i,
                    status.message(),
                    status.code(),
                    retry_after
                );

                // If we get retry-after, wait that long
                if let Some(seconds) = retry_after {
                    if seconds > 0 && seconds < 5 {
                        println!("    Waiting {}s before continuing...", seconds);
                        sleep(Duration::from_secs(seconds)).await;
                    }
                }
            }
        }

        // Small delay to avoid overwhelming
        sleep(Duration::from_millis(50)).await;
    }

    println!(
        "\nResults: {} successful, {} rate limited\n",
        success_count, rate_limited_count
    );

    // Test 2: Wait for tokens to refill
    println!("=== Waiting 2 seconds for tokens to refill ===\n");
    sleep(Duration::from_secs(2)).await;

    println!("Sending 5 more requests after cooldown...");
    for i in 1..=5 {
        let request = tonic::Request::new(HelloRequest {
            name: format!("After-Cooldown-{}", i),
        });

        match client.say_hello(request).await {
            Ok(response) => {
                println!(
                    "  Request {}: SUCCESS - {}",
                    i,
                    response.into_inner().message
                );
            }
            Err(status) => {
                println!(
                    "  Request {}: RATE LIMITED - {} (code: {})",
                    i,
                    status.message(),
                    status.code()
                );
            }
        }

        sleep(Duration::from_millis(100)).await;
    }

    // Test 3: Test expensive operation (2 req/s limit)
    println!("\n=== Testing ProcessData (2 req/s limit, burst 3) ===\n");
    println!("Sending 5 rapid expensive requests (should hit rate limit after ~3)...");

    success_count = 0;
    rate_limited_count = 0;

    for i in 1..=5 {
        let request = tonic::Request::new(DataRequest {
            data: format!("expensive-data-{}", i),
            complexity: 5,
        });

        match client.process_data(request).await {
            Ok(response) => {
                success_count += 1;
                let reply = response.into_inner();
                println!(
                    "  Request {}: SUCCESS - {} (took {}ms)",
                    i, reply.result, reply.processing_time_ms
                );
            }
            Err(status) => {
                rate_limited_count += 1;
                println!(
                    "  Request {}: RATE LIMITED - {} (code: {})",
                    i,
                    status.message(),
                    status.code()
                );
            }
        }

        sleep(Duration::from_millis(100)).await;
    }

    println!(
        "\nExpensive operation results: {} successful, {} rate limited\n",
        success_count, rate_limited_count
    );

    // Test 4: Streaming RPC
    println!("=== Testing SayHelloMany (streaming) ===\n");

    let request = tonic::Request::new(HelloRequest {
        name: "Streaming Client".to_string(),
    });

    match client.say_hello_many(request).await {
        Ok(response) => {
            let mut stream = response.into_inner();
            let mut count = 0;

            println!("Receiving streaming responses:");
            while let Some(reply) = stream.message().await? {
                count += 1;
                println!("  Message {}: {}", count, reply.message);
            }
        }
        Err(status) => {
            println!(
                "Streaming request RATE LIMITED: {} (code: {})",
                status.message(),
                status.code()
            );
        }
    }

    println!("\n=== Test completed ===");

    Ok(())
}
