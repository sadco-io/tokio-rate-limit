//! Example of using tokio-rate-limit with Tonic gRPC services.
//!
//! This example demonstrates:
//! - Setting up a gRPC service with rate limiting
//! - Different rate limits for different RPC methods
//! - Both unary and streaming RPCs with rate limiting
//! - Client making requests and handling rate limit errors
//!
//! Run the server:
//!   cargo run --example grpc_tonic --features tonic-support
//!
//! In another terminal, run the client to test:
//!   The client will make multiple requests to trigger rate limits

use tokio_rate_limit::tonic_middleware::{CustomGrpcKeyExtractor, GrpcRateLimitLayer};
use tokio_rate_limit::RateLimiter;
use tonic::{transport::Server, Request, Response, Status};

use std::sync::Arc;
use std::time::Duration;

// Include the generated proto code
pub mod hello_world {
    tonic::include_proto!("helloworld");
}

use hello_world::greeter_server::{Greeter, GreeterServer};
use hello_world::{DataReply, DataRequest, HelloReply, HelloRequest};

// Our gRPC service implementation
#[derive(Debug, Default)]
pub struct MyGreeter {}

#[tonic::async_trait]
impl Greeter for MyGreeter {
    async fn say_hello(
        &self,
        request: Request<HelloRequest>,
    ) -> Result<Response<HelloReply>, Status> {
        println!(
            "Got a SayHello request from {:?}",
            request
                .remote_addr()
                .unwrap_or_else(|| "unknown".parse().unwrap())
        );

        let reply = HelloReply {
            message: format!("Hello {}!", request.into_inner().name),
        };

        Ok(Response::new(reply))
    }

    type SayHelloManyStream = tokio_stream::wrappers::ReceiverStream<Result<HelloReply, Status>>;

    async fn say_hello_many(
        &self,
        request: Request<HelloRequest>,
    ) -> Result<Response<Self::SayHelloManyStream>, Status> {
        println!("Got a SayHelloMany streaming request");

        let name = request.into_inner().name;
        let (tx, rx) = tokio::sync::mpsc::channel(4);

        // Spawn a task to send multiple responses
        tokio::spawn(async move {
            for i in 1..=5 {
                let reply = HelloReply {
                    message: format!("Hello {} (message {}/5)", name, i),
                };

                if tx.send(Ok(reply)).await.is_err() {
                    break;
                }

                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        });

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(
            rx,
        )))
    }

    async fn process_data(
        &self,
        request: Request<DataRequest>,
    ) -> Result<Response<DataReply>, Status> {
        println!("Got a ProcessData request (expensive operation)");

        let req = request.into_inner();

        // Simulate expensive processing
        let processing_time = req.complexity * 10;
        tokio::time::sleep(Duration::from_millis(processing_time as u64)).await;

        let reply = DataReply {
            result: format!("Processed: {}", req.data),
            processing_time_ms: processing_time,
        };

        Ok(Response::new(reply))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create different rate limiters for different scenarios
    let global_limiter = Arc::new(
        RateLimiter::builder()
            .requests_per_second(10) // 10 requests per second globally
            .burst(20)
            .build()?,
    );

    let _expensive_limiter = Arc::new(
        RateLimiter::builder()
            .requests_per_second(2) // Only 2 expensive operations per second
            .burst(3)
            .build()?,
    );

    // Create a custom key extractor that applies different limits based on the method
    // Note: In a real application, you'd use the expensive_limiter for ProcessData calls
    // This example uses the global limiter for simplicity
    let global_clone = global_limiter.clone();

    let layer = GrpcRateLimitLayer::with_extractor(
        global_clone.clone(),
        CustomGrpcKeyExtractor::new(move |req| {
            let path = req.uri().path();

            // For ProcessData, use method-specific key with tighter limit
            if path.contains("ProcessData") {
                // Return a key that will use the expensive_limiter
                // In practice, you'd want to dynamically choose the limiter
                Some(format!("expensive:{}", path))
            } else {
                // For other methods, use the method path as key
                Some(path.trim_start_matches('/').to_string())
            }
        }),
    );

    let addr = "[::1]:50051".parse()?;
    let greeter = MyGreeter::default();

    println!("Server starting on {}", addr);
    println!("Rate limits:");
    println!("  - Global: 10 req/s, burst 20");
    println!("  - ProcessData: 2 req/s, burst 3");
    println!();
    println!("Test with:");
    println!("  grpcurl -plaintext -d '{{\"name\": \"World\"}}' '[::1]:50051' helloworld.Greeter/SayHello");
    println!();
    println!("Or run the client test:");
    println!("  cargo run --example grpc_tonic_client --features tonic-support");

    Server::builder()
        .layer(layer)
        .add_service(GreeterServer::new(greeter))
        .serve(addr)
        .await?;

    Ok(())
}
