//! Example of using tokio-rate-limit with Tonic gRPC services.
//!
//! This example demonstrates:
//! - Setting up a gRPC service with wire-compliant rate limiting (`RESOURCE_EXHAUSTED`)
//! - Weighted per-RPC token costs via `.with_cost(...)` for expensive methods
//! - Both unary and streaming RPCs with rate limiting
//! - Client making requests and handling rate limit errors
//!
//! Run the server:
//!   cargo run -p grpc-hello --bin grpc-hello
//!
//! In another terminal, run the client to test:
//!   cargo run -p grpc-hello --bin grpc-hello-client

use tokio_rate_limit::tonic_middleware::GrpcRateLimitLayer;
use tokio_rate_limit::RateLimiter;
use tonic::{transport::Server, Request, Response, Status};

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
        match request.remote_addr() {
            Some(addr) => println!("Got a SayHello request from {addr}"),
            None => println!("Got a SayHello request from unknown peer"),
        }

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
    // Per-method rate limiter: 10 tokens/s, burst of 20 tokens.
    let limiter = RateLimiter::builder()
        .requests_per_second(10)
        .burst(20)
        .build()?;

    // Charge 6 tokens per expensive `ProcessData` RPC (~1.6/s sustained, burst of 3)
    // and 1 token for lightweight RPCs (10/s sustained, burst of 20).
    let layer = GrpcRateLimitLayer::new(limiter).with_cost(|req| {
        if req.uri().path().ends_with("/ProcessData") {
            6
        } else {
            1
        }
    });

    let addr = "[::1]:50051".parse()?;
    let greeter = MyGreeter::default();

    println!("Server starting on {}", addr);
    println!("Rate limits (per RPC method):");
    println!("  - Standard RPCs (cost 1): 10 req/s, burst 20");
    println!("  - ProcessData   (cost 6): ~1.6 req/s, burst 3");
    println!();
    println!("Test with:");
    println!("  grpcurl -plaintext -d '{{\"name\": \"World\"}}' '[::1]:50051' helloworld.Greeter/SayHello");
    println!();
    println!("Or run the client test:");
    println!("  cargo run -p grpc-hello --bin grpc-hello-client");

    Server::builder()
        .layer(layer)
        .add_service(GreeterServer::new(greeter))
        .serve(addr)
        .await?;

    Ok(())
}
