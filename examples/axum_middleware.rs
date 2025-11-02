//! Axum middleware example with IP-based rate limiting.
//!
//! This example demonstrates how to use the rate limiter as Axum middleware
//! to automatically rate limit incoming HTTP requests based on client IP.
//!
//! Run with:
//! ```bash
//! cargo run --example axum_middleware --features middleware
//! ```
//!
//! Then test with:
//! ```bash
//! # Send requests and see rate limiting in action
//! for i in {1..10}; do curl -v http://localhost:3000/api/data; done
//! ```

use axum::{routing::get, Json, Router};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio_rate_limit::{middleware::RateLimitLayer, RateLimiter};

#[derive(Debug, Serialize, Deserialize)]
struct ApiResponse {
    message: String,
    request_number: usize,
}

#[tokio::main]
async fn main() {
    // Create a rate limiter: 5 requests per second, burst of 10
    let limiter = Arc::new(
        RateLimiter::builder()
            .requests_per_second(5)
            .burst(10)
            .build()
            .expect("Failed to create rate limiter"),
    );

    println!("Starting Axum server with rate limiting...");
    println!("Rate limit: 5 requests/second, burst: 10");
    println!("Server listening on http://localhost:3000\n");
    println!("Test with: for i in {{1..15}}; do curl -v http://localhost:3000/api/data; done\n");

    // Build the application with routes and middleware
    let app: Router = Router::new()
        .route("/", get(index))
        .route("/api/data", get(api_handler))
        .route("/api/info", get(info_handler))
        // Apply rate limiting to all routes
        .layer(RateLimitLayer::new(limiter));

    // Start the server
    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000")
        .await
        .unwrap();

    println!("Ready to accept connections!");

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .await
    .unwrap();
}

async fn index() -> &'static str {
    "Welcome! Try accessing /api/data or /api/info"
}

// Counter for demonstration
static REQUEST_COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

async fn api_handler() -> Json<ApiResponse> {
    let count = REQUEST_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;

    Json(ApiResponse {
        message: "This is a rate-limited API endpoint".to_string(),
        request_number: count,
    })
}

async fn info_handler() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "service": "Rate Limited API",
        "rate_limit": {
            "requests_per_second": 5,
            "burst": 10,
        },
        "note": "Rate limits apply per IP address"
    }))
}
