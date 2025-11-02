//! Custom key extraction example with header-based rate limiting.
//!
//! This example demonstrates how to use custom key extraction to rate limit
//! based on user IDs, API keys, or other custom headers instead of IP addresses.
//!
//! Run with:
//! ```bash
//! cargo run --example custom_key_extraction --features middleware
//! ```
//!
//! Then test with:
//! ```bash
//! # User 1 gets rate limited
//! for i in {1..5}; do curl -H "X-User-Id: user-1" http://localhost:3000/api/data; done
//!
//! # User 2 has independent limit
//! for i in {1..5}; do curl -H "X-User-Id: user-2" http://localhost:3000/api/data; done
//!
//! # API key based rate limiting
//! curl -H "X-API-Key: key-123" http://localhost:3000/api/secure
//! ```

use axum::{
    body::Body, extract::Request, http::StatusCode, middleware as axum_middleware,
    response::Response, routing::get, Json, Router,
};
use std::sync::Arc;
use tokio_rate_limit::{
    middleware::{CustomKeyExtractor, RateLimitLayer},
    RateLimiter,
};

#[tokio::main]
async fn main() {
    println!("Starting Axum server with custom key extraction...\n");

    // Create two rate limiters with different limits
    let user_limiter = Arc::new(
        RateLimiter::builder()
            .requests_per_second(3)
            .burst(5)
            .build()
            .expect("Failed to create user limiter"),
    );

    let api_key_limiter = Arc::new(
        RateLimiter::builder()
            .requests_per_second(10)
            .burst(20)
            .build()
            .expect("Failed to create API key limiter"),
    );

    // Router with user-based rate limiting (using X-User-Id header)
    let user_routes = Router::new()
        .route("/api/data", get(user_data_handler))
        .route("/api/profile", get(profile_handler))
        .layer(RateLimitLayer::with_extractor(
            user_limiter,
            CustomKeyExtractor::new(|req: &Request<Body>| {
                // Extract user ID from header
                req.headers()
                    .get("X-User-Id")
                    .and_then(|v| v.to_str().ok())
                    .map(|s| format!("user:{}", s))
            }),
        ));

    // Router with API key-based rate limiting
    let api_routes = Router::new()
        .route("/api/secure", get(secure_handler))
        .layer(RateLimitLayer::with_extractor(
            api_key_limiter,
            CustomKeyExtractor::new(|req: &Request<Body>| {
                // Extract API key from header
                req.headers()
                    .get("X-API-Key")
                    .and_then(|v| v.to_str().ok())
                    .map(|s| format!("apikey:{}", s))
            }),
        ));

    // Combine routes
    let app: Router = Router::new()
        .route("/", get(index))
        .merge(user_routes)
        .merge(api_routes)
        .layer(axum_middleware::from_fn(logging_middleware));

    println!("Configuration:");
    println!("  User endpoints (/api/data, /api/profile): 3 req/s, burst 5");
    println!("  API key endpoints (/api/secure): 10 req/s, burst 20\n");
    println!("Server listening on http://localhost:3000\n");
    println!("Test commands:");
    println!("  # Rate limit by user ID:");
    println!(
        "  for i in {{1..8}}; do curl -H 'X-User-Id: alice' http://localhost:3000/api/data; done\n"
    );
    println!("  # Rate limit by API key:");
    println!("  for i in {{1..15}}; do curl -H 'X-API-Key: key-123' http://localhost:3000/api/secure; done\n");

    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000")
        .await
        .unwrap();

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .await
    .unwrap();
}

async fn index() -> &'static str {
    r#"
Rate Limiting Examples:

1. User-based rate limiting (3 req/s):
   curl -H "X-User-Id: alice" http://localhost:3000/api/data
   curl -H "X-User-Id: bob" http://localhost:3000/api/profile

2. API key-based rate limiting (10 req/s):
   curl -H "X-API-Key: key-123" http://localhost:3000/api/secure

Each user ID and API key has independent rate limits.
"#
}

async fn user_data_handler() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "message": "User data endpoint",
        "rate_limit": "3 requests/second per user"
    }))
}

async fn profile_handler() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "message": "User profile endpoint",
        "rate_limit": "3 requests/second per user"
    }))
}

async fn secure_handler() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "message": "Secure API endpoint",
        "rate_limit": "10 requests/second per API key"
    }))
}

// Simple logging middleware to show request details
async fn logging_middleware(req: Request, next: axum_middleware::Next) -> Response {
    let method = req.method().clone();
    let uri = req.uri().clone();
    let user_id = req
        .headers()
        .get("X-User-Id")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_else(|| "-".to_string());
    let api_key = req
        .headers()
        .get("X-API-Key")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_else(|| "-".to_string());

    let response = next.run(req).await;

    let status = response.status();
    let rate_limit_remaining = response
        .headers()
        .get("X-RateLimit-Remaining")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("-");

    let status_symbol = if status == StatusCode::TOO_MANY_REQUESTS {
        "✗"
    } else {
        "✓"
    };

    println!(
        "{} {} {} [user: {}, key: {}, remaining: {}]",
        status_symbol, method, uri, user_id, api_key, rate_limit_remaining
    );

    response
}
