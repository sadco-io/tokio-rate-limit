//! # tokio-rate-limit
//!
//! High-performance rate limiting library with lock-free token accounting and sharded state management.
//!
//! ## Features
//!
//! - **Lock-free token accounting**: Atomic operations on token counts (17M+ ops/sec)
//! - **256-shard state management**: Lock-free concurrent hashmap (flurry) with micro-sharding
//! - **Pluggable algorithms**: Token bucket, leaky bucket, probabilistic sampling
//! - **Axum middleware**: Drop-in rate limiting for web apps
//! - **Zero allocations**: In the hot path
//!
//! ## Architecture
//!
//! This library uses a hybrid approach for maximum performance:
//! - **Token updates**: True lock-free atomic compare-and-swap operations
//! - **Key lookup**: 256-shard lock-free concurrent hashmap (flurry) for near-linear multi-threaded scaling
//!
//! This design provides excellent concurrency while maintaining per-key isolation.
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use tokio_rate_limit::{RateLimiter, RateLimiterConfig};
//!
//! #[tokio::main]
//! async fn main() {
//!     let limiter = RateLimiter::new(RateLimiterConfig {
//!         requests_per_second: 100,
//!         burst: 200,
//!     });
//!
//!     let decision = limiter.check("client-id").await.unwrap();
//!     if decision.permitted {
//!         // Process request
//!     } else {
//!         // Rate limit exceeded, retry after decision.retry_after
//!     }
//! }
//! ```

#![warn(missing_docs, rust_2024_compatibility)]
#![deny(unsafe_code)]

pub mod algorithm;
mod error;
mod limiter;

#[cfg(feature = "middleware")]
pub mod middleware;

#[cfg(feature = "tonic-support")]
pub mod tonic_middleware;

pub use error::{Error, Result};
pub use limiter::{RateLimitDecision, RateLimiter, RateLimiterBuilder, RateLimiterConfig};

/// Re-export of the rate limiter algorithm trait for custom implementations
pub use algorithm::Algorithm;
