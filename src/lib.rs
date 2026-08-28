//! # tokio-rate-limit
//!
//! High-performance rate limiting library with lock-free token accounting and sharded state management.
//!
//! ## Features
//!
//! - **Lock-free token accounting**: Atomic CAS on per-key buckets
//! - **256-shard state management**: Lock-free concurrent hashmap (flurry)
//! - **Pluggable algorithms**: Token bucket, leaky bucket, probabilistic sampling
//! - **Axum / Tonic middleware**: Drop-in HTTP and gRPC rate limiting
//!
//! Absolute throughput is hardware-specific; compare against `governor` with
//! `benches/comparison.rs` on the machine you care about. The hot path still
//! allocates on first sight of a key (the map insert).
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
//!     })
//!     .unwrap();
//!
//!     let decision = limiter.check("client-id");
//!     if decision.permitted {
//!         // Process request
//!     } else {
//!         // Rate limit exceeded, retry after decision.retry_after
//!     }
//! }
//! ```

#![warn(missing_docs, rust_2024_compatibility)]
#![deny(unsafe_code)]
#![cfg_attr(docsrs, feature(doc_cfg))]

pub mod algorithm;
mod error;
mod limiter;

#[cfg(feature = "middleware")]
#[cfg_attr(docsrs, doc(cfg(feature = "middleware")))]
pub mod middleware;

#[cfg(feature = "tonic-support")]
#[cfg_attr(docsrs, doc(cfg(feature = "tonic-support")))]
pub mod tonic_middleware;

pub use error::{Error, Result};
pub use limiter::{RateLimitDecision, RateLimiter, RateLimiterBuilder, RateLimiterConfig};

/// Re-export of the rate limiter algorithm trait for custom implementations
pub use algorithm::Algorithm;
