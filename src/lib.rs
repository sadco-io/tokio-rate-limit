//! # tokio-rate-limit
//!
//! High-performance rate limiting library with lock-free GCRA token accounting and sharded state management.
//!
//! ## Features
//!
//! - **Lock-free token accounting**: Single-word atomic CAS on per-key Theoretical Arrival Time (GCRA)
//! - **256-shard state management**: Seeded `ahash` shard routing over lock-free concurrent hashmaps (`flurry`)
//! - **Generic & zero-alloc keys**: Rate limit by `String`/`&str`, `IpAddr`, `u64`, tuples, or custom key types
//! - **Pluggable algorithms**: Token bucket, leaky bucket, probabilistic sampling
//! - **Axum / Tonic middleware**: Drop-in HTTP and gRPC rate limiting with zero-allocation hot paths
//!
//! Absolute throughput is hardware-specific; compare against `governor` with
//! `benches/comparison.rs` on the machine you care about. For string keys, the
//! hot path only allocates on first sight of a key (the map insert).
//!
//! ## Architecture
//!
//! This library uses a hybrid approach for high throughput and predictable tail latency:
//! - **Quota updates**: Single-word lock-free atomic compare-and-swap (GCRA Theoretical Arrival Time)
//! - **Key lookup**: 256-shard lock-free concurrent hashmap (`flurry`) with per-instance seeded hashing
//! - **Bounded TTL eviction**: Incremental round-robin shard sweeps without full-map latency spikes
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
pub use limiter::{
    IntoRateLimiter, RateLimitDecision, RateLimiter, RateLimiterBuilder, RateLimiterConfig,
};

/// Re-export of the rate limiter algorithm and key traits.
pub use algorithm::{Algorithm, RateLimitKey, ToKey};
