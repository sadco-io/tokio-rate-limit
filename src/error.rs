//! Error types for the rate limiting library.

use thiserror::Error;

/// Error types that can occur during rate limiting operations.
#[derive(Debug, Error)]
pub enum Error {
    /// Rate limit has been exceeded for the given key.
    #[error("Rate limit exceeded")]
    RateLimitExceeded,

    /// Invalid configuration was provided.
    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    /// Configuration error.
    #[error("Configuration error: {0}")]
    Config(String),

    /// An internal error occurred.
    #[error("Internal error: {0}")]
    InternalError(String),
}

/// A specialized Result type for rate limiting operations.
pub type Result<T> = std::result::Result<T, Error>;
