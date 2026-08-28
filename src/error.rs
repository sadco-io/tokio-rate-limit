//! Error types for the rate limiting library.

use thiserror::Error;

/// Error types that can occur during rate limiting operations.
#[derive(Debug, Error)]
pub enum Error {
    /// Configuration error.
    #[error("Configuration error: {0}")]
    Config(String),
}

/// A specialized Result type for rate limiting operations.
pub type Result<T> = std::result::Result<T, Error>;
