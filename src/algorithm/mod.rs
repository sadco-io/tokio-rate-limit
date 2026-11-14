//! Rate limiting algorithms and key traits.

mod leaky_bucket;
mod probabilistic_token_bucket;
mod token_bucket;

pub(crate) mod internal;

pub use leaky_bucket::LeakyBucket;
pub use probabilistic_token_bucket::ProbabilisticTokenBucket;
pub use token_bucket::TokenBucket;

use crate::limiter::RateLimitDecision;
use std::borrow::Borrow;
use std::fmt;
use std::hash::Hash;
use std::sync::Arc;

/// Private module for the sealed trait pattern.
///
/// This prevents external implementations of the [`Algorithm`] trait while
/// maintaining flexibility for internal algorithm implementations.
mod private {
    pub trait Sealed {}
}

/// Marker trait for types that can be used as per-client rate limit keys.
///
/// Automatically implemented for any type that satisfies
/// `Clone + Ord + Hash + Debug + Send + Sync + 'static` (for example `String`,
/// [`std::net::IpAddr`], [`std::net::SocketAddr`], `u64`, `Arc<str>`, or custom
/// composite key structs and tuples).
pub trait RateLimitKey: Clone + Ord + Hash + fmt::Debug + Send + Sync + 'static {}

impl<T> RateLimitKey for T where T: Clone + Ord + Hash + fmt::Debug + Send + Sync + 'static {}

/// Converts a borrowed lookup key into an owned map key on first sight (cache miss).
///
/// This allows zero-allocation lookups on existing keys (such as `&str` against
/// a `String`-keyed limiter) while only allocating an owned key when inserting a
/// brand-new entry.
pub trait ToKey<K> {
    /// Produces an owned key `K` from `self`.
    fn to_key(&self) -> K;
}

impl<K: Clone> ToKey<K> for K {
    #[inline]
    fn to_key(&self) -> K {
        self.clone()
    }
}

impl ToKey<String> for str {
    #[inline]
    fn to_key(&self) -> String {
        self.to_owned()
    }
}

impl ToKey<Arc<str>> for str {
    #[inline]
    fn to_key(&self) -> Arc<str> {
        Arc::from(self)
    }
}

impl ToKey<Box<str>> for str {
    #[inline]
    fn to_key(&self) -> Box<str> {
        Box::from(self)
    }
}

impl<T: Clone> ToKey<Vec<T>> for [T] {
    #[inline]
    fn to_key(&self) -> Vec<T> {
        self.to_vec()
    }
}

/// Trait for rate limiting algorithms.
///
/// Implementations of this trait define how rate limiting decisions are made.
/// The trait is synchronous: every in-tree algorithm is a lock-free atomic and
/// concurrent-hashmap operation. [`RateLimiter::acquire`](crate::RateLimiter::acquire)
/// provides the async wrapper that sleeps on `retry_after`.
///
/// # Sealed Trait
///
/// This trait is sealed and cannot be implemented outside of this crate.
///
/// # Available Algorithms
///
/// - [`TokenBucket`] — bursts up to capacity, refills at a constant rate (default)
/// - [`LeakyBucket`] — leaky-bucket-as-a-meter traffic policer, drains at a steady rate
/// - [`ProbabilisticTokenBucket`] — samples 1 in `sample_rate` requests; soft limiting
pub trait Algorithm: fmt::Debug + Send + Sync + private::Sealed {
    /// Owned key type stored in per-key state maps (defaults to `String` on concrete algorithms).
    type Key: RateLimitKey;

    /// Checks if a single-token (`cost = 1`) request for `key` should be permitted.
    #[inline]
    fn check<Q>(&self, key: &Q) -> RateLimitDecision
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Ord + fmt::Debug + ToKey<Self::Key> + ?Sized,
    {
        self.check_with_cost(key, 1)
    }

    /// Checks if a request with the given `cost` should be permitted.
    ///
    /// - `cost == 0` is rejected without consuming quota (`retry_after: None`).
    /// - `cost > limit` (unfulfillable even by a full bucket) is rejected without
    ///   consuming quota and returns `retry_after: None`.
    fn check_with_cost<Q>(&self, key: &Q, cost: u64) -> RateLimitDecision
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Ord + fmt::Debug + ToKey<Self::Key> + ?Sized;

    /// Inspects the current rate limit state for `key` without consuming tokens
    /// or allocating an entry if `key` is not currently tracked.
    fn peek<Q>(&self, key: &Q) -> RateLimitDecision
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Ord + ?Sized;

    /// Removes the tracked state for `key`, restoring it to full quota.
    ///
    /// Returns `true` if an entry existed and was removed.
    fn reset<Q>(&self, key: &Q) -> bool
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Ord + ?Sized;

    /// Clears all tracked keys across all shards.
    fn clear(&self);

    /// Returns the approximate total number of keys currently tracked across all shards.
    fn len(&self) -> usize;

    /// Returns `true` if no keys are currently tracked.
    #[inline]
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Immediately sweeps all shards for expired idle entries (when an `idle_ttl` is set)
    /// and returns the number of evicted keys.
    fn cleanup(&self) -> usize;
}
