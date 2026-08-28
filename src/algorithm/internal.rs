//! Shared internals for in-tree algorithms.
//!
//! Integer refill math, wait/reset helpers, HTTP delay rounding, and the
//! thread-local RNG used for cleanup sampling.

use crate::limiter::RateLimitDecision;
use std::time::Duration;

/// Sub-token scale used by every in-tree bucket.
pub(crate) const SCALE: u64 = 1000;

/// Nanoseconds in one second.
pub(crate) const NANOS_PER_SEC: u128 = 1_000_000_000;

thread_local! {
    static RNG_STATE: std::cell::Cell<u64> = std::cell::Cell::new(rng_seed());
}

fn rng_seed() -> u64 {
    let stack_probe = 0u8;
    let mut seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0x9e37_79b9_7f4a_7c15)
        ^ (std::ptr::addr_of!(stack_probe) as u64).rotate_left(32);
    seed = seed.wrapping_add(0x9e37_79b9_7f4a_7c15);
    seed = (seed ^ (seed >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    seed = (seed ^ (seed >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    seed ^ (seed >> 31)
}

/// Fast thread-local xorshift64.
#[inline]
pub(crate) fn fast_random() -> u64 {
    RNG_STATE.with(|state| {
        let mut x = state.get();
        if x == 0 {
            x = 1;
        }
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        state.set(x);
        x
    })
}

/// True on ~1% of calls. Used to amortize TTL walks.
#[inline]
pub(crate) fn should_cleanup() -> bool {
    fast_random() % 100 == 0
}

/// Scaled tokens accrued over `elapsed_nanos` at `rate_scaled` per second.
#[inline]
pub(crate) fn refill_tokens(elapsed_nanos: u64, rate_scaled: u64) -> u64 {
    if elapsed_nanos == 0 || rate_scaled == 0 {
        return 0;
    }
    let added = (u128::from(elapsed_nanos) * u128::from(rate_scaled)) / NANOS_PER_SEC;
    added.min(u128::from(u64::MAX)) as u64
}

/// Inverse of [`refill_tokens`]: nanos that `added` scaled tokens account for.
#[inline]
pub(crate) fn nanos_for_tokens(added: u64, rate_scaled: u64) -> u64 {
    if added == 0 || rate_scaled == 0 {
        return 0;
    }
    let nanos = (u128::from(added) * NANOS_PER_SEC) / u128::from(rate_scaled);
    nanos.min(u128::from(u64::MAX)) as u64
}

/// Wait until `tokens_needed` accrue at `rate_per_second`. Fractional, 1ms floor.
/// No `ceil` — callers that sleep on this value must not over-wait by a full
/// second at rates above 1 tok/s.
pub(crate) fn wait_for_tokens(tokens_needed: u64, rate_per_second: u64) -> Duration {
    if rate_per_second == 0 {
        return Duration::from_secs(1);
    }
    let seconds = tokens_needed as f64 / rate_per_second as f64;
    Duration::from_secs_f64(seconds.max(0.001))
}

/// Time until `missing` tokens refill. `None` if the bucket never refills.
pub(crate) fn time_until_full(missing: u64, rate_per_second: u64) -> Option<Duration> {
    if rate_per_second == 0 {
        return None;
    }
    if missing == 0 {
        return Some(Duration::from_secs(0));
    }
    Some(wait_for_tokens(missing, rate_per_second))
}

/// RFC 9110 delay-seconds / IETF RateLimit-Reset: integer seconds.
///
/// Positive sub-second waits round up to 1 so clients never see `0` and retry
/// immediately. A true zero duration stays 0 (bucket already full).
pub(crate) fn http_seconds_ceil(d: Duration) -> u64 {
    if d.is_zero() {
        0
    } else {
        d.as_secs_f64().ceil().max(1.0) as u64
    }
}

/// Build the standard in-tree decision (token-bucket shaped remaining).
pub(crate) fn token_decision(
    permitted: bool,
    remaining: u64,
    limit: u64,
    rate_per_second: u64,
    cost: u64,
) -> RateLimitDecision {
    let retry_after = if !permitted {
        Some(wait_for_tokens(
            cost.saturating_sub(remaining),
            rate_per_second,
        ))
    } else {
        None
    };
    RateLimitDecision {
        permitted,
        retry_after,
        remaining: Some(remaining),
        limit,
        reset: time_until_full(limit.saturating_sub(remaining), rate_per_second),
    }
}

/// Decision for `cost == 0` (documented as invalid). Does not consume.
pub(crate) fn zero_cost_decision(limit: u64) -> RateLimitDecision {
    RateLimitDecision {
        permitted: false,
        retry_after: None,
        remaining: None,
        limit,
        reset: None,
    }
}
