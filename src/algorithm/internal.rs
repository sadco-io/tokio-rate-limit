//! Shared internals for in-tree algorithms and middleware.
//!
//! Includes GCRA Theoretical Arrival Time (TAT) state, 256-shard concurrent map
//! with per-instance seeded `ahash` routing and bounded round-robin TTL eviction,
//! integer refill helpers, HTTP delay rounding, and stack IP formatting.

use crate::algorithm::{RateLimitKey, ToKey};
use crate::limiter::RateLimitDecision;
use ahash::RandomState as AHashRandomState;
use flurry::HashMap as FlurryHashMap;
use std::borrow::Borrow;
use std::collections::hash_map::RandomState as StdRandomState;
#[cfg(any(feature = "middleware", feature = "tonic-support"))]
use std::fmt;
use std::hash::{BuildHasher, Hash};
#[cfg(any(feature = "middleware", feature = "tonic-support"))]
use std::net::IpAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Sub-token scale used by [`ProbabilisticTokenBucket`](super::ProbabilisticTokenBucket).
pub(crate) const SCALE: u64 = 1000;

/// Nanoseconds in one second.
pub(crate) const NANOS_PER_SEC: u128 = 1_000_000_000;

/// Number of shards per concurrent map (power of 2; top 8 bits of 64-bit hash).
pub(crate) const NUM_SHARDS: usize = 256;

/// Number of shards swept per incremental background cleanup pass.
const CLEANUP_BATCH_SHARDS: usize = 16;

/// Maximum burst capacity for GCRA TAT buckets (`u64::MAX / 2`).
pub(crate) const MAX_TAT_BURST: u64 = u64::MAX / 2;

/// Maximum rate per second for GCRA TAT buckets (1 token per nanosecond = 1B req/s).
pub(crate) const MAX_TAT_RATE_PER_SEC: u64 = 1_000_000_000;

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

/// Saturating conversion from `Duration` to `u64` nanoseconds (never wraps `u128` via `as u64`).
#[inline]
pub(crate) fn duration_to_nanos_sat(d: Duration) -> u64 {
    u64::try_from(d.as_nanos()).unwrap_or(u64::MAX)
}

/// Build an OS-seeded `ahash::RandomState` without requiring `getrandom`.
fn seeded_ahash() -> AHashRandomState {
    let s0 = StdRandomState::new();
    let s1 = StdRandomState::new();
    AHashRandomState::with_seeds(
        s0.hash_one(0u64),
        s0.hash_one(1u64),
        s1.hash_one(2u64),
        s1.hash_one(3u64),
    )
}

/// 256-shard concurrent map backed by `flurry::HashMap` with OS-seeded `ahash`
/// shard routing and non-blocking round-robin TTL eviction.
pub(crate) struct ShardedMap<K, V> {
    shards: Box<[FlurryHashMap<K, Arc<V>>; NUM_SHARDS]>,
    hasher: AHashRandomState,
    cleanup_cursor: AtomicUsize,
    cleanup_lock: AtomicBool,
}

impl<K: RateLimitKey, V: Send + Sync + 'static> ShardedMap<K, V> {
    pub(crate) fn new() -> Self {
        let shards: Vec<FlurryHashMap<K, Arc<V>>> =
            (0..NUM_SHARDS).map(|_| FlurryHashMap::new()).collect();
        let shards: Box<[FlurryHashMap<K, Arc<V>>; NUM_SHARDS]> = shards
            .into_boxed_slice()
            .try_into()
            .unwrap_or_else(|_| unreachable!("exact NUM_SHARDS elements"));

        Self {
            shards,
            hasher: seeded_ahash(),
            cleanup_cursor: AtomicUsize::new(0),
            cleanup_lock: AtomicBool::new(false),
        }
    }

    #[inline]
    pub(crate) fn shard_for<Q: Hash + ?Sized>(&self, key: &Q) -> &FlurryHashMap<K, Arc<V>> {
        let hash = self.hasher.hash_one(key);
        let idx = (hash >> 56) as usize;
        &self.shards[idx]
    }

    /// Look up `key` or insert a fresh state constructed with `init`, then run `f`
    /// while holding the shard epoch guard (zero `Arc` refcount traffic on hits).
    #[inline]
    pub(crate) fn with_or_insert<Q, Init, F, R>(&self, key: &Q, init: Init, f: F) -> R
    where
        K: Borrow<Q>,
        Q: Hash + Ord + ToKey<K> + ?Sized,
        Init: FnOnce() -> V,
        F: FnOnce(&V) -> R,
    {
        let shard = self.shard_for(key);
        let guard = shard.guard();
        let state: &V = match shard.get(key, &guard) {
            Some(existing) => existing,
            None => {
                let new_state = Arc::new(init());
                match shard.try_insert(key.to_key(), new_state, &guard) {
                    Ok(inserted) => inserted,
                    Err(not_inserted) => not_inserted.current,
                }
            }
        };
        f(state)
    }

    /// Read-only lookup without inserting if absent.
    #[inline]
    pub(crate) fn with_existing<Q, F, R>(&self, key: &Q, f: F) -> Option<R>
    where
        K: Borrow<Q>,
        Q: Hash + Ord + ?Sized,
        F: FnOnce(&V) -> R,
    {
        let shard = self.shard_for(key);
        let guard = shard.guard();
        shard.get(key, &guard).map(|v| f(v))
    }

    /// Remove a single key if present.
    pub(crate) fn remove<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Ord + ?Sized,
    {
        let shard = self.shard_for(key);
        let guard = shard.guard();
        shard.remove(key, &guard).is_some()
    }

    /// Clear all shards.
    pub(crate) fn clear(&self) {
        for shard in self.shards.iter() {
            let guard = shard.guard();
            shard.clear(&guard);
        }
    }

    /// Approximate total entry count across all shards.
    pub(crate) fn len(&self) -> usize {
        self.shards.iter().map(|s| s.len()).sum()
    }

    /// Amortized, non-blocking bounded shard sweep (~1 shard/call average, 16 shards
    /// on 1-in-16 calls, skipping immediately if another thread is already sweeping).
    #[inline]
    pub(crate) fn maybe_cleanup<P>(&self, sample_period_mask: u64, should_evict: P)
    where
        P: Fn(&V) -> bool,
    {
        if (fast_random() & sample_period_mask) != 0 {
            return;
        }
        if self
            .cleanup_lock
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            return;
        }
        let start = self
            .cleanup_cursor
            .fetch_add(CLEANUP_BATCH_SHARDS, Ordering::Relaxed);
        for offset in 0..CLEANUP_BATCH_SHARDS {
            let idx = (start.wrapping_add(offset)) & (NUM_SHARDS - 1);
            Self::sweep_shard(&self.shards[idx], &should_evict);
        }
        self.cleanup_lock.store(false, Ordering::Release);
    }

    /// Deterministic sweep of all 256 shards, returning the number of removed keys.
    pub(crate) fn cleanup_all<P>(&self, should_evict: P) -> usize
    where
        P: Fn(&V) -> bool,
    {
        let mut removed = 0;
        for shard in self.shards.iter() {
            removed += Self::sweep_shard(shard, &should_evict);
        }
        removed
    }

    fn sweep_shard<P>(shard: &FlurryHashMap<K, Arc<V>>, should_evict: &P) -> usize
    where
        P: Fn(&V) -> bool,
    {
        if shard.is_empty() {
            return 0;
        }
        let guard = shard.guard();
        let keys_to_remove: Vec<K> = shard
            .iter(&guard)
            .filter_map(|(key, state)| {
                if should_evict(state) {
                    Some(key.clone())
                } else {
                    None
                }
            })
            .collect();

        let mut removed = 0;
        for key in keys_to_remove {
            if shard.remove(&key, &guard).is_some() {
                removed += 1;
            }
        }
        removed
    }
}

/// Parameters for a GCRA Theoretical Arrival Time (TAT) bucket.
#[derive(Debug, Clone, Copy)]
pub(crate) struct TatParams {
    /// Maximum burst / bucket capacity in whole tokens.
    pub(crate) capacity: u64,
    /// Nanoseconds required to replenish/leak 1 token (`0` means quota never replenishes).
    pub(crate) emission_interval_nanos: u64,
    /// Total burst window in nanoseconds (`capacity * emission_interval_nanos`).
    pub(crate) tau_nanos: u128,
}

impl TatParams {
    pub(crate) fn from_rate_per_sec(capacity: u64, rate_per_second: u64) -> (Self, u64, u64) {
        let safe_capacity = capacity.min(MAX_TAT_BURST);
        let safe_rate = rate_per_second.min(MAX_TAT_RATE_PER_SEC);
        let emission_interval_nanos = if safe_rate == 0 {
            0
        } else {
            ((NANOS_PER_SEC + u128::from(safe_rate) / 2) / u128::from(safe_rate)).max(1) as u64
        };
        let tau_nanos = u128::from(safe_capacity) * u128::from(emission_interval_nanos);
        (
            Self {
                capacity: safe_capacity,
                emission_interval_nanos,
                tau_nanos,
            },
            safe_capacity,
            safe_rate,
        )
    }

    pub(crate) fn from_emission_interval(capacity: u64, replenish_interval: Duration) -> Self {
        let safe_capacity = capacity.min(MAX_TAT_BURST);
        let emission_interval_nanos = duration_to_nanos_sat(replenish_interval).max(1);
        let tau_nanos = u128::from(safe_capacity) * u128::from(emission_interval_nanos);
        Self {
            capacity: safe_capacity,
            emission_interval_nanos,
            tau_nanos,
        }
    }

    /// Decision for an untouched bucket (not yet present in the map).
    #[inline]
    pub(crate) fn untouched_decision(&self) -> RateLimitDecision {
        let permitted = self.capacity >= 1;
        RateLimitDecision {
            permitted,
            retry_after: None,
            remaining: Some(self.capacity),
            limit: self.capacity,
            reset: if self.emission_interval_nanos == 0 {
                None
            } else {
                Some(Duration::ZERO)
            },
        }
    }

    /// Decision when `cost > capacity` (unfulfillable request).
    #[inline]
    pub(crate) fn unfulfillable_decision(
        &self,
        existing_tat: Option<u64>,
        now_nanos: u64,
    ) -> RateLimitDecision {
        if self.emission_interval_nanos == 0 {
            let consumed = existing_tat.unwrap_or(0);
            return RateLimitDecision {
                permitted: false,
                retry_after: None,
                remaining: Some(self.capacity.saturating_sub(consumed)),
                limit: self.capacity,
                reset: None,
            };
        }

        let current_tat = existing_tat.unwrap_or(0);
        let allow_at_most = u128::from(now_nanos) + self.tau_nanos;
        let t = u128::from(self.emission_interval_nanos);
        let remaining = ((allow_at_most.saturating_sub(u128::from(current_tat))) / t)
            .min(u128::from(self.capacity)) as u64;
        let reset = Some(Duration::from_nanos(current_tat.saturating_sub(now_nanos)));

        RateLimitDecision {
            permitted: false,
            retry_after: None,
            remaining: Some(remaining),
            limit: self.capacity,
            reset,
        }
    }
}

/// Single-atomic GCRA Theoretical Arrival Time (TAT) per-key state.
///
/// Replaces separate `tokens` + `last_refill_nanos` atomics with a single
/// `tat_nanos: AtomicU64`, eliminating refill/debit split races and cutting
/// per-request atomic RMW operations to a single CAS.
pub(crate) struct AtomicTatState {
    /// Theoretical Arrival Time in nanoseconds (or consumed count if `emission_interval == 0`).
    tat_nanos: AtomicU64,
    /// Last access timestamp in nanoseconds (written only when `idle_ttl` is configured).
    last_access_nanos: AtomicU64,
}

impl AtomicTatState {
    #[inline]
    pub(crate) fn new(now_nanos: u64) -> Self {
        Self {
            tat_nanos: AtomicU64::new(0),
            last_access_nanos: AtomicU64::new(now_nanos),
        }
    }

    #[inline]
    pub(crate) fn raw_tat(&self) -> u64 {
        self.tat_nanos.load(Ordering::Relaxed)
    }

    /// Returns `true` only when the key has been idle for at least `ttl_nanos`
    /// AND its quota has completely recovered (`now_nanos >= tat_nanos`),
    /// preventing premature eviction from resetting an exhausted client's limit.
    #[inline]
    pub(crate) fn is_evictable(
        &self,
        now_nanos: u64,
        ttl_nanos: u64,
        emission_interval_nanos: u64,
    ) -> bool {
        let last_access = self.last_access_nanos.load(Ordering::Relaxed);
        if now_nanos.saturating_sub(last_access) < ttl_nanos {
            return false;
        }
        if emission_interval_nanos == 0 {
            return self.tat_nanos.load(Ordering::Relaxed) == 0;
        }
        now_nanos >= self.tat_nanos.load(Ordering::Relaxed)
    }

    /// Read-only inspection of current state without consuming tokens or updating `last_access`.
    pub(crate) fn peek(&self, params: &TatParams, now_nanos: u64) -> RateLimitDecision {
        if params.emission_interval_nanos == 0 {
            let consumed = self.tat_nanos.load(Ordering::Relaxed);
            let remaining = params.capacity.saturating_sub(consumed);
            return RateLimitDecision {
                permitted: remaining >= 1,
                retry_after: None,
                remaining: Some(remaining),
                limit: params.capacity,
                reset: None,
            };
        }

        let current_tat = self.tat_nanos.load(Ordering::Relaxed);
        let t = u128::from(params.emission_interval_nanos);
        let allow_at_most = u128::from(now_nanos) + params.tau_nanos;
        let remaining = ((allow_at_most.saturating_sub(u128::from(current_tat))) / t)
            .min(u128::from(params.capacity)) as u64;
        let base = u128::from(current_tat.max(now_nanos));
        let next_tat = base + t;
        let permitted = params.capacity >= 1 && next_tat <= allow_at_most;
        let retry_after = if permitted || params.capacity == 0 {
            None
        } else {
            let wait_nanos = (next_tat - allow_at_most).min(u128::from(u64::MAX)) as u64;
            Some(Duration::from_nanos(wait_nanos.max(1)))
        };
        let reset = Some(Duration::from_nanos(current_tat.saturating_sub(now_nanos)));

        RateLimitDecision {
            permitted,
            retry_after,
            remaining: Some(remaining),
            limit: params.capacity,
            reset,
        }
    }

    /// Attempt to consume `cost` tokens (caller guarantees `1 <= cost <= params.capacity`).
    #[inline]
    pub(crate) fn try_consume(
        &self,
        params: &TatParams,
        now_nanos: u64,
        cost: u64,
        track_access: bool,
    ) -> RateLimitDecision {
        if track_access {
            self.last_access_nanos.store(now_nanos, Ordering::Relaxed);
        }

        if params.emission_interval_nanos == 0 {
            loop {
                let consumed = self.tat_nanos.load(Ordering::Relaxed);
                let next = consumed.saturating_add(cost);
                if next > params.capacity {
                    return RateLimitDecision {
                        permitted: false,
                        retry_after: None,
                        remaining: Some(params.capacity.saturating_sub(consumed)),
                        limit: params.capacity,
                        reset: None,
                    };
                }
                if self
                    .tat_nanos
                    .compare_exchange_weak(consumed, next, Ordering::AcqRel, Ordering::Relaxed)
                    .is_ok()
                {
                    return RateLimitDecision {
                        permitted: true,
                        retry_after: None,
                        remaining: Some(params.capacity - next),
                        limit: params.capacity,
                        reset: None,
                    };
                }
            }
        }

        let t = u128::from(params.emission_interval_nanos);
        let increment = u128::from(cost) * t;
        let allow_at_most = u128::from(now_nanos) + params.tau_nanos;

        loop {
            let current_tat = self.tat_nanos.load(Ordering::Relaxed);
            let base = u128::from(current_tat.max(now_nanos));
            let new_tat = base + increment;

            if new_tat > allow_at_most {
                let wait_nanos = (new_tat - allow_at_most).min(u128::from(u64::MAX)) as u64;
                let remaining = ((allow_at_most.saturating_sub(u128::from(current_tat))) / t)
                    .min(u128::from(params.capacity)) as u64;
                let reset = Some(Duration::from_nanos(current_tat.saturating_sub(now_nanos)));

                return RateLimitDecision {
                    permitted: false,
                    retry_after: Some(Duration::from_nanos(wait_nanos.max(1))),
                    remaining: Some(remaining),
                    limit: params.capacity,
                    reset,
                };
            }

            let new_tat_u64 = new_tat.min(u128::from(u64::MAX)) as u64;
            if self
                .tat_nanos
                .compare_exchange_weak(
                    current_tat,
                    new_tat_u64,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                let remaining =
                    ((allow_at_most - new_tat) / t).min(u128::from(params.capacity)) as u64;
                let reset = Some(Duration::from_nanos(new_tat_u64.saturating_sub(now_nanos)));

                return RateLimitDecision {
                    permitted: true,
                    retry_after: None,
                    remaining: Some(remaining),
                    limit: params.capacity,
                    reset,
                };
            }
        }
    }
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
#[cfg(any(feature = "middleware", feature = "tonic-support"))]
pub(crate) fn http_seconds_ceil(d: Duration) -> u64 {
    if d.is_zero() {
        0
    } else {
        d.as_secs_f64().ceil().max(1.0) as u64
    }
}

/// Build the standard decision for [`ProbabilisticTokenBucket`](super::ProbabilisticTokenBucket).
///
/// When `cost > limit`, `retry_after` is `None` because the request can never succeed.
pub(crate) fn token_decision(
    permitted: bool,
    remaining: u64,
    limit: u64,
    rate_per_second: u64,
    cost: u64,
) -> RateLimitDecision {
    let retry_after = if !permitted && cost <= limit && rate_per_second > 0 {
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
#[inline]
pub(crate) fn zero_cost_decision(limit: u64) -> RateLimitDecision {
    RateLimitDecision {
        permitted: false,
        retry_after: None,
        remaining: None,
        limit,
        reset: None,
    }
}

/// Stack-allocated textual representation of an [`IpAddr`] (at most 45 ASCII bytes),
/// avoiding heap allocation in HTTP and gRPC IP key extractors.
#[cfg(any(feature = "middleware", feature = "tonic-support"))]
#[derive(Clone, Copy)]
pub struct StackIp {
    buf: [u8; 45],
    len: u8,
}

#[cfg(any(feature = "middleware", feature = "tonic-support"))]
impl StackIp {
    /// Formats `ip` onto the stack without allocating on the heap.
    #[inline]
    pub fn from_ip(ip: IpAddr) -> Self {
        struct StackWriter<'a> {
            buf: &'a mut [u8],
            pos: usize,
        }
        impl fmt::Write for StackWriter<'_> {
            #[inline]
            fn write_str(&mut self, s: &str) -> fmt::Result {
                let bytes = s.as_bytes();
                let end = self.pos + bytes.len();
                if end > self.buf.len() {
                    return Err(fmt::Error);
                }
                self.buf[self.pos..end].copy_from_slice(bytes);
                self.pos = end;
                Ok(())
            }
        }

        let mut buf = [0u8; 45];
        let mut writer = StackWriter {
            buf: &mut buf,
            pos: 0,
        };
        let _ = fmt::Write::write_fmt(&mut writer, format_args!("{ip}"));
        let len = writer.pos as u8;
        Self { buf, len }
    }

    /// Returns the formatted IP address as a `&str`.
    #[inline]
    pub fn as_str(&self) -> &str {
        std::str::from_utf8(&self.buf[..usize::from(self.len)]).unwrap_or("")
    }
}

#[cfg(any(feature = "middleware", feature = "tonic-support"))]
impl fmt::Debug for StackIp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self.as_str(), f)
    }
}

#[cfg(any(feature = "middleware", feature = "tonic-support"))]
impl fmt::Display for StackIp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self.as_str(), f)
    }
}

#[cfg(any(feature = "middleware", feature = "tonic-support"))]
impl PartialEq for StackIp {
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}

#[cfg(any(feature = "middleware", feature = "tonic-support"))]
impl Eq for StackIp {}

#[cfg(any(feature = "middleware", feature = "tonic-support"))]
impl Hash for StackIp {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.as_str().hash(state);
    }
}
