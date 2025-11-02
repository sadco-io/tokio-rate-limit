//! False sharing investigation
//!
//! Tests if adding cache line padding improves multi-threaded performance

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;

/// Original unpadded version (24 bytes, fits in one cache line)
#[repr(C)]
struct UnpaddedState {
    tokens: AtomicU64,            // 8 bytes
    last_refill_nanos: AtomicU64, // 8 bytes
    last_access_nanos: AtomicU64, // 8 bytes
}

/// Padded version - each field on its own cache line
#[repr(C, align(64))]
struct PaddedState {
    tokens: AtomicU64,
    _pad1: [u8; 56], // Pad to 64 bytes
    last_refill_nanos: AtomicU64,
    _pad2: [u8; 56],
    last_access_nanos: AtomicU64,
    _pad3: [u8; 56],
}

impl UnpaddedState {
    fn new() -> Self {
        Self {
            tokens: AtomicU64::new(1_000_000),
            last_refill_nanos: AtomicU64::new(0),
            last_access_nanos: AtomicU64::new(0),
        }
    }

    fn update(&self) {
        // Simulate token consumption
        loop {
            let current = self.tokens.load(Ordering::Relaxed);
            if current > 0 {
                if self
                    .tokens
                    .compare_exchange(current, current - 1, Ordering::AcqRel, Ordering::Relaxed)
                    .is_ok()
                {
                    self.last_access_nanos.store(current, Ordering::Relaxed);
                    break;
                }
            } else {
                break;
            }
        }
    }
}

impl PaddedState {
    fn new() -> Self {
        Self {
            tokens: AtomicU64::new(1_000_000),
            _pad1: [0; 56],
            last_refill_nanos: AtomicU64::new(0),
            _pad2: [0; 56],
            last_access_nanos: AtomicU64::new(0),
            _pad3: [0; 56],
        }
    }

    fn update(&self) {
        // Simulate token consumption
        loop {
            let current = self.tokens.load(Ordering::Relaxed);
            if current > 0 {
                if self
                    .tokens
                    .compare_exchange(current, current - 1, Ordering::AcqRel, Ordering::Relaxed)
                    .is_ok()
                {
                    self.last_access_nanos.store(current, Ordering::Relaxed);
                    break;
                }
            } else {
                break;
            }
        }
    }
}

/// Benchmark unpadded version
fn bench_unpadded(c: &mut Criterion) {
    let mut group = c.benchmark_group("false_sharing/unpadded");

    for num_threads in [1, 2, 4, 8, 16] {
        group.throughput(Throughput::Elements(1));

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}_threads", num_threads)),
            &num_threads,
            |b, &threads| {
                let state = Arc::new(UnpaddedState::new());

                b.iter_custom(|iters| {
                    let barrier = Arc::new(std::sync::Barrier::new(threads));
                    let mut handles = vec![];

                    for _ in 0..threads {
                        let state = Arc::clone(&state);
                        let barrier = Arc::clone(&barrier);

                        let handle = thread::spawn(move || {
                            barrier.wait();
                            let start = std::time::Instant::now();

                            for _ in 0..iters {
                                state.update();
                                black_box(&state);
                            }

                            start.elapsed()
                        });

                        handles.push(handle);
                    }

                    handles
                        .into_iter()
                        .map(|h| h.join().unwrap())
                        .max()
                        .unwrap()
                });
            },
        );
    }

    group.finish();
}

/// Benchmark padded version
fn bench_padded(c: &mut Criterion) {
    let mut group = c.benchmark_group("false_sharing/padded");

    for num_threads in [1, 2, 4, 8, 16] {
        group.throughput(Throughput::Elements(1));

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}_threads", num_threads)),
            &num_threads,
            |b, &threads| {
                let state = Arc::new(PaddedState::new());

                b.iter_custom(|iters| {
                    let barrier = Arc::new(std::sync::Barrier::new(threads));
                    let mut handles = vec![];

                    for _ in 0..threads {
                        let state = Arc::clone(&state);
                        let barrier = Arc::clone(&barrier);

                        let handle = thread::spawn(move || {
                            barrier.wait();
                            let start = std::time::Instant::now();

                            for _ in 0..iters {
                                state.update();
                                black_box(&state);
                            }

                            start.elapsed()
                        });

                        handles.push(handle);
                    }

                    handles
                        .into_iter()
                        .map(|h| h.join().unwrap())
                        .max()
                        .unwrap()
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_unpadded, bench_padded);
criterion_main!(benches);
