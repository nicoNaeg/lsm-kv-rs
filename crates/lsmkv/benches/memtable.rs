//! What each memtable costs, and what concurrent readers do to a writer.
//!
//! ```text
//! cargo bench --bench memtable
//! ```
//!
//! Both implementations run the same measurements. The `BTreeMap` is the
//! baseline; the skiplist was written against the last group here, where a
//! writer under a reader-preferring `RwLock` waits for a gap between readers
//! that a steady stream of them never leaves.
//!
//! Tables are rebuilt outside the timed sections. Criterion runs millions of
//! iterations, and a table that kept growing across them would measure memory
//! pressure rather than the structure. The skiplist makes that mandatory rather
//! than tidy: an overwrite there adds a version instead of replacing one, so a
//! run against a single table would exhaust its arena.

use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use lsmkv::memtable::{self, Kind, Memtable};

/// Entries the table already holds when a measurement starts.
const FILLED: usize = 100_000;
/// Inserts timed as one iteration.
const BATCH: usize = 10_000;
/// Batches one table takes before it is rebuilt, so the skiplist arena holds.
const BATCHES_PER_TABLE: usize = 8;
const VALUE_SIZE: usize = 100;

/// Room for the fill plus every batch a table takes, at the 96 bytes per entry
/// the skiplist sizes its arena from.
const BUDGET: usize = (FILLED + BATCH * BATCHES_PER_TABLE) * 96;

const KINDS: [(&str, Kind); 2] = [("btree", Kind::BTree), ("skiplist", Kind::Skiplist)];

fn key(i: usize) -> [u8; 16] {
    let mut key = [0u8; 16];
    key[..8].copy_from_slice(b"user:000");
    key[8..].copy_from_slice(&(i as u64).to_be_bytes());
    key
}

fn empty(kind: Kind) -> Arc<dyn Memtable> {
    memtable::build(kind, BUDGET)
}

fn filled(kind: Kind) -> Arc<dyn Memtable> {
    let table = empty(kind);
    let value = vec![b'v'; VALUE_SIZE];
    for i in 0..FILLED {
        table.insert(&key(i), i as u64 + 1, Some(value.clone()));
    }
    table
}

/// Writes `BATCH` keys that no earlier batch wrote, so both implementations
/// grow by the same amount whatever they do with an overwrite.
fn insert_batch(table: &dyn Memtable, batch: usize, value: &[u8]) {
    let first = FILLED + batch * BATCH;
    for i in first..first + BATCH {
        table.insert(&key(i), i as u64 + 1, Some(value.to_vec()));
    }
}

fn insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("memtable/insert");
    group.throughput(Throughput::Elements(BATCH as u64));
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(3));
    let value = vec![b'v'; VALUE_SIZE];

    for (name, kind) in KINDS {
        group.bench_function(format!("{name}, into an empty table"), |b| {
            b.iter_batched_ref(
                || empty(kind),
                |table| insert_batch(table.as_ref(), 0, &value),
                BatchSize::PerIteration,
            );
        });

        group.bench_function(format!("{name}, into a table of 100k"), |b| {
            b.iter_batched_ref(
                || filled(kind),
                |table| insert_batch(table.as_ref(), 0, &value),
                BatchSize::PerIteration,
            );
        });
    }

    group.finish();
}

fn get(c: &mut Criterion) {
    let mut group = c.benchmark_group("memtable/get");

    for (name, kind) in KINDS {
        let table = filled(kind);

        group.bench_function(format!("{name}, hit"), |b| {
            let mut i = 0usize;
            b.iter(|| {
                let found = table.get(&key(i % FILLED));
                i += 1;
                black_box(found)
            });
        });

        group.bench_function(format!("{name}, miss"), |b| {
            let mut i = FILLED + BATCH * BATCHES_PER_TABLE;
            b.iter(|| {
                let found = table.get(&key(i));
                i += 1;
                black_box(found)
            });
        });
    }

    group.finish();
}

fn insert_under_readers(c: &mut Criterion) {
    let mut group = c.benchmark_group("memtable/insert under readers");
    group.throughput(Throughput::Elements(BATCH as u64));
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(3));
    let value = vec![b'v'; VALUE_SIZE];

    for (name, kind) in KINDS {
        for readers in [0usize, 1, 4, 8] {
            group.bench_function(format!("{name}, {readers} readers"), |b| {
                b.iter_custom(|iters| {
                    let mut elapsed = Duration::ZERO;
                    let mut left = iters;

                    while left > 0 {
                        // Neither the table nor the reader threads are timed,
                        // only the batches written while the readers run.
                        let table = filled(kind);
                        let stop = Arc::new(AtomicBool::new(false));
                        let mut threads = Vec::with_capacity(readers);
                        for reader in 0..readers {
                            let (table, stop) = (Arc::clone(&table), Arc::clone(&stop));
                            threads.push(thread::spawn(move || {
                                let mut i = reader;
                                while !stop.load(Ordering::Relaxed) {
                                    black_box(table.get(&key(i % FILLED)));
                                    i += 1;
                                }
                            }));
                        }

                        let batches = usize::try_from(left)
                            .unwrap_or(BATCHES_PER_TABLE)
                            .min(BATCHES_PER_TABLE);
                        let started = Instant::now();
                        for batch in 0..batches {
                            insert_batch(table.as_ref(), batch, &value);
                        }
                        elapsed += started.elapsed();

                        stop.store(true, Ordering::Relaxed);
                        for thread in threads {
                            thread.join().expect("join a reader");
                        }
                        left -= batches as u64;
                    }

                    elapsed
                });
            });
        }
    }

    group.finish();
}

criterion_group!(benches, insert, get, insert_under_readers);
criterion_main!(benches);
