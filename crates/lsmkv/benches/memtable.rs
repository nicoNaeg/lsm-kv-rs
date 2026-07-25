//! What the memtable costs, and what concurrent readers do to a writer.
//!
//! ```text
//! cargo bench --bench memtable
//! ```
//!
//! Inserts are measured in batches against a table that is rebuilt for every
//! iteration, outside the timed section: a table that kept growing across the
//! millions of iterations criterion runs would measure memory pressure instead
//! of the structure.
//!
//! The last group is the one stage 8 exists for. A `BTreeMap` behind an
//! `RwLock` blocks every reader for the length of an insert, so a writer and
//! its readers cannot overlap; a skiplist is measured against these numbers.

use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use lsmkv::{BTreeMemtable, Memtable};

/// Entries the table already holds when a measurement starts.
const FILLED: usize = 100_000;
/// Inserts timed as one iteration.
const BATCH: usize = 10_000;
const VALUE_SIZE: usize = 100;

fn key(i: usize) -> [u8; 16] {
    let mut key = [0u8; 16];
    key[..8].copy_from_slice(b"user:000");
    key[8..].copy_from_slice(&(i as u64).to_be_bytes());
    key
}

fn filled() -> BTreeMemtable {
    let table = BTreeMemtable::new();
    let value = vec![b'v'; VALUE_SIZE];
    for i in 0..FILLED {
        table.insert(&key(i), i as u64 + 1, Some(value.clone()));
    }
    table
}

fn insert_batch(table: &BTreeMemtable, value: &[u8]) {
    for i in FILLED..FILLED + BATCH {
        table.insert(&key(i), i as u64 + 1, Some(value.to_vec()));
    }
}

fn insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("memtable/insert");
    group.throughput(Throughput::Elements(BATCH as u64));
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(3));
    let value = vec![b'v'; VALUE_SIZE];

    group.bench_function("into an empty table", |b| {
        b.iter_batched_ref(
            BTreeMemtable::new,
            |table| insert_batch(table, &value),
            BatchSize::PerIteration,
        );
    });

    group.bench_function("into a table of 100k", |b| {
        b.iter_batched_ref(
            filled,
            |table| insert_batch(table, &value),
            BatchSize::PerIteration,
        );
    });

    group.finish();
}

fn get(c: &mut Criterion) {
    let mut group = c.benchmark_group("memtable/get");
    let table = filled();

    group.bench_function("hit", |b| {
        let mut i = 0usize;
        b.iter(|| {
            let found = table.get(&key(i % FILLED));
            i += 1;
            black_box(found)
        });
    });

    group.bench_function("miss", |b| {
        let mut i = FILLED;
        b.iter(|| {
            let found = table.get(&key(i));
            i += 1;
            black_box(found)
        });
    });

    group.finish();
}

fn insert_under_readers(c: &mut Criterion) {
    let mut group = c.benchmark_group("memtable/insert under readers");
    group.throughput(Throughput::Elements(BATCH as u64));
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(3));
    let value = vec![b'v'; VALUE_SIZE];

    for readers in [0usize, 1, 4, 8] {
        group.bench_function(format!("{readers} readers"), |b| {
            let stop = Arc::new(AtomicBool::new(false));
            let shared = Arc::new(filled());
            let mut threads = Vec::with_capacity(readers);

            for reader in 0..readers {
                let (table, stop) = (Arc::clone(&shared), Arc::clone(&stop));
                threads.push(thread::spawn(move || {
                    let mut i = reader;
                    while !stop.load(Ordering::Relaxed) {
                        black_box(table.get(&key(i % FILLED)));
                        i += 1;
                    }
                }));
            }

            // The readers hammer the table the writer is inserting into, so
            // this measures the contention and not two unrelated structures.
            // Every iteration writes the same batch of keys, which keeps the
            // table at a fixed size across the run: what varies between the
            // configurations below is the number of readers, nothing else.
            b.iter(|| insert_batch(&shared, &value));

            stop.store(true, Ordering::Relaxed);
            for thread in threads {
                thread.join().expect("join a reader");
            }
        });
    }

    group.finish();
}

criterion_group!(benches, insert, get, insert_under_readers);
criterion_main!(benches);
