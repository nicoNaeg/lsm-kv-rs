//! What a Bloom filter probe costs, and how little the probe count changes it.
//!
//! ```text
//! cargo bench --bench bloom
//! ```
//!
//! Double hashing derives every probe from one hash of the key, so raising the
//! probe count should cost memory reads and not hashing. These numbers are what
//! says whether that held.

use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use lsmkv::Bloom;
use lsmkv::bloom::hash;

const KEYS: usize = 100_000;

fn key(i: usize) -> [u8; 16] {
    let mut key = [0u8; 16];
    key[..8].copy_from_slice(b"user:000");
    key[8..].copy_from_slice(&(i as u64).to_be_bytes());
    key
}

fn hashes() -> Vec<u64> {
    (0..KEYS).map(|i| hash(&key(i))).collect()
}

fn probe(c: &mut Criterion) {
    let mut group = c.benchmark_group("bloom/probe");
    let hashes = hashes();

    for bits_per_key in [8usize, 10, 16] {
        let filter = Bloom::build(&hashes, bits_per_key);

        group.bench_function(format!("{bits_per_key} bits per key, hit"), |b| {
            let mut i = 0usize;
            b.iter(|| {
                let found = filter.may_contain(&key(i % KEYS));
                i += 1;
                black_box(found)
            });
        });

        group.bench_function(format!("{bits_per_key} bits per key, miss"), |b| {
            let mut i = KEYS;
            b.iter(|| {
                let found = filter.may_contain(&key(i));
                i += 1;
                black_box(found)
            });
        });
    }

    group.finish();
}

fn hashing(c: &mut Criterion) {
    // The probe numbers above include this, so it is what tells whether they
    // are dominated by hashing or by the memory reads.
    c.bench_function("bloom/hash one key", |b| {
        let mut i = 0usize;
        b.iter(|| {
            let hashed = hash(&key(i));
            i += 1;
            black_box(hashed)
        });
    });
}

fn build(c: &mut Criterion) {
    let hashes = hashes();
    let mut group = c.benchmark_group("bloom/build");
    group.sample_size(20);

    group.bench_function(format!("{KEYS} keys, 10 bits per key"), |b| {
        b.iter(|| black_box(Bloom::build(&hashes, 10)));
    });

    group.finish();
}

criterion_group!(benches, probe, hashing, build);
criterion_main!(benches);
