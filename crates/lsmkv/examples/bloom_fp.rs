//! Measures what the Bloom filters buy, against what the theory promises.
//!
//! ```text
//! cargo run --release --example bloom_fp
//! ```
//!
//! First the filter alone, at several densities, over the number of keys a 4 MiB
//! memtable holds. Then the store itself: lookups for keys it never held, across
//! files whose key ranges overlap, counting the block reads the filters removed.

use std::time::{Duration, Instant};

use lsmkv::bloom::{self, Bloom};
use lsmkv::{Config, Engine, SyncPolicy};

/// Keys a 4 MiB memtable holds at 16 byte keys and 100 byte values.
const KEYS: usize = 36_000;
const PROBES: usize = 200_000;
const DENSITIES: [usize; 4] = [8, 10, 12, 16];

/// Keys the store is filled with, and lookups run against it.
const STORE_KEYS: usize = 6_000;
const STORE_PROBES: usize = 10_000;
/// Small enough that the keys above spread over about ten files.
const MEMTABLE_BYTES: usize = 64 * 1024;
/// Coprime with `STORE_KEYS`, so stepping by it visits every key once in an
/// order that leaves the files overlapping instead of neatly partitioned.
const SCATTER: usize = 7_919;

fn main() {
    filter_densities();
    println!();
    store_lookups();
}

fn filter_densities() {
    let hashes: Vec<u64> = (0..KEYS)
        .map(|i| bloom::hash(format!("key:{i}").as_bytes()))
        .collect();

    println!("Bloom filter over {KEYS} keys, {PROBES} lookups for keys it never held");
    println!();
    println!("| bits per key | probes | filter | measured | theory |");
    println!("|--------------|--------|--------|----------|--------|");
    for bits_per_key in DENSITIES {
        let filter = Bloom::build(&hashes, bits_per_key);
        let positives = (0..PROBES)
            .filter(|i| filter.may_contain(format!("absent:{i}").as_bytes()))
            .count();

        println!(
            "| {bits_per_key} | {} | {} KiB | {} | {} |",
            filter.probes(),
            filter.bits() / 8 / 1024,
            percent(positives, PROBES),
            theoretical(bits_per_key, filter.probes()),
        );
    }
}

fn store_lookups() {
    let dir = std::env::temp_dir().join(format!("lsmkv-bloom-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let engine = Engine::open(
        &dir,
        Config {
            // The durability policy is not what is being measured here.
            sync: SyncPolicy::Interval(std::time::Duration::from_millis(10)),
            memtable_bytes: MEMTABLE_BYTES,
        },
    )
    .expect("open the store");

    let value = vec![b'v'; 100];
    for i in 0..STORE_KEYS {
        let key = format!("key:{:06}", (i * SCATTER) % STORE_KEYS);
        engine.set(key.as_bytes(), &value).expect("set");
    }
    engine.flush().expect("flush");

    let files = engine.table_count();
    let started = Instant::now();
    for i in 0..STORE_PROBES {
        // Sorts between keys the store holds, so no key range can reject it.
        let key = format!("key:{:06}x", i % STORE_KEYS);
        assert!(engine.get(key.as_bytes()).expect("get").is_none());
    }
    let elapsed = started.elapsed();
    let reads = engine.block_reads();
    let without = files * STORE_PROBES;

    println!(
        "Store of {STORE_KEYS} keys in {files} files, {STORE_PROBES} lookups for keys it never held"
    );
    println!();
    println!("| block reads with filters | without | removed | per lookup |");
    println!("|--------------------------|---------|---------|------------|");
    println!(
        "| {reads} | {without} | {} | {} |",
        percent(without - usize::try_from(reads).expect("count"), without),
        micros_each(elapsed, STORE_PROBES),
    );

    drop(engine);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Time each of `count` operations took, in microseconds.
#[allow(clippy::cast_precision_loss)]
fn micros_each(elapsed: Duration, count: usize) -> String {
    format!("{:.1} µs", elapsed.as_secs_f64() * 1e6 / count as f64)
}

/// `count` out of `total`, as a percentage with two decimals.
#[allow(clippy::cast_precision_loss)]
fn percent(count: usize, total: usize) -> String {
    format!("{:.2} %", count as f64 * 100.0 / total as f64)
}

/// False positive rate the theory gives for this density: `(1 - e^(-k/bpk))^k`.
#[allow(clippy::cast_precision_loss)]
fn theoretical(bits_per_key: usize, probes: u8) -> String {
    let k = f64::from(probes);
    let rate = (1.0 - (-k / bits_per_key as f64).exp()).powf(k);
    format!("{:.2} %", rate * 100.0)
}
