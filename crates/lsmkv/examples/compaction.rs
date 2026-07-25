//! Measures what compaction costs and what it buys.
//!
//! ```text
//! cargo run --release --example compaction
//! ```
//!
//! Fills two stores with the same keys, each written twice, in a scattered order
//! so the files the flushes produce overlap in key range. One has its level 0
//! trigger set out of reach, so nothing is ever compacted and every file stays at
//! level 0; the other is left to compact. Both are then measured the same way.

use std::path::Path;
use std::time::{Duration, Instant};

use lsmkv::{Config, Engine, SyncPolicy};

const KEYS: usize = 20_000;
/// Every key is written this many times, so half the data on disk is obsolete
/// and compaction has something to reclaim.
const ROUNDS: usize = 2;
const VALUE_SIZE: usize = 100;
const LOOKUPS: usize = 20_000;
/// Small enough that the keys above spread over a few dozen files and reach
/// level 2, which is what makes the level shape visible at this scale.
const MEMTABLE_BYTES: usize = 64 * 1024;
/// Coprime with `KEYS`, so stepping by it visits every key once in an order that
/// leaves the files overlapping instead of neatly partitioned.
const SCATTER: usize = 7_919;

struct Reading {
    shape: &'static str,
    levels: Vec<usize>,
    disk_bytes: u64,
    ingested: u64,
    written: u64,
    lookup: Duration,
    absent_block_reads: u64,
    /// Bytes the live keys would occupy with no obsolete version kept.
    live: u64,
}

fn main() {
    let root = std::env::temp_dir().join(format!("lsmkv-compaction-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);

    let flat = fill(
        "flushes only",
        &root.join("flat"),
        Config {
            sync: SyncPolicy::Interval(Duration::from_millis(10)),
            memtable_bytes: MEMTABLE_BYTES,
            // Out of reach, so no compaction is ever due.
            l0_trigger: usize::MAX,
            ..Config::default()
        },
    );
    let leveled = fill(
        "leveled",
        &root.join("leveled"),
        Config {
            sync: SyncPolicy::Interval(Duration::from_millis(10)),
            memtable_bytes: MEMTABLE_BYTES,
            ..Config::default()
        },
    );

    report(&[flat, leveled]);
    let _ = std::fs::remove_dir_all(&root);
}

fn fill(shape: &'static str, dir: &Path, config: Config) -> Reading {
    let engine = Engine::open(dir, config).expect("open the store");
    let value = vec![b'v'; VALUE_SIZE];
    let mut ingested = 0u64;

    for round in 0..ROUNDS {
        for i in 0..KEYS {
            let key = format!("key:{:06}", (i * SCATTER + round) % KEYS);
            engine.set(key.as_bytes(), &value).expect("set");
            ingested += (key.len() + value.len()) as u64;
        }
    }
    engine.flush().expect("flush");
    engine.compact().expect("compact");

    let started = Instant::now();
    for i in 0..LOOKUPS {
        let key = format!("key:{:06}", (i * SCATTER) % KEYS);
        assert!(engine.get(key.as_bytes()).expect("get").is_some());
    }
    let lookup = started.elapsed() / u32::try_from(LOOKUPS).expect("count");

    let before_absent = engine.block_reads();
    for i in 0..LOOKUPS {
        // Sorts between two keys the store holds, so only a filter can reject it.
        let key = format!("key:{:06}x", i % KEYS);
        assert!(engine.get(key.as_bytes()).expect("get").is_none());
    }

    Reading {
        shape,
        levels: engine.level_sizes(),
        disk_bytes: disk_bytes(dir),
        ingested,
        written: engine.bytes_written(),
        lookup,
        absent_block_reads: engine.block_reads() - before_absent,
        live: ingested / ROUNDS as u64,
    }
}

/// Bytes the sorted files occupy.
fn disk_bytes(dir: &Path) -> u64 {
    std::fs::read_dir(dir)
        .expect("read the directory")
        .filter_map(std::result::Result::ok)
        .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "sst"))
        .filter_map(|entry| entry.metadata().ok())
        .map(|metadata| metadata.len())
        .sum()
}

fn report(readings: &[Reading]) {
    println!(
        "{KEYS} keys of {VALUE_SIZE} byte values written {ROUNDS} times, {LOOKUPS} lookups of each kind"
    );
    println!();
    println!(
        "| shape | files per level | on disk | write amplification | space amplification | mean lookup | blocks read for absent keys |"
    );
    println!(
        "|-------|-----------------|---------|---------------------|---------------------|-------------|-----------------------------|"
    );
    for reading in readings {
        println!(
            "| {} | {:?} | {} KiB | {:.2} | {:.2} | {:.2} µs | {} |",
            reading.shape,
            reading.levels,
            reading.disk_bytes / 1024,
            ratio(reading.written, reading.ingested),
            ratio(reading.disk_bytes, reading.live),
            reading.lookup.as_secs_f64() * 1e6,
            reading.absent_block_reads,
        );
    }
}

#[allow(clippy::cast_precision_loss)]
fn ratio(value: u64, over: u64) -> f64 {
    if over == 0 {
        return 0.0;
    }
    value as f64 / over as f64
}
