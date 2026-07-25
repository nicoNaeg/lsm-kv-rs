//! What a lookup in a sorted file costs.
//!
//! ```text
//! cargo bench --bench sstable
//! ```
//!
//! The file is read back right after it is written, so it sits in the page
//! cache: these numbers are the cost of the sparse index, the block read and
//! the search inside the block, not of reaching the device. That is the part
//! the format controls.
//!
//! Only even keys go into the file, so the odd ones are absent while still
//! sitting inside its key range: that is what makes the Bloom filter, rather
//! than the range check, the thing answering a miss.

use std::fs::{self, File};
use std::hint::black_box;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;

use criterion::{Criterion, criterion_group, criterion_main};
use lsmkv::sstable::{SsTable, Writer};

const ENTRIES: usize = 100_000;
const VALUE_SIZE: usize = 100;

fn key(i: usize) -> [u8; 16] {
    let mut key = [0u8; 16];
    key[..8].copy_from_slice(b"user:000");
    key[8..].copy_from_slice(&(i as u64).to_be_bytes());
    key
}

fn present(n: usize) -> [u8; 16] {
    key(n * 2)
}

fn absent(n: usize) -> [u8; 16] {
    key(n * 2 + 1)
}

/// A directory holding the benchmark's file, removed when it drops.
struct Scratch(PathBuf);

impl Scratch {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("lsmkv-sstable-bench-{}", std::process::id()));
        fs::create_dir_all(&path).expect("create the benchmark directory");
        Self(path)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn build(scratch: &Scratch) -> SsTable {
    let path = scratch.0.join("bench.sst");
    let value = vec![b'v'; VALUE_SIZE];
    let mut writer = Writer::create(&path).expect("create the file");
    for i in 0..ENTRIES {
        writer
            .add(&present(i), i as u64 + 1, Some(&value))
            .expect("add an entry");
    }
    writer.finish().expect("finish the file");
    SsTable::open(&path).expect("open the file")
}

fn get(c: &mut Criterion) {
    let scratch = Scratch::new();
    let table = build(&scratch);
    let mut group = c.benchmark_group("sstable/get");

    group.bench_function("hit", |b| {
        let mut i = 0usize;
        b.iter(|| {
            let found = table.get(&present(i % ENTRIES)).expect("get");
            i += 1;
            black_box(found)
        });
    });

    group.bench_function("miss inside the key range", |b| {
        let mut i = 0usize;
        b.iter(|| {
            let found = table.get(&absent(i % ENTRIES)).expect("get");
            i += 1;
            black_box(found)
        });
    });

    group.finish();
}

/// One 4 KiB positional read and nothing else, so the difference against a
/// lookup above is what verifying the block and searching it add. Same file,
/// same page cache, so the syscall is the only thing in common.
fn block_read(c: &mut Criterion) {
    let scratch = Scratch::new();
    let table = build(&scratch);
    let file = File::open(table.path()).expect("open the file again");
    let blocks = table.block_count() as u64;
    let mut block = vec![0u8; 4096];

    c.bench_function("sstable/read one 4 KiB block", |b| {
        let mut i = 0u64;
        b.iter(|| {
            file.read_at(&mut block, (i % blocks) * 4096)
                .expect("read a block");
            i += 1;
            black_box(&block);
        });
    });

    // The same read into a buffer allocated per call, which is what the lookup
    // path does. Against the one above, this is what that allocation costs.
    c.bench_function("sstable/read one 4 KiB block, allocating", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let mut block = vec![0u8; 4096];
            file.read_at(&mut block, (i % blocks) * 4096)
                .expect("read a block");
            i += 1;
            black_box(block)
        });
    });
}

criterion_group!(benches, get, block_read);
criterion_main!(benches);
