//! LSM-tree key-value storage engine.
//!
//! Writes land in a write-ahead log and a sorted in-memory table; the table is
//! flushed to an immutable sorted file once it grows past a threshold, and
//! those files are merged by background compaction. Reads walk the memory table
//! first, then the files from newest to oldest, skipping files whose Bloom
//! filter rules the key out.
//!
//! The engine is synchronous and owns a single data directory. Network serving
//! and any async runtime live outside this crate.

pub mod bloom;
mod checksum;
mod coding;
pub mod compaction;
mod engine;
pub mod error;
mod lookup;
pub mod manifest;
pub mod memtable;
pub mod sstable;
#[cfg(test)]
mod testutil;
pub mod wal;

pub use bloom::Bloom;
pub use engine::{Config, Engine};
pub use error::{Error, Result};
pub use lookup::Lookup;
pub use manifest::Manifest;
pub use memtable::{Kind as MemtableKind, Memtable};
pub use sstable::SsTable;
pub use wal::{Record, SyncPolicy, Wal};
