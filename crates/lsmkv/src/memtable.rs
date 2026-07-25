//! Sorted in-memory table: the first place a write lands and the first place a
//! read looks.
//!
//! Entries are kept sorted by key because a full table is flushed to disk in
//! one sequential pass, and that file has to come out sorted. Each entry
//! carries the log sequence number of the mutation that produced it, which is
//! what keeps the table equal to what a replay of the log would rebuild.
//!
//! Two implementations sit behind [`Memtable`]. [`BTreeMemtable`] is a
//! `BTreeMap` under one `RwLock`, the baseline. [`SkipMemtable`] is a
//! hand-written lock-free skiplist, written against the numbers the baseline
//! produced under concurrent readers.

mod btree;
mod skiplist;

pub use btree::BTreeMemtable;
pub use skiplist::SkipMemtable;

use std::sync::Arc;

use crate::error::Result;
use crate::lookup::Lookup;

/// What [`Memtable::for_each`] calls for every entry: the key, the sequence
/// number of the mutation that produced it, and its value or `None` for a
/// tombstone.
pub type Visit<'a> = &'a mut dyn FnMut(&[u8], u64, Option<&[u8]>) -> Result<()>;

/// A sorted table of the most recent value known for each key.
///
/// Every method takes `&self`: the table is shared across threads behind an
/// [`Arc`] and does its own synchronization.
pub trait Memtable: Send + Sync + std::fmt::Debug {
    /// Applies a mutation carrying log sequence number `seq`, and reports
    /// whether it was the newest one for that key.
    ///
    /// Concurrent writers append to the log in one order and reach this table
    /// in another, so the mutation with the higher sequence number wins
    /// whatever the arrival order was. The log order is the truth, and this is
    /// what keeps memory equal to what recovery rebuilds from it.
    fn insert(&self, key: &[u8], seq: u64, value: Option<Vec<u8>>) -> bool;

    /// Looks `key` up.
    fn get(&self, key: &[u8]) -> Lookup;

    /// Number of distinct keys held, tombstones included.
    fn len(&self) -> usize;

    /// Whether the table holds nothing at all.
    fn is_empty(&self) -> bool;

    /// Visits the newest version of every key, in key order, which is the order
    /// a sorted file on disk needs them in.
    ///
    /// Only ever called on a table that no longer takes writes.
    ///
    /// # Errors
    ///
    /// Whatever `visit` returns, at the first entry that fails.
    fn for_each(&self, visit: Visit<'_>) -> Result<()>;

    /// Key and value bytes held, which is what the flush threshold is compared
    /// against. Excludes the per-entry bookkeeping of the structure itself.
    ///
    /// An implementation working out of a fixed arena may report more than that
    /// once it is running out of room, so that the engine freezes the table
    /// before the arena is exhausted. Reporting a size is the only lever it has
    /// on when that happens.
    fn approx_bytes(&self) -> usize;
}

/// Which implementation [`build`] returns.
///
/// Re-exported at the crate root as `MemtableKind`, where `Kind` alone would
/// say nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Kind {
    /// A `BTreeMap` behind one `RwLock`.
    #[default]
    BTree,
    /// A lock-free skiplist.
    Skiplist,
}

/// Builds an empty table of the requested kind, for a store whose tables are
/// frozen at `bytes`.
///
/// The budget matters to the skiplist, which allocates its arena up front from
/// it, and not to the `BTreeMap`, which grows as it goes.
pub fn build(kind: Kind, bytes: usize) -> Arc<dyn Memtable> {
    match kind {
        Kind::BTree => Arc::new(BTreeMemtable::new()),
        Kind::Skiplist => Arc::new(SkipMemtable::new(bytes)),
    }
}

#[cfg(all(test, not(loom)))]
mod tests;
