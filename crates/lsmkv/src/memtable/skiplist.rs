//! Lock-free skiplist, written against what the `BTreeMap` baseline measured:
//! an insert there excludes every reader for its whole duration, and a steady
//! stream of readers starves the writer. Here readers never block and writers
//! publish with a compare-and-swap.
//!
//! No pointers and no `unsafe`. Nodes live in an arena allocated once, and a
//! link is an index into it rather than an address, which is what an arena
//! allocator reduces a pointer to anyway. Two properties of an LSM memtable are
//! what make this work:
//!
//! - it never frees a single node, it is dropped whole once its flush is done,
//!   so there is nothing to reclaim under readers and no need for epochs or
//!   hazard pointers;
//! - a published node is never modified, because an overwrite inserts a new
//!   version rather than editing the old one, so a reader that reaches a node
//!   can read it without synchronizing against anything.
//!
//! Nodes are ordered by key ascending, then by sequence number descending, so
//! the versions of one key sit together with the newest first. A lookup takes
//! the first one it finds, and a flush takes the first of each group.
//!
//! The arena is sized from the memtable budget and cannot grow. The engine
//! freezes a table when [`SkipMemtable::approx_bytes`] reaches that budget, so
//! that method reports node pressure as well as bytes: it is the only lever
//! this structure has on when it stops being written to.

use std::cmp::Ordering as Order;
use std::fmt;
use std::sync::OnceLock;

// Under `--cfg loom` the links come from loom, which explores the orderings
// they are read and written with. `OnceLock` stays the standard library's in
// both builds: loom has no equivalent, and what it would be checking there is
// the standard library's own release-acquire pair rather than this module's.
#[cfg(loom)]
use loom::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
#[cfg(not(loom))]
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};

use super::{Memtable, Visit};
use crate::error::Result;
use crate::lookup::Lookup;

/// Tower levels a node can reach. 12 levels at one quarter branching addresses
/// about 4^12 entries, far past what a memtable budget allows.
const MAX_HEIGHT: usize = 12;
/// One node in four is promoted to the level above.
const BRANCHING_SHIFT: u32 = 2;
/// End of a level, since index 0 is the head.
const NIL: u32 = u32::MAX;
/// The head sentinel, whose key is never compared against anything.
const HEAD: u32 = 0;

/// Entry bytes assumed when sizing the arena from the budget. Smaller entries
/// than this are what node pressure exists to catch.
const ASSUMED_ENTRY_BYTES: usize = 96;
/// Link slots per node. The expected tower is 1.33 levels at one quarter
/// branching; four leaves room for the tail of that distribution.
const LINKS_PER_NODE: usize = 4;
/// Nodes held back so that writers already past the threshold check still find
/// room. The blocking pool bounds how many of those there can be.
const MARGIN: usize = 1024;

/// A sorted table of every version written, newest first within a key.
pub struct SkipMemtable {
    nodes: Box<[Node]>,
    links: Box<[AtomicU32]>,
    next_node: AtomicUsize,
    next_link: AtomicUsize,
    /// Levels any node has reached. A search starts here rather than at
    /// `MAX_HEIGHT`, and everything above is known to be empty: a node raises
    /// this before it links itself anywhere.
    top: AtomicUsize,
    /// Key and value bytes, the same accounting the `BTreeMap` reports.
    bytes: AtomicUsize,
    budget: usize,
    /// Node count at which `approx_bytes` starts reporting the full budget.
    pressure_at: usize,
}

#[derive(Debug, Default)]
struct Node {
    entry: OnceLock<Entry>,
}

/// Written once, before the node is reachable, and never modified after.
#[derive(Debug)]
struct Entry {
    key: Box<[u8]>,
    seq: u64,
    /// `None` is a tombstone.
    value: Option<Box<[u8]>>,
    /// Where this node's tower starts in `links`, and how many levels it has.
    tower: usize,
    height: usize,
}

impl SkipMemtable {
    /// An empty table for a store that freezes its tables at `budget` bytes.
    ///
    /// The arena is allocated here and never grows, so this costs memory before
    /// a single key is written. That is the price of never taking a lock.
    pub fn new(budget: usize) -> Self {
        Self::with_capacity(budget, budget / ASSUMED_ENTRY_BYTES + MARGIN + 1)
    }

    /// The arena sized directly, for tests that cannot afford the real one.
    fn with_capacity(budget: usize, capacity: usize) -> Self {
        let nodes: Box<[Node]> = (0..capacity).map(|_| Node::default()).collect();
        let links: Box<[AtomicU32]> = (0..capacity * LINKS_PER_NODE)
            .map(|_| AtomicU32::new(NIL))
            .collect();

        // The head owns the first tower. Its key is never read: a search only
        // ever compares the nodes it steps onto, never the one it starts from.
        nodes[HEAD as usize].entry.get_or_init(|| Entry {
            key: Box::default(),
            seq: 0,
            value: None,
            tower: 0,
            height: MAX_HEIGHT,
        });

        Self {
            nodes,
            links,
            next_node: AtomicUsize::new(1),
            next_link: AtomicUsize::new(MAX_HEIGHT),
            top: AtomicUsize::new(1),
            bytes: AtomicUsize::new(0),
            budget,
            pressure_at: capacity.saturating_sub(MARGIN).max(1),
        }
    }

    fn entry(&self, index: u32) -> &Entry {
        self.nodes[index as usize]
            .entry
            .get()
            .expect("a node is published before it can be reached")
    }

    fn next(&self, index: u32, level: usize) -> u32 {
        let entry = self.entry(index);
        // A search only ever steps onto a node through a link at that level, so
        // the node it steps onto always has a tower that reaches it. Nothing
        // else keeps this read inside the node's own slots.
        debug_assert!(
            level < entry.height,
            "read level {level} of a node {} levels tall",
            entry.height
        );
        self.links[entry.tower + level].load(Ordering::Acquire)
    }

    /// Where `(key, seq)` belongs: at every level, the last node before it and
    /// the first node not before it.
    ///
    /// The two come out of the same walk on purpose. Reading the successor
    /// again afterwards can see a node linked in the meantime, and splicing
    /// against that pair puts this node ahead of one that sorts before it. The
    /// compare-and-swap does not catch it: all it checks is that the link did
    /// not move, and it did not.
    ///
    /// `seq` of `u64::MAX` means "before every version of this key", which is
    /// what a lookup wants.
    fn descend(
        &self,
        key: &[u8],
        seq: u64,
        preds: &mut [u32; MAX_HEIGHT],
        succs: &mut [u32; MAX_HEIGHT],
    ) {
        let top = self.top.load(Ordering::Acquire);
        // Nothing is linked above `top`, since a node raises it before it links
        // itself, so those levels are known empty without being read.
        preds[top..].fill(HEAD);
        succs[top..].fill(NIL);

        let mut node = HEAD;
        for level in (0..top).rev() {
            let mut next = self.next(node, level);
            while next != NIL && self.order(next, key, seq) == Order::Less {
                node = next;
                next = self.next(node, level);
            }
            preds[level] = node;
            succs[level] = next;
        }
    }

    /// Raises the searched height to `height`, before anything is linked there.
    fn raise_top(&self, height: usize) {
        let mut top = self.top.load(Ordering::Relaxed);
        while height > top {
            match self
                .top
                .compare_exchange_weak(top, height, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return,
                Err(current) => top = current,
            }
        }
    }

    /// How the node at `index` sorts against `(key, seq)`: by key ascending,
    /// then by sequence number descending.
    fn order(&self, index: u32, key: &[u8], seq: u64) -> Order {
        let entry = self.entry(index);
        entry
            .key
            .as_ref()
            .cmp(key)
            .then_with(|| seq.cmp(&entry.seq))
    }

    /// Reserves a tower, shortened to what the link arena has left.
    ///
    /// A compare-and-swap rather than a fetch-and-add, so an attempt that does
    /// not fit leaves nothing reserved behind it.
    fn reserve_tower(&self, height: usize) -> Option<(usize, usize)> {
        let mut start = self.next_link.load(Ordering::Relaxed);
        loop {
            let height = height.min(self.links.len() - start);
            if height == 0 {
                return None;
            }
            match self.next_link.compare_exchange_weak(
                start,
                start + height,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Some((start, height)),
                Err(current) => start = current,
            }
        }
    }
}

impl Memtable for SkipMemtable {
    /// # Panics
    ///
    /// Panics if the arena is exhausted, which the margin held back by
    /// [`SkipMemtable::new`] and the pressure reported by `approx_bytes` are
    /// there to make unreachable: the engine freezes the table well before the
    /// last node is taken.
    fn insert(&self, key: &[u8], seq: u64, value: Option<Vec<u8>>) -> bool {
        let index = self.next_node.fetch_add(1, Ordering::Relaxed);
        assert!(
            index < self.nodes.len(),
            "the memtable arena is exhausted, which means the table was not frozen in time"
        );
        let index = u32::try_from(index).expect("the arena is far smaller than u32");

        let height = height_for(u64::from(index));
        let (tower, height) = self
            .reserve_tower(height)
            .expect("the link arena is sized so a tower of one level always fits");

        let value = value.map(Vec::into_boxed_slice);
        self.bytes.fetch_add(
            key.len() + value.as_ref().map_or(0, |value| value.len()),
            Ordering::Relaxed,
        );
        self.nodes[index as usize]
            .entry
            .set(Entry {
                key: key.into(),
                seq,
                value,
                tower,
                height,
            })
            .expect("a node is reserved by exactly one writer");

        // Before any linking, so a search already walks the levels this node
        // is about to appear on.
        self.raise_top(height);

        let mut preds = [HEAD; MAX_HEIGHT];
        let mut succs = [NIL; MAX_HEIGHT];
        let newest = loop {
            self.descend(key, seq, &mut preds, &mut succs);
            self.links[tower].store(succs[0], Ordering::Release);

            // The bottom level is the one that has to be ordered: it holds
            // every node, and a lookup that stops early here reports a key
            // that is present as missing.
            let pred = preds[0];
            if self.links[self.entry(pred).tower]
                .compare_exchange(succs[0], index, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                // Nothing sorts between this node and its predecessor, so a
                // newer version of the same key could only be that predecessor.
                break pred == HEAD || self.entry(pred).key.as_ref() != key;
            }
        };

        // The upper levels are what makes a search fast, not what makes it
        // correct. A search only ever steps onto a node that sorts before what
        // it is looking for, so it lands short of its target whatever order the
        // express lanes are in, and the bottom level finishes the walk. They
        // are kept ordered for the search to stay logarithmic, which is why a
        // failed attempt walks again instead of retrying against a pair the
        // list no longer holds.
        for level in 1..height {
            loop {
                let pred = preds[level];
                self.links[tower + level].store(succs[level], Ordering::Release);
                if self.links[self.entry(pred).tower + level]
                    .compare_exchange(succs[level], index, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    break;
                }
                self.descend(key, seq, &mut preds, &mut succs);
            }
        }

        newest
    }

    fn get(&self, key: &[u8]) -> Lookup {
        let mut preds = [HEAD; MAX_HEIGHT];
        let mut succs = [NIL; MAX_HEIGHT];
        // Ahead of every version of this key, so the first node the search
        // lands on is the newest one.
        self.descend(key, u64::MAX, &mut preds, &mut succs);
        let candidate = succs[0];
        if candidate == NIL {
            return Lookup::Missing;
        }

        let entry = self.entry(candidate);
        if entry.key.as_ref() != key {
            return Lookup::Missing;
        }
        match &entry.value {
            Some(value) => Lookup::Found(value.to_vec()),
            None => Lookup::Deleted,
        }
    }

    /// Walks the bottom level, so this costs the length of the table. It is
    /// called by tests and diagnostics, never on the read or write path.
    fn len(&self) -> usize {
        let mut count = 0;
        let mut previous: Option<&[u8]> = None;
        let mut node = self.next(HEAD, 0);
        while node != NIL {
            let entry = self.entry(node);
            if previous != Some(entry.key.as_ref()) {
                count += 1;
                previous = Some(entry.key.as_ref());
            }
            node = self.next(node, 0);
        }
        count
    }

    fn is_empty(&self) -> bool {
        self.next(HEAD, 0) == NIL
    }

    fn for_each(&self, visit: Visit<'_>) -> Result<()> {
        let mut previous: Option<&[u8]> = None;
        let mut node = self.next(HEAD, 0);
        while node != NIL {
            let entry = self.entry(node);
            // Versions of a key are adjacent and the newest comes first, so the
            // ones after it are what the flush is meant to drop.
            if previous != Some(entry.key.as_ref()) {
                visit(&entry.key, entry.seq, entry.value.as_deref())?;
                previous = Some(entry.key.as_ref());
            }
            node = self.next(node, 0);
        }
        Ok(())
    }

    fn approx_bytes(&self) -> usize {
        let bytes = self.bytes.load(Ordering::Relaxed);
        let nodes = self.next_node.load(Ordering::Relaxed);
        if nodes < self.pressure_at {
            return bytes;
        }
        // Out of room before the byte budget was reached, which entries smaller
        // than the arena was sized for will do. Reporting the budget is what
        // gets the table frozen.
        bytes.max(self.budget)
    }
}

impl fmt::Debug for SkipMemtable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SkipMemtable")
            .field("versions", &(self.next_node.load(Ordering::Relaxed) - 1))
            .field("capacity", &self.nodes.len())
            .field("bytes", &self.bytes.load(Ordering::Relaxed))
            .field("top", &self.top.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

/// Tower height for the node at `index`, one quarter branching.
///
/// Derived from the index rather than from a random source, so a failing test
/// fails the same way twice. The mixer is splitmix64, as in the Bloom filter.
fn height_for(index: u64) -> usize {
    let mut bits = mix(index);
    let mut height = 1;
    while height < MAX_HEIGHT && bits.trailing_zeros() >= BRANCHING_SHIFT {
        height += 1;
        bits >>= BRANCHING_SHIFT;
    }
    height
}

fn mix(value: u64) -> u64 {
    let mut z = value.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    const BUDGET: usize = 64 * 1024;

    #[test]
    fn versions_of_a_key_sit_together_newest_first() {
        let table = SkipMemtable::new(BUDGET);
        table.insert(b"b", 1, Some(b"1".to_vec()));
        table.insert(b"a", 2, Some(b"2".to_vec()));
        table.insert(b"b", 3, Some(b"3".to_vec()));

        let mut seen = Vec::new();
        let mut node = table.next(HEAD, 0);
        while node != NIL {
            let entry = table.entry(node);
            seen.push((entry.key.to_vec(), entry.seq));
            node = table.next(node, 0);
        }

        assert_eq!(
            seen,
            vec![(b"a".to_vec(), 2), (b"b".to_vec(), 3), (b"b".to_vec(), 1),]
        );
    }

    #[test]
    fn a_flush_sees_only_the_newest_version_of_each_key() {
        let table = SkipMemtable::new(BUDGET);
        table.insert(b"a", 1, Some(b"old".to_vec()));
        table.insert(b"a", 5, Some(b"new".to_vec()));
        table.insert(b"b", 3, None);

        let mut seen = Vec::new();
        table
            .for_each(&mut |key, seq, value| {
                seen.push((key.to_vec(), seq, value.map(<[u8]>::to_vec)));
                Ok(())
            })
            .expect("visit");

        assert_eq!(
            seen,
            vec![
                (b"a".to_vec(), 5, Some(b"new".to_vec())),
                (b"b".to_vec(), 3, None),
            ]
        );
    }

    #[test]
    fn towers_spread_across_levels() {
        let heights: Vec<usize> = (0..4096).map(height_for).collect();
        let tallest = heights.iter().copied().max().expect("some heights");
        let single = heights.iter().filter(|&&h| h == 1).count();

        assert!(tallest > 3, "no tower reached level 4, got {tallest}");
        assert!(
            (2400..3400).contains(&single),
            "{single} of 4096 towers were one level, expected about three quarters"
        );
    }

    #[test]
    fn node_pressure_reports_the_budget_before_the_arena_runs_out() {
        let table = SkipMemtable::new(BUDGET);
        // Entries far smaller than the arena was sized for, which is the case
        // byte accounting alone would miss.
        let mut key = 0u64;
        while table.next_node.load(Ordering::Relaxed) < table.pressure_at {
            table.insert(&key.to_be_bytes(), key + 1, None);
            key += 1;
        }

        assert!(
            table.approx_bytes() >= BUDGET,
            "{} bytes reported with {} of {} nodes taken",
            table.approx_bytes(),
            table.next_node.load(Ordering::Relaxed),
            table.nodes.len()
        );
        assert!(
            table.next_node.load(Ordering::Relaxed) + MARGIN <= table.nodes.len(),
            "the margin has to survive the writers still in flight"
        );
    }
}

/// Model checking of the linking protocol.
///
/// ```text
/// LOOM_MAX_BRANCHES=50000 RUSTFLAGS="--cfg loom" cargo test -p lsmkv --lib loom
/// ```
///
/// The branch budget is raised because the model has to fill the table before
/// the interesting part: two one-level towers never touch the linking above the
/// bottom level, so a model small enough for the default budget checks nothing.
///
/// These were verified to fail when the bottom-level successor is read again
/// after the walk instead of coming out of it, which is the defect they exist
/// for. A model that cannot fail is not a test.
///
/// loom runs every interleaving of the threads below and every ordering the
/// memory model allows for the atomics they touch, which is what says the
/// release-acquire pairs on the links are the right ones rather than the ones
/// that happen to work on this machine.
///
/// What it covers is this module's links. Publishing the payload goes through
/// `OnceLock`, which loom has no version of, so that pair is the standard
/// library's guarantee and not something checked here.
#[cfg(all(test, loom))]
mod loom_tests {
    use super::*;
    use crate::lookup::Lookup;

    const CAPACITY: usize = 24;
    const BUDGET: usize = 4096;

    /// Nodes are numbered in reservation order, and a node's tower height is
    /// derived from its number, so filling the table to here puts the two
    /// concurrent writers below on numbers 19 and 20. Both are two levels tall,
    /// which is what makes them contend for a predecessor above the bottom
    /// level. Two one-level towers only ever touch level 0, and a model built
    /// out of those exercises none of the linking this checks.
    const FILL: usize = 18;

    fn filled() -> SkipMemtable {
        let table = SkipMemtable::with_capacity(BUDGET, CAPACITY);
        for i in 0..FILL {
            let key = [b'k', b'0' + (i / 10) as u8, b'0' + (i % 10) as u8];
            table.insert(&key, i as u64 + 1, Some(b"v".to_vec()));
        }
        for number in (FILL + 1)..=(FILL + 2) {
            assert!(
                height_for(number as u64) > 1,
                "node {number} is one level tall, the model would check nothing"
            );
        }
        table
    }

    #[test]
    fn two_writers_above_the_bottom_level_are_both_found() {
        loom::model(|| {
            let table = loom::sync::Arc::new(filled());

            let writer = {
                let table = loom::sync::Arc::clone(&table);
                loom::thread::spawn(move || {
                    table.insert(b"k18", 100, Some(b"a".to_vec()));
                })
            };
            table.insert(b"k19", 101, Some(b"b".to_vec()));
            writer.join().expect("join");

            // Splicing against a successor that moved leaves one of the two
            // ahead of a node that sorts before it, and the search that walks
            // the upper level then steps past whatever sits between them.
            assert_eq!(table.get(b"k18"), Lookup::Found(b"a".to_vec()));
            assert_eq!(table.get(b"k19"), Lookup::Found(b"b".to_vec()));
            // The node the two were spliced after has to still lead to them.
            assert_eq!(table.get(b"k17"), Lookup::Found(b"v".to_vec()));
        });
    }

    #[test]
    fn two_versions_of_one_key_resolve_to_the_newer() {
        loom::model(|| {
            let table = loom::sync::Arc::new(filled());

            let writer = {
                let table = loom::sync::Arc::clone(&table);
                loom::thread::spawn(move || {
                    table.insert(b"k18", 100, Some(b"old".to_vec()));
                })
            };
            table.insert(b"k18", 101, Some(b"new".to_vec()));
            writer.join().expect("join");

            // Ordered by sequence number descending, so the newer version sits
            // ahead of the older one whichever thread got there first.
            assert_eq!(table.get(b"k18"), Lookup::Found(b"new".to_vec()));
        });
    }

    #[test]
    fn a_reader_never_sees_a_half_linked_node() {
        loom::model(|| {
            let table = loom::sync::Arc::new(filled());

            let writer = {
                let table = loom::sync::Arc::clone(&table);
                loom::thread::spawn(move || {
                    table.insert(b"k18", 100, Some(b"a".to_vec()));
                })
            };
            // Concurrent with the insert: either the key is not there yet or it
            // reads back whole. A torn link would show up as anything else.
            let seen = table.get(b"k18");
            writer.join().expect("join");

            assert!(
                seen == Lookup::Missing || seen == Lookup::Found(b"a".to_vec()),
                "{seen:?}"
            );
            assert_eq!(table.get(b"k18"), Lookup::Found(b"a".to_vec()));
        });
    }
}
