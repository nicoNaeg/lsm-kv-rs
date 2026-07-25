//! Picking what to compact, and merging it.
//!
//! Level 0 holds whatever the flushes produced, so its files overlap and each
//! one costs a lookup: what matters there is how many there are. Deeper levels
//! hold files that never overlap, so a lookup consults one file per level
//! whatever their number: what matters there is how many bytes the level holds
//! against its budget, which grows by `fanout` at every step.
//!
//! A compaction merges one level into the next: every file of level 0, or one
//! file of a deeper level, plus every file of the level below whose key range it
//! touches. The output is written back at the level below, cut into files of a
//! bounded size, and is non-overlapping by construction since it comes out of a
//! single sorted stream.

use std::cmp::Ordering;
use std::collections::BinaryHeap;

use crate::error::Result;
use crate::manifest::{FileMeta, Snapshot};
use crate::sstable::{Entry, Scan};

/// What a compaction will do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Plan {
    /// Level being drained.
    pub level: usize,
    /// Files to merge, from `level` and from the level below.
    pub inputs: Vec<FileMeta>,
}

impl Plan {
    /// Level the merged output is written to.
    pub fn output_level(&self) -> usize {
        self.level + 1
    }

    /// Numbers of the files this compaction consumes.
    pub fn input_numbers(&self) -> Vec<u64> {
        self.inputs.iter().map(|file| file.number).collect()
    }
}

/// Byte budget of a level. Level 0 has none, since it is triggered by count.
pub fn budget(level: usize, level_bytes: u64, fanout: u64) -> u64 {
    level_bytes.saturating_mul(fanout.saturating_pow(u32::try_from(level).unwrap_or(u32::MAX) - 1))
}

/// Chooses the level most in need of a compaction, or `None` if none is.
///
/// The score of a level is how far past its limit it sits, so comparing scores
/// compares levels that are measured in different units.
pub fn plan(snapshot: &Snapshot, l0_trigger: usize, level_bytes: u64, fanout: u64) -> Option<Plan> {
    let mut chosen = None;
    let mut best = 1.0;

    let l0_files = snapshot.at_level(0).count();
    if l0_files >= l0_trigger && l0_trigger > 0 {
        // Ratio of files, which is what a lookup pays at level 0.
        best = ratio(l0_files as u64, l0_trigger as u64);
        chosen = Some(0);
    }

    for level in 1..=snapshot.deepest_level() {
        let bytes = snapshot.bytes_at_level(level);
        let limit = budget(level, level_bytes, fanout);
        let score = ratio(bytes, limit);
        if score > best {
            best = score;
            chosen = Some(level);
        }
    }

    chosen.map(|level| Plan {
        level,
        inputs: inputs_for(snapshot, level),
    })
}

/// The files a compaction of `level` consumes.
fn inputs_for(snapshot: &Snapshot, level: usize) -> Vec<FileMeta> {
    let mut inputs: Vec<FileMeta> = if level == 0 {
        // Level 0 files overlap each other, so they all go in together.
        snapshot.at_level(0).cloned().collect()
    } else {
        // The file that has been waiting longest, which is the lowest numbered.
        snapshot
            .at_level(level)
            .min_by_key(|file| file.number)
            .cloned()
            .into_iter()
            .collect()
    };

    let (min, max) = range_of(&inputs);
    let overlapping: Vec<FileMeta> = snapshot
        .at_level(level + 1)
        .filter(|file| file.overlaps(&min, &max))
        .cloned()
        .collect();
    inputs.extend(overlapping);
    inputs
}

/// Key range covering every file in `files`.
fn range_of(files: &[FileMeta]) -> (Vec<u8>, Vec<u8>) {
    let min = files
        .iter()
        .map(|file| file.min_key.clone())
        .min()
        .unwrap_or_default();
    let max = files
        .iter()
        .map(|file| file.max_key.clone())
        .max()
        .unwrap_or_default();
    (min, max)
}

/// Whether a tombstone has to be written out rather than dropped.
///
/// Dropping it is only safe when no file below the output level could still hold
/// a value for that key. Drop one too early and the older value comes back from
/// the dead the next time the key is read.
pub fn tombstone_needed(snapshot: &Snapshot, output_level: usize, key: &[u8]) -> bool {
    snapshot
        .files
        .iter()
        .any(|file| file.level > output_level && file.covers(key))
}

// Scores are ratios of counts and byte totals, far below where f64 stops being
// exact, and only ever compared against each other.
#[allow(clippy::cast_precision_loss)]
fn ratio(value: u64, limit: u64) -> f64 {
    if limit == 0 {
        return f64::INFINITY;
    }
    value as f64 / limit as f64
}

/// The merged stream of several sorted files, newest version of each key first
/// and older ones dropped.
///
/// Sources are pulled one entry at a time, so a merge holds one block of each
/// input rather than any whole file.
#[derive(Debug)]
pub struct Merge<'a> {
    sources: Vec<Scan<'a>>,
    heap: BinaryHeap<Head>,
    failure: Option<crate::error::Error>,
}

/// The next entry of one source, ordered so the heap yields the smallest key,
/// and for equal keys the newest version.
#[derive(Debug)]
struct Head {
    entry: Entry,
    source: usize,
}

impl Ord for Head {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reversed on the key, since a binary heap pops its greatest element and
        // a merge wants the smallest key. Sequence numbers are unique per
        // mutation, so the higher one is the later write.
        other
            .entry
            .key
            .cmp(&self.entry.key)
            .then_with(|| self.entry.seq.cmp(&other.entry.seq))
            .then_with(|| other.source.cmp(&self.source))
    }
}

impl PartialOrd for Head {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Head {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Head {}

impl<'a> Merge<'a> {
    /// Merges `sources`, which each have to be sorted by key.
    pub fn new(sources: Vec<Scan<'a>>) -> Self {
        let mut merge = Self {
            sources,
            heap: BinaryHeap::new(),
            failure: None,
        };
        for source in 0..merge.sources.len() {
            merge.pull(source);
        }
        merge
    }

    /// Moves one entry from a source into the heap.
    fn pull(&mut self, source: usize) {
        match self.sources[source].next() {
            Some(Ok(entry)) => self.heap.push(Head { entry, source }),
            Some(Err(err)) => self.failure = self.failure.take().or(Some(err)),
            None => {}
        }
    }
}

impl Iterator for Merge<'_> {
    type Item = Result<Entry>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(err) = self.failure.take() {
            self.heap.clear();
            return Some(Err(err));
        }

        let head = self.heap.pop()?;
        self.pull(head.source);

        // Everything else carrying this key is an older version of it.
        while self
            .heap
            .peek()
            .is_some_and(|next| next.entry.key == head.entry.key)
        {
            let stale = self.heap.pop().expect("peeked");
            self.pull(stale.source);
        }

        Some(Ok(head.entry))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sstable::{SsTable, Writer};
    use crate::testutil::TempDir;

    fn meta(number: u64, level: usize, min: &[u8], max: &[u8], bytes: u64) -> FileMeta {
        FileMeta {
            number,
            level,
            min_key: min.to_vec(),
            max_key: max.to_vec(),
            bytes,
            entries: 1,
        }
    }

    #[test]
    fn nothing_to_do_on_a_store_within_its_limits() {
        let snapshot = Snapshot {
            files: vec![meta(1, 0, b"a", b"z", 100)],
            ..Snapshot::default()
        };

        assert_eq!(plan(&snapshot, 4, 1000, 10), None);
    }

    #[test]
    fn level_zero_is_compacted_on_file_count() {
        let snapshot = Snapshot {
            files: vec![
                meta(1, 0, b"a", b"c", 10),
                meta(2, 0, b"b", b"d", 10),
                meta(3, 1, b"a", b"b", 10),
                meta(4, 1, b"y", b"z", 10),
            ],
            ..Snapshot::default()
        };

        let plan = plan(&snapshot, 2, 1_000_000, 10).expect("a plan");

        assert_eq!(plan.level, 0);
        assert_eq!(plan.output_level(), 1);
        // Both level 0 files, plus the level 1 file their range touches, and not
        // the one it does not.
        assert_eq!(plan.input_numbers(), vec![1, 2, 3]);
    }

    #[test]
    fn a_deeper_level_is_compacted_on_bytes() {
        let snapshot = Snapshot {
            files: vec![
                meta(1, 1, b"a", b"c", 600),
                meta(2, 1, b"d", b"f", 600),
                meta(3, 2, b"a", b"b", 10),
            ],
            ..Snapshot::default()
        };

        // Level 1 holds 1200 bytes against a budget of 1000.
        let plan = plan(&snapshot, 4, 1000, 10).expect("a plan");

        assert_eq!(plan.level, 1);
        // The oldest file of level 1, plus what it overlaps below.
        assert_eq!(plan.input_numbers(), vec![1, 3]);
    }

    #[test]
    fn the_neediest_level_wins() {
        let snapshot = Snapshot {
            files: vec![
                meta(1, 0, b"a", b"c", 10),
                meta(2, 0, b"b", b"d", 10),
                // Level 1 is ten times past its budget, level 0 only twice.
                meta(3, 1, b"a", b"z", 10_000),
            ],
            ..Snapshot::default()
        };

        let plan = plan(&snapshot, 1, 1000, 10).expect("a plan");

        assert_eq!(plan.level, 1);
    }

    #[test]
    fn budgets_grow_by_the_fanout() {
        assert_eq!(budget(1, 1000, 10), 1000);
        assert_eq!(budget(2, 1000, 10), 10_000);
        assert_eq!(budget(3, 1000, 10), 100_000);
    }

    #[test]
    fn a_tombstone_is_kept_only_while_something_below_could_hold_the_key() {
        let snapshot = Snapshot {
            files: vec![meta(1, 3, b"d", b"m", 10)],
            ..Snapshot::default()
        };

        assert!(
            tombstone_needed(&snapshot, 2, b"f"),
            "level 3 covers the key, so dropping it would resurrect a value"
        );
        assert!(
            !tombstone_needed(&snapshot, 2, b"z"),
            "no file below holds that key"
        );
        assert!(
            !tombstone_needed(&snapshot, 3, b"f"),
            "the output is already as deep as that file"
        );
    }

    /// Key, sequence number, and value or tombstone.
    type TestEntry<'a> = (&'a [u8], u64, Option<&'a [u8]>);

    /// Writes a file holding `entries`, in the order given.
    fn write(path: &std::path::Path, entries: &[TestEntry<'_>]) -> SsTable {
        let mut writer = Writer::create(path).expect("create");
        for (key, seq, value) in entries {
            writer.add(key, *seq, *value).expect("add");
        }
        writer.finish().expect("finish");
        SsTable::open(path).expect("open")
    }

    #[test]
    fn a_merge_keeps_the_newest_version_of_each_key() {
        let dir = TempDir::new();
        let newer = write(
            &dir.join("1.sst"),
            &[(b"a", 10, Some(b"new")), (b"c", 11, None)],
        );
        let older = write(
            &dir.join("2.sst"),
            &[
                (b"a", 1, Some(b"old")),
                (b"b", 2, Some(b"kept")),
                (b"c", 3, Some(b"deleted")),
            ],
        );

        let merged: Vec<Entry> = Merge::new(vec![newer.scan(), older.scan()])
            .map(|entry| entry.expect("entry"))
            .collect();

        assert_eq!(merged.len(), 3, "one entry per key");
        assert_eq!(merged[0].key, b"a");
        assert_eq!(merged[0].value, Some(b"new".to_vec()));
        assert_eq!(merged[1].key, b"b");
        assert_eq!(merged[1].value, Some(b"kept".to_vec()));
        assert_eq!(merged[2].key, b"c");
        assert_eq!(merged[2].value, None, "the tombstone wins over the value");
    }

    #[test]
    fn a_merge_of_one_source_changes_nothing() {
        let dir = TempDir::new();
        let table = write(&dir.join("1.sst"), &[(b"a", 1, Some(b"1"))]);

        let merged: Vec<Entry> = Merge::new(vec![table.scan()])
            .map(|entry| entry.expect("entry"))
            .collect();

        assert_eq!(merged.len(), 1);
    }

    #[test]
    fn a_merge_of_nothing_yields_nothing() {
        assert_eq!(Merge::new(Vec::new()).count(), 0);
    }

    #[test]
    fn a_merge_interleaves_disjoint_sources() {
        let dir = TempDir::new();
        let left = write(
            &dir.join("1.sst"),
            &[(b"a", 1, Some(b"1")), (b"c", 3, Some(b"3"))],
        );
        let right = write(
            &dir.join("2.sst"),
            &[(b"b", 2, Some(b"2")), (b"d", 4, Some(b"4"))],
        );

        let keys: Vec<Vec<u8>> = Merge::new(vec![left.scan(), right.scan()])
            .map(|entry| entry.expect("entry").key)
            .collect();

        assert_eq!(keys, [b"a", b"b", b"c", b"d"]);
    }
}
