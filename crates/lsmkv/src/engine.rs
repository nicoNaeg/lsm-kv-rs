//! The storage engine: a log, the sorted table it feeds, and the files that
//! table is flushed to.
//!
//! A write goes to the log, then to the active memtable. Once that table passes
//! its size limit it is frozen, a fresh log is started, and a background thread
//! writes the frozen table out as a sorted file and deletes the logs it made
//! redundant. A read walks the active table, then the frozen ones, then the
//! files from newest to oldest, and stops at the first level that answers.
//!
//! The manifest records which files are live. Writing it is the commit point of
//! a flush: a table it does not name is an orphan a crash left behind, which
//! opening the store deletes, and the log that fed it is still there to replay.
//!
//! Locks are always taken in this order: the log, the durable state, the flush
//! queue, then the state in memory.

use std::fs::{self, File};
use std::ops::ControlFlow;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, PoisonError, RwLock};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::error::{Error, Result};
use crate::lookup::Lookup;
use crate::manifest::{FileMeta, Manifest, Snapshot};
use crate::memtable::Memtable;
use crate::sstable::{SsTable, Writer};
use crate::wal::{Record, SyncPolicy, Wal};

const POISONED: &str = "a thread panicked while holding the engine";
const LOG_EXT: &str = "wal";
const TABLE_EXT: &str = "sst";
/// Extension a table carries until it is complete and on the device.
const PARTIAL_EXT: &str = "tmp";
/// Delay before a failed flush is attempted again.
const RETRY_DELAY: Duration = Duration::from_secs(1);

/// How a store is opened.
#[derive(Debug, Clone, Copy)]
pub struct Config {
    /// When the log is pushed to the device.
    pub sync: SyncPolicy,
    /// Key and value bytes the active memtable holds before it is frozen and
    /// written to disk.
    pub memtable_bytes: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            sync: SyncPolicy::default(),
            memtable_bytes: 4 * 1024 * 1024,
        }
    }
}

/// A key-value store over one data directory.
///
/// Every method takes `&self`: the store is shared across threads behind an
/// [`Arc`], and does its own locking.
#[derive(Debug)]
pub struct Engine {
    shared: Arc<Shared>,
    flusher: Option<JoinHandle<()>>,
}

#[derive(Debug)]
struct Shared {
    dir: PathBuf,
    config: Config,
    /// The current log. Writers hold it for reading, which is what stops a
    /// rotation from splitting a write between two logs.
    wal: RwLock<Wal>,
    /// The file set as it is recorded on disk, with the manifest that records
    /// it. Held while a change is committed, so only one flush or compaction
    /// changes the file set at a time.
    durable: Mutex<Durable>,
    /// Signals work to the flush thread, and progress back to whoever waits.
    /// Locked before `state`, never after.
    queue: Mutex<FlushQueue>,
    progress: Condvar,
    state: RwLock<State>,
    /// Next number for a log or a table. The two share one numbering space.
    next_number: AtomicU64,
    /// Failure of a background flush, reported to the next caller that can.
    flush_error: Mutex<Option<Error>>,
}

#[derive(Debug)]
struct Durable {
    manifest: Manifest,
    snapshot: Snapshot,
}

#[derive(Debug, Default)]
struct FlushQueue {
    /// Frozen memtables not yet on disk.
    pending: usize,
    shutdown: bool,
}

#[derive(Debug)]
struct State {
    active: Arc<Memtable>,
    /// Logs holding the active table's records.
    active_logs: Vec<PathBuf>,
    /// Tables frozen but not yet on disk, newest first.
    frozen: Vec<Frozen>,
    /// Files by level. Level 0 holds files that may overlap, newest first;
    /// deeper levels hold files that never overlap, sorted by key.
    levels: Vec<Vec<Arc<SsTable>>>,
}

/// A memtable waiting to become a file, and the logs that still hold it.
#[derive(Debug, Clone)]
struct Frozen {
    memtable: Arc<Memtable>,
    logs: Vec<PathBuf>,
}

impl Engine {
    /// Opens the store held in `dir`, creating the directory if needed and
    /// rebuilding memory from the logs left behind.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Corrupt`] if a log or a table is not readable as one,
    /// and [`Error::Io`] if the directory or its files cannot be opened.
    pub fn open(dir: impl AsRef<Path>, config: Config) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir).map_err(|err| Error::io(&dir, err))?;
        let manifest = Manifest::new(&dir);
        let listing = Listing::scan(&dir)?;

        // A table a crash caught mid-write is unusable, and the logs it was
        // built from are still there, so it is dropped rather than opened.
        for path in &listing.partial {
            fs::remove_file(path).map_err(|err| Error::io(path, err))?;
        }

        let snapshot = match manifest.load()? {
            Some(snapshot) => snapshot,
            None if listing.tables.is_empty() => Snapshot::default(),
            None => {
                return Err(Error::corrupt(
                    &dir,
                    "tables are present but the manifest naming them is gone",
                ));
            }
        };

        // A table the manifest does not name belongs to a flush or a compaction
        // a crash interrupted before it committed. Its data is still in a log.
        for number in &listing.tables {
            if !snapshot.files.iter().any(|file| file.number == *number) {
                let path = dir.join(file_name(*number, TABLE_EXT));
                fs::remove_file(&path).map_err(|err| Error::io(&path, err))?;
            }
        }

        let levels = open_levels(&dir, &snapshot)?;
        let active = Memtable::new();
        let mut active_logs = Vec::new();
        let mut next_number = listing.next_number.max(snapshot.next_number);
        // Numbering picks up where the manifest left it, so it survives a store
        // whose logs have all been flushed away.
        let mut last_seq = snapshot.last_seq;

        // Every log still present holds records no table carries yet, so they
        // all rebuild the one active table.
        let wal = if let Some((newest, older)) = listing.logs.split_last() {
            for number in older {
                let path = dir.join(file_name(*number, LOG_EXT));
                for record in Wal::replay(&path)? {
                    last_seq = last_seq.max(apply(&active, record?));
                }
                active_logs.push(path);
            }
            let path = dir.join(file_name(*newest, LOG_EXT));
            let (wal, records) = Wal::recover(&path, config.sync, last_seq)?;
            for record in records {
                apply(&active, record);
            }
            active_logs.push(path);
            wal
        } else {
            let path = dir.join(file_name(next_number, LOG_EXT));
            next_number += 1;
            // From what the manifest recorded, not from zero: a store can hold
            // files without holding a log.
            let wal = Wal::create(&path, config.sync, last_seq)?;
            active_logs.push(path);
            wal
        };

        let shared = Arc::new(Shared {
            dir,
            config,
            wal: RwLock::new(wal),
            queue: Mutex::new(FlushQueue::default()),
            progress: Condvar::new(),
            durable: Mutex::new(Durable { manifest, snapshot }),
            state: RwLock::new(State {
                active: Arc::new(active),
                active_logs,
                frozen: Vec::new(),
                levels,
            }),
            next_number: AtomicU64::new(next_number),
            flush_error: Mutex::new(None),
        });
        let flusher = Some(spawn_flusher(&shared));

        Ok(Self { shared, flusher })
    }

    /// Reads `key`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Corrupt`] if a table block does not match its checksum,
    /// and [`Error::Io`] if it cannot be read.
    ///
    /// # Panics
    ///
    /// Panics if a previous writer panicked while holding the store.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let state = self.shared.state.read().expect(POISONED);

        if let ControlFlow::Break(answer) = answer(state.active.get(key)) {
            return Ok(answer);
        }
        for frozen in &state.frozen {
            if let ControlFlow::Break(answer) = answer(frozen.memtable.get(key)) {
                return Ok(answer);
            }
        }
        for level in &state.levels {
            for table in level {
                if let ControlFlow::Break(answer) = answer(table.get(key)?) {
                    return Ok(answer);
                }
            }
        }
        Ok(None)
    }

    /// Binds `key` to `value`.
    ///
    /// The record reaches the log first, and becomes visible in memory only
    /// once the sync policy considers it durable.
    ///
    /// # Errors
    ///
    /// Returns [`Error::TooLarge`] if the key or the value exceeds 4 GiB, and
    /// [`Error::Io`] if the log cannot be written or flushed. A background flush
    /// that failed is reported here, once.
    ///
    /// # Panics
    ///
    /// Panics if a previous writer panicked while holding the log or the store.
    pub fn set(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.shared.write(key, Some(value))
    }

    /// Deletes `key`, writing a tombstone over it.
    ///
    /// # Errors
    ///
    /// Same as [`Engine::set`].
    ///
    /// # Panics
    ///
    /// Same as [`Engine::set`].
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.shared.write(key, None)
    }

    /// Pushes the log to the device.
    ///
    /// Only [`SyncPolicy::Interval`] leaves anything for this to do; the other
    /// policies have already flushed by the time a write returns.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Io`] if the flush fails.
    ///
    /// # Panics
    ///
    /// Panics if a previous writer panicked while holding the log.
    pub fn sync(&self) -> Result<()> {
        self.shared.wal.read().expect(POISONED).sync()
    }

    /// Freezes the active memtable and waits until it is a file on disk. Does
    /// nothing if the table is empty.
    ///
    /// # Errors
    ///
    /// Whatever the rotation or the flush failed with.
    ///
    /// # Panics
    ///
    /// Panics if a previous writer panicked while holding the log or the store.
    pub fn flush(&self) -> Result<()> {
        self.shared.rotate(0)?;
        self.shared.wait_for_flush()
    }

    /// Waits until every frozen memtable is on disk.
    ///
    /// # Errors
    ///
    /// Whatever a background flush failed with.
    ///
    /// # Panics
    ///
    /// Panics if a previous writer panicked while holding the store.
    pub fn wait_for_flush(&self) -> Result<()> {
        self.shared.wait_for_flush()
    }

    /// Files the store holds, across every level.
    ///
    /// # Panics
    ///
    /// Panics if a previous writer panicked while holding the store.
    pub fn table_count(&self) -> usize {
        self.shared
            .state
            .read()
            .expect(POISONED)
            .levels
            .iter()
            .map(Vec::len)
            .sum()
    }

    /// Files held at each level, level 0 first.
    ///
    /// # Panics
    ///
    /// Panics if a previous writer panicked while holding the store.
    pub fn level_sizes(&self) -> Vec<usize> {
        self.shared
            .state
            .read()
            .expect(POISONED)
            .levels
            .iter()
            .map(Vec::len)
            .collect()
    }

    /// Highest sequence number the store has handed out.
    ///
    /// # Panics
    ///
    /// Panics if a previous writer panicked while holding the log.
    pub fn last_sequence(&self) -> u64 {
        self.shared.wal.read().expect(POISONED).last_sequence()
    }

    /// Blocks read from files since the store was opened, summed over the files
    /// it currently holds.
    ///
    /// A lookup a Bloom filter or a key range rejects adds nothing here, which
    /// is what makes the filters measurable.
    ///
    /// # Panics
    ///
    /// Panics if a previous writer panicked while holding the store.
    pub fn block_reads(&self) -> u64 {
        self.shared
            .state
            .read()
            .expect(POISONED)
            .levels
            .iter()
            .flatten()
            .map(|table| table.block_reads())
            .sum()
    }

    /// Directory the store lives in.
    pub fn dir(&self) -> &Path {
        &self.shared.dir
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        self.shared
            .queue
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .shutdown = true;
        self.shared.progress.notify_all();
        if let Some(flusher) = self.flusher.take() {
            let _ = flusher.join();
        }
        // Whatever is still in memory is still in a log, and recovery replays
        // logs, so there is nothing to write out here.
    }
}

impl Shared {
    fn write(&self, key: &[u8], value: Option<&[u8]>) -> Result<()> {
        if let Some(err) = self.take_flush_error() {
            return Err(err);
        }

        let bytes = {
            // Held for reading across both steps: a rotation takes this lock
            // for writing, so the table this record lands in is the one the log
            // it went to belongs to. Splitting the two would let a flush drop
            // the log holding a record that never reached a file.
            let wal = self.wal.read().expect(POISONED);
            let seq = match value {
                Some(value) => wal.set(key, value)?,
                None => wal.delete(key)?,
            };

            let state = self.state.read().expect(POISONED);
            state.active.insert(key, seq, value.map(<[u8]>::to_vec));
            state.active.approx_bytes()
        };

        if bytes >= self.config.memtable_bytes {
            self.rotate(self.config.memtable_bytes)?;
        }
        Ok(())
    }

    /// Freezes the active memtable and starts a fresh log, unless the table is
    /// empty or still smaller than `min_bytes`.
    fn rotate(&self, min_bytes: usize) -> Result<()> {
        let mut wal = self.wal.write().expect(POISONED);
        let mut queue = self.queue.lock().expect(POISONED);
        let mut state = self.state.write().expect(POISONED);

        // Another writer may have rotated while this one waited for the lock.
        if state.active.is_empty() || state.active.approx_bytes() < min_bytes {
            return Ok(());
        }

        let number = self.next_number.fetch_add(1, Ordering::Relaxed);
        let path = self.dir.join(file_name(number, LOG_EXT));
        let fresh = Wal::create(&path, self.config.sync, wal.last_sequence())?;
        let previous = std::mem::replace(&mut *wal, fresh);

        let frozen = Frozen {
            memtable: Arc::clone(&state.active),
            logs: std::mem::replace(&mut state.active_logs, vec![path]),
        };
        state.frozen.insert(0, frozen);
        state.active = Arc::new(Memtable::new());
        // Always the count of frozen tables rather than a running total, so no
        // failure path can leave the two disagreeing.
        queue.pending = state.frozen.len();

        drop(state);
        drop(queue);
        drop(wal);
        // Closing the old log flushes it one last time, which is not something
        // to do while writers are waiting on the lock.
        drop(previous);
        self.progress.notify_all();
        Ok(())
    }

    /// The oldest frozen memtable, waiting for one to appear.
    ///
    /// `retry` spaces out the attempts after a failure instead of spinning on
    /// the same table.
    fn next_flush(&self, retry: bool) -> Option<Frozen> {
        let mut queue = self.queue.lock().expect(POISONED);
        loop {
            if queue.shutdown {
                return None;
            }
            if queue.pending > 0 {
                if retry {
                    let (guard, _) = self
                        .progress
                        .wait_timeout(queue, RETRY_DELAY)
                        .expect(POISONED);
                    queue = guard;
                    if queue.shutdown {
                        return None;
                    }
                }
                let state = self.state.read().expect(POISONED);
                return state.frozen.last().cloned();
            }
            queue = self.progress.wait(queue).expect(POISONED);
        }
    }

    fn flush(&self, frozen: &Frozen) -> Result<()> {
        let number = self.next_number.fetch_add(1, Ordering::Relaxed);
        let table_path = self.dir.join(file_name(number, TABLE_EXT));
        let partial_path = self
            .dir
            .join(format!("{}.{PARTIAL_EXT}", file_name(number, TABLE_EXT)));

        let mut writer = Writer::create(&partial_path)?;
        frozen
            .memtable
            .for_each(|key, seq, value| writer.add(key, seq, value))?;
        writer.finish()?;

        // The table takes its final name only once it is complete and on the
        // device, so a crash mid-write leaves a file recovery can discard
        // rather than one it would have to trust.
        fs::rename(&partial_path, &table_path).map_err(|err| Error::io(&table_path, err))?;
        // The directory entry has to be durable before the manifest names it.
        sync_dir(&self.dir)?;
        let table = Arc::new(SsTable::open(&table_path)?);

        // Writing the manifest is what makes the table part of the store.
        self.commit(&[meta_of(number, 0, &table)], &[], |state| {
            debug_assert!(
                state
                    .frozen
                    .last()
                    .is_some_and(|oldest| Arc::ptr_eq(&oldest.memtable, &frozen.memtable)),
                "the flusher publishes the table it was handed"
            );
            // Newer than every file, older than everything still in memory.
            state.levels[0].insert(0, table);
            state.frozen.pop();
        })?;

        // Before reporting progress, so a caller that waited for the flush can
        // count on the logs it replaced being gone.
        for log in &frozen.logs {
            fs::remove_file(log).map_err(|err| Error::io(log, err))?;
        }
        self.mark_progress();
        Ok(())
    }

    /// Republishes how much work is left, and wakes whoever waits on it.
    fn mark_progress(&self) {
        let mut queue = self.queue.lock().expect(POISONED);
        queue.pending = self.state.read().expect(POISONED).frozen.len();
        drop(queue);
        self.progress.notify_all();
    }

    /// Records a change to the file set, then publishes it in memory.
    ///
    /// The manifest write is the commit point, so it happens before anything in
    /// memory moves, and while holding no lock a reader needs.
    fn commit(
        &self,
        added: &[FileMeta],
        removed: &[u64],
        publish: impl FnOnce(&mut State),
    ) -> Result<()> {
        let last_seq = self.wal.read().expect(POISONED).last_sequence();
        let mut durable = self.durable.lock().expect(POISONED);

        let mut snapshot = durable.snapshot.clone();
        snapshot
            .files
            .retain(|file| !removed.contains(&file.number));
        snapshot.files.extend_from_slice(added);
        snapshot.last_seq = snapshot.last_seq.max(last_seq);
        snapshot.next_number = self.next_number.load(Ordering::Relaxed);
        durable.manifest.store(&snapshot)?;
        durable.snapshot = snapshot;

        publish(&mut self.state.write().expect(POISONED));
        Ok(())
    }

    fn wait_for_flush(&self) -> Result<()> {
        let mut queue = self.queue.lock().expect(POISONED);
        loop {
            if let Some(err) = self.take_flush_error() {
                return Err(err);
            }
            if queue.pending == 0 {
                return Ok(());
            }
            queue = self.progress.wait(queue).expect(POISONED);
        }
    }

    fn take_flush_error(&self) -> Option<Error> {
        self.flush_error
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    }

    fn set_flush_error(&self, err: Error) {
        let mut slot = self
            .flush_error
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if slot.is_none() {
            *slot = Some(err);
        }
        drop(slot);
        // Whoever is waiting on a flush has to hear about the failure.
        self.progress.notify_all();
    }
}

fn spawn_flusher(shared: &Arc<Shared>) -> JoinHandle<()> {
    let shared = Arc::clone(shared);
    thread::Builder::new()
        .name("lsmkv-flush".to_owned())
        .spawn(move || {
            let mut retry = false;
            while let Some(frozen) = shared.next_flush(retry) {
                retry = match shared.flush(&frozen) {
                    Ok(()) => false,
                    Err(err) => {
                        shared.set_flush_error(err);
                        true
                    }
                };
            }
        })
        .expect("spawn the flush thread")
}

/// Turns a lookup into an answer, or asks the caller to try an older level.
fn answer(lookup: Lookup) -> ControlFlow<Option<Vec<u8>>> {
    match lookup {
        Lookup::Found(value) => ControlFlow::Break(Some(value)),
        Lookup::Deleted => ControlFlow::Break(None),
        Lookup::Missing => ControlFlow::Continue(()),
    }
}

/// Applies a replayed record and returns its sequence number.
fn apply(memtable: &Memtable, record: Record) -> u64 {
    let seq = record.seq();
    match record {
        Record::Set { key, value, .. } => memtable.insert(&key, seq, Some(value)),
        Record::Delete { key, .. } => memtable.insert(&key, seq, None),
    };
    seq
}

/// Opens every file the snapshot names, sorted the way each level wants.
fn open_levels(dir: &Path, snapshot: &Snapshot) -> Result<Vec<Vec<Arc<SsTable>>>> {
    let mut levels = vec![Vec::new(); snapshot.deepest_level() + 1];
    let mut files = snapshot.files.clone();
    files.sort_unstable_by_key(|file| file.number);

    for file in &files {
        let table = Arc::new(SsTable::open(dir.join(file_name(file.number, TABLE_EXT)))?);
        levels[file.level].push(table);
    }

    // Level 0 files overlap, so a lookup consults them newest first. Deeper
    // levels do not overlap, and key order is what a search there follows.
    levels[0].reverse();
    for level in levels.iter_mut().skip(1) {
        level.sort_by(|left, right| left.min_key().cmp(right.min_key()));
    }
    Ok(levels)
}

/// What the manifest records about a file just written.
fn meta_of(number: u64, level: usize, table: &SsTable) -> FileMeta {
    FileMeta {
        number,
        level,
        min_key: table.min_key().to_vec(),
        max_key: table.max_key().to_vec(),
        bytes: table.size_bytes(),
        entries: table.entries(),
    }
}

fn file_name(number: u64, extension: &str) -> String {
    format!("{number:06}.{extension}")
}

fn sync_dir(dir: &Path) -> Result<()> {
    let handle = File::open(dir).map_err(|err| Error::io(dir, err))?;
    handle.sync_all().map_err(|err| Error::io(dir, err))
}

/// What a data directory holds, by file number.
#[derive(Debug, Default)]
struct Listing {
    /// Log numbers, ascending.
    logs: Vec<u64>,
    /// Table numbers, ascending.
    tables: Vec<u64>,
    /// Tables left half-written by a crash.
    partial: Vec<PathBuf>,
    next_number: u64,
}

impl Listing {
    fn scan(dir: &Path) -> Result<Self> {
        let mut listing = Self::default();
        let entries = fs::read_dir(dir).map_err(|err| Error::io(dir, err))?;
        for entry in entries {
            let path = entry.map_err(|err| Error::io(dir, err))?.path();
            match path.extension().and_then(|ext| ext.to_str()) {
                Some(PARTIAL_EXT) => listing.partial.push(path),
                Some(LOG_EXT) => {
                    if let Some(number) = file_number(&path) {
                        listing.logs.push(number);
                    }
                }
                Some(TABLE_EXT) => {
                    if let Some(number) = file_number(&path) {
                        listing.tables.push(number);
                    }
                }
                _ => {}
            }
        }

        listing.logs.sort_unstable();
        listing.tables.sort_unstable();
        listing.next_number = listing
            .logs
            .iter()
            .chain(listing.tables.iter())
            .max()
            .map_or(1, |highest| highest + 1);
        Ok(listing)
    }
}

fn file_number(path: &Path) -> Option<u64> {
    path.file_stem()?.to_str()?.parse().ok()
}

#[cfg(test)]
mod tests {
    use std::thread;

    use super::*;
    use crate::testutil::TempDir;

    fn config(memtable_bytes: usize) -> Config {
        Config {
            sync: SyncPolicy::Group,
            memtable_bytes,
        }
    }

    fn open(dir: &TempDir, memtable_bytes: usize) -> Engine {
        Engine::open(dir.join("store"), config(memtable_bytes)).expect("open")
    }

    fn value(engine: &Engine, key: &[u8]) -> Option<Vec<u8>> {
        engine.get(key).expect("get")
    }

    fn count_files(engine: &Engine, extension: &str) -> usize {
        fs::read_dir(engine.dir())
            .expect("read the directory")
            .filter(|entry| {
                entry.as_ref().is_ok_and(|entry| {
                    entry.path().extension().and_then(|ext| ext.to_str()) == Some(extension)
                })
            })
            .count()
    }

    #[test]
    fn a_value_is_written_and_read_back() {
        let dir = TempDir::new();
        let engine = open(&dir, 4096);

        engine.set(b"user:1", b"nicolas").expect("set");

        assert_eq!(value(&engine, b"user:1"), Some(b"nicolas".to_vec()));
        assert_eq!(value(&engine, b"user:2"), None);
    }

    #[test]
    fn a_delete_hides_the_value() {
        let dir = TempDir::new();
        let engine = open(&dir, 4096);

        engine.set(b"key", b"value").expect("set");
        engine.delete(b"key").expect("delete");

        assert_eq!(value(&engine, b"key"), None);
    }

    #[test]
    fn the_store_comes_back_as_it_was_left() {
        let dir = TempDir::new();
        {
            let engine = open(&dir, 4096);
            engine.set(b"kept", b"1").expect("set");
            engine.set(b"replaced", b"first").expect("set");
            engine.set(b"replaced", b"second").expect("set");
            engine.set(b"removed", b"gone").expect("set");
            engine.delete(b"removed").expect("delete");
        }

        let engine = open(&dir, 4096);
        assert_eq!(value(&engine, b"kept"), Some(b"1".to_vec()));
        assert_eq!(value(&engine, b"replaced"), Some(b"second".to_vec()));
        assert_eq!(value(&engine, b"removed"), None);
    }

    #[test]
    fn writes_after_a_reopen_still_win_over_replayed_ones() {
        let dir = TempDir::new();
        {
            let engine = open(&dir, 4096);
            engine.set(b"key", b"before").expect("set");
        }

        let engine = open(&dir, 4096);
        engine.set(b"key", b"after").expect("set");
        drop(engine);

        let engine = open(&dir, 4096);
        assert_eq!(value(&engine, b"key"), Some(b"after".to_vec()));
    }

    #[test]
    fn a_full_memtable_becomes_a_table() {
        let dir = TempDir::new();
        let engine = open(&dir, 4096);

        assert_eq!(engine.table_count(), 0);
        engine.set(b"key", b"value").expect("set");
        engine.flush().expect("flush");

        assert_eq!(engine.table_count(), 1);
        assert_eq!(value(&engine, b"key"), Some(b"value".to_vec()));
    }

    #[test]
    fn flushing_an_empty_store_does_nothing() {
        let dir = TempDir::new();
        let engine = open(&dir, 4096);

        engine.flush().expect("flush");

        assert_eq!(engine.table_count(), 0);
    }

    #[test]
    fn a_log_is_deleted_once_its_table_exists() {
        let dir = TempDir::new();
        let engine = open(&dir, 4096);
        engine.set(b"key", b"value").expect("set");
        assert_eq!(count_files(&engine, LOG_EXT), 1);

        engine.flush().expect("flush");

        assert_eq!(count_files(&engine, TABLE_EXT), 1);
        assert_eq!(
            count_files(&engine, LOG_EXT),
            1,
            "the log the table replaced is gone, the fresh one remains"
        );
    }

    #[test]
    fn a_key_reached_through_an_older_table_is_still_found() {
        let dir = TempDir::new();
        let engine = open(&dir, 4096);

        engine.set(b"first", b"1").expect("set");
        engine.flush().expect("flush");
        engine.set(b"second", b"2").expect("set");
        engine.flush().expect("flush");
        engine.set(b"third", b"3").expect("set");

        assert_eq!(engine.table_count(), 2);
        assert_eq!(value(&engine, b"first"), Some(b"1".to_vec()));
        assert_eq!(value(&engine, b"second"), Some(b"2".to_vec()));
        assert_eq!(value(&engine, b"third"), Some(b"3".to_vec()));
    }

    #[test]
    fn a_tombstone_in_a_newer_table_hides_a_value_in_an_older_one() {
        let dir = TempDir::new();
        {
            let engine = open(&dir, 4096);
            engine.set(b"key", b"value").expect("set");
            engine.flush().expect("flush");
            engine.delete(b"key").expect("delete");
            engine.flush().expect("flush");
            assert_eq!(engine.table_count(), 2);
            assert_eq!(value(&engine, b"key"), None);
        }

        // The tombstone has to survive the reopen too, or the older table's
        // value would come back from the dead.
        let engine = open(&dir, 4096);
        assert_eq!(value(&engine, b"key"), None);
    }

    #[test]
    fn a_newer_value_wins_over_the_one_in_a_table() {
        let dir = TempDir::new();
        let engine = open(&dir, 4096);

        engine.set(b"key", b"old").expect("set");
        engine.flush().expect("flush");
        engine.set(b"key", b"new").expect("set");
        engine.flush().expect("flush");

        assert_eq!(value(&engine, b"key"), Some(b"new".to_vec()));
    }

    #[test]
    fn the_manifest_records_what_the_store_holds() {
        let dir = TempDir::new();
        let engine = open(&dir, 4096);
        engine.set(b"alpha", b"1").expect("set");
        engine.set(b"omega", b"2").expect("set");
        engine.flush().expect("flush");

        let snapshot = Manifest::new(engine.dir())
            .load()
            .expect("load")
            .expect("a flushed store has a manifest");

        assert_eq!(snapshot.files.len(), 1);
        let file = &snapshot.files[0];
        assert_eq!(file.level, 0);
        assert_eq!(file.min_key, b"alpha");
        assert_eq!(file.max_key, b"omega");
        assert_eq!(file.entries, 2);
        assert_eq!(snapshot.last_seq, 2);
    }

    #[test]
    fn a_table_the_manifest_does_not_name_is_deleted_at_open() {
        let dir = TempDir::new();
        let engine = open(&dir, 4096);
        engine.set(b"key", b"value").expect("set");
        engine.flush().expect("flush");

        // What a crash between writing a table and committing it leaves.
        let orphan = engine.dir().join(file_name(999, TABLE_EXT));
        fs::write(&orphan, b"a table nothing points at").expect("write");
        drop(engine);

        let engine = open(&dir, 4096);

        assert!(!orphan.exists(), "an uncommitted table must not be kept");
        assert_eq!(engine.table_count(), 1);
        assert_eq!(value(&engine, b"key"), Some(b"value".to_vec()));
    }

    #[test]
    fn tables_without_a_manifest_are_refused() {
        let dir = TempDir::new();
        let engine = open(&dir, 4096);
        engine.set(b"key", b"value").expect("set");
        engine.flush().expect("flush");
        let manifest = Manifest::new(engine.dir()).path().to_path_buf();
        drop(engine);

        fs::remove_file(&manifest).expect("remove the manifest");

        // Opening anyway would have to guess at levels and recency, and guessing
        // wrong resurrects stale values.
        let err = Engine::open(dir.join("store"), config(4096)).expect_err("must refuse");
        assert!(matches!(err, Error::Corrupt { .. }), "{err}");
    }

    #[test]
    fn sequence_numbers_survive_a_store_whose_logs_are_all_flushed() {
        let dir = TempDir::new();
        let engine = open(&dir, 4096);
        engine.set(b"a", b"1").expect("set");
        engine.set(b"b", b"2").expect("set");
        engine.flush().expect("flush");
        assert_eq!(engine.last_sequence(), 2);
        assert_eq!(count_files(&engine, LOG_EXT), 1, "the flushed log is gone");
        drop(engine);

        // Nothing on disk holds those records any more, so only the manifest can
        // say where the numbering stood.
        let engine = open(&dir, 4096);
        assert_eq!(engine.last_sequence(), 2);
        engine.set(b"c", b"3").expect("set");
        assert_eq!(engine.last_sequence(), 3);
    }

    #[test]
    fn a_partial_table_left_by_a_crash_is_dropped() {
        let dir = TempDir::new();
        let engine = open(&dir, 4096);
        engine.set(b"key", b"value").expect("set");
        let leftover = engine.dir().join("000042.sst.tmp");
        fs::write(&leftover, b"half a table").expect("write");
        drop(engine);

        let engine = open(&dir, 4096);

        assert!(!leftover.exists(), "a partial table must not be kept");
        assert_eq!(value(&engine, b"key"), Some(b"value".to_vec()));
    }

    #[test]
    fn every_key_survives_repeated_flushes_and_a_reopen() {
        const KEYS: usize = 400;

        let dir = TempDir::new();
        {
            // Small enough that the writes below flush several times.
            let engine = open(&dir, 512);
            for i in 0..KEYS {
                let key = format!("key:{i:04}");
                engine
                    .set(key.as_bytes(), format!("value:{i}").as_bytes())
                    .expect("set");
            }
            engine.wait_for_flush().expect("wait");
            assert!(engine.table_count() > 1, "the writes must have flushed");
        }

        let engine = open(&dir, 512);
        for i in 0..KEYS {
            let key = format!("key:{i:04}");
            assert_eq!(
                value(&engine, key.as_bytes()),
                Some(format!("value:{i}").into_bytes()),
                "key {i}"
            );
        }
    }

    #[test]
    fn memory_and_the_files_agree_after_concurrent_writers() {
        const WRITERS: usize = 8;
        const PER_WRITER: usize = 200;
        // Only 40 distinct keys, so the threshold has to sit below what those
        // keys weigh for the writes to flush at all.
        const KEYS: usize = 40;

        let dir = TempDir::new();
        let engine = open(&dir, 256);

        thread::scope(|scope| {
            for writer in 0..WRITERS {
                let engine = &engine;
                scope.spawn(move || {
                    for i in 0..PER_WRITER {
                        let key = format!("key:{}", i % KEYS);
                        if i % 7 == 0 {
                            engine.delete(key.as_bytes()).expect("delete");
                        } else {
                            let value = format!("{writer}:{i}");
                            engine.set(key.as_bytes(), value.as_bytes()).expect("set");
                        }
                    }
                });
            }
        });
        engine.wait_for_flush().expect("wait");

        let keys: Vec<String> = (0..KEYS).map(|i| format!("key:{i}")).collect();
        let in_memory: Vec<Option<Vec<u8>>> =
            keys.iter().map(|k| value(&engine, k.as_bytes())).collect();
        assert!(engine.table_count() > 1, "the writes must have flushed");
        drop(engine);

        let engine = open(&dir, 256);
        let recovered: Vec<Option<Vec<u8>>> =
            keys.iter().map(|k| value(&engine, k.as_bytes())).collect();

        assert_eq!(
            in_memory, recovered,
            "what the store answered must be what it answers after recovery"
        );
    }

    #[test]
    fn the_engine_is_shared_across_threads() {
        const fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Engine>();
    }
}
