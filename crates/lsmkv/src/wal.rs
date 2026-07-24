//! Write-ahead log: every mutation is appended here before it is visible in
//! memory, so a crash costs nothing that was acknowledged.
//!
//! The file starts with a magic and a format version, then holds records back
//! to back. Appends are serialized by one lock and go straight to the file, one
//! write syscall each; how often those bytes are pushed to the device is set by
//! [`SyncPolicy`].
//!
//! Recovery reads the file back with [`Wal::recover`], which drops a torn tail
//! (a record a crash interrupted mid-write) before the log is reopened for
//! appending.

mod reader;
mod record;

pub use reader::Replay;
pub use record::Record;

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, PoisonError};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::error::{Error, Result};

const MAGIC: &[u8] = b"LSMKVWAL";
const FORMAT_VERSION: u32 = 1;
const FILE_HEADER_LEN: usize = 12;
/// Same, for arithmetic on file offsets.
const FILE_HEADER_LEN_U64: u64 = FILE_HEADER_LEN as u64;

const _: () = assert!(MAGIC.len() + 4 == FILE_HEADER_LEN);

/// A panic while the append lock was held leaves a half-written record behind,
/// after which nothing appended can be recovered.
const POISONED: &str = "a writer panicked while appending to the log";

/// When appended bytes are pushed to the device.
///
/// `Always` and `Group` offer the same guarantee, that an append returns only
/// once its record is durable; they differ in what a batch of concurrent
/// appends costs. `Interval` trades a bounded loss window for appends that
/// never wait on the device.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SyncPolicy {
    /// Flush inside every append, one device round trip per record, appends
    /// serialized behind each other.
    Always,
    /// Appends that arrive while a flush is in flight share the next one, so
    /// one device round trip covers everything the concurrent writers appended.
    /// This is the group commit of `RocksDB` and `PostgreSQL`, and the default.
    #[default]
    Group,
    /// A background thread flushes at a fixed interval. Appends never wait, and
    /// a crash loses at most the records written since the last flush.
    Interval(Duration),
}

/// An append-only log of mutations.
///
/// Cloning is not needed: every method takes `&self`, so the log is shared
/// across threads behind an [`Arc`].
#[derive(Debug)]
pub struct Wal {
    shared: Arc<Shared>,
    syncer: Option<JoinHandle<()>>,
}

#[derive(Debug)]
struct Shared {
    path: PathBuf,
    file: File,
    policy: SyncPolicy,
    /// Serializes appends and carries the encoding buffer they reuse.
    append: Mutex<Vec<u8>>,
    /// Records whose bytes reached the operating system. Bumped under the
    /// append lock, read without it.
    written: AtomicU64,
    sync: Mutex<SyncState>,
    synced: Condvar,
    flushes: AtomicU64,
    /// Failure of a background flush, reported on the next call that can.
    background_error: Mutex<Option<Error>>,
    shutdown: Mutex<bool>,
    stopping: Condvar,
}

#[derive(Debug, Default)]
struct SyncState {
    /// Records covered by a completed flush.
    durable: u64,
    /// Whether a flush is in flight, so latecomers wait for it instead of
    /// starting a second one.
    in_progress: bool,
}

impl Wal {
    /// Opens the log at `path`, replaying what it holds.
    ///
    /// The file is created if it does not exist. A tail left incomplete by a
    /// crash is discarded before the log is reopened for appending, so the
    /// returned log always continues from the last intact record.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Corrupt`] if the file is not a log of this format, and
    /// [`Error::Io`] if it cannot be read, created or truncated.
    pub fn recover(path: impl AsRef<Path>, policy: SyncPolicy) -> Result<(Self, Vec<Record>)> {
        let path = path.as_ref();
        let mut replay = Replay::open(path)?;
        let mut records = Vec::new();
        for record in &mut replay {
            records.push(record?);
        }
        let valid_len = replay.valid_len();
        let torn = replay.torn_at().is_some();
        drop(replay);

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|err| Error::io(path, err))?;

        if valid_len == 0 {
            // A new file, or one whose header never reached the disk.
            truncate(&file, path, 0)?;
            let mut header = Vec::with_capacity(FILE_HEADER_LEN);
            header.extend_from_slice(MAGIC);
            header.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
            (&file)
                .write_all(&header)
                .map_err(|err| Error::io(path, err))?;
            file.sync_all().map_err(|err| Error::io(path, err))?;
        } else if torn {
            // Appending after the torn bytes would leave every later record
            // behind a record replay refuses to read.
            truncate(&file, path, valid_len)?;
        }

        Ok((Self::new(path.to_path_buf(), file, policy), records))
    }

    /// Reads a log file without opening it for appending.
    ///
    /// # Errors
    ///
    /// Same as [`Wal::recover`], minus what only applies to appending.
    pub fn replay(path: impl AsRef<Path>) -> Result<Replay> {
        Replay::open(path.as_ref())
    }

    fn new(path: PathBuf, file: File, policy: SyncPolicy) -> Self {
        let shared = Arc::new(Shared {
            path,
            file,
            policy,
            append: Mutex::new(Vec::new()),
            written: AtomicU64::new(0),
            sync: Mutex::new(SyncState::default()),
            synced: Condvar::new(),
            flushes: AtomicU64::new(0),
            background_error: Mutex::new(None),
            shutdown: Mutex::new(false),
            stopping: Condvar::new(),
        });

        let syncer = match policy {
            SyncPolicy::Interval(interval) => Some(spawn_syncer(&shared, interval)),
            SyncPolicy::Always | SyncPolicy::Group => None,
        };

        Self { shared, syncer }
    }

    /// Appends `key` bound to `value`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::TooLarge`] if the key or the value exceeds 4 GiB, and
    /// [`Error::Io`] if the record cannot be written or flushed.
    ///
    /// # Panics
    ///
    /// Panics if a previous writer panicked while appending, which leaves the
    /// file with a partially written record.
    pub fn set(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.shared.append(key, Some(value))
    }

    /// Appends a tombstone for `key`.
    ///
    /// # Errors
    ///
    /// Same as [`Wal::set`].
    ///
    /// # Panics
    ///
    /// Same as [`Wal::set`].
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.shared.append(key, None)
    }

    /// Pushes everything appended so far to the device.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Io`] if the flush fails.
    ///
    /// # Panics
    ///
    /// Panics if a previous writer panicked while appending.
    pub fn sync(&self) -> Result<()> {
        let target = self.shared.written.load(Ordering::Acquire);
        self.shared.sync_through(target)
    }

    /// Device flushes issued since the log was opened.
    ///
    /// Under [`SyncPolicy::Group`] the ratio of appends to flushes is what
    /// group commit buys; the benchmark and the tests both read it here.
    pub fn flush_count(&self) -> u64 {
        self.shared.flushes.load(Ordering::Relaxed)
    }

    /// Records appended since the log was opened.
    pub fn record_count(&self) -> u64 {
        self.shared.written.load(Ordering::Acquire)
    }

    /// Path of the log file.
    pub fn path(&self) -> &Path {
        &self.shared.path
    }

    /// Policy the log was opened with.
    pub fn policy(&self) -> SyncPolicy {
        self.shared.policy
    }
}

impl Drop for Wal {
    fn drop(&mut self) {
        if let Some(syncer) = self.syncer.take() {
            *self
                .shared
                .shutdown
                .lock()
                .unwrap_or_else(PoisonError::into_inner) = true;
            self.shared.stopping.notify_all();
            let _ = syncer.join();
        }
        // Best effort: a failure here has nobody to report to, which is why
        // callers that need the guarantee call sync() first.
        let _ = self.shared.flush();
    }
}

impl Shared {
    fn append(&self, key: &[u8], value: Option<&[u8]>) -> Result<()> {
        if let Some(err) = self.take_background_error() {
            return Err(err);
        }

        let seq = {
            let mut buf = self.append.lock().expect(POISONED);
            record::encode(&mut buf, key, value)?;
            (&self.file)
                .write_all(&buf)
                .map_err(|err| Error::io(&self.path, err))?;
            let seq = self.written.fetch_add(1, Ordering::Release) + 1;
            if self.policy == SyncPolicy::Always {
                // Held on purpose: one flush per append, serialized behind the
                // append lock, is exactly what this policy promises.
                self.flush()?;
            }
            seq
        };

        if self.policy == SyncPolicy::Group {
            self.sync_through(seq)?;
        }
        Ok(())
    }

    /// Returns once record `seq` is durable, flushing if nobody else is.
    fn sync_through(&self, seq: u64) -> Result<()> {
        let mut state = self.sync.lock().expect(POISONED);
        loop {
            if state.durable >= seq {
                return Ok(());
            }
            if state.in_progress {
                state = self.synced.wait(state).expect(POISONED);
                continue;
            }

            // This thread owns the next flush. It covers every record written
            // so far, which includes its own and everything appended by the
            // writers that will wait on it.
            let target = self.written.load(Ordering::Acquire);
            state.in_progress = true;
            drop(state);

            let flushed = self.flush();

            state = self.sync.lock().expect(POISONED);
            state.in_progress = false;
            if flushed.is_ok() {
                state.durable = state.durable.max(target);
            }
            self.synced.notify_all();
            flushed?;
        }
    }

    fn flush(&self) -> Result<()> {
        self.flushes.fetch_add(1, Ordering::Relaxed);
        // sync_data is fdatasync on Linux and F_FULLFSYNC on macOS, where it
        // drains the drive's own write cache. The macOS numbers are therefore
        // the cost of a real device flush, not of a cache handoff.
        self.file
            .sync_data()
            .map_err(|err| Error::io(&self.path, err))
    }

    fn take_background_error(&self) -> Option<Error> {
        self.background_error
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    }

    fn set_background_error(&self, err: Error) {
        let mut slot = self
            .background_error
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if slot.is_none() {
            *slot = Some(err);
        }
    }

    /// Waits up to `timeout`, returning whether the log is shutting down.
    fn wait_while_running(&self, timeout: Duration) -> bool {
        let guard = self.shutdown.lock().unwrap_or_else(PoisonError::into_inner);
        let (guard, _) = self
            .stopping
            .wait_timeout(guard, timeout)
            .unwrap_or_else(PoisonError::into_inner);
        *guard
    }
}

fn spawn_syncer(shared: &Arc<Shared>, interval: Duration) -> JoinHandle<()> {
    let shared = Arc::clone(shared);
    thread::Builder::new()
        .name("lsmkv-wal-sync".to_owned())
        .spawn(move || {
            loop {
                let stopping = shared.wait_while_running(interval);
                let target = shared.written.load(Ordering::Acquire);
                if let Err(err) = shared.sync_through(target) {
                    shared.set_background_error(err);
                }
                if stopping {
                    break;
                }
            }
        })
        .expect("spawn the log flush thread")
}

fn truncate(file: &File, path: &Path, len: u64) -> Result<()> {
    file.set_len(len).map_err(|err| Error::io(path, err))?;
    file.sync_all().map_err(|err| Error::io(path, err))
}

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;
    use std::sync::Barrier;

    use super::*;
    use crate::testutil::TempDir;

    fn records(path: &Path) -> Vec<Record> {
        let (_wal, records) = Wal::recover(path, SyncPolicy::Group).expect("recover");
        records
    }

    fn set_len(path: &Path, len: u64) {
        OpenOptions::new()
            .write(true)
            .open(path)
            .expect("open for truncation")
            .set_len(len)
            .expect("truncate");
    }

    fn file_len(path: &Path) -> u64 {
        std::fs::metadata(path).expect("metadata").len()
    }

    #[test]
    fn a_new_log_starts_empty_and_carries_a_header() {
        let dir = TempDir::new();
        let path = dir.join("wal");

        let (wal, replayed) = Wal::recover(&path, SyncPolicy::Group).expect("recover");

        assert!(replayed.is_empty());
        assert_eq!(file_len(wal.path()), FILE_HEADER_LEN_U64);
    }

    #[test]
    fn records_replay_in_the_order_they_were_appended() {
        let dir = TempDir::new();
        let path = dir.join("wal");

        {
            let (wal, _) = Wal::recover(&path, SyncPolicy::Group).expect("recover");
            wal.set(b"a", b"1").expect("set");
            wal.set(b"b", b"2").expect("set");
            wal.delete(b"a").expect("delete");
            wal.set(b"a", b"3").expect("set");
        }

        assert_eq!(
            records(&path),
            vec![
                Record::Set {
                    key: b"a".to_vec(),
                    value: b"1".to_vec()
                },
                Record::Set {
                    key: b"b".to_vec(),
                    value: b"2".to_vec()
                },
                Record::Delete { key: b"a".to_vec() },
                Record::Set {
                    key: b"a".to_vec(),
                    value: b"3".to_vec()
                },
            ]
        );
    }

    #[test]
    fn a_torn_tail_is_dropped_and_the_log_keeps_appending() {
        let dir = TempDir::new();
        let path = dir.join("wal");

        let intact_len = {
            let (wal, _) = Wal::recover(&path, SyncPolicy::Always).expect("recover");
            wal.set(b"first", b"1").expect("set");
            wal.set(b"second", b"2").expect("set");
            let len = file_len(&path);
            wal.set(b"third", b"3").expect("set");
            len
        };

        // What a crash in the middle of the third append leaves behind.
        set_len(&path, intact_len + 3);

        let (wal, replayed) = Wal::recover(&path, SyncPolicy::Always).expect("recover");
        assert_eq!(replayed.len(), 2);
        assert_eq!(replayed[1].key(), b"second");
        assert_eq!(file_len(&path), intact_len);

        wal.set(b"fourth", b"4").expect("set");
        drop(wal);

        let replayed = records(&path);
        assert_eq!(replayed.len(), 3);
        assert_eq!(replayed[2].key(), b"fourth");
    }

    #[test]
    fn a_record_whose_checksum_fails_ends_recovery() {
        let dir = TempDir::new();
        let path = dir.join("wal");

        let first_len = {
            let (wal, _) = Wal::recover(&path, SyncPolicy::Always).expect("recover");
            wal.set(b"first", b"1").expect("set");
            let len = file_len(&path);
            wal.set(b"second", b"2").expect("set");
            len
        };

        let mut bytes = std::fs::read(&path).expect("read");
        let payload_start = usize::try_from(first_len).expect("offset") + 8;
        bytes[payload_start] ^= 0b0100_0000;
        std::fs::write(&path, &bytes).expect("write");

        let mut replay = Wal::replay(&path).expect("replay");
        let replayed: Vec<Record> = replay.by_ref().map(|r| r.expect("record")).collect();

        assert_eq!(replayed.len(), 1);
        assert_eq!(replay.torn_at(), Some(first_len));
        assert_eq!(replay.valid_len(), first_len);
    }

    #[test]
    fn a_file_that_is_not_a_log_is_rejected() {
        let dir = TempDir::new();
        let path = dir.join("wal");
        std::fs::write(&path, b"this is not a log file at all").expect("write");

        let err = Wal::recover(&path, SyncPolicy::Group).expect_err("must reject");
        assert!(matches!(err, Error::Corrupt { .. }), "{err}");
    }

    #[test]
    fn an_empty_file_is_recovered_as_a_fresh_log() {
        let dir = TempDir::new();
        let path = dir.join("wal");
        std::fs::write(&path, b"").expect("write");

        let (wal, replayed) = Wal::recover(&path, SyncPolicy::Group).expect("recover");
        assert!(replayed.is_empty());
        wal.set(b"k", b"v").expect("set");
        drop(wal);

        assert_eq!(records(&path).len(), 1);
    }

    #[test]
    fn a_header_lost_to_a_crash_is_rewritten() {
        let dir = TempDir::new();
        let path = dir.join("wal");
        std::fs::write(&path, &MAGIC[..4]).expect("write");

        let (wal, replayed) = Wal::recover(&path, SyncPolicy::Group).expect("recover");
        assert!(replayed.is_empty());
        assert_eq!(file_len(&path), FILE_HEADER_LEN_U64);
        wal.set(b"k", b"v").expect("set");
        drop(wal);

        assert_eq!(records(&path).len(), 1);
    }

    #[test]
    fn every_append_flushes_under_always() {
        let dir = TempDir::new();
        let path = dir.join("wal");
        let (wal, _) = Wal::recover(&path, SyncPolicy::Always).expect("recover");

        for i in 0..10u8 {
            wal.set(&[i], b"v").expect("set");
        }

        assert_eq!(wal.flush_count(), 10);
    }

    #[test]
    fn group_commit_amortizes_flushes_across_concurrent_writers() {
        const WRITERS: usize = 8;
        const PER_WRITER: usize = 200;

        let dir = TempDir::new();
        let path = dir.join("wal");
        let (wal, _) = Wal::recover(&path, SyncPolicy::Group).expect("recover");
        let start = Barrier::new(WRITERS);

        thread::scope(|scope| {
            for writer in 0..WRITERS {
                let wal = &wal;
                let start = &start;
                scope.spawn(move || {
                    start.wait();
                    for i in 0..PER_WRITER {
                        let key = format!("{writer}:{i}");
                        wal.set(key.as_bytes(), b"v").expect("set");
                    }
                });
            }
        });

        let appends = (WRITERS * PER_WRITER) as u64;
        assert_eq!(wal.record_count(), appends);
        assert!(
            wal.flush_count() < appends,
            "{} flushes for {appends} appends, nothing was amortized",
            wal.flush_count()
        );
        drop(wal);

        assert_eq!(records(&path).len(), WRITERS * PER_WRITER);
    }

    #[test]
    fn the_interval_policy_flushes_in_the_background() {
        let dir = TempDir::new();
        let path = dir.join("wal");
        let (wal, _) =
            Wal::recover(&path, SyncPolicy::Interval(Duration::from_millis(5))).expect("recover");

        for i in 0..100u8 {
            wal.set(&[i], b"v").expect("set");
        }
        wal.sync().expect("sync");
        assert!(wal.flush_count() >= 1);
        drop(wal);

        assert_eq!(records(&path).len(), 100);
    }

    #[test]
    fn the_log_is_shared_across_threads() {
        const fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Wal>();
    }
}
