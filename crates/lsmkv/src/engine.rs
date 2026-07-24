//! The storage engine: a write-ahead log and the sorted table it feeds.

use std::fs;
use std::path::{Path, PathBuf};

use crate::error::{Error, Result};
use crate::lookup::Lookup;
use crate::memtable::Memtable;
use crate::wal::{Record, SyncPolicy, Wal};

/// Name of the log inside the data directory.
const WAL_FILE: &str = "wal";

/// A key-value store over one data directory.
///
/// Every method takes `&self`: the engine is shared across threads behind an
/// [`Arc`](std::sync::Arc), and does its own locking.
#[derive(Debug)]
pub struct Engine {
    dir: PathBuf,
    wal: Wal,
    memtable: Memtable,
}

impl Engine {
    /// Opens the store held in `dir`, creating the directory if needed and
    /// rebuilding memory from the log.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Corrupt`] if the log is not readable as one, and
    /// [`Error::Io`] if the directory or the log cannot be opened.
    pub fn open(dir: impl AsRef<Path>, sync: SyncPolicy) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir).map_err(|err| Error::io(&dir, err))?;

        let (wal, records) = Wal::recover(dir.join(WAL_FILE), sync)?;
        let memtable = Memtable::new();
        for (index, record) in records.into_iter().enumerate() {
            // Replay order is append order, which is what the log's own
            // sequence numbers count.
            let seq = index as u64 + 1;
            match record {
                Record::Set { key, value } => memtable.insert(&key, seq, Some(value)),
                Record::Delete { key } => memtable.insert(&key, seq, None),
            };
        }

        Ok(Self { dir, wal, memtable })
    }

    /// Reads `key`.
    ///
    /// # Errors
    ///
    /// None today. The signature already carries the failures the read path
    /// gains once it reaches the files on disk.
    ///
    /// # Panics
    ///
    /// Panics if a previous writer panicked while holding the memtable.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        match self.memtable.get(key) {
            Lookup::Found(value) => Ok(Some(value)),
            // Once SSTables exist, only `Missing` continues the search: a
            // tombstone is an answer, and it stops the lookup here.
            Lookup::Deleted | Lookup::Missing => Ok(None),
        }
    }

    /// Binds `key` to `value`.
    ///
    /// The record reaches the log first, and becomes visible in memory only
    /// once the sync policy considers it durable.
    ///
    /// # Errors
    ///
    /// Returns [`Error::TooLarge`] if the key or the value exceeds 4 GiB, and
    /// [`Error::Io`] if the log cannot be written or flushed.
    ///
    /// # Panics
    ///
    /// Panics if a previous writer panicked while appending or while holding
    /// the memtable.
    pub fn set(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let seq = self.wal.set(key, value)?;
        self.memtable.insert(key, seq, Some(value.to_vec()));
        Ok(())
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
        let seq = self.wal.delete(key)?;
        self.memtable.insert(key, seq, None);
        Ok(())
    }

    /// Pushes everything written so far to the device.
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
    /// Panics if a previous writer panicked while appending.
    pub fn sync(&self) -> Result<()> {
        self.wal.sync()
    }

    /// Keys held in memory, tombstones included.
    ///
    /// # Panics
    ///
    /// Panics if a previous writer panicked while holding the memtable.
    pub fn len(&self) -> usize {
        self.memtable.len()
    }

    /// Whether the store holds nothing.
    ///
    /// # Panics
    ///
    /// Panics if a previous writer panicked while holding the memtable.
    pub fn is_empty(&self) -> bool {
        self.memtable.is_empty()
    }

    /// Directory the store lives in.
    pub fn dir(&self) -> &Path {
        &self.dir
    }
}

#[cfg(test)]
mod tests {
    use std::thread;

    use super::*;
    use crate::testutil::TempDir;

    fn open(dir: &TempDir) -> Engine {
        Engine::open(dir.join("store"), SyncPolicy::Group).expect("open")
    }

    fn value(engine: &Engine, key: &[u8]) -> Option<Vec<u8>> {
        engine.get(key).expect("get")
    }

    #[test]
    fn a_value_is_written_and_read_back() {
        let dir = TempDir::new();
        let engine = open(&dir);

        engine.set(b"user:1", b"nicolas").expect("set");

        assert_eq!(value(&engine, b"user:1"), Some(b"nicolas".to_vec()));
        assert_eq!(value(&engine, b"user:2"), None);
    }

    #[test]
    fn a_delete_hides_the_value() {
        let dir = TempDir::new();
        let engine = open(&dir);

        engine.set(b"key", b"value").expect("set");
        engine.delete(b"key").expect("delete");

        assert_eq!(value(&engine, b"key"), None);
        assert_eq!(engine.len(), 1, "the tombstone still holds the key");
    }

    #[test]
    fn the_store_comes_back_as_it_was_left() {
        let dir = TempDir::new();
        {
            let engine = open(&dir);
            engine.set(b"kept", b"1").expect("set");
            engine.set(b"replaced", b"first").expect("set");
            engine.set(b"replaced", b"second").expect("set");
            engine.set(b"removed", b"gone").expect("set");
            engine.delete(b"removed").expect("delete");
        }

        let engine = open(&dir);
        assert_eq!(value(&engine, b"kept"), Some(b"1".to_vec()));
        assert_eq!(value(&engine, b"replaced"), Some(b"second".to_vec()));
        assert_eq!(value(&engine, b"removed"), None);
    }

    #[test]
    fn writes_after_a_reopen_still_win_over_replayed_ones() {
        let dir = TempDir::new();
        {
            let engine = open(&dir);
            engine.set(b"key", b"before").expect("set");
        }

        let engine = open(&dir);
        engine.set(b"key", b"after").expect("set");
        assert_eq!(value(&engine, b"key"), Some(b"after".to_vec()));
        drop(engine);

        // The sequence numbers of the new writes have to continue past the
        // replayed ones, otherwise the older record wins on the next recovery.
        let engine = open(&dir);
        assert_eq!(value(&engine, b"key"), Some(b"after".to_vec()));
    }

    #[test]
    fn memory_and_the_log_agree_after_concurrent_writers() {
        const WRITERS: usize = 8;
        const PER_WRITER: usize = 100;
        const KEYS: usize = 20;

        let dir = TempDir::new();
        let engine = open(&dir);

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

        let keys: Vec<String> = (0..KEYS).map(|i| format!("key:{i}")).collect();
        let in_memory: Vec<Option<Vec<u8>>> =
            keys.iter().map(|k| value(&engine, k.as_bytes())).collect();
        drop(engine);

        let engine = open(&dir);
        let replayed: Vec<Option<Vec<u8>>> =
            keys.iter().map(|k| value(&engine, k.as_bytes())).collect();

        assert_eq!(
            in_memory, replayed,
            "what memory held must be what the log rebuilds"
        );
    }

    #[test]
    fn the_engine_is_shared_across_threads() {
        const fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Engine>();
    }
}
