//! What the store holds: which files, at which level, and where its numbering
//! stands.
//!
//! The whole state is rewritten into a temporary file and renamed into place on
//! every change, which makes that rename the commit point of a flush or a
//! compaction. A table the manifest does not name is an orphan a crash left
//! behind, and opening the store deletes it.
//!
//! The state stays small, a few tens of bytes per file, so rewriting it whole
//! costs less than the machinery to replay a log of edits, and an atomic rename
//! is a sturdier primitive than the tail of a log.
//!
//! ```text
//! manifest := magic[8] | version:u32 | last_seq:u64 | next_number:u64
//!             | file_count:u32 | file* | crc32:u32
//! file     := number:u64 | level:u32 | bytes:u64 | entries:u64
//!             | min_key_len:u32 | min_key | max_key_len:u32 | max_key
//! ```

use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::checksum::crc32;
use crate::coding::{push_field, take, take_field, take_u32, take_u64};
use crate::error::{Error, Result};

const MAGIC: &[u8] = b"LSMKVMAN";
const FORMAT_VERSION: u32 = 1;
const FILE_NAME: &str = "MANIFEST";
const TEMP_NAME: &str = "MANIFEST.tmp";

/// One file the store holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileMeta {
    /// File number, which also names it on disk and orders it in time.
    pub number: u64,
    /// Level it sits at. Level 0 holds files that may overlap.
    pub level: usize,
    /// Smallest key in the file.
    pub min_key: Vec<u8>,
    /// Largest key in the file.
    pub max_key: Vec<u8>,
    /// Size of the file on disk.
    pub bytes: u64,
    /// Entries it holds, tombstones included.
    pub entries: u64,
}

impl FileMeta {
    /// Whether this file's key range touches `[min, max]`.
    pub fn overlaps(&self, min: &[u8], max: &[u8]) -> bool {
        self.min_key.as_slice() <= max && min <= self.max_key.as_slice()
    }

    /// Whether this file's key range holds `key`.
    pub fn covers(&self, key: &[u8]) -> bool {
        self.min_key.as_slice() <= key && key <= self.max_key.as_slice()
    }
}

/// The complete state of a store.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Snapshot {
    /// Live files, in no particular order.
    pub files: Vec<FileMeta>,
    /// Highest sequence number the store has used. Recovery starts from here,
    /// so numbering survives a store whose logs have all been flushed away.
    pub last_seq: u64,
    /// Next number to give a log or a table.
    pub next_number: u64,
}

impl Snapshot {
    /// Files at `level`.
    pub fn at_level(&self, level: usize) -> impl Iterator<Item = &FileMeta> {
        self.files.iter().filter(move |file| file.level == level)
    }

    /// Deepest level holding a file.
    pub fn deepest_level(&self) -> usize {
        self.files.iter().map(|file| file.level).max().unwrap_or(0)
    }

    /// Bytes held at `level`.
    pub fn bytes_at_level(&self, level: usize) -> u64 {
        self.at_level(level).map(|file| file.bytes).sum()
    }

    fn encode(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::with_capacity(64 + self.files.len() * 64);
        buf.extend_from_slice(MAGIC);
        buf.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        buf.extend_from_slice(&self.last_seq.to_le_bytes());
        buf.extend_from_slice(&self.next_number.to_le_bytes());

        let count = u32::try_from(self.files.len()).map_err(|_| Error::TooLarge {
            len: self.files.len(),
        })?;
        buf.extend_from_slice(&count.to_le_bytes());
        for file in &self.files {
            let level =
                u32::try_from(file.level).map_err(|_| Error::TooLarge { len: file.level })?;
            buf.extend_from_slice(&file.number.to_le_bytes());
            buf.extend_from_slice(&level.to_le_bytes());
            buf.extend_from_slice(&file.bytes.to_le_bytes());
            buf.extend_from_slice(&file.entries.to_le_bytes());
            push_field(&mut buf, &file.min_key)?;
            push_field(&mut buf, &file.max_key)?;
        }

        let checksum = crc32(&buf);
        buf.extend_from_slice(&checksum.to_le_bytes());
        Ok(buf)
    }

    fn decode(bytes: &[u8]) -> Option<Self> {
        let split = bytes.len().checked_sub(4)?;
        let (body, checksum) = bytes.split_at(split);
        if crc32(body) != u32::from_le_bytes(checksum.try_into().ok()?) {
            return None;
        }

        let mut cursor = body;
        if take(&mut cursor, MAGIC.len())? != MAGIC || take_u32(&mut cursor)? != FORMAT_VERSION {
            return None;
        }
        let last_seq = take_u64(&mut cursor)?;
        let next_number = take_u64(&mut cursor)?;

        let count = take_u32(&mut cursor)?;
        let mut files = Vec::with_capacity(count as usize);
        for _ in 0..count {
            files.push(FileMeta {
                number: take_u64(&mut cursor)?,
                level: usize::try_from(take_u32(&mut cursor)?).ok()?,
                bytes: take_u64(&mut cursor)?,
                entries: take_u64(&mut cursor)?,
                min_key: take_field(&mut cursor)?.to_vec(),
                max_key: take_field(&mut cursor)?.to_vec(),
            });
        }

        cursor.is_empty().then_some(Self {
            files,
            last_seq,
            next_number,
        })
    }
}

/// Reads and writes the state of the store held in one directory.
#[derive(Debug)]
pub struct Manifest {
    dir: PathBuf,
    path: PathBuf,
    temp: PathBuf,
}

impl Manifest {
    /// The manifest of the store in `dir`, whether or not it exists yet.
    pub fn new(dir: impl AsRef<Path>) -> Self {
        let dir = dir.as_ref().to_path_buf();
        Self {
            path: dir.join(FILE_NAME),
            temp: dir.join(TEMP_NAME),
            dir,
        }
    }

    /// Reads the state, or `None` if the store has none yet.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Corrupt`] if the file does not check out, and
    /// [`Error::Io`] if it cannot be read.
    pub fn load(&self) -> Result<Option<Snapshot>> {
        let bytes = match fs::read(&self.path) {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(Error::io(&self.path, err)),
        };
        Snapshot::decode(&bytes)
            .map(Some)
            .ok_or_else(|| Error::corrupt(&self.path, "not a readable manifest"))
    }

    /// Replaces the state with `snapshot`, atomically.
    ///
    /// # Errors
    ///
    /// Returns [`Error::TooLarge`] if a key does not fit its length field, and
    /// [`Error::Io`] if the file cannot be written, renamed or flushed.
    pub fn store(&self, snapshot: &Snapshot) -> Result<()> {
        let bytes = snapshot.encode()?;

        let mut file = File::create(&self.temp).map_err(|err| Error::io(&self.temp, err))?;
        file.write_all(&bytes)
            .map_err(|err| Error::io(&self.temp, err))?;
        // On the device before the rename, so the rename can only ever publish
        // a complete manifest.
        file.sync_all().map_err(|err| Error::io(&self.temp, err))?;
        drop(file);

        fs::rename(&self.temp, &self.path).map_err(|err| Error::io(&self.path, err))?;
        // And the rename itself has to be durable, since it is the commit point
        // for everything the snapshot describes.
        let dir = File::open(&self.dir).map_err(|err| Error::io(&self.dir, err))?;
        dir.sync_all().map_err(|err| Error::io(&self.dir, err))
    }

    /// Path of the manifest file.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::TempDir;

    fn meta(number: u64, level: usize, min: &[u8], max: &[u8]) -> FileMeta {
        FileMeta {
            number,
            level,
            min_key: min.to_vec(),
            max_key: max.to_vec(),
            bytes: 4096,
            entries: 10,
        }
    }

    fn snapshot() -> Snapshot {
        Snapshot {
            files: vec![
                meta(1, 0, b"a", b"m"),
                meta(2, 0, b"b", b"z"),
                meta(3, 1, b"a", b"f"),
            ],
            last_seq: 4242,
            next_number: 7,
        }
    }

    #[test]
    fn a_snapshot_survives_a_roundtrip() {
        let dir = TempDir::new();
        let manifest = Manifest::new(dir.join("store"));
        std::fs::create_dir_all(dir.join("store")).expect("create");

        assert_eq!(manifest.load().expect("load"), None);
        manifest.store(&snapshot()).expect("store");

        assert_eq!(manifest.load().expect("load"), Some(snapshot()));
    }

    #[test]
    fn an_empty_snapshot_survives_a_roundtrip() {
        let dir = TempDir::new();
        std::fs::create_dir_all(dir.join("store")).expect("create");
        let manifest = Manifest::new(dir.join("store"));

        manifest.store(&Snapshot::default()).expect("store");

        assert_eq!(manifest.load().expect("load"), Some(Snapshot::default()));
    }

    #[test]
    fn storing_twice_leaves_the_second_state() {
        let dir = TempDir::new();
        std::fs::create_dir_all(dir.join("store")).expect("create");
        let manifest = Manifest::new(dir.join("store"));

        manifest.store(&snapshot()).expect("store");
        let mut second = snapshot();
        second.files.remove(0);
        second.last_seq = 9999;
        manifest.store(&second).expect("store");

        assert_eq!(manifest.load().expect("load"), Some(second));
        assert!(
            !dir.join("store").join(TEMP_NAME).exists(),
            "the temporary file must not survive the rename"
        );
    }

    #[test]
    fn a_corrupted_manifest_is_reported() {
        let dir = TempDir::new();
        std::fs::create_dir_all(dir.join("store")).expect("create");
        let manifest = Manifest::new(dir.join("store"));
        manifest.store(&snapshot()).expect("store");

        let mut bytes = std::fs::read(manifest.path()).expect("read");
        bytes[20] ^= 0b0000_1000;
        std::fs::write(manifest.path(), &bytes).expect("write");

        let err = manifest.load().expect_err("must reject");
        assert!(matches!(err, Error::Corrupt { .. }), "{err}");
    }

    #[test]
    fn a_truncated_manifest_is_reported() {
        let dir = TempDir::new();
        std::fs::create_dir_all(dir.join("store")).expect("create");
        let manifest = Manifest::new(dir.join("store"));
        manifest.store(&snapshot()).expect("store");

        let bytes = std::fs::read(manifest.path()).expect("read");
        std::fs::write(manifest.path(), &bytes[..bytes.len() - 8]).expect("write");

        assert!(manifest.load().is_err());
    }

    #[test]
    fn levels_are_queryable() {
        let snapshot = snapshot();

        assert_eq!(snapshot.at_level(0).count(), 2);
        assert_eq!(snapshot.at_level(1).count(), 1);
        assert_eq!(snapshot.deepest_level(), 1);
        assert_eq!(snapshot.bytes_at_level(0), 8192);
    }

    #[test]
    fn key_ranges_answer_overlap_and_cover() {
        let file = meta(1, 0, b"d", b"m");

        assert!(file.covers(b"d"));
        assert!(file.covers(b"m"));
        assert!(!file.covers(b"c"));
        assert!(!file.covers(b"n"));

        assert!(file.overlaps(b"a", b"d"), "touching at the low end");
        assert!(file.overlaps(b"m", b"z"), "touching at the high end");
        assert!(file.overlaps(b"e", b"f"), "fully inside");
        assert!(!file.overlaps(b"a", b"c"));
        assert!(!file.overlaps(b"n", b"z"));
    }
}
