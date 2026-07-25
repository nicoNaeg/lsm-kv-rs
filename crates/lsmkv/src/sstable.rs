//! Immutable sorted file: a full memtable written out in one sequential pass,
//! read back through a sparse index.
//!
//! ```text
//! file   := block* filter index footer
//! block  := record* crc32:u32
//! record := seq:u64 | kind:u8 = 0 | key_len:u32 | key | value_len:u32 | value
//!         | seq:u64 | kind:u8 = 1 | key_len:u32 | key
//! filter := bloom bits | probes:u8 | crc32:u32
//! index  := min_key_len:u32 | min_key | max_key_len:u32 | max_key
//!           | entry* | crc32:u32
//! entry  := key_len:u32 | first key of the block | offset:u64 | len:u32
//! footer := index_offset:u64 | index_len:u32 | filter_offset:u64
//!           | filter_len:u32 | entries:u64 | crc32:u32 | version:u32
//!           | magic[8]
//! ```
//!
//! Records are sorted, and the index holds the first key of each block, so a
//! lookup binary searches the index in memory and reads exactly one block from
//! disk. Lengths cover the checksum that closes the block they describe.
//!
//! The checksum unit is the block, which is also the unit of I/O: one read, one
//! verification, amortized over the tens of records a block holds.
//!
//! The index and the Bloom filter are both loaded when the file is opened, so a
//! lookup for a key the file does not hold usually costs no I/O at all.

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::bloom::{self, Bloom};
use crate::checksum::crc32;
use crate::coding::{
    length_of, push_field, take_field, take_u8, take_u32, take_u64, u32_at, u64_at,
};
use crate::error::{Error, Result};
use crate::lookup::Lookup;

/// Target size of a data block. A block is closed once a record takes it past
/// this, so a record larger than a block gets a block of its own.
const BLOCK_SIZE: usize = 4096;

const MAGIC: &[u8] = b"LSMKVSST";
const FORMAT_VERSION: u32 = 2;
/// Index offset and length, filter offset and length, entry count, checksum,
/// version, magic.
const FOOTER_LEN: usize = 8 + 4 + 8 + 4 + 8 + 4 + 4 + 8;
/// Bytes of the footer the checksum covers.
const FOOTER_CHECKED: usize = 32;
const FOOTER_LEN_U64: u64 = FOOTER_LEN as u64;
/// Length of the checksum that closes a block.
const CRC_LEN: usize = 4;

const KIND_SET: u8 = 0;
const KIND_DELETE: u8 = 1;

/// One block, as the index describes it.
#[derive(Debug)]
struct BlockRef {
    first_key: Vec<u8>,
    offset: u64,
    /// Bytes to read, checksum included.
    len: u32,
}

/// Builds a sorted file, one block at a time.
///
/// Keys have to be added in increasing order, which is what the memtable hands
/// out for free.
#[derive(Debug)]
pub struct Writer {
    path: PathBuf,
    file: BufWriter<File>,
    block: Vec<u8>,
    blocks: Vec<BlockRef>,
    /// One hash per entry, kept for the filter built in `finish`.
    hashes: Vec<u64>,
    first_key: Vec<u8>,
    min_key: Vec<u8>,
    max_key: Vec<u8>,
    offset: u64,
    entries: u64,
}

impl Writer {
    /// Creates `path`, truncating anything already there.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Io`] if the file cannot be created.
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)
            .map_err(|err| Error::io(&path, err))?;

        Ok(Self {
            path,
            file: BufWriter::new(file),
            block: Vec::with_capacity(BLOCK_SIZE * 2),
            blocks: Vec::new(),
            hashes: Vec::new(),
            first_key: Vec::new(),
            min_key: Vec::new(),
            max_key: Vec::new(),
            offset: 0,
            entries: 0,
        })
    }

    /// Appends one entry. `value` is `None` for a tombstone.
    ///
    /// # Errors
    ///
    /// Returns [`Error::TooLarge`] if the key or the value exceeds 4 GiB, and
    /// [`Error::Io`] if a full block cannot be written.
    ///
    /// # Panics
    ///
    /// Panics in debug builds if `key` is not greater than the previous one.
    pub fn add(&mut self, key: &[u8], seq: u64, value: Option<&[u8]>) -> Result<()> {
        debug_assert!(
            self.entries == 0 || key > self.max_key.as_slice(),
            "entries must be added in increasing key order"
        );

        if self.block.is_empty() {
            self.first_key.clear();
            self.first_key.extend_from_slice(key);
        }
        if self.entries == 0 {
            self.min_key.extend_from_slice(key);
        }
        self.max_key.clear();
        self.max_key.extend_from_slice(key);
        self.hashes.push(bloom::hash(key));
        self.entries += 1;

        self.block.extend_from_slice(&seq.to_le_bytes());
        if let Some(value) = value {
            self.block.push(KIND_SET);
            push_field(&mut self.block, key)?;
            push_field(&mut self.block, value)?;
        } else {
            self.block.push(KIND_DELETE);
            push_field(&mut self.block, key)?;
        }

        if self.block.len() >= BLOCK_SIZE {
            self.close_block()?;
        }
        Ok(())
    }

    /// Writes the index and the footer, then flushes the file to the device.
    ///
    /// # Errors
    ///
    /// Returns [`Error::TooLarge`] if the index does not fit its own length
    /// fields, and [`Error::Io`] if the file cannot be written or flushed.
    pub fn finish(mut self) -> Result<()> {
        self.close_block()?;

        let filter = Bloom::build(&self.hashes, bloom::BITS_PER_KEY);
        let filter_offset = self.offset;
        let mut filter_block = filter.as_bytes().to_vec();
        filter_block.extend_from_slice(&crc32(filter.as_bytes()).to_le_bytes());
        let filter_len = length_of(&filter_block)?;
        self.write(&filter_block)?;

        let index_offset = self.offset;
        let mut index = Vec::new();
        push_field(&mut index, &self.min_key)?;
        push_field(&mut index, &self.max_key)?;
        for block in &self.blocks {
            push_field(&mut index, &block.first_key)?;
            index.extend_from_slice(&block.offset.to_le_bytes());
            index.extend_from_slice(&block.len.to_le_bytes());
        }
        index.extend_from_slice(&crc32(&index).to_le_bytes());
        let index_len = length_of(&index)?;
        self.write(&index)?;

        let mut footer = Vec::with_capacity(FOOTER_LEN);
        footer.extend_from_slice(&index_offset.to_le_bytes());
        footer.extend_from_slice(&index_len.to_le_bytes());
        footer.extend_from_slice(&filter_offset.to_le_bytes());
        footer.extend_from_slice(&filter_len.to_le_bytes());
        footer.extend_from_slice(&self.entries.to_le_bytes());
        footer.extend_from_slice(&crc32(&footer).to_le_bytes());
        footer.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        footer.extend_from_slice(MAGIC);
        self.write(&footer)?;

        let file = self
            .file
            .into_inner()
            .map_err(|err| Error::io(&self.path, err.into_error()))?;
        // The caller deletes the log this file replaces, so the bytes have to
        // be on the device before it returns.
        file.sync_all().map_err(|err| Error::io(&self.path, err))
    }

    fn close_block(&mut self) -> Result<()> {
        if self.block.is_empty() {
            return Ok(());
        }
        let crc = crc32(&self.block);
        self.block.extend_from_slice(&crc.to_le_bytes());

        let len = length_of(&self.block)?;
        let offset = self.offset;
        let block = std::mem::take(&mut self.block);
        self.write(&block)?;
        self.block = block;
        self.block.clear();

        self.blocks.push(BlockRef {
            first_key: std::mem::take(&mut self.first_key),
            offset,
            len,
        });
        Ok(())
    }

    fn write(&mut self, bytes: &[u8]) -> Result<()> {
        self.file
            .write_all(bytes)
            .map_err(|err| Error::io(&self.path, err))?;
        self.offset += bytes.len() as u64;
        Ok(())
    }
}

/// A sorted file, opened for reading.
///
/// The index and the key range live in memory; a lookup reads one block.
#[derive(Debug)]
pub struct SsTable {
    path: PathBuf,
    file: File,
    blocks: Vec<BlockRef>,
    filter: Bloom,
    min_key: Vec<u8>,
    max_key: Vec<u8>,
    entries: u64,
    bytes: u64,
    /// Blocks actually read from the device, which is what the filter is meant
    /// to keep down.
    block_reads: AtomicU64,
}

impl SsTable {
    /// Opens `path` and loads its index.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Corrupt`] if the footer or the index does not check
    /// out, and [`Error::Io`] if the file cannot be read.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = File::open(&path).map_err(|err| Error::io(&path, err))?;
        let file_len = file.metadata().map_err(|err| Error::io(&path, err))?.len();
        if file_len < FOOTER_LEN_U64 {
            return Err(Error::corrupt(&path, "shorter than a footer"));
        }

        let mut footer = [0u8; FOOTER_LEN];
        file.read_exact_at(&mut footer, file_len - FOOTER_LEN_U64)
            .map_err(|err| Error::io(&path, err))?;
        if &footer[FOOTER_LEN - MAGIC.len()..] != MAGIC {
            return Err(Error::corrupt(&path, "not a sorted table"));
        }
        let version = u32_at(&footer, FOOTER_LEN - MAGIC.len() - 4);
        if version != FORMAT_VERSION {
            return Err(Error::corrupt(
                &path,
                format!("format version {version}, expected {FORMAT_VERSION}"),
            ));
        }
        if crc32(&footer[..FOOTER_CHECKED]) != u32_at(&footer, FOOTER_CHECKED) {
            return Err(Error::corrupt(&path, "footer checksum mismatch"));
        }

        let index_offset = u64_at(&footer, 0);
        let index_len = u32_at(&footer, 8);
        let filter_offset = u64_at(&footer, 12);
        let filter_len = u32_at(&footer, 20);
        let entries = u64_at(&footer, 24);
        let trailer = file_len - FOOTER_LEN_U64;
        if index_offset + u64::from(index_len) > trailer
            || filter_offset + u64::from(filter_len) > trailer
        {
            return Err(Error::corrupt(&path, "footer points past the file"));
        }

        let index = read_checked(&file, &path, index_offset, index_len)?;
        let mut cursor = index.as_slice();
        let (min_key, max_key, blocks) = parse_index(&mut cursor)
            .ok_or_else(|| Error::corrupt(&path, "index is not readable"))?;

        let filter = read_checked(&file, &path, filter_offset, filter_len)?;
        let filter = Bloom::decode(&filter)
            .ok_or_else(|| Error::corrupt(&path, "filter is not readable"))?;

        Ok(Self {
            path,
            file,
            blocks,
            filter,
            min_key,
            max_key,
            entries,
            bytes: file_len,
            block_reads: AtomicU64::new(0),
        })
    }

    /// Looks `key` up, reading at most one block.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Corrupt`] if the block does not match its checksum or
    /// cannot be parsed, and [`Error::Io`] if it cannot be read.
    pub fn get(&self, key: &[u8]) -> Result<Lookup> {
        if key < self.min_key.as_slice() || key > self.max_key.as_slice() {
            return Ok(Lookup::Missing);
        }
        // A filter that says no is never wrong, and saying no here is what
        // saves the block read below.
        if !self.filter.may_contain(key) {
            return Ok(Lookup::Missing);
        }
        // The last block whose first key is not past the one we want is the
        // only block that can hold it.
        let candidate = self
            .blocks
            .partition_point(|block| block.first_key.as_slice() <= key);
        let Some(block) = candidate.checked_sub(1).and_then(|i| self.blocks.get(i)) else {
            return Ok(Lookup::Missing);
        };

        self.block_reads.fetch_add(1, Ordering::Relaxed);
        let bytes = read_checked(&self.file, &self.path, block.offset, block.len)?;
        scan_block(&bytes, key).ok_or_else(|| Error::corrupt(&self.path, "block is not readable"))
    }

    /// Smallest key in the file.
    pub fn min_key(&self) -> &[u8] {
        &self.min_key
    }

    /// Largest key in the file.
    pub fn max_key(&self) -> &[u8] {
        &self.max_key
    }

    /// Entries the file holds, tombstones included.
    pub fn entries(&self) -> u64 {
        self.entries
    }

    /// Size of the file on disk.
    pub fn size_bytes(&self) -> u64 {
        self.bytes
    }

    /// Blocks the file is cut into, which is also the size of its index.
    pub fn block_count(&self) -> usize {
        self.blocks.len()
    }

    /// Blocks read from the device since the file was opened. A lookup the
    /// filter rejects adds nothing here.
    pub fn block_reads(&self) -> u64 {
        self.block_reads.load(Ordering::Relaxed)
    }

    /// Bits the Bloom filter holds.
    pub fn filter_bits(&self) -> usize {
        self.filter.bits()
    }

    /// Path of the file.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Reads `len` bytes at `offset` and verifies the checksum that closes them.
fn read_checked(file: &File, path: &Path, offset: u64, len: u32) -> Result<Vec<u8>> {
    let len = usize::try_from(len).map_err(|_| Error::corrupt(path, "length out of range"))?;
    if len < CRC_LEN {
        return Err(Error::corrupt(path, "block shorter than its checksum"));
    }
    let mut bytes = vec![0u8; len];
    file.read_exact_at(&mut bytes, offset)
        .map_err(|err| Error::io(path, err))?;

    let split = len - CRC_LEN;
    if crc32(&bytes[..split]) != u32_at(&bytes, split) {
        return Err(Error::corrupt(path, "checksum mismatch"));
    }
    bytes.truncate(split);
    Ok(bytes)
}

fn parse_index(cursor: &mut &[u8]) -> Option<(Vec<u8>, Vec<u8>, Vec<BlockRef>)> {
    let min_key = take_field(cursor)?.to_vec();
    let max_key = take_field(cursor)?.to_vec();
    let mut blocks = Vec::new();
    while !cursor.is_empty() {
        let first_key = take_field(cursor)?.to_vec();
        let offset = take_u64(cursor)?;
        let len = take_u32(cursor)?;
        blocks.push(BlockRef {
            first_key,
            offset,
            len,
        });
    }
    Some((min_key, max_key, blocks))
}

/// Walks a block for `key`. Returns `None` if the block is malformed.
fn scan_block(mut cursor: &[u8], key: &[u8]) -> Option<Lookup> {
    while !cursor.is_empty() {
        let _seq = take_u64(&mut cursor)?;
        let kind = take_u8(&mut cursor)?;
        let found = take_field(&mut cursor)?;
        let value = match kind {
            KIND_SET => Some(take_field(&mut cursor)?),
            KIND_DELETE => None,
            _ => return None,
        };

        match found.cmp(key) {
            std::cmp::Ordering::Less => {}
            std::cmp::Ordering::Equal => {
                return Some(value.map_or(Lookup::Deleted, |value| Lookup::Found(value.to_vec())));
            }
            // Records are sorted, so nothing further down can match.
            std::cmp::Ordering::Greater => return Some(Lookup::Missing),
        }
    }
    Some(Lookup::Missing)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;
    use crate::testutil::TempDir;

    /// Writes `count` entries keyed `key:000000` and up, every seventh a
    /// tombstone, and returns the file.
    fn write_table(path: &Path, count: usize) -> SsTable {
        let mut writer = Writer::create(path).expect("create");
        for i in 0..count {
            let key = format!("key:{i:06}");
            let value = format!("value:{i}");
            let seq = i as u64 + 1;
            if i % 7 == 0 {
                writer.add(key.as_bytes(), seq, None).expect("add");
            } else {
                writer
                    .add(key.as_bytes(), seq, Some(value.as_bytes()))
                    .expect("add");
            }
        }
        writer.finish().expect("finish");
        SsTable::open(path).expect("open")
    }

    #[test]
    fn every_entry_is_read_back() {
        let dir = TempDir::new();
        let table = write_table(&dir.join("t.sst"), 500);

        assert_eq!(table.entries(), 500);
        for i in 0..500 {
            let key = format!("key:{i:06}");
            let expected = if i % 7 == 0 {
                Lookup::Deleted
            } else {
                Lookup::Found(format!("value:{i}").into_bytes())
            };
            assert_eq!(table.get(key.as_bytes()).expect("get"), expected, "key {i}");
        }
    }

    #[test]
    fn a_lookup_spans_several_blocks() {
        let dir = TempDir::new();
        let table = write_table(&dir.join("t.sst"), 500);

        assert!(
            table.block_count() > 1,
            "500 entries must not fit in one 4 KiB block"
        );
        assert_eq!(table.min_key(), b"key:000000");
        assert_eq!(table.max_key(), b"key:000499");
    }

    #[test]
    fn keys_outside_the_file_are_missing() {
        let dir = TempDir::new();
        let table = write_table(&dir.join("t.sst"), 100);

        assert_eq!(table.get(b"aaa").expect("get"), Lookup::Missing);
        assert_eq!(table.get(b"zzz").expect("get"), Lookup::Missing);
        assert_eq!(table.get(b"key:000042x").expect("get"), Lookup::Missing);
    }

    #[test]
    fn a_single_entry_file_works() {
        let dir = TempDir::new();
        let path = dir.join("t.sst");
        let mut writer = Writer::create(&path).expect("create");
        writer.add(b"only", 1, Some(b"value")).expect("add");
        writer.finish().expect("finish");

        let table = SsTable::open(&path).expect("open");
        assert_eq!(table.block_count(), 1);
        assert_eq!(
            table.get(b"only").expect("get"),
            Lookup::Found(b"value".to_vec())
        );
        assert_eq!(table.get(b"other").expect("get"), Lookup::Missing);
    }

    #[test]
    fn keys_and_values_are_binary_safe() {
        let dir = TempDir::new();
        let path = dir.join("t.sst");
        let mut writer = Writer::create(&path).expect("create");
        writer.add(&[0, 1, 255], 1, Some(&[0, 0, 0])).expect("add");
        writer.add(&[0, 2], 2, Some(b"")).expect("add");
        writer.finish().expect("finish");

        let table = SsTable::open(&path).expect("open");
        assert_eq!(
            table.get(&[0, 1, 255]).expect("get"),
            Lookup::Found(vec![0, 0, 0])
        );
        assert_eq!(table.get(&[0, 2]).expect("get"), Lookup::Found(Vec::new()));
    }

    #[test]
    fn a_value_larger_than_a_block_gets_its_own_block() {
        let dir = TempDir::new();
        let path = dir.join("t.sst");
        let big = vec![b'x'; BLOCK_SIZE * 3];
        let mut writer = Writer::create(&path).expect("create");
        writer.add(b"a", 1, Some(&big)).expect("add");
        writer.add(b"b", 2, Some(b"small")).expect("add");
        writer.finish().expect("finish");

        let table = SsTable::open(&path).expect("open");
        assert_eq!(table.block_count(), 2);
        assert_eq!(table.get(b"a").expect("get"), Lookup::Found(big));
    }

    #[test]
    fn keys_the_file_never_held_cost_no_block_read() {
        const PROBES: usize = 1000;

        let dir = TempDir::new();
        let table = write_table(&dir.join("t.sst"), 2000);
        assert_eq!(table.block_reads(), 0);

        for i in 0..PROBES {
            // Sorts between two keys the file holds, so the key range cannot
            // reject it and only the filter can.
            let key = format!("key:{i:06}x");
            assert_eq!(table.get(key.as_bytes()).expect("get"), Lookup::Missing);
        }

        // Ten bits per key puts the theoretical count at eight of a thousand.
        assert!(
            table.block_reads() < 30,
            "{} block reads for {PROBES} absent keys",
            table.block_reads()
        );
    }

    #[test]
    fn a_key_the_file_holds_still_costs_one_block_read() {
        let dir = TempDir::new();
        let table = write_table(&dir.join("t.sst"), 2000);

        table.get(b"key:001000").expect("get");

        assert_eq!(table.block_reads(), 1);
    }

    #[test]
    fn the_filter_is_sized_from_the_entry_count() {
        let dir = TempDir::new();
        let table = write_table(&dir.join("t.sst"), 1000);

        assert_eq!(table.filter_bits(), 1000 * bloom::BITS_PER_KEY);
    }

    #[test]
    fn a_corrupted_block_is_reported() {
        let dir = TempDir::new();
        let path = dir.join("t.sst");
        write_table(&path, 200);

        let mut bytes = fs::read(&path).expect("read");
        bytes[64] ^= 0b0010_0000;
        fs::write(&path, &bytes).expect("write");

        let table = SsTable::open(&path).expect("open");
        let err = table
            .get(b"key:000000")
            .expect_err("must report corruption");
        assert!(matches!(err, Error::Corrupt { .. }), "{err}");
    }

    #[test]
    fn a_corrupted_footer_is_reported() {
        let dir = TempDir::new();
        let path = dir.join("t.sst");
        write_table(&path, 20);

        let mut bytes = fs::read(&path).expect("read");
        let len = bytes.len();
        bytes[len - FOOTER_LEN] ^= 0b0000_0001;
        fs::write(&path, &bytes).expect("write");

        let err = SsTable::open(&path).expect_err("must reject");
        assert!(matches!(err, Error::Corrupt { .. }), "{err}");
    }

    #[test]
    fn a_truncated_file_is_rejected() {
        let dir = TempDir::new();
        let path = dir.join("t.sst");
        write_table(&path, 20);

        let bytes = fs::read(&path).expect("read");
        fs::write(&path, &bytes[..bytes.len() / 2]).expect("write");

        assert!(SsTable::open(&path).is_err());
    }

    #[test]
    fn a_file_that_is_not_a_table_is_rejected() {
        let dir = TempDir::new();
        let path = dir.join("t.sst");
        fs::write(&path, vec![0u8; FOOTER_LEN * 2]).expect("write");

        let err = SsTable::open(&path).expect_err("must reject");
        assert!(matches!(err, Error::Corrupt { .. }), "{err}");
    }

    #[test]
    fn the_table_is_shared_across_threads() {
        const fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<SsTable>();
    }
}
