//! Sequential replay of a log file.

use std::fs::File;
use std::io::{self, BufReader, Read};
use std::path::{Path, PathBuf};

use super::record::{self, HEADER_LEN, HEADER_LEN_U64, Record};
use super::{FILE_HEADER_LEN, FILE_HEADER_LEN_U64, FORMAT_VERSION, MAGIC};
use crate::checksum::crc32_parts;
use crate::error::{Error, Result};

/// Records held by a log file, in the order they were appended.
///
/// Iteration stops at the first record that is not complete and intact, which
/// is what a crash in the middle of an append leaves at the end of the file.
/// [`Replay::torn_at`] then reports where that happened and
/// [`Replay::valid_len`] the length the file should be truncated to.
#[derive(Debug)]
pub struct Replay {
    path: PathBuf,
    reader: Option<BufReader<File>>,
    file_len: u64,
    pos: u64,
    torn_at: Option<u64>,
}

impl Replay {
    /// Opens `path` for replay. A file that does not exist replays as empty.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Corrupt`] if the file does not carry the log header,
    /// and [`Error::Io`] if it cannot be read.
    pub(crate) fn open(path: &Path) -> Result<Self> {
        let file = match File::open(path) {
            Ok(file) => file,
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                return Ok(Self::empty(path, None));
            }
            Err(err) => return Err(Error::io(path, err)),
        };

        let file_len = file.metadata().map_err(|err| Error::io(path, err))?.len();
        if file_len == 0 {
            return Ok(Self::empty(path, None));
        }
        if file_len < FILE_HEADER_LEN_U64 {
            // Crashed between creating the file and writing its header.
            return Ok(Self::empty(path, Some(0)));
        }

        let mut reader = BufReader::new(file);
        let mut header = [0u8; FILE_HEADER_LEN];
        reader
            .read_exact(&mut header)
            .map_err(|err| Error::io(path, err))?;
        if &header[..MAGIC.len()] != MAGIC {
            return Err(Error::corrupt(path, "not a log file"));
        }
        let version = u32_at(&header, MAGIC.len());
        if version != FORMAT_VERSION {
            return Err(Error::corrupt(
                path,
                format!("format version {version}, expected {FORMAT_VERSION}"),
            ));
        }

        Ok(Self {
            path: path.to_path_buf(),
            reader: Some(reader),
            file_len,
            pos: FILE_HEADER_LEN_U64,
            torn_at: None,
        })
    }

    fn empty(path: &Path, torn_at: Option<u64>) -> Self {
        Self {
            path: path.to_path_buf(),
            reader: None,
            file_len: 0,
            pos: 0,
            torn_at,
        }
    }

    /// Offset just past the last intact record, and the length the file is
    /// truncated to when a torn tail is discarded. Zero means the file does not
    /// even hold a usable header.
    pub fn valid_len(&self) -> u64 {
        self.pos
    }

    /// Offset of the first incomplete record, if the log ends on one.
    ///
    /// Meaningful once iteration has finished.
    pub fn torn_at(&self) -> Option<u64> {
        self.torn_at
    }

    fn read_record(&mut self) -> Result<Option<Record>> {
        let remaining = self.file_len - self.pos;
        if remaining == 0 {
            return Ok(None);
        }
        if remaining < HEADER_LEN_U64 {
            return Ok(self.tear());
        }

        let reader = self
            .reader
            .as_mut()
            .expect("iteration ends without a reader");
        let mut header = [0u8; HEADER_LEN];
        reader
            .read_exact(&mut header)
            .map_err(|err| Error::io(&self.path, err))?;
        let crc = u32_at(&header, 0);
        let len = u64::from(u32_at(&header, 4));

        // A length corrupted by a partial write must not become a huge
        // allocation, so it is bounded by what the file actually holds.
        if len > remaining - HEADER_LEN_U64 {
            return Ok(self.tear());
        }
        let mut payload = vec![0u8; usize::try_from(len).map_err(|_| overflow(&self.path))?];
        reader
            .read_exact(&mut payload)
            .map_err(|err| Error::io(&self.path, err))?;

        if crc32_parts(&header[4..], &payload) != crc {
            return Ok(self.tear());
        }
        let Some(record) = record::decode(&payload) else {
            return Ok(self.tear());
        };

        self.pos += HEADER_LEN_U64 + len;
        Ok(Some(record))
    }

    /// Marks the tail as incomplete and ends iteration.
    fn tear(&mut self) -> Option<Record> {
        self.torn_at = Some(self.pos);
        None
    }
}

impl Iterator for Replay {
    type Item = Result<Record>;

    fn next(&mut self) -> Option<Self::Item> {
        self.reader.as_ref()?;
        match self.read_record() {
            Ok(Some(record)) => Some(Ok(record)),
            Ok(None) => {
                self.reader = None;
                None
            }
            Err(err) => {
                self.reader = None;
                Some(Err(err))
            }
        }
    }
}

fn u32_at(bytes: &[u8], offset: usize) -> u32 {
    let mut field = [0u8; 4];
    field.copy_from_slice(&bytes[offset..offset + 4]);
    u32::from_le_bytes(field)
}

fn overflow(path: &Path) -> Error {
    Error::corrupt(path, "record longer than this platform can address")
}
