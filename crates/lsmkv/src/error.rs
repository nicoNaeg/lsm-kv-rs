//! Error type shared by every engine component.

use std::error::Error as StdError;
use std::fmt;
use std::io;
use std::path::{Path, PathBuf};

/// Result of any fallible engine operation.
pub type Result<T> = std::result::Result<T, Error>;

/// Everything the engine can fail on.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// A file operation failed. Carries the path, which `io::Error` drops.
    Io {
        /// File the operation applied to.
        path: PathBuf,
        /// Underlying operating system error.
        source: io::Error,
    },
    /// A file does not match the format the engine wrote, or its checksum does
    /// not match its contents.
    Corrupt {
        /// File the engine was reading.
        path: PathBuf,
        /// What the engine expected and what it found.
        detail: String,
    },
    /// A key or a value is longer than the on-disk format can describe.
    TooLarge {
        /// Length that was offered.
        len: usize,
    },
}

impl Error {
    /// Attaches `path` to an I/O error raised while operating on it.
    pub fn io(path: impl Into<PathBuf>, source: io::Error) -> Self {
        Self::Io {
            path: path.into(),
            source,
        }
    }

    /// Reports a file whose contents the engine cannot trust.
    pub fn corrupt(path: &Path, detail: impl Into<String>) -> Self {
        Self::Corrupt {
            path: path.to_path_buf(),
            detail: detail.into(),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io { path, source } => write!(f, "{}: {source}", path.display()),
            Self::Corrupt { path, detail } => write!(f, "{}: corrupt: {detail}", path.display()),
            Self::TooLarge { len } => {
                write!(
                    f,
                    "{len} bytes exceeds the 4 GiB limit for one key or value"
                )
            }
        }
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            Self::Corrupt { .. } | Self::TooLarge { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn io_error_keeps_the_path_and_the_cause() {
        let err = Error::io(
            "data/000001.wal",
            io::Error::new(io::ErrorKind::UnexpectedEof, "unexpected end of file"),
        );

        assert_eq!(err.to_string(), "data/000001.wal: unexpected end of file");
        assert!(err.source().is_some());
    }

    #[test]
    fn corrupt_error_reports_the_file_and_the_detail() {
        let err = Error::corrupt(Path::new("data/000001.sst"), "checksum mismatch on block 3");

        assert_eq!(
            err.to_string(),
            "data/000001.sst: corrupt: checksum mismatch on block 3"
        );
    }
}
