//! On-disk layout of one log record.
//!
//! ```text
//! record  := crc: u32 | len: u32 | payload[len]
//! payload := kind: u8 = 0 | key_len: u32 | key | value_len: u32 | value   (set)
//!          | kind: u8 = 1 | key_len: u32 | key                            (delete)
//! ```
//!
//! Integers are little endian. The checksum covers `len` as well as the
//! payload, so a length corrupted by a partial write is caught like any other
//! corruption instead of being trusted as a size.

use crate::checksum::crc32;
use crate::error::{Error, Result};

/// Bytes preceding the payload: the checksum and the payload length.
pub(crate) const HEADER_LEN: usize = 8;
/// Same, for arithmetic on file offsets.
pub(crate) const HEADER_LEN_U64: u64 = HEADER_LEN as u64;

const KIND_SET: u8 = 0;
const KIND_DELETE: u8 = 1;

/// One mutation, as stored in the log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Record {
    /// A key bound to a value.
    Set {
        /// Key that was written.
        key: Vec<u8>,
        /// Value it was bound to.
        value: Vec<u8>,
    },
    /// A key deleted, kept as a tombstone until compaction drops it.
    Delete {
        /// Key that was deleted.
        key: Vec<u8>,
    },
}

impl Record {
    /// Key this record applies to.
    pub fn key(&self) -> &[u8] {
        match self {
            Self::Set { key, .. } | Self::Delete { key } => key,
        }
    }
}

/// Encodes one record into `buf`, replacing what it held.
///
/// `value` is `None` for a delete.
pub(crate) fn encode(buf: &mut Vec<u8>, key: &[u8], value: Option<&[u8]>) -> Result<()> {
    let key_len = length_of(key)?;

    buf.clear();
    buf.extend_from_slice(&[0; HEADER_LEN]);
    if let Some(value) = value {
        let value_len = length_of(value)?;
        buf.push(KIND_SET);
        buf.extend_from_slice(&key_len.to_le_bytes());
        buf.extend_from_slice(key);
        buf.extend_from_slice(&value_len.to_le_bytes());
        buf.extend_from_slice(value);
    } else {
        buf.push(KIND_DELETE);
        buf.extend_from_slice(&key_len.to_le_bytes());
        buf.extend_from_slice(key);
    }

    let payload_len = length_of(&buf[HEADER_LEN..])?;
    buf[4..8].copy_from_slice(&payload_len.to_le_bytes());
    let crc = crc32(&buf[4..]);
    buf[0..4].copy_from_slice(&crc.to_le_bytes());
    Ok(())
}

/// Decodes a payload whose checksum already matched, or `None` if it does not
/// describe a record this version understands.
pub(crate) fn decode(payload: &[u8]) -> Option<Record> {
    let (&kind, rest) = payload.split_first()?;
    let (key, rest) = take_field(rest)?;
    match kind {
        KIND_SET => {
            let (value, rest) = take_field(rest)?;
            rest.is_empty().then(|| Record::Set {
                key: key.to_vec(),
                value: value.to_vec(),
            })
        }
        KIND_DELETE => rest
            .is_empty()
            .then(|| Record::Delete { key: key.to_vec() }),
        _ => None,
    }
}

fn take_field(bytes: &[u8]) -> Option<(&[u8], &[u8])> {
    let (len, rest) = bytes.split_at_checked(4)?;
    let len = usize::try_from(u32::from_le_bytes(len.try_into().ok()?)).ok()?;
    rest.split_at_checked(len)
}

fn length_of(bytes: &[u8]) -> Result<u32> {
    u32::try_from(bytes.len()).map_err(|_| Error::TooLarge { len: bytes.len() })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(key: &[u8], value: Option<&[u8]>) -> Record {
        let mut buf = Vec::new();
        encode(&mut buf, key, value).expect("encode");
        decode(&buf[HEADER_LEN..]).expect("decode")
    }

    #[test]
    fn set_and_delete_survive_a_roundtrip() {
        assert_eq!(
            roundtrip(b"user:1", Some(b"nicolas")),
            Record::Set {
                key: b"user:1".to_vec(),
                value: b"nicolas".to_vec(),
            }
        );
        assert_eq!(
            roundtrip(b"user:1", None),
            Record::Delete {
                key: b"user:1".to_vec(),
            }
        );
    }

    #[test]
    fn keys_and_values_are_binary_safe() {
        let key = [0u8, 0xFF, b'\n', 0x80];
        let value = [0u8; 3];
        assert_eq!(
            roundtrip(&key, Some(&value)),
            Record::Set {
                key: key.to_vec(),
                value: value.to_vec(),
            }
        );
    }

    #[test]
    fn empty_key_and_empty_value_are_encodable() {
        assert_eq!(
            roundtrip(b"", Some(b"")),
            Record::Set {
                key: Vec::new(),
                value: Vec::new(),
            }
        );
    }

    #[test]
    fn the_encoded_length_matches_the_payload() {
        let mut buf = Vec::new();
        encode(&mut buf, b"k", Some(b"vv")).expect("encode");
        let len = u32::from_le_bytes(buf[4..8].try_into().expect("four bytes"));
        assert_eq!(
            usize::try_from(len).expect("length"),
            buf.len() - HEADER_LEN
        );
    }

    #[test]
    fn a_truncated_payload_does_not_decode() {
        let mut buf = Vec::new();
        encode(&mut buf, b"key", Some(b"value")).expect("encode");
        let payload = &buf[HEADER_LEN..];
        assert!(decode(&payload[..payload.len() - 1]).is_none());
    }

    #[test]
    fn an_unknown_kind_does_not_decode() {
        let mut buf = Vec::new();
        encode(&mut buf, b"key", Some(b"value")).expect("encode");
        let mut payload = buf[HEADER_LEN..].to_vec();
        payload[0] = 42;
        assert!(decode(&payload).is_none());
    }

    #[test]
    fn trailing_bytes_do_not_decode() {
        let mut buf = Vec::new();
        encode(&mut buf, b"key", None).expect("encode");
        let mut payload = buf[HEADER_LEN..].to_vec();
        payload.push(0);
        assert!(decode(&payload).is_none());
    }
}
