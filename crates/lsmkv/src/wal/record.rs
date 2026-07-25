//! On-disk layout of one log record.
//!
//! ```text
//! record  := crc: u32 | len: u32 | payload[len]
//! payload := seq: u64 | kind: u8 = 0 | key_len: u32 | key | value_len: u32 | value
//!          | seq: u64 | kind: u8 = 1 | key_len: u32 | key
//! ```
//!
//! Integers are little endian. The checksum covers `len` as well as the
//! payload, so a length corrupted by a partial write is caught like any other
//! corruption instead of being trusted as a size.
//!
//! The sequence number sits in the record rather than being derived from its
//! position, so each file is self-describing: logs can be rotated and deleted
//! without the numbering of the ones that remain depending on them.

use crate::checksum::crc32;
use crate::coding::{length_of, take_field, take_u8, take_u64};
use crate::error::Result;

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
        /// Sequence number the log gave this mutation.
        seq: u64,
        /// Key that was written.
        key: Vec<u8>,
        /// Value it was bound to.
        value: Vec<u8>,
    },
    /// A key deleted, kept as a tombstone until compaction drops it.
    Delete {
        /// Sequence number the log gave this mutation.
        seq: u64,
        /// Key that was deleted.
        key: Vec<u8>,
    },
}

impl Record {
    /// Sequence number this record was written with.
    pub fn seq(&self) -> u64 {
        match self {
            Self::Set { seq, .. } | Self::Delete { seq, .. } => *seq,
        }
    }

    /// Key this record applies to.
    pub fn key(&self) -> &[u8] {
        match self {
            Self::Set { key, .. } | Self::Delete { key, .. } => key,
        }
    }
}

/// Appends one record to `buf`, after whatever it already holds.
///
/// `value` is `None` for a delete. On failure `buf` is left exactly as it was,
/// since the records already in it belong to a group that is still going to be
/// written.
pub(crate) fn encode(buf: &mut Vec<u8>, seq: u64, key: &[u8], value: Option<&[u8]>) -> Result<()> {
    let start = buf.len();
    let encoded = encode_at(buf, start, seq, key, value);
    if encoded.is_err() {
        buf.truncate(start);
    }
    encoded
}

fn encode_at(
    buf: &mut Vec<u8>,
    start: usize,
    seq: u64,
    key: &[u8],
    value: Option<&[u8]>,
) -> Result<()> {
    let key_len = length_of(key)?;

    buf.extend_from_slice(&[0; HEADER_LEN]);
    buf.extend_from_slice(&seq.to_le_bytes());
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

    let payload_len = length_of(&buf[start + HEADER_LEN..])?;
    buf[start + 4..start + 8].copy_from_slice(&payload_len.to_le_bytes());
    let crc = crc32(&buf[start + 4..]);
    buf[start..start + 4].copy_from_slice(&crc.to_le_bytes());
    Ok(())
}

/// Decodes a payload whose checksum already matched, or `None` if it does not
/// describe a record this version understands.
pub(crate) fn decode(payload: &[u8]) -> Option<Record> {
    let mut cursor = payload;
    let seq = take_u64(&mut cursor)?;
    let kind = take_u8(&mut cursor)?;
    let key = take_field(&mut cursor)?.to_vec();

    match kind {
        KIND_SET => {
            let value = take_field(&mut cursor)?.to_vec();
            cursor.is_empty().then_some(Record::Set { seq, key, value })
        }
        KIND_DELETE => cursor.is_empty().then_some(Record::Delete { seq, key }),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(seq: u64, key: &[u8], value: Option<&[u8]>) -> Record {
        let mut buf = Vec::new();
        encode(&mut buf, seq, key, value).expect("encode");
        decode(&buf[HEADER_LEN..]).expect("decode")
    }

    #[test]
    fn set_and_delete_survive_a_roundtrip() {
        assert_eq!(
            roundtrip(7, b"user:1", Some(b"nicolas")),
            Record::Set {
                seq: 7,
                key: b"user:1".to_vec(),
                value: b"nicolas".to_vec(),
            }
        );
        assert_eq!(
            roundtrip(8, b"user:1", None),
            Record::Delete {
                seq: 8,
                key: b"user:1".to_vec(),
            }
        );
    }

    #[test]
    fn records_encode_back_to_back_in_one_buffer() {
        let mut buf = Vec::new();
        encode(&mut buf, 1, b"a", Some(b"1")).expect("encode");
        let first_len = buf.len();
        encode(&mut buf, 2, b"b", None).expect("encode");

        assert_eq!(
            decode(&buf[HEADER_LEN..first_len]).expect("decode the first"),
            Record::Set {
                seq: 1,
                key: b"a".to_vec(),
                value: b"1".to_vec(),
            }
        );
        assert_eq!(
            decode(&buf[first_len + HEADER_LEN..]).expect("decode the second"),
            Record::Delete {
                seq: 2,
                key: b"b".to_vec(),
            }
        );
    }

    #[test]
    fn keys_and_values_are_binary_safe() {
        let key = [0u8, 0xFF, b'\n', 0x80];
        let value = [0u8; 3];
        assert_eq!(
            roundtrip(1, &key, Some(&value)),
            Record::Set {
                seq: 1,
                key: key.to_vec(),
                value: value.to_vec(),
            }
        );
    }

    #[test]
    fn empty_key_and_empty_value_are_encodable() {
        assert_eq!(
            roundtrip(1, b"", Some(b"")),
            Record::Set {
                seq: 1,
                key: Vec::new(),
                value: Vec::new(),
            }
        );
    }

    #[test]
    fn a_large_sequence_number_survives() {
        assert_eq!(roundtrip(u64::MAX, b"k", None).seq(), u64::MAX);
    }

    #[test]
    fn the_encoded_length_matches_the_payload() {
        let mut buf = Vec::new();
        encode(&mut buf, 1, b"k", Some(b"vv")).expect("encode");
        let len = u32::from_le_bytes(buf[4..8].try_into().expect("four bytes"));
        assert_eq!(
            usize::try_from(len).expect("length"),
            buf.len() - HEADER_LEN
        );
    }

    #[test]
    fn a_truncated_payload_does_not_decode() {
        let mut buf = Vec::new();
        encode(&mut buf, 1, b"key", Some(b"value")).expect("encode");
        let payload = &buf[HEADER_LEN..];
        assert!(decode(&payload[..payload.len() - 1]).is_none());
    }

    #[test]
    fn a_payload_shorter_than_a_sequence_number_does_not_decode() {
        assert!(decode(&[0, 0, 0]).is_none());
    }

    #[test]
    fn an_unknown_kind_does_not_decode() {
        let mut buf = Vec::new();
        encode(&mut buf, 1, b"key", Some(b"value")).expect("encode");
        let mut payload = buf[HEADER_LEN..].to_vec();
        payload[8] = 42;
        assert!(decode(&payload).is_none());
    }

    #[test]
    fn trailing_bytes_do_not_decode() {
        let mut buf = Vec::new();
        encode(&mut buf, 1, b"key", None).expect("encode");
        let mut payload = buf[HEADER_LEN..].to_vec();
        payload.push(0);
        assert!(decode(&payload).is_none());
    }
}
