//! Framing primitives shared by the on-disk formats.
//!
//! Every format in the engine is a sequence of little-endian integers and
//! length-prefixed byte strings. Reading walks a cursor that never panics on a
//! truncated or corrupted input: it returns `None`, and the caller turns that
//! into a corruption report naming the file.

use crate::error::{Error, Result};

/// Appends `field`, prefixed by its length.
pub(crate) fn push_field(buf: &mut Vec<u8>, field: &[u8]) -> Result<()> {
    buf.extend_from_slice(&length_of(field)?.to_le_bytes());
    buf.extend_from_slice(field);
    Ok(())
}

/// Length of `bytes` as the formats store it.
pub(crate) fn length_of(bytes: &[u8]) -> Result<u32> {
    u32::try_from(bytes.len()).map_err(|_| Error::TooLarge { len: bytes.len() })
}

/// Takes `len` bytes off the front of the cursor.
pub(crate) fn take<'a>(cursor: &mut &'a [u8], len: usize) -> Option<&'a [u8]> {
    let (head, rest) = cursor.split_at_checked(len)?;
    *cursor = rest;
    Some(head)
}

/// Takes a length-prefixed byte string off the front of the cursor.
pub(crate) fn take_field<'a>(cursor: &mut &'a [u8]) -> Option<&'a [u8]> {
    let len = usize::try_from(take_u32(cursor)?).ok()?;
    take(cursor, len)
}

pub(crate) fn take_u8(cursor: &mut &[u8]) -> Option<u8> {
    Some(take(cursor, 1)?[0])
}

pub(crate) fn take_u32(cursor: &mut &[u8]) -> Option<u32> {
    Some(u32::from_le_bytes(take(cursor, 4)?.try_into().ok()?))
}

pub(crate) fn take_u64(cursor: &mut &[u8]) -> Option<u64> {
    Some(u64::from_le_bytes(take(cursor, 8)?.try_into().ok()?))
}

/// Reads a `u32` at a known offset, for fixed-layout headers and footers.
pub(crate) fn u32_at(bytes: &[u8], offset: usize) -> u32 {
    let mut field = [0u8; 4];
    field.copy_from_slice(&bytes[offset..offset + 4]);
    u32::from_le_bytes(field)
}

/// Reads a `u64` at a known offset.
pub(crate) fn u64_at(bytes: &[u8], offset: usize) -> u64 {
    let mut field = [0u8; 8];
    field.copy_from_slice(&bytes[offset..offset + 8]);
    u64::from_le_bytes(field)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_field_survives_a_roundtrip() {
        let mut buf = Vec::new();
        push_field(&mut buf, b"key").expect("push");
        push_field(&mut buf, b"").expect("push");

        let mut cursor = buf.as_slice();
        assert_eq!(take_field(&mut cursor), Some(b"key".as_slice()));
        assert_eq!(take_field(&mut cursor), Some(b"".as_slice()));
        assert!(cursor.is_empty());
    }

    #[test]
    fn integers_survive_a_roundtrip() {
        let mut buf = vec![7u8];
        buf.extend_from_slice(&42u32.to_le_bytes());
        buf.extend_from_slice(&u64::MAX.to_le_bytes());

        let mut cursor = buf.as_slice();
        assert_eq!(take_u8(&mut cursor), Some(7));
        assert_eq!(take_u32(&mut cursor), Some(42));
        assert_eq!(take_u64(&mut cursor), Some(u64::MAX));
        assert_eq!(take_u8(&mut cursor), None);
    }

    #[test]
    fn a_truncated_input_yields_nothing() {
        let mut cursor: &[u8] = &[3, 0, 0, 0, b'a'];
        assert_eq!(take_field(&mut cursor), None, "length outruns the input");

        let mut cursor: &[u8] = &[1, 2];
        assert_eq!(take_u32(&mut cursor), None);
    }

    #[test]
    fn fixed_offsets_read_back() {
        let mut bytes = 1u32.to_le_bytes().to_vec();
        bytes.extend_from_slice(&2u64.to_le_bytes());

        assert_eq!(u32_at(&bytes, 0), 1);
        assert_eq!(u64_at(&bytes, 4), 2);
    }
}
