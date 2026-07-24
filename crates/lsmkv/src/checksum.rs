//! CRC-32 (IEEE 802.3), the checksum stored with every record on disk.
//!
//! Table driven, one byte per step. The table is built at compile time, so the
//! crate keeps no static initialization and no dependency for this.

const POLYNOMIAL: u32 = 0xEDB8_8320;
const INIT: u32 = 0xFFFF_FFFF;

const TABLE: [u32; 256] = build_table();

const fn build_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut byte: u32 = 0;
    while byte < 256 {
        let mut crc = byte;
        let mut bit = 0;
        while bit < 8 {
            crc = if crc & 1 == 1 {
                (crc >> 1) ^ POLYNOMIAL
            } else {
                crc >> 1
            };
            bit += 1;
        }
        table[byte as usize] = crc;
        byte += 1;
    }
    table
}

const fn update(mut crc: u32, bytes: &[u8]) -> u32 {
    let mut i = 0;
    while i < bytes.len() {
        let index = ((crc ^ bytes[i] as u32) & 0xFF) as usize;
        crc = (crc >> 8) ^ TABLE[index];
        i += 1;
    }
    crc
}

/// CRC-32 of `bytes`.
pub(crate) const fn crc32(bytes: &[u8]) -> u32 {
    !update(INIT, bytes)
}

/// CRC-32 of two chunks read back separately, as a record header and its
/// payload are.
pub(crate) const fn crc32_parts(first: &[u8], second: &[u8]) -> u32 {
    !update(update(INIT, first), second)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_the_standard_check_value() {
        // Check value published with the CRC-32 specification.
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
        assert_eq!(crc32(b""), 0);
    }

    #[test]
    fn splitting_the_input_does_not_change_the_result() {
        let whole = b"the quick brown fox";
        assert_eq!(crc32_parts(&whole[..4], &whole[4..]), crc32(whole));
        assert_eq!(crc32_parts(whole, b""), crc32(whole));
    }

    #[test]
    fn one_flipped_bit_changes_the_checksum() {
        let mut corrupted = *b"123456789";
        corrupted[3] ^= 0b0000_0001;
        assert_ne!(crc32(&corrupted), crc32(b"123456789"));
    }
}
