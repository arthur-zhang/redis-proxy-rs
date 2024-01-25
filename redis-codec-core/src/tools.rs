
#[inline]
pub fn str3icmp(str: &[u8], c0: u8, c1: u8, c2: u8) -> bool {
    (str[0] == c0 || str[0] == (c0 ^ 0x20))
        && (str[1] == c1 || str[1] == (c1 ^ 0x20))
        && (str[2] == c2 || str[2] == (c2 ^ 0x20))
}

#[inline]
pub fn str4icmp(str: &[u8], c0: u8, c1: u8, c2: u8, c3: u8) -> bool {
    str3icmp(&str[0..3], c0, c1, c2)
        && (str[3] == c3 || str[3] == (c3 ^ 0x20))
}

#[inline]
pub fn str5icmp(str: &[u8], c0: u8, c1: u8, c2: u8, c3: u8, c4: u8) -> bool {
    str4icmp(&str[0..4], c0, c1, c2, c3)
        && (str[4] == c4 || str[4] == (c4 ^ 0x20))
}

#[inline]
pub fn str6icmp(str: &[u8], c0: u8, c1: u8, c2: u8, c3: u8, c4: u8, c5: u8) -> bool {
    str5icmp(&str[0..5], c0, c1, c2, c3, c4)
        && (str[5] == c5 || str[5] == (c5 ^ 0x20))
}

#[inline]
pub fn str7icmp(str: &[u8], c0: u8, c1: u8, c2: u8, c3: u8, c4: u8, c5: u8, c6: u8) -> bool {
    str6icmp(&str[0..6], c0, c1, c2, c3, c4, c5)
        && (str[6] == c6 || str[6] == (c6 ^ 0x20))
}

#[inline]
pub fn str8icmp(
    str: &[u8],
    c0: u8,
    c1: u8,
    c2: u8,
    c3: u8,
    c4: u8,
    c5: u8,
    c6: u8,
    c7: u8,
) -> bool {
    str7icmp(&str[0..7], c0, c1, c2, c3, c4, c5, c6)
        && (str[7] == c7 || str[7] == (c7 ^ 0x20))
}

#[inline]
pub fn str9icmp(
    str: &[u8],
    c0: u8,
    c1: u8,
    c2: u8,
    c3: u8,
    c4: u8,
    c5: u8,
    c6: u8,
    c7: u8,
    c8: u8,
) -> bool {
    str8icmp(&str[0..8], c0, c1, c2, c3, c4, c5, c6, c7)
        && (str[8] == c8 || str[8] == (c8 ^ 0x20))
}

#[inline]
pub fn str10icmp(
    str: &[u8],
    c0: u8,
    c1: u8,
    c2: u8,
    c3: u8,
    c4: u8,
    c5: u8,
    c6: u8,
    c7: u8,
    c8: u8,
    c9: u8,
) -> bool {
    str9icmp(&str[0..9], c0, c1, c2, c3, c4, c5, c6, c7, c8)
        && (str[9] == c9 || str[9] == (c9 ^ 0x20))
}

#[inline]
pub fn str11icmp(
    str: &[u8],
    c0: u8,
    c1: u8,
    c2: u8,
    c3: u8,
    c4: u8,
    c5: u8,
    c6: u8,
    c7: u8,
    c8: u8,
    c9: u8,
    c10: u8,
) -> bool {
    str10icmp(&str[0..10], c0, c1, c2, c3, c4, c5, c6, c7, c8, c9)
        && (str[10] == c10 || str[10] == (c10 ^ 0x20))
}

#[inline]
pub fn str12icmp(
    str: &[u8],
    c0: u8,
    c1: u8,
    c2: u8,
    c3: u8,
    c4: u8,
    c5: u8,
    c6: u8,
    c7: u8,
    c8: u8,
    c9: u8,
    c10: u8,
    c11: u8,
) -> bool {
    str11icmp(&str[0..11], c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10)
        && (str[11] == c11 || str[11] == (c11 ^ 0x20))
}

#[inline]
pub fn str13icmp(str: &[u8], c0: u8, c1: u8, c2: u8, c3: u8, c4: u8, c5: u8, c6: u8, c7: u8, c8: u8, c9: u8, c10: u8, c11: u8, c12: u8) -> bool {
    str12icmp(&str[0..12], c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11)
        && (str[12] == c12 || str[12] == (c12 ^ 0x20))
}

#[inline]
pub fn str14icmp(str: &[u8], c0: u8, c1: u8, c2: u8, c3: u8, c4: u8, c5: u8, c6: u8, c7: u8, c8: u8, c9: u8, c10: u8, c11: u8, c12: u8, c13: u8) -> bool {
    str13icmp(&str[0..13], c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12)
        && (str[13] == c13 || str[13] == (c13 ^ 0x20))
}

#[inline]
pub fn str15icmp(str: &[u8], c0: u8, c1: u8, c2: u8, c3: u8, c4: u8, c5: u8, c6: u8, c7: u8, c8: u8, c9: u8, c10: u8, c11: u8, c12: u8, c13: u8, c14: u8) -> bool {
    str14icmp(&str[0..14], c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13)
        && (str[14] == c14 || str[14] == (c14 ^ 0x20))
}

#[inline]
pub fn str16icmp(str: &[u8], c0: u8, c1: u8, c2: u8, c3: u8, c4: u8, c5: u8, c6: u8, c7: u8, c8: u8, c9: u8, c10: u8, c11: u8, c12: u8, c13: u8, c14: u8, c15: u8) -> bool {
    str15icmp(&str[0..15], c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14)
        && (str[15] == c15 || str[15] == (c15 ^ 0x20))
}

#[inline]
pub fn str17icmp(str: &[u8], c0: u8, c1: u8, c2: u8, c3: u8, c4: u8, c5: u8, c6: u8, c7: u8, c8: u8, c9: u8, c10: u8, c11: u8, c12: u8, c13: u8, c14: u8, c15: u8, c16: u8) -> bool {
    str16icmp(&str[0..16], c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15)
        && (str[16] == c16 || str[16] == (c16 ^ 0x20))
}



#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stricmp() {
        assert!(str3icmp(b"abc", b'a', b'b', b'c'));
        assert!(str3icmp(b"ABC", b'a', b'b', b'c'));
        assert!(str3icmp(b"aBc", b'a', b'b', b'c'));
        assert!(str3icmp(b"AbC", b'a', b'b', b'c'));
        assert!(str4icmp(b"abcd", b'a', b'b', b'c', b'd'));
        assert!(str4icmp(b"ABCD", b'a', b'b', b'c', b'd'));
        assert!(str4icmp(b"abCD", b'a', b'b', b'c', b'd'));
        assert!(str5icmp(b"abcde", b'a', b'b', b'c', b'd', b'e'));
        assert!(str5icmp(b"ABCDE", b'a', b'b', b'c', b'd', b'e'));
        assert!(str5icmp(b"abCDe", b'a', b'b', b'c', b'd', b'e'));
        assert!(str6icmp(b"abcdef", b'a', b'b', b'c', b'd', b'e', b'f'));
        assert!(str6icmp(b"ABCDEF", b'a', b'b', b'c', b'd', b'e', b'f'));
        assert!(str6icmp(b"abCDef", b'a', b'b', b'c', b'd', b'e', b'f'));
        assert!(str7icmp(b"abcdefg", b'a', b'b', b'c', b'd', b'e', b'f', b'g'));
        assert!(str7icmp(b"ABCDEFG", b'a', b'b', b'c', b'd', b'e', b'f', b'g'));
        assert!(str7icmp(b"abCDefg", b'a', b'b', b'c', b'd', b'e', b'f', b'g'));
        assert!(!str7icmp(b"0bCDefg", b'a', b'b', b'c', b'd', b'e', b'f', b'g'));
        assert!(str8icmp(b"abcdefgh", b'a', b'b', b'c', b'd', b'e', b'f', b'g', b'h'));
        assert!(str8icmp(b"ABCDEFGH", b'a', b'b', b'c', b'd', b'e', b'f', b'g', b'h'));
        assert!(str8icmp(b"abCDefgh", b'a', b'b', b'c', b'd', b'e', b'f', b'g', b'h'));
        assert!(str9icmp(b"abcdefghi", b'a', b'b', b'c', b'd', b'e', b'f', b'g', b'h', b'i'));
        assert!(str9icmp(b"ABCDEFGHI", b'a', b'b', b'c', b'd', b'e', b'f', b'g', b'h', b'i'));
        assert!(str9icmp(b"abCDefghi", b'a', b'b', b'c', b'd', b'e', b'f', b'g', b'h', b'i'));
        assert!(str10icmp(b"abcdefghij", b'a', b'b', b'c', b'd', b'e', b'f', b'g', b'h', b'i', b'j'));
        assert!(str10icmp(b"ABCDEFGHIJ", b'a', b'b', b'c', b'd', b'e', b'f', b'g', b'h', b'i', b'j'));
    }
}