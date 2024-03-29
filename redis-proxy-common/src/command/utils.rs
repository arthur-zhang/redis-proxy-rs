use std::str::FromStr;
use std::usize;

use bytes::Bytes;

use redis_command::{CommandFlags, Group, Index, KeyNum, Keyword, Range};
use redis_command_gen::{CmdType, COMMAND_ATTRIBUTES};

#[inline]
pub fn to_lower_effective(origin: &[u8]) -> Vec<u8> {
    let mut target = origin.to_vec();
    for c in &mut target.iter_mut() {
        *c |= 0b0010_0000 & (*c != b'_') as u8 * 0b0010_0000;
    }
    target
}

pub fn has_flag(cmd: &CmdType, flag: CommandFlags) -> bool {
    matches!(COMMAND_ATTRIBUTES.get(cmd), Some(cmd) if cmd.command_flags.contains(flag))
}

pub fn is_group_of(cmd: &CmdType, group: Group) -> bool {
    matches!(COMMAND_ATTRIBUTES.get(cmd), Some(cmd) if cmd.group == group)
}

pub fn has_key(cmd: &CmdType) -> bool {
    matches!(COMMAND_ATTRIBUTES.get(cmd), Some(cmd) if cmd.key_specs.is_some())
}

pub fn get_cmd_key_bulks<'a>(cmd: &CmdType, bulk_args: &'a Vec<Bytes>) -> Option<Vec<&'a [u8]>> {
    if bulk_args.len() < 2 {
        return None;
    }
    let cmd = COMMAND_ATTRIBUTES.get(cmd);
    if cmd.is_none() {
        return None;
    }
    let cmd = cmd.unwrap();

    if let Some(key_specs) = &cmd.key_specs {
        let mut key_bulks = vec![];
        for key_spec in key_specs {
            match (&key_spec.begin_search.index, &key_spec.begin_search.keyword,
                   &key_spec.find_keys.range, &key_spec.find_keys.keynum) {
                (Some(index), None, Some(range), None) => {
                    if let Some((first, last)) = range_from_index(index, range, bulk_args) {
                        let mut i = first;
                        while i <= last && i < bulk_args.len() {
                            key_bulks.push(bulk_args[i].as_ref());
                            i += range.step;
                        }
                    }
                }
                (Some(index), None, None, Some(keynum)) => {
                    if let Some((first, key_nums)) = first_and_keynums_from_index(index, keynum, bulk_args) {
                        let mut nums = 0;
                        let mut i = first;
                        while nums < key_nums && i < bulk_args.len() {
                            key_bulks.push(bulk_args[i].as_ref());
                            nums += 1;
                            i += keynum.step;
                        }
                    }
                }
                (None, Some(keyword), Some(find_keys_range), None) => {
                    if let Some((keyword_index, find_last_index)) = range_from_keyword(keyword, find_keys_range, bulk_args) {
                        let mut i = keyword_index + 1;
                        while i <= find_last_index && i < bulk_args.len() {
                            key_bulks.push(bulk_args[i].as_ref());
                            i += find_keys_range.step;
                        }
                    }
                }
                _ => { //other case do not exist in redis command
                    continue;
                }
            }
        }
        if key_bulks.is_empty() {
            return None;
        }
        return Some(key_bulks);
    }
    None
}

fn range_from_index(index: &Index, range: &Range, bulk_args: &Vec<Bytes>) -> Option<(usize, usize)> {
    let bulks_length = bulk_args.len();
    if bulks_length < index.pos + 1 {
        return None;
    }

    let find_last_index = if range.lastkey >= 0 {
        index.pos + range.lastkey as usize
    } else {
        let mut find_last_index = bulks_length - range.lastkey.abs() as usize;
        if range.lastkey == -1 && range.limit > 1 {
            find_last_index = index.pos + (find_last_index - index.pos) / range.limit;
        }
        find_last_index
    };

    return Some((index.pos, find_last_index));
}

fn first_and_keynums_from_index(index: &Index, keynum: &KeyNum, bulk_args: &Vec<Bytes>) -> Option<(usize, usize)> {
    let bulks_length = bulk_args.len();
    if bulks_length < (index.pos + keynum.keynumidx + 1) {
        return None;
    }

    let key_num_bytes = &bulk_args[index.pos + keynum.keynumidx];
    let key_nums = unsafe { std::str::from_utf8_unchecked(key_num_bytes.as_ref()) };
    let key_nums = usize::from_str(key_nums);
    if key_nums.is_err() {
        return None;
    }
    Some((index.pos + keynum.keynumidx + keynum.firstkey, key_nums.unwrap()))
}

fn range_from_keyword(keyword: &Keyword, range: &Range, bulk_args: &Vec<Bytes>) -> Option<(usize, usize)> {
    let bulks_length = bulk_args.len();
    if keyword.startfrom >= 0 && bulks_length < (keyword.startfrom + 1 + 1) as usize {
        return None;
    }

    let (mut keyword_index, reverse) = if keyword.startfrom >= 0 {
        (keyword.startfrom as usize, false)
    } else {
        (bulks_length - keyword.startfrom.abs() as usize, true)
    };

    let mut keyword_bytes = &bulk_args[keyword_index];
    let mut keyword_str = unsafe { std::str::from_utf8_unchecked(keyword_bytes.as_ref()) };
    while !keyword_str.eq_ignore_ascii_case(&keyword.keyword) && keyword_index < (bulks_length - 1) {
        keyword_index = if reverse { keyword_index - 1 } else { keyword_index + 1 };
        keyword_bytes = &bulk_args[keyword_index];
        keyword_str = unsafe { std::str::from_utf8_unchecked(keyword_bytes.as_ref()) };
    }
    if keyword_index >= (bulks_length - 1) {
        return None;
    }

    if range.lastkey >= 0 {
        Some((keyword_index, keyword_index + 1 + range.lastkey as usize))
    } else {
        let mut find_last_index = bulks_length - range.lastkey.abs() as usize;
        if range.lastkey == -1 && range.limit > 1 {
            find_last_index = keyword_index + (find_last_index - keyword_index) / range.limit;
        }
        Some((keyword_index, find_last_index))
    }
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use bytes::Bytes;

    use redis_command::CommandFlags;
    use redis_command_gen::CmdType;

    #[test]
    fn test_to_lower_effective() {
        let origin = b"HELLO_WORLD-";
        let target = super::to_lower_effective(origin);
        assert_eq!(target, b"hello_world-");
    }

    #[test]
    fn test_has_flag() {
        let cmd = CmdType::from_str("SET").unwrap();
        assert_eq!(super::has_flag(&cmd, CommandFlags::Write), true);
        let cmd = CmdType::from_str("GET").unwrap();
        assert_eq!(super::has_flag(&cmd, CommandFlags::Write), false);
    }

    #[test]
    fn test_get_cmd_key_bulks() {
        /* index and range
         */
        let cmd = CmdType::from_str("SET").unwrap();
        let bulk_args = vec![Bytes::from("set"), Bytes::from("key"), Bytes::from("value")];
        let key_bulks = super::get_cmd_key_bulks(&cmd, &bulk_args);
        assert_eq!(key_bulks, Some(vec![b"key".as_ref()]));

        let cmd = CmdType::from_str("LCS").unwrap();
        let bulk_args = vec![Bytes::from("lcs"), Bytes::from("key1"), Bytes::from("key2"), Bytes::from("IDX"), Bytes::from("MINMATCHLEN"), Bytes::from("4"), Bytes::from("WITHMATCHLEN")];
        let key_bulks = super::get_cmd_key_bulks(&cmd, &bulk_args);
        assert_eq!(key_bulks, Some(vec![b"key1".as_ref(), b"key2".as_ref()]));

        let cmd = CmdType::from_str("MGET").unwrap();
        let bulk_args = vec![Bytes::from("mget"), Bytes::from("key1"), Bytes::from("key2")];
        let key_bulks = super::get_cmd_key_bulks(&cmd, &bulk_args);
        assert_eq!(key_bulks, Some(vec![b"key1".as_ref(), b"key2".as_ref()]));

        let cmd = CmdType::from_str("MSET").unwrap();
        let bulk_args = vec![Bytes::from("mset"), Bytes::from("key1"), Bytes::from("value1"), Bytes::from("key2"), Bytes::from("value2")];
        let key_bulks = super::get_cmd_key_bulks(&cmd, &bulk_args);
        assert_eq!(key_bulks, Some(vec![b"key1".as_ref(), b"key2".as_ref()]));

        let cmd = CmdType::from_str("BLPOP").unwrap();
        let bulk_args = vec![Bytes::from("blpop"), Bytes::from("key1"), Bytes::from("key2"), Bytes::from("key3"), Bytes::from("0")];
        let key_bulks = super::get_cmd_key_bulks(&cmd, &bulk_args);
        assert_eq!(key_bulks, Some(vec![b"key1".as_ref(), b"key2".as_ref(), b"key3".as_ref()]));

        let cmd = CmdType::from_str("PFMERGE").unwrap();
        let bulk_args = vec![Bytes::from("pfmerge"), Bytes::from("key1"), Bytes::from("key2"), Bytes::from("key3")];
        let key_bulks = super::get_cmd_key_bulks(&cmd, &bulk_args);
        assert_eq!(key_bulks, Some(vec![b"key1".as_ref(), b"key2".as_ref(), b"key3".as_ref()]));

        /* index and keynum
         */
        let cmd = CmdType::from_str("LMPOP").unwrap();
        let bulk_args = vec![Bytes::from("lmpop"), Bytes::from("3"), Bytes::from("key1"), Bytes::from("key2"), Bytes::from("key3"), Bytes::from("LEFT"), Bytes::from("COUNT"), Bytes::from("3")];
        let key_bulks = super::get_cmd_key_bulks(&cmd, &bulk_args);
        assert_eq!(key_bulks, Some(vec![b"key1".as_ref(), b"key2".as_ref(), b"key3".as_ref()]));

        let cmd = CmdType::from_str("BLMPOP").unwrap();
        let bulk_args = vec![Bytes::from("blmpop"), Bytes::from("1000"), Bytes::from("3"), Bytes::from("key1"), Bytes::from("key2"), Bytes::from("key3"), Bytes::from("LEFT"), Bytes::from("COUNT"), Bytes::from("3")];
        let key_bulks = super::get_cmd_key_bulks(&cmd, &bulk_args);
        assert_eq!(key_bulks, Some(vec![b"key1".as_ref(), b"key2".as_ref(), b"key3".as_ref()]));

        /* keyword and range
         */
        let cmd = CmdType::from_str("GEORADIUS").unwrap();
        let bulk_args = vec![Bytes::from("georadius"), Bytes::from("key1"), Bytes::from("15"), Bytes::from("37"), Bytes::from("300"), Bytes::from("km"), Bytes::from("WITHCOORD"), Bytes::from("WITHDIST"), Bytes::from("WITHHASH"), Bytes::from("COUNT"), Bytes::from("3"), Bytes::from("ASC"), Bytes::from("store"), Bytes::from("key2"), Bytes::from("STOREDIST"), Bytes::from("key3")];
        let key_bulks = super::get_cmd_key_bulks(&cmd, &bulk_args);
        assert_eq!(key_bulks, Some(vec![b"key1".as_ref(), b"key2".as_ref(), b"key3".as_ref()]));

        let cmd = CmdType::from_str("XREAD").unwrap();
        let bulk_args = vec![Bytes::from("xread"), Bytes::from("count"), Bytes::from("1"), Bytes::from("block"), Bytes::from("1000"), Bytes::from("streams"), Bytes::from("key1"), Bytes::from("key2"), Bytes::from("key3"), Bytes::from("id1"), Bytes::from("id2"), Bytes::from("id3")];
        let key_bulks = super::get_cmd_key_bulks(&cmd, &bulk_args);
        assert_eq!(key_bulks, Some(vec![b"key1".as_ref(), b"key2".as_ref(), b"key3".as_ref()]));

        let cmd = CmdType::from_str("MIGRATE").unwrap();
        let bulk_args = vec![Bytes::from("migrate"), Bytes::from("127.0.0.1"), Bytes::from("9001"), Bytes::from("key1"), Bytes::from("0"), Bytes::from("1000"), Bytes::from("COPY"), Bytes::from("REPLACE"), Bytes::from("AUTH"), Bytes::from("123456"), Bytes::from("AUTH2"), Bytes::from("admin"), Bytes::from("root"), Bytes::from("keys"), Bytes::from("key2"), Bytes::from("key3")];
        let key_bulks = super::get_cmd_key_bulks(&cmd, &bulk_args);
        assert_eq!(key_bulks, Some(vec![b"key1".as_ref(), b"key2".as_ref(), b"key3".as_ref()]));

        /* keyword and keynum
         */
        //so such command case
    }
}