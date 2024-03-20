use std::str::FromStr;

use bytes::Bytes;
use lazy_static::lazy_static;
use smol_str::SmolStr;

use crate::command::{CommandFlags, Group};
use crate::command::holder::COMMAND_ATTRIBUTES;

lazy_static!{
    // some special command types
    pub static ref CMD_TYPE_ALL: SmolStr = SmolStr::from("*");
    pub static ref CMD_TYPE_UNKNOWN: SmolStr = SmolStr::from("unknown");
    
    pub static ref CMD_TYPE_AUTH: SmolStr = SmolStr::from("auth");
    pub static ref CMD_TYPE_SELECT: SmolStr = SmolStr::from("select");
}

#[inline]
pub fn to_lower_effective(origin: &[u8]) -> Vec<u8> {
    let mut target = origin.to_vec();
    for c in &mut target.iter_mut() {
        *c |= 0b0010_0000 & (*c != b'_') as u8 * 0b0010_0000;
    }
    target
}

pub fn is_write_cmd(cmd: &SmolStr) -> bool {
    if let Some(cmd) = COMMAND_ATTRIBUTES.get(cmd) {
        return cmd.command_flags & (CommandFlags::Write as u32) > 0
    }
    return false
}

pub fn is_connection_cmd(cmd: &SmolStr) -> bool {
    if let Some(cmd) = COMMAND_ATTRIBUTES.get(cmd) {
        return cmd.group == Group::Connection
    }
    return false
}

pub fn get_cmd_key_bulks<'a>(cmd: &SmolStr, bulk_args: &'a Vec<Bytes>) -> Option<Vec<&'a [u8]>> {
    if bulk_args.len() < 2 {
        return None
    }
    let cmd = COMMAND_ATTRIBUTES.get(cmd);
    if cmd.is_none() {
        return None
    }
    let cmd = cmd.unwrap();

    if let Some(key_specs) = &cmd.key_specs {
        let mut key_bulks = vec![];
        for key_spec in key_specs {
            if let Some(begin_search_index) = &key_spec.begin_search.index {
                if let Some(find_keys_range) = &key_spec.find_keys.range {
                    let bulks_length = bulk_args.len();
                    if bulks_length < begin_search_index.pos + 1 {
                        continue;
                    }
                    
                    let find_last_index = if find_keys_range.lastkey >= 0 {
                        begin_search_index.pos + find_keys_range.lastkey as usize
                    } else {
                        let mut find_last_index = bulks_length - find_keys_range.lastkey.abs() as usize;
                        if find_keys_range.lastkey == -1 && find_keys_range.limit > 1 {
                            find_last_index = begin_search_index.pos + (find_last_index - begin_search_index.pos) / find_keys_range.limit;
                        }
                        find_last_index
                    };

                    let mut i = begin_search_index.pos;
                    while i <= find_last_index && i < bulks_length {
                        key_bulks.push(bulk_args[i].as_ref());
                        i += find_keys_range.step;
                    }
                } else if let Some(keynum) = &key_spec.find_keys.keynum {
                    let bulks_length = bulk_args.len();
                    if bulks_length < (begin_search_index.pos + keynum.keynumidx + 1) {
                        continue;
                    }
                    
                    let key_num_bytes = &bulk_args[begin_search_index.pos + keynum.keynumidx];
                    let key_nums = unsafe {std::str::from_utf8_unchecked(key_num_bytes.as_ref())};
                    let key_nums = u64::from_str(key_nums);
                    if key_nums.is_err() { 
                        continue;
                    }
                    let key_nums = key_nums.unwrap();
                    let first_index = begin_search_index.pos + keynum.keynumidx + keynum.firstkey;

                    let mut nums = 0;
                    let mut i = first_index;
                    while nums < key_nums && i < bulks_length {
                        key_bulks.push(bulk_args[i].as_ref());
                        nums += 1;
                        i += keynum.step;
                    }
                }
            } else if let Some(keyword) = &key_spec.begin_search.keyword {
                if let Some(find_keys_range) = &key_spec.find_keys.range {
                    let bulks_length = bulk_args.len();
                    if keyword.startfrom >= 0 && bulks_length < (keyword.startfrom + 1 + 1) as usize {
                        continue;
                    }

                    let mut keyword_index = if keyword.startfrom >= 0 {
                        keyword.startfrom as usize
                    } else {
                        bulks_length - keyword.startfrom.abs() as usize
                    };
                    
                    let mut keyword_bytes = &bulk_args[keyword_index];
                    let mut keyword_str = unsafe {std::str::from_utf8_unchecked(keyword_bytes.as_ref())};
                    while !keyword_str.eq_ignore_ascii_case(&keyword.keyword) && keyword_index < (bulks_length - 1) {
                        keyword_index += 1;
                        keyword_bytes = &bulk_args[keyword_index];
                        keyword_str = unsafe {std::str::from_utf8_unchecked(keyword_bytes.as_ref())};
                    }
                    if keyword_index >= (bulks_length - 1) {
                        continue;
                    }

                    let find_last_index = if find_keys_range.lastkey >= 0 {
                        keyword_index + 1 + find_keys_range.lastkey as usize
                    } else {
                        let mut find_last_index = bulks_length - find_keys_range.lastkey.abs() as usize;
                        if find_keys_range.lastkey == -1 && find_keys_range.limit > 1 {
                            find_last_index = keyword_index + (find_last_index - keyword_index) / find_keys_range.limit;
                        }
                        find_last_index
                    };

                    let mut i = keyword_index + 1;
                    while i <= find_last_index && i < bulks_length {
                        key_bulks.push(bulk_args[i as usize].as_ref());
                        i += find_keys_range.step;
                    }
                }
            } else if let Some(_) = &key_spec.begin_search.index {
                //there is not such command yet.
            }
        }
        if key_bulks.is_empty() {
            return None
        }
        return Some(key_bulks)
    }
    None
}