use std::ops::Range;

use lazy_static::lazy_static;
use smol_str::SmolStr;

use crate::command::{CommandFlags, Group};
use crate::command::holder::{COMMAND_ATTRIBUTES, MULTIPART_COMMANDS};

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

pub fn is_multipart_cmd(cmd: &SmolStr, bulk_size: u64) -> bool {
    MULTIPART_COMMANDS.get(cmd).is_some() && bulk_size > 1
}

pub fn get_cmd_key_bulk_index(cmd: &SmolStr, bulk_size: u64, bulks: &Option<Vec<Range<usize>>>) -> Vec<u64> {
    if let Some(cmd) = COMMAND_ATTRIBUTES.get(cmd) {
        if let Some(key_specs) = &cmd.key_specs {
            //todo
        }
    }
    //(1_u64..bulk_size).collect()
    vec![] //default do not read any key
}