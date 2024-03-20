use std::fmt::{Debug, Formatter};
use std::ops::Range;

use bytes::Bytes;
use smol_str::SmolStr;

use crate::command::utils::CMD_TYPE_UNKNOWN;

pub mod tools;
pub mod command;

pub struct ReqFrameData {
    pub is_head_frame: bool,
    pub cmd_type: SmolStr,
    bulks: Option<Vec<Range<usize>>>,
    pub cmd_bulk_size: u64,
    key_bulk_indices: Vec<u64>,
    pub raw_bytes: bytes::Bytes,
    pub end_of_body: bool,
}

impl ReqFrameData {
    pub fn new(is_first_frame: bool,
               cmd_type: SmolStr,
               bulks: Option<Vec<Range<usize>>>,
               cmd_bulk_size: u64,
               key_bulk_indices: Vec<u64>,
               raw_bytes: bytes::Bytes,
               is_done: bool) -> Self {
        Self {
            is_head_frame: is_first_frame,
            cmd_type,
            bulks,
            cmd_bulk_size,
            key_bulk_indices,
            raw_bytes,
            end_of_body: is_done,
        }
    }
    pub fn bulks(&self) -> Option<Vec<&[u8]>> {
        if let Some(ref ranges) = self.bulks {
            if ranges.is_empty() {
                return None;
            }
            return Some(ranges.iter().map(|it| &self.raw_bytes[it.start..it.end]).collect());
        }
        return None;
    }

    pub fn keys(&self) -> Option<Vec<&[u8]>> {
        if let Some(ref ranges) = self.bulks {
            if ranges.is_empty() {
                return None;
            }
            if self.key_bulk_indices.is_empty() {
                return None;
            }
            return Some(self.key_bulk_indices.iter().map(|index|
                &self.raw_bytes[ranges[*index as usize].start..ranges[*index as usize].end]).collect()
            );
        }
        return None;
    }
}

impl Debug for ReqFrameData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DecodedFrame")
            .field("data", &std::str::from_utf8(&self.raw_bytes))
            .field("is_head_frame", &self.is_head_frame)
            .field("cmd_type", &self.cmd_type)
            .field("bulks", &self.bulks)
            .field("end_of_body", &self.end_of_body)
            .field("args", &self.bulks())
            .finish()
    }
}

impl PartialEq for ReqFrameData {
    fn eq(&self, other: &Self) -> bool {
        self.is_head_frame == other.is_head_frame
            && self.cmd_type == other.cmd_type
            && self.bulks == other.bulks
            && self.raw_bytes == other.raw_bytes
            && self.end_of_body == other.end_of_body
    }
}

pub struct ReqPkt {
    pub bulk_args: Vec<Bytes>,
    pub bytes_total: usize,
}

impl ReqPkt {
    pub fn cmd_type(&self) -> &SmolStr {
        // todo!()
        &CMD_TYPE_UNKNOWN
    }
    pub fn keys(&self)-> Option<Vec<&[u8]>> {
        // todo!()
        None
    }
}