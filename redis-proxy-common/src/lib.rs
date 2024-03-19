use std::fmt::{Debug, Formatter};
use std::ops::Range;

use crate::cmd::CmdType;

pub mod cmd;
pub mod tools;
pub mod command;

// #[derive(Clone)]
pub struct ReqFrameData {
    pub is_head_frame: bool,
    pub cmd_type: CmdType,
    bulk_read_args: Option<Vec<Range<usize>>>,
    pub raw_bytes: bytes::Bytes,
    pub end_of_body: bool,
}

impl ReqFrameData {
    pub fn new(is_first_frame: bool, cmd_type: CmdType, bulk_read_args: Option<Vec<Range<usize>>>, raw_bytes: bytes::Bytes, is_done: bool) -> Self {
        Self {
            is_head_frame: is_first_frame,
            cmd_type,
            bulk_read_args,
            raw_bytes,
            end_of_body: is_done,
        }
    }
    pub fn args(&self) -> Option<Vec<&[u8]>> {
        if let Some(ref ranges) = self.bulk_read_args {
            if ranges.is_empty() {
                return None;
            }
            return Some(ranges.iter().map(|it| &self.raw_bytes[it.start..it.end]).collect());
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
            .field("bulk_read_args", &self.bulk_read_args)
            .field("end_of_body", &self.end_of_body)
            .field("args", &self.args())
            .finish()
    }
}

impl PartialEq for ReqFrameData {
    fn eq(&self, other: &Self) -> bool {
        self.is_head_frame == other.is_head_frame
            && self.cmd_type == other.cmd_type
            && self.bulk_read_args == other.bulk_read_args
            && self.raw_bytes == other.raw_bytes
            && self.end_of_body == other.end_of_body
    }
}