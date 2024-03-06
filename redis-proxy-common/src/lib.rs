use std::fmt::{Debug, Formatter};
use std::ops::Range;
use std::sync::Arc;

use crate::cmd::CmdType;

pub mod cmd;
pub mod tools;

pub type TDecodedFrame = Arc<ReqFrameData>;

#[derive(Clone)]
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
            // .field("is_eager", &self.is_eager)
            .finish()
    }
}