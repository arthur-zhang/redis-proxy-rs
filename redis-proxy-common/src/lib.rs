use std::fmt::{Debug, Formatter};
use std::ops::Range;
use crate::cmd::CmdType;

pub mod cmd;
pub mod tools;

pub struct DecodedFrame {
    pub frame_start: bool,
    pub cmd_type: Option<CmdType>,
    pub eager_read_list: Option<Vec<Range<usize>>>,
    pub raw_bytes: bytes::Bytes,
    pub is_eager: bool,
    pub is_done: bool,
}

impl Debug for DecodedFrame {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DecodedFrame")
            .field("data", &std::str::from_utf8(&self.raw_bytes))
            .field("is_eager", &self.is_eager)
            .finish()
    }
}