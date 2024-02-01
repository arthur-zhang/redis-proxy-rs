use std::cmp::min;
use std::fmt::{Debug, Display, Formatter};
use std::ops::Range;

use bytes::{Buf, BytesMut};
use log::debug;
use tokio_util::codec::Decoder;

use crate::cmd::CmdType;
use crate::error::DecodeError;
use crate::tools::{CR, is_digit, LF, offset_from};

pub struct KeyAwareDecoder {
    state: State,
    cmd_type: CmdType,
    bulk_size: u64,
    pending_integer: u64,

    eager_read_size: u64,
    bulk_read_index: u64,
    eager_read_list: Vec<Range<usize>>,

}

impl Decoder for KeyAwareDecoder {
    type Item = DecodedFrame;
    type Error = DecodeError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() { return Ok(None); }
        let mut p = src.as_ref();

        debug!("------------------------------------");
        while (p.has_remaining() || self.state == State::ValueComplete) {
            debug!("state: {:?}, p: {:?}", self.state, std::str::from_utf8(p));
            match self.state {
                State::ValueRootStart => {
                    self.reset();
                    if p[0] != b'*' { return Err(DecodeError::UnexpectedErr); }
                    self.state = State::NArgInteger;
                    p.advance(1);
                }
                State::NArgInteger => {
                    if p[0] == CR {
                        self.state = State::NArgIntegerLF;
                    } else if is_digit(p[0]) {
                        self.pending_integer = 10 * self.pending_integer + (p[0] - b'0') as u64;
                    } else {
                        return Err(DecodeError::InvalidProtocol);
                    }
                    p.advance(1);
                }
                State::NArgIntegerLF => {
                    if p[0] != LF { return Err(DecodeError::InvalidProtocol); }
                    self.bulk_size = self.pending_integer;
                    self.pending_integer = 0;
                    self.state = State::CmdIntegerStart;
                    p.advance(1);
                }
                State::CmdIntegerStart => {
                    if p[0] != b'$' { return Err(DecodeError::UnexpectedErr); }
                    self.state = State::CmdInteger;
                    p.advance(1);
                }
                State::CmdInteger => {
                    if p[0] == CR {
                        self.state = State::CmdIntegerLF;
                    } else if is_digit(p[0]) {
                        self.pending_integer = 10 * self.pending_integer + (p[0] - b'0') as u64;
                    } else {
                        return Err(DecodeError::InvalidProtocol);
                    }
                    p.advance(1);
                }
                State::CmdIntegerLF => {
                    if p[0] != LF { return Err(DecodeError::InvalidProtocol); }
                    self.state = State::Cmd;
                    p.advance(1);
                }
                State::Cmd => {
                    if p.len() < self.pending_integer as usize {
                        return Ok(None);
                    }
                    self.cmd_type = CmdType::from(&p[..self.pending_integer as usize]);
                    p.advance(self.pending_integer as usize);
                    self.state = State::CmdCR;
                }
                State::CmdCR => {
                    if p[0] != CR { return Err(DecodeError::InvalidProtocol); }
                    p.advance(1);
                    self.state = State::CmdLF;
                }
                State::CmdLF => {
                    if p[0] != LF { return Err(DecodeError::InvalidProtocol); }
                    self.eager_read_size = self.eager_read_count();
                    self.bulk_read_index = 1;
                    self.pending_integer = 0;
                    p.advance(1);

                    if self.bulk_read_index == self.bulk_size {
                        self.state = State::ValueComplete;
                    } else {
                        self.state = State::IntegerStart;
                    }
                }

                State::IntegerStart => {
                    if p[0] != b'$' {
                        return Err(DecodeError::InvalidProtocol);
                    }
                    self.pending_integer = 0;
                    p.advance(1);
                    self.state = State::Integer;
                }
                State::Integer => {
                    if p[0] == CR {
                        self.state = State::IntegerLF;
                    } else if is_digit(p[0]) {
                        self.pending_integer = 10 * self.pending_integer + (p[0] - b'0') as u64;
                    } else {
                        return Err(DecodeError::InvalidProtocol);
                    }
                    p.advance(1);
                }
                State::IntegerLF => {
                    if p[0] != LF { return Err(DecodeError::InvalidProtocol); }
                    p.advance(1);
                    self.state = State::BulkStringBody;
                }
                State::BulkStringBody => {
                    let is_eager = self.bulk_read_index < self.eager_read_size;

                    if is_eager {
                        if p.len() < self.pending_integer as usize {
                            return Ok(None);
                        }
                        let start_index = offset_from(p.as_ptr(), src.as_ptr());
                        let end_index = start_index + self.pending_integer as usize;
                        self.eager_read_list.push(start_index..end_index);
                    }
                    let n = min(p.len(), self.pending_integer as usize);
                    self.pending_integer -= n as u64;
                    p.advance(n);

                    if self.pending_integer == 0 {
                        self.state = State::ArgCR;
                    }
                }
                State::ArgCR => {
                    if p[0] != CR { return Err(DecodeError::InvalidProtocol); }
                    p.advance(1);
                    self.state = State::ArgLF;
                }
                State::ArgLF => {
                    if p[0] != LF { return Err(DecodeError::InvalidProtocol); }
                    p.advance(1);
                    self.bulk_read_index += 1;

                    if self.bulk_read_index == self.eager_read_size || self.bulk_read_index == self.bulk_size {
                        self.state = State::ValueComplete;
                    }
                }
                State::ValueComplete => {
                    if self.bulk_read_index == self.bulk_size {
                        self.state = State::ValueRootStart;
                    } else {
                        self.state = State::IntegerStart;
                    }
                    break;
                }
            }
        }

        let consumed = offset_from(p.as_ptr(), src.as_ptr());
        let bytes = src.split_to(consumed).freeze();
        let is_eager = self.bulk_read_index <= self.eager_read_size;

        let is_done = self.bulk_read_index == self.bulk_size;

        Ok(Some(DecodedFrame { raw_bytes: bytes, is_eager, is_done }))
    }
}

impl KeyAwareDecoder {
    pub fn new() -> Self {
        Self {
            state: State::ValueRootStart,
            cmd_type: CmdType::UNKNOWN,
            bulk_size: 0,
            pending_integer: 0,
            eager_read_size: 0,
            bulk_read_index: 0,
            eager_read_list: Vec::new(),
        }
    }
    pub fn reset(&mut self) {
        self.state = State::ValueRootStart;
        self.cmd_type = CmdType::UNKNOWN;
        self.bulk_size = 0;
        self.pending_integer = 0;
        self.eager_read_size = 0;
        self.bulk_read_index = 0;
        self.eager_read_list.clear();
    }

    fn eager_read_count(&self) -> u64 {
        let key_count = self.cmd_type.redis_key_count();
        if key_count == 0 {
            return 0;
        }
        if key_count >= 2 {
            return self.bulk_size;
        }
        return min(key_count as u64 + 1, self.bulk_size);
    }

    // pub fn is_eager(&self) -> bool {
    //     self.eager_read_size != 0 && self.bulk_read_index < self.eager_read_size
    // }
    pub fn cmd_type(&self) -> &CmdType {
        &self.cmd_type
    }
    pub fn eager_read_list(&self) -> &Vec<Range<usize>> {
        &self.eager_read_list
    }
}

#[derive(Debug, PartialOrd, PartialEq)]
enum State {
    ValueRootStart,

    NArgInteger,
    NArgIntegerLF,

    CmdIntegerStart,
    CmdInteger,
    CmdIntegerLF,
    Cmd,
    CmdCR,
    CmdLF,
    IntegerStart,
    Integer,
    IntegerLF,
    BulkStringBody,
    ArgCR,
    ArgLF,
    ValueComplete,
}


pub struct DecodedFrame {
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


#[cfg(test)]
mod tests {
    use log::debug;

    use super::*;

    #[test]
    fn test_ping() {
        std::env::set_var("RUST_LOG", "debug");
        env_logger::init();

        let resp = "*1\r\n$4\r\nping\r\n*1\r\n$4\r\nping\r\n";
        let mut decoder = KeyAwareDecoder::new();
        let mut bytes_mut = BytesMut::from(resp);
        while let Some(ret) = decoder.decode(&mut bytes_mut).unwrap() {
            debug!("ret: {:?}", ret);
        }
    }

    #[test]
    fn test_parse() {
        std::env::set_var("RUST_LOG", "debug");

        env_logger::init();

        let resp = "*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n";
        let mut decoder = KeyAwareDecoder::new();
        let mut bytes_mut = BytesMut::from(resp);
        while let Some(ret) = decoder.decode(&mut bytes_mut).unwrap() {
            debug!("ret: {:?}", ret);
        }
    }
}