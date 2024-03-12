use std::cmp::min;
use std::fmt::{Debug, Display};
use std::ops::Range;
use std::time::Instant;

use bytes::{Buf, Bytes, BytesMut};
use log::debug;
use tokio_util::codec::{Decoder, Encoder};

use redis_proxy_common::cmd::{CmdType, KeyInfo};
use redis_proxy_common::ReqFrameData;
use redis_proxy_common::tools::{CR, is_digit, LF, offset_from};

use crate::error::DecodeError;

pub struct ReqPktDecoder {
    state: State,
    cmd_type: CmdType,
    bulk_size: u64,
    pending_integer: u64,
    bulk_read_size: u64,
    bulk_read_index: u64,
    bulk_read_args: Option<Vec<Range<usize>>>,
    req_start: Instant,
}

impl PartialEq for ReqPktDecoder {
    fn eq(&self, other: &Self) -> bool {
        self.state == other.state &&
            self.state == other.state &&
            self.cmd_type == other.cmd_type &&
            self.bulk_size == other.bulk_size &&
            self.pending_integer == other.pending_integer &&
            self.bulk_read_size == other.bulk_read_size &&
            self.bulk_read_index == other.bulk_read_index &&
            self.bulk_read_args == other.bulk_read_args
    }
}

impl Eq for ReqPktDecoder {}

impl Encoder<Bytes> for ReqPktDecoder {
    type Error = anyhow::Error;

    fn encode(&mut self, item: Bytes, dst: &mut BytesMut) -> Result<(), Self::Error> {
        dst.extend_from_slice(&item);
        Ok(())
    }
}


impl Decoder for ReqPktDecoder {
    type Item = ReqFrameData;
    type Error = DecodeError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() { return Ok(None); }
        let mut p = src.as_ref();

        let mut frame_start = false;

        let self_snapshot = self.clone();
        let mut need_more_data = false;

        while p.has_remaining() || self.state == State::ValueComplete {
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
                        need_more_data = true;
                        break;
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
                    self.bulk_read_size = self.bulk_read_count();
                    self.bulk_read_index = 1;
                    self.pending_integer = 0;
                    p.advance(1);
                    frame_start = true;

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
                    // read more
                    if self.bulk_read_index < self.bulk_read_size {
                        if p.len() < self.pending_integer as usize {
                            p.advance(p.len());
                            need_more_data = true;
                            break;
                        }
                        let start_index = offset_from(p.as_ptr(), src.as_ptr());
                        let end_index = start_index + self.pending_integer as usize;
                        if let Some(ref mut it) = self.bulk_read_args {
                            it.push(start_index..end_index);
                        }
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

                    if self.bulk_read_index == self.bulk_read_size || self.bulk_read_index == self.bulk_size {
                        self.state = State::ValueComplete;
                    } else {
                        self.state = State::IntegerStart;
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


        // check why we are here
        // 1. Not enough data
        // 2. A frame is complete(or a partial bulk string body)

        if p.is_empty() && self.bulk_read_index < self.bulk_read_size {
            need_more_data = true;
        }

        if need_more_data {
            *self = self_snapshot;
            return Ok(None);
        }

        let consumed = offset_from(p.as_ptr(), src.as_ptr());
        let bytes = src.split_to(consumed).freeze();

        let is_done = self.bulk_read_index == self.bulk_size;

        let list = self.bulk_read_args.take();
        self.bulk_read_args = Some(Vec::new());

        Ok(Some(ReqFrameData::new(frame_start, self.cmd_type.clone(), list, bytes, is_done)))
    }
}

impl ReqPktDecoder {
    pub fn new() -> Self {
        Self {
            state: State::ValueRootStart,
            cmd_type: CmdType::UNKNOWN,
            bulk_size: u64::MAX,
            pending_integer: 0,
            bulk_read_size: u64::MAX,
            bulk_read_index: 0,
            bulk_read_args: Some(Vec::new()),
            req_start: Instant::now(),
        }
    }
    pub fn reset(&mut self) {
        self.state = State::ValueRootStart;
        self.cmd_type = CmdType::UNKNOWN;
        self.bulk_size = u64::MAX;
        self.pending_integer = 0;
        self.bulk_read_size = u64::MAX;
        self.bulk_read_index = 0;
        self.bulk_read_args.as_mut().map(Vec::clear);
        self.req_start = Instant::now();
    }

    fn bulk_read_count(&self) -> u64 {
        let key_info = self.cmd_type.redis_key_info();
        if matches!(key_info, KeyInfo::OneKey) {
            return 2;
        }
        return self.bulk_size;
    }

    // pub fn is_eager(&self) -> bool {
    //     self.eager_read_size != 0 && self.bulk_read_index < self.eager_read_size
    // }
    pub fn cmd_type(&self) -> &CmdType {
        &self.cmd_type
    }
    pub fn req_start(&self) -> Instant {
        self.req_start
    }
    pub fn eager_read_list(&self) -> &Option<Vec<Range<usize>>> {
        &self.bulk_read_args
    }
}

#[derive(Debug, PartialOrd, PartialEq, Copy, Clone)]
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

#[cfg(test)]
mod tests {
    use log::debug;

    use super::*;

    #[test]
    fn test_ping() {
        std::env::set_var("RUST_LOG", "debug");
        env_logger::init();

        let resp = "*1\r\n$4\r\nping\r\n*1\r\n$4\r\nping\r\n";
        let mut decoder = ReqPktDecoder::new();
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
        let mut decoder = ReqPktDecoder::new();
        let mut bytes_mut = BytesMut::from(resp);
        while let Some(ret) = decoder.decode(&mut bytes_mut).unwrap() {
            debug!("ret: {:?}", ret);
        }
    }

    #[test]
    fn test_partial() {
        let cmd = b"*3\r\n$3\r\nSET\r\n$16\r\nkey:__rand_int__\r\n$3\r\nxxx\r\n";
        let mut decoder = ReqPktDecoder::new();
        let mut bytes_mut = BytesMut::from(&cmd[..1]);
        println!("bytes_mut: {:?}", bytes_mut);
        let res = decoder.decode(&mut bytes_mut).unwrap();
        println!("res: {:?}", res);
        assert_eq!(res.is_none(), true);
        let decoder1 = ReqPktDecoder {
            state: State::ValueRootStart,
            cmd_type: CmdType::UNKNOWN,
            bulk_size: u64::MAX,
            pending_integer: 0,
            bulk_read_size: u64::MAX,
            bulk_read_index: 0,
            bulk_read_args: Some(vec![]),
            req_start: Instant::now(),
        };
        for i in 0..36 {
            bytes_mut.clear();
            bytes_mut.extend_from_slice(&cmd[..i]);

            println!("bytes_mut: {:?}", bytes_mut);
            let res = decoder.decode(&mut bytes_mut).unwrap();
            assert_eq!(res.is_none(), true);
            assert_eq!(decoder, decoder1);
        }
        bytes_mut.clear();
        bytes_mut.extend_from_slice(&cmd[..36]);
        let res = decoder.decode(&mut bytes_mut).unwrap();

        assert_eq!(res.is_some(), true);

        println!("res: {:?}", res);
        let decoder2 = ReqPktDecoder {
            state: State::IntegerStart,
            cmd_type: CmdType::SET,
            bulk_size: 3,
            pending_integer: 0,
            bulk_read_size: 2,
            bulk_read_index: 2,
            bulk_read_args: Some(vec![]),
            req_start: Instant::now(),
        };
        assert_eq!(decoder, decoder2);

        assert_eq!(res.unwrap(), ReqFrameData::new(true, CmdType::SET,
                                                   Some(vec![18..34]),
                                                   Bytes::from(&cmd[..36]), false));
        assert_eq!(true, bytes_mut.is_empty());

        let decoder3 = ReqPktDecoder {
            state: State::ArgLF,
            cmd_type: CmdType::SET,
            bulk_size: 3,
            pending_integer: 0,
            bulk_read_size: 2,
            bulk_read_index: 2,
            bulk_read_args: Some(vec![]),
            req_start: Instant::now(),
        };
        // println!("{}", cmd.len());
        bytes_mut.extend_from_slice(&cmd[36..44]);
        let res = decoder.decode(&mut bytes_mut).unwrap();
        println!("req frame data: {:?}", res);
        assert_eq!(res.is_some(), true);
        assert_eq!(decoder, decoder3);
        assert_eq!(res.unwrap(), ReqFrameData::new(false, CmdType::SET,
                                                   Some(vec![]),
                                                   Bytes::from(&cmd[36..44]), false));
        assert_eq!(true, bytes_mut.is_empty());


        let decoder4 = ReqPktDecoder {
            state: State::ValueRootStart,
            cmd_type: CmdType::SET,
            bulk_size: 3,
            pending_integer: 0,
            bulk_read_size: 2,
            bulk_read_index: 3,
            bulk_read_args: Some(vec![]),
            req_start: Instant::now(),
        };
        bytes_mut.extend_from_slice(&cmd[44..45]);
        let res = decoder.decode(&mut bytes_mut).unwrap();
        println!("req frame data: {:?}", res);
        assert_eq!(res.is_some(), true);
        assert_eq!(decoder, decoder4);
        assert_eq!(res.unwrap(), ReqFrameData::new(false, CmdType::SET,
                                                   Some(vec![]),
                                                   Bytes::from(&cmd[44..45]), true));
        assert_eq!(true, bytes_mut.is_empty());
    }
}