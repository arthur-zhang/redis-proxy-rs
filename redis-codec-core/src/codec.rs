use std::cmp::min;
use std::fmt::{Debug, Display, Formatter};
use std::ops::Range;

use bytes::{Buf, BytesMut};
use log::debug;
use tokio_util::codec::Decoder;

use crate::cmd::CmdType;

pub struct PartialDecoder {
    bulk_len: usize,
    read_narg: usize,
    state: State,
    cmd_type: CmdType,
    eager_read: usize,
    eager_read_list: Vec<Range<usize>>,
    arg_len: usize,
    // tmp_eager_read: usize,
    tmp_token_size: usize,
}


pub enum State {
    SW_START,
    SW_NARG,
    SW_NARG_LF,
    SW_REQ_TYPE_LEN,
    SW_REQ_TYPE_LEN_LF,
    SW_REQ_TYPE,
    SW_REQ_TYPE_LF,
    SW_REMAINING,

    SW_ARG_LEN,
    SW_ARG_LEN_LF,
    SW_ARG,
    SW_ARG_LF,

    SW_DONE,
}

#[derive(Debug)]
pub enum PartialResp {
    Eager(bytes::Bytes),
    Lazy(bytes::Bytes),
}

impl Decoder for PartialDecoder {
    type Item = PartialResp;
    type Error = DecodeError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        debug!("---------------------------");
        if src.is_empty() {
            return Ok(None);
        }
        let mut p = MyPtr::new(src.as_ref());

        let mut tmp_len = 0usize;
        let mut token_started = false;
        while (p.has_remaining()) {
            let ch = p.first();
            match self.state {
                State::SW_START => {
                    if p.first() != b'*' {
                        return Err(DecodeError::InvalidProtocol);
                    }
                    self.state = State::SW_NARG;
                }
                State::SW_NARG => {
                    if is_digit(ch) {
                        self.bulk_len = self.bulk_len * 10 + (ch - b'0') as usize;
                    } else if ch == CR {
                        self.state = State::SW_NARG_LF;
                    } else {
                        return Err(DecodeError::InvalidProtocol);
                    }
                }
                State::SW_NARG_LF => {
                    if ch != LF {
                        return Err(DecodeError::InvalidProtocol);
                    }
                    self.state = State::SW_REQ_TYPE_LEN;
                }
                State::SW_REQ_TYPE_LEN => {
                    if !token_started {
                        if ch != b'$' {
                            return Err(DecodeError::InvalidProtocol);
                        }
                        token_started = true;
                        tmp_len = 0;
                    } else if is_digit(ch) {
                        tmp_len = tmp_len * 10 + (ch - b'0') as usize;
                    } else if ch == CR {
                        token_started = false;
                        self.state = State::SW_REQ_TYPE_LEN_LF;
                    } else {
                        return Err(DecodeError::InvalidProtocol);
                    }
                }
                State::SW_REQ_TYPE_LEN_LF => {
                    if ch != LF {
                        return Err(DecodeError::InvalidProtocol);
                    }
                    self.state = State::SW_REQ_TYPE;
                }
                State::SW_REQ_TYPE => {
                    debug!("SW_REQ_TYPE: {:?}", p);
                    if !token_started {
                        token_started = true;
                    }
                    if p.len() < tmp_len {
                        return Ok(None);
                    }

                    let cmd_type = CmdType::from(&p.inner[0..tmp_len]);
                    self.cmd_type = cmd_type;

                    p.advance(tmp_len);
                    token_started = false;
                    self.state = State::SW_REQ_TYPE_LF;
                }
                State::SW_REQ_TYPE_LF => {
                    debug!("SW_REQ_TYPE_LF: {:?}", p);
                    if ch != LF {
                        return Err(DecodeError::InvalidProtocol);
                    }
                    self.read_narg += 1;

                    // calc how many args should be read eagerly
                    self.eager_read = self.eager_read_count();

                    token_started = false;
                    if self.read_narg == self.bulk_len {
                        self.state = State::SW_DONE;
                        let bytes = src.split_to(p.consumed()).freeze();
                        return Ok(Some(PartialResp::Eager(bytes)));
                    } else {
                        self.state = State::SW_ARG_LEN;
                    }
                }
                State::SW_ARG_LEN => {
                    debug!("SW_ARG_LEN: {:?}", p);
                    if !token_started {
                        if ch != b'$' {
                            return Err(DecodeError::InvalidProtocol);
                        }
                        token_started = true;
                        tmp_len = 0;
                    } else if is_digit(ch) {
                        tmp_len = tmp_len * 10 + (ch - b'0') as usize;
                    } else if ch == CR {
                        token_started = false;
                        self.state = State::SW_ARG_LEN_LF;
                    } else {
                        return Err(DecodeError::InvalidProtocol);
                    }
                }
                State::SW_ARG_LEN_LF => {
                    debug!("SW_ARG_LEN_LF: {:?}", p);
                    if ch != LF {
                        return Err(DecodeError::InvalidProtocol);
                    }
                    self.arg_len = tmp_len;
                    token_started = false;
                    self.state = State::SW_ARG;
                }
                // read arg content
                State::SW_ARG => {
                    debug!("SW_ARG: {:?}", p);
                    if !token_started {
                        token_started = true;
                    }

                    if p.len() + self.tmp_token_size < self.arg_len {
                        // eager read mode, return none to wait for more data
                        if self.is_eager() {
                            return Ok(None);
                        }
                        // lazy read mode, return raw data
                        let len = p.len();
                        let bytes = src.split_to(p.consumed()).freeze();
                        self.tmp_token_size += len;
                        return Ok(Some(PartialResp::Lazy(bytes)));
                    }

                    if self.is_eager() {
                        let consumed = p.consumed();
                        let range = consumed..(consumed + self.arg_len);
                        self.eager_read_list.push(range);
                    }


                    p.advance(self.arg_len - self.tmp_token_size);
                    self.tmp_token_size = 0;
                    self.state = State::SW_ARG_LF;
                }

                State::SW_ARG_LF => {
                    debug!("SW_ARG_LF: {:?}", p);
                    if ch != LF {
                        return Err(DecodeError::InvalidProtocol);
                    }
                    self.read_narg += 1;
                    token_started = false;

                    if self.read_narg <= self.eager_read {}

                    if self.read_narg == self.bulk_len {
                        self.state = State::SW_DONE;
                        p.advance(1);
                        break;
                    } else {
                        if self.read_narg == self.eager_read {
                            if p.has_remaining() {
                                // consume \n
                                p.advance(1);
                            }

                            self.state = State::SW_ARG_LEN;
                            break;
                        }
                        self.state = State::SW_ARG_LEN;
                    }
                }
                State::SW_DONE => {
                    break;
                }

                _ => {}
            }
            p.advance(1);
        }
        let bytes = src.split_to(p.consumed()).freeze();

        if self.is_eager() {
            return Ok(Some(PartialResp::Eager(bytes)));
        } else {
            return Ok(Some(PartialResp::Lazy(bytes)));
        }
    }
}

impl PartialDecoder {
    pub fn new() -> Self {
        Self {
            bulk_len: 0,
            read_narg: 0,
            state: State::SW_START,
            cmd_type: CmdType::UNKNOWN,
            eager_read: 0,
            eager_read_list: vec![],
            arg_len: 0,
            tmp_token_size: 0,
        }
    }
    pub fn eager_read_count(&self) -> usize {
        let key_count = self.cmd_type.redis_key_count();
        if key_count >= 2 {
            return self.bulk_len;
        }
        return min(key_count as usize + 1, self.bulk_len);
    }

    pub fn is_eager(&self) -> bool {
        self.read_narg <= self.eager_read
    }
}


pub struct MyPtr<'a> {
    inner: &'a [u8],
    advance_count: usize,
}

impl<'a> Display for MyPtr<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", std::str::from_utf8(self.inner))
    }
}

impl<'a> Debug for MyPtr<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", std::str::from_utf8(self.inner))
    }
}

impl<'a> MyPtr<'a> {
    pub fn new(inner: &'a [u8]) -> Self {
        Self {
            inner,
            advance_count: 0,
        }
    }
    fn has_remaining(&self) -> bool {
        !self.inner.is_empty()
    }
    fn advance(&mut self, cnt: usize) {
        self.advance_count += cnt;
        self.inner = &self.inner[cnt..];
    }
    fn first(&self) -> u8 {
        self.inner[0]
    }
    fn len(&self) -> usize {
        self.inner.len()
    }
    fn consumed(&self) -> usize {
        self.advance_count
    }
}

const CR: u8 = b'\r';
const LF: u8 = b'\n';

#[inline]
fn is_digit(b: u8) -> bool {
    b >= b'0' && b <= b'9'
}

#[derive(Debug, PartialEq)]
pub enum DecodeError {
    InvalidProtocol,
    NotEnoughData,
    UnexpectedErr,
    IOError,
}

impl From<std::io::Error> for DecodeError {
    fn from(e: std::io::Error) -> Self {
        DecodeError::IOError
    }
}

impl From<anyhow::Error> for DecodeError {
    fn from(e: anyhow::Error) -> Self {
        DecodeError::UnexpectedErr
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse() {
        env_logger::init();

        let resp = "*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n";
        let mut decoder = PartialDecoder::new();
        let mut bytes_mut = BytesMut::from(resp);
        let ret = decoder.decode(&mut bytes_mut).unwrap().unwrap();
        match &ret {
            PartialResp::Eager(it) => {
                for x in &decoder.eager_read_list {
                    let data = &it[x.clone()];
                    println!("x: {:?}", std::str::from_utf8(data));
                }
            }
            PartialResp::Lazy(_) => {}
        }
        println!("ret: {:?}", ret);
        let ret = decoder.decode(&mut bytes_mut).unwrap().unwrap();
        println!("ret: {:?}", ret);
    }

    #[test]
    fn test_parse_del() {
        env_logger::init();

        let resp = "*5\r\n$3\r\nDEL\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n";
        let mut decoder = PartialDecoder::new();
        let mut bytes_mut = BytesMut::from(resp);
        let ret = decoder.decode(&mut bytes_mut).unwrap().unwrap();
        match &ret {
            PartialResp::Eager(it) => {
                for x in &decoder.eager_read_list {
                    let data = &it[x.clone()];
                    println!("x: {:?}", std::str::from_utf8(data));
                }
            }
            PartialResp::Lazy(_) => {}
        }
        println!("ret: {:?}", ret);
        let ret = decoder.decode(&mut bytes_mut).unwrap();
        println!("ret: {:?}", ret);
    }
}