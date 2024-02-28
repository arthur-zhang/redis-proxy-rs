use bytes::{Buf, BytesMut};
use tokio_util::codec::Decoder;

use redis_proxy_common::ensure;
use redis_proxy_common::tools::{CR, is_digit, LF, offset_from};

use crate::error::DecodeError;

#[derive(Debug)]
pub struct RespPktDecoder {
    state: State,
    pending_integer: PendingInteger,
    stack: Vec<RespType>,
    is_done: bool,
    is_error: bool,
}

impl RespPktDecoder {
    pub fn new() -> RespPktDecoder {
        Self {
            state: State::ValueRootStart,
            pending_integer: PendingInteger::new(),
            stack: Vec::new(),
            is_done: false,
            is_error: false,
        }
    }
    fn update_top_resp_type(&mut self, resp_type: RespType) {
        if let Some(cur_value) = self.stack.last_mut() {
            *cur_value = resp_type;
        }
    }
}


impl Decoder for RespPktDecoder {
    type Item = ResFramedData;
    type Error = DecodeError;


    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() { return Ok(None); }
        let mut p = src.as_ref();
        let mut is_done = false;
        while p.has_remaining() || self.state == State::ValueComplete {
            match self.state {
                State::ValueRootStart => {
                    self.stack.push(RespType::Null);
                    self.state = State::ValueStart;
                    self.is_error = false;
                    is_done = false;
                }
                State::ValueStart => {
                    self.pending_integer.reset();
                    match p[0] {
                        b'*' => {
                            self.state = State::IntegerStart;
                            self.update_top_resp_type(RespType::Array(Arr::default()));
                        }
                        b'$' => {
                            self.update_top_resp_type(RespType::BulkString);
                            self.state = State::IntegerStart;
                        }
                        b'-' => {
                            self.update_top_resp_type(RespType::Error);
                            self.state = State::SimpleString;
                            self.is_error = true;
                        }
                        b'+' => {
                            self.update_top_resp_type(RespType::SimpleString);
                            self.state = State::SimpleString;
                        }
                        b':' => {
                            self.update_top_resp_type(RespType::Integer);
                            self.state = State::IntegerStart;
                        }
                        _ => { return Err(DecodeError::InvalidProtocol); }
                    }
                    p.advance(1);
                }
                State::IntegerStart => {
                    if p[0] == b'-' {
                        self.pending_integer.neg = true;
                        p.advance(1);
                    }
                    self.state = State::Integer;
                }
                State::Integer => {
                    if p[0] == CR {
                        self.state = State::IntegerLF;
                    } else if is_digit(p[0]) {
                        self.pending_integer.value = self.pending_integer.value * 10 + (p[0] - b'0') as u64;
                    } else {
                        return Err(DecodeError::InvalidProtocol);
                    }
                    p.advance(1);
                }
                State::IntegerLF => {
                    ensure!(p[0] == LF, DecodeError::InvalidProtocol);
                    p.advance(1);
                    let current_value = self.stack.last_mut().ok_or(DecodeError::InvalidProtocol)?;
                    match current_value {
                        RespType::Array(ref mut arr) => {
                            if self.pending_integer.neg {
                                // null array, convert to null
                                self.update_top_resp_type(RespType::Null);
                                self.state = State::ValueComplete;
                            } else if self.pending_integer.value == 0 {
                                // empty array
                                self.state = State::ValueComplete;
                            } else {
                                arr.size = self.pending_integer.value as usize;
                                self.stack.push(RespType::Null);
                                self.state = State::ValueStart;
                            }
                        }
                        RespType::Integer => {
                            self.state = State::ValueComplete;
                        }
                        RespType::BulkString => {
                            if self.pending_integer.neg {
                                // Null bulk string. Switch type to null and move to value complete.
                                self.state = State::ValueComplete;
                            } else {
                                self.state = State::BulkStringBody;
                            }
                        }
                        _ => { return Err(DecodeError::InvalidProtocol); }
                    }
                }
                State::BulkStringBody => {
                    let length_to_read = std::cmp::min(self.pending_integer.value, p.remaining() as u64);
                    self.pending_integer.value -= length_to_read;
                    p.advance(length_to_read as usize);

                    if self.pending_integer.value == 0 {
                        self.state = State::CR;
                    }
                }
                State::CR => {
                    if p[0] != CR { return Err(DecodeError::InvalidProtocol); }
                    p.advance(1);
                    self.state = State::LF;
                }
                State::LF => {
                    if p[0] != LF { return Err(DecodeError::InvalidProtocol); }
                    p.advance(1);
                    self.state = State::ValueComplete;
                }
                State::SimpleString => {
                    if p[0] == CR {
                        self.state = State::LF;
                    }
                    p.advance(1);
                }
                State::ValueComplete => {
                    ensure!(!self.stack.is_empty(), DecodeError::InvalidProtocol);
                    let _ = self.stack.pop();

                    if self.stack.is_empty() {
                        // all done
                        self.state = State::ValueRootStart;
                        is_done = true;
                        break;
                    }
                    let cur_value = self.stack.last_mut().unwrap();

                    if let RespType::Array(arr) = cur_value {
                        if arr.cur_idx < arr.size - 1 {
                            arr.cur_idx += 1;
                            self.stack.push(RespType::Null);
                            self.state = State::ValueStart;
                        }
                    } else {
                        return Err(DecodeError::InvalidProtocol);
                    }
                }
            }
        }

        let consumed = offset_from(p.as_ptr(), src.as_ptr());
        let data = src.split_to(consumed).freeze();
        Ok(Some(ResFramedData { data, is_done, is_error: self.is_error }))
    }
}

pub struct ResFramedData {
    pub data: bytes::Bytes,
    pub is_done: bool,
    pub is_error: bool,
}

#[derive(Debug, PartialOrd, PartialEq)]
enum State {
    ValueRootStart,
    ValueStart,
    IntegerStart,
    Integer,
    IntegerLF,
    BulkStringBody,
    CR,
    LF,
    SimpleString,
    ValueComplete,
}

#[derive(Debug, Clone)]
enum RespType {
    Null,
    SimpleString,
    BulkString,
    Integer,
    Error,
    Array(Arr),
    CompositeArray,
}

#[derive(Debug, Clone, Default)]
struct Arr {
    size: usize,
    cur_idx: usize,
}

impl Arr {
    fn new(size: usize, cur_idx: usize) -> Self {
        Self {
            size,
            cur_idx,
        }
    }
}


impl RespType {
    fn is_array(&self) -> bool {
        match self {
            RespType::Array(_) => true,
            _ => false,
        }
    }

    fn as_array_mut(&mut self) -> Option<&mut Arr> {
        match self {
            RespType::Array(v) => Some(v),
            _ => None,
        }
    }
    fn do_as_array(&mut self, f: &mut dyn FnMut(&mut Arr)) {
        match self {
            RespType::Array(ref mut v) => {
                f(v);
            }
            _ => {}
        }
    }
    fn do_as_integer(&self, f: &mut dyn FnMut(u64)) {
        match self {
            RespType::Integer => {
                f(0);
            }
            _ => {}
        }
    }
}

#[derive(Debug)]
struct PendingInteger {
    value: u64,
    neg: bool,
}

impl PendingInteger {
    fn new() -> Self {
        Self {
            value: 0,
            neg: false,
        }
    }
    #[inline]
    fn reset(&mut self) {
        self.value = 0;
        self.neg = false;
    }
}

#[cfg(test)]
mod tests {
    use log::debug;

    use super::*;

    #[test]
    fn test_parse() {
        let bytes = "*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n".as_bytes();
        let mut decoder = RespPktDecoder {
            state: State::ValueRootStart,
            pending_integer: PendingInteger::new(),
            stack: Vec::new(),
        };
        let mut buf = BytesMut::from(bytes);
        let result = decoder.decode(&mut buf).unwrap().unwrap();
        debug!("result: {:?}", std::str::from_utf8(result.data.as_ref()));
    }

    #[test]
    fn test_parse_string() {
        let bytes = "$5\r\nhello\r\n".as_bytes();
        let mut decoder = RespPktDecoder {
            state: State::ValueRootStart,
            pending_integer: PendingInteger::new(),
            stack: Vec::new(),
        };
        let mut buf = BytesMut::from(bytes);
        let result = decoder.decode(&mut buf).unwrap().unwrap();
        debug!("result: {:?}", std::str::from_utf8(result.data.as_ref()));
    }

    #[test]
    fn test_ping() {
        let bytes = "*1\r\n$".as_bytes();
        let mut decoder = RespPktDecoder {
            state: State::ValueRootStart,
            pending_integer: PendingInteger::new(),
            stack: Vec::new(),
        };
        let mut buf = BytesMut::from(bytes);
        let result = decoder.decode(&mut buf).unwrap().unwrap();
        debug!("result: {:?}", std::str::from_utf8(result.data.as_ref()));
        debug!("decoder: {:?}", decoder);
        buf.extend_from_slice("4\r\nPING\r\n".as_bytes());
        let result = decoder.decode(&mut buf).unwrap().unwrap();
        debug!("result: {:?}", std::str::from_utf8(result.data.as_ref()));
        debug!("decoder: {:?}", decoder);
    }

    #[test]
    fn test_empty_array() {
        let bytes = "*0\r\n".as_bytes();
        let mut decoder = RespPktDecoder::new();
        let mut buf = BytesMut::from(bytes);
        let result = decoder.decode(&mut buf).unwrap().unwrap();
        debug!("result: {:?}", std::str::from_utf8(result.data.as_ref()));
        debug!("decoder: {:?}", decoder);
    }

    #[test]
    fn test_integer_resp() {
        let bytes = ":2\r\n".as_bytes();
        let mut decoder = RespPktDecoder::new();
        let mut buf = BytesMut::from(bytes);
        let result = decoder.decode(&mut buf).unwrap().unwrap();
        debug!("result: {:?}", std::str::from_utf8(result.data.as_ref()));
        debug!("decoder: {:?}", decoder);
    }

    #[test]
    fn test_null_bulk_string() {
        let bytes = "$-1\r\n".as_bytes();
        let mut decoder = RespPktDecoder::new();
        let mut buf = BytesMut::from(bytes);
        let result = decoder.decode(&mut buf).unwrap().unwrap();
        debug!("result: {:?}", std::str::from_utf8(result.data.as_ref()));
        debug!("decoder: {:?}", decoder);
    }

    #[test]
    fn test_continuous_resp() {
        let bytes = "$-1\r\n$-1\r\n".as_bytes();
        let mut decoder = RespPktDecoder::new();
        let mut buf = BytesMut::from(bytes);
        let result = decoder.decode(&mut buf).unwrap().unwrap();
        debug!("result: {:?}", std::str::from_utf8(result.data.as_ref()));
        debug!("decoder: {:?}", decoder);
        let result = decoder.decode(&mut buf).unwrap().unwrap();
        debug!("result: {:?}", std::str::from_utf8(result.data.as_ref()));
        debug!("decoder: {:?}", decoder);
    }

    #[test]
    fn test_array() {
        let content = include_bytes!("/Users/arthur/Downloads/resp.txt.pcapng").as_ref();

        let mut bytes_mut = BytesMut::from(content);
        let mut decoder = RespPktDecoder::new();
        let result = decoder.decode(&mut bytes_mut).unwrap().unwrap();
        debug!("result: {:?}", std::str::from_utf8(&result.data[0..100]));
    }
}