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
}

impl RespPktDecoder {
    pub fn new() -> RespPktDecoder {
        Self {
            state: State::ValueRootStart,
            pending_integer: PendingInteger::new(),
            stack: Vec::new(),
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
        let mut res_is_ok = false;
        while p.has_remaining() || self.state == State::ValueComplete {
            match self.state {
                State::ValueRootStart => {
                    self.stack.push(RespType::Null);
                    self.state = State::ValueStart;
                    res_is_ok = false;
                    is_done = false;
                }
                State::ValueStart => {
                    self.pending_integer.reset();
                    match p[0] {
                        b'*' | b'~' | b'>' => {
                            self.update_top_resp_type(RespType::Collection(Collection::default()));
                            self.state = State::IntegerStart;
                            res_is_ok = true;
                        }
                        b'$' => {
                            self.update_top_resp_type(RespType::BulkString);
                            self.state = State::IntegerStart;
                            res_is_ok = true;
                        }
                        b'-' => {
                            self.update_top_resp_type(RespType::Error);
                            self.state = State::SimpleString;
                            res_is_ok = false;
                        }
                        b'+' => {
                            self.update_top_resp_type(RespType::SimpleString);
                            self.state = State::SimpleString;
                            res_is_ok = true;
                        }
                        b':' => {
                            self.update_top_resp_type(RespType::Integer);
                            self.state = State::IntegerStart;
                            res_is_ok = true;
                        }
                        // the following is resp3 type

                        // The null data type represents non-existent values. "_\r\n"
                        b'_' => {
                            self.update_top_resp_type(RespType::SimpleString);
                            self.state = State::SimpleString;
                            res_is_ok = true;
                        }
                        // RESP booleans are encoded as follows: "#<t|f>\r\n"
                        b'#' => {
                            self.update_top_resp_type(RespType::SimpleString);
                            self.state = State::SimpleString;
                            res_is_ok = true;
                        }
                        // The Double RESP type encodes a double-precision floating point value. Doubles are encoded as follows:
                        // ,[<+|->]<integral>[.<fractional>][<E|e>[sign]<exponent>]\r\n
                        b',' => {
                            self.update_top_resp_type(RespType::SimpleString);
                            self.state = State::SimpleString;
                            res_is_ok = true;
                        }
                        //This type can encode integer values outside the range of signed 64-bit integers.
                        // Big numbers use the following encoding:
                        // ([+|-]<number>\r\n
                        b'(' => {
                            self.update_top_resp_type(RespType::SimpleString);
                            self.state = State::SimpleString;
                            res_is_ok = true;
                        }
                        // This type combines the purpose of simple errors with the expressive power of bulk strings.
                        // It is encoded as:
                        // !<length>\r\n<error>\r\n
                        b'!' => {
                            self.update_top_resp_type(RespType::BulkString);
                            self.state = State::IntegerStart;
                            res_is_ok = false;
                        }
                        // This type is similar to the bulk string, with the addition of providing a hint about the data's encoding.
                        // A verbatim string's RESP encoding is as follows:
                        // =<length>\r\n<encoding>:<data>\r\n
                        b'=' => {
                            self.update_top_resp_type(RespType::BulkString);
                            self.state = State::IntegerStart;
                            res_is_ok = true;
                        }
                        // Maps
                        // The RESP map encodes a collection of key-value tuples, i.e., a dictionary or a hash.
                        // It is encoded as follows:
                        // %<number-of-entries>\r\n<key-1><value-1>...<key-n><value-n>
                        b'%' => {
                            self.update_top_resp_type(RespType::Map(Collection::default()));
                            self.state = State::IntegerStart;
                            res_is_ok = true;
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
                    let is_map_type = matches!(current_value, RespType::Map(_));
                    match current_value {
                        RespType::Collection(arr) | RespType::Map(arr) => {
                            if self.pending_integer.neg {
                                // null array, convert to null
                                self.update_top_resp_type(RespType::Null);
                                self.state = State::ValueComplete;
                            } else if self.pending_integer.value == 0 {
                                // empty array
                                self.state = State::ValueComplete;
                            } else {
                                if is_map_type {
                                    arr.size = 2 * self.pending_integer.value as usize;
                                } else {
                                    arr.size = self.pending_integer.value as usize;
                                }
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

                    match cur_value {
                        RespType::Collection(col) | RespType::Map(col) => {
                            if col.cur_idx < col.size - 1 {
                                col.cur_idx += 1;
                                self.stack.push(RespType::Null);
                                self.state = State::ValueStart;
                            }
                        }
                        _ => {
                            return Err(DecodeError::InvalidProtocol);
                        }
                    }
                }
            }
        }

        let consumed = offset_from(p.as_ptr(), src.as_ptr());
        let data = src.split_to(consumed).freeze();
        Ok(Some(ResFramedData { data, is_done, res_is_ok }))
    }
}

#[derive(Debug)]
pub struct ResFramedData {
    pub data: bytes::Bytes,
    pub is_done: bool,
    pub res_is_ok: bool,
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
    Collection(Collection),
    Map(Collection),
}

#[derive(Debug, Clone, Default)]
struct Collection {
    size: usize,
    cur_idx: usize,
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
        let bytes = "$-1\r\n$-1\r\n$1\r\na\r\n".as_bytes();
        let mut decoder = RespPktDecoder::new();
        let mut buf = BytesMut::from(bytes);
        let result = decoder.decode(&mut buf).unwrap().unwrap();
        println!("result: {:?}", result);
        println!("decoder: {:?}", decoder);
        let result = decoder.decode(&mut buf).unwrap().unwrap();
        println!("result: {:?}", result);
        println!("decoder: {:?}", decoder);

        let result = decoder.decode(&mut buf).unwrap().unwrap();
        println!("result: {:?}", result);
        println!("decoder: {:?}", decoder);
    }

    #[test]
    fn test_not_exist() {
        let bytes = "$-1\r\n".as_bytes();
        let mut decoder = RespPktDecoder::new();
        let mut buf = BytesMut::from(bytes);
        let result = decoder.decode(&mut buf).unwrap().unwrap();
        println!("result:{:?}", result);
    }

    #[test]
    fn test_null() {
        let bytes = "_\r\n".as_bytes();
        let mut decoder = RespPktDecoder::new();
        let mut buf = BytesMut::from(bytes);
        let result = decoder.decode(&mut buf).unwrap().unwrap();
        assert_eq!(result.data.as_ref(), "_\r\n".as_bytes());
        assert_eq!(result.res_is_ok, true);
        assert_eq!(result.is_done, true);
    }

    #[test]
    fn test_boolean() {
        let bytes = "#t\r\n".as_bytes();
        let mut decoder = RespPktDecoder::new();
        let mut buf = BytesMut::from(bytes);
        let result = decoder.decode(&mut buf).unwrap().unwrap();
        assert_eq!(result.data.as_ref(), "#t\r\n".as_bytes());
        assert_eq!(result.res_is_ok, true);
        assert_eq!(result.is_done, true);
    }

    #[test]
    fn test_double() {
        let bytes = ",+1.23\r\n".as_bytes();
        let mut decoder = RespPktDecoder::new();
        let mut buf = BytesMut::from(bytes);
        let result = decoder.decode(&mut buf).unwrap().unwrap();
        assert_eq!(result.data.as_ref(), ",+1.23\r\n".as_bytes());
        assert_eq!(result.res_is_ok, true);
        assert_eq!(result.is_done, true);
    }

    #[test]
    fn test_bulk_err() {
        let bytes = "!21\r\nSYNTAX invalid syntax\r\n".as_bytes();
        let mut decoder = RespPktDecoder::new();
        let mut buf = BytesMut::from(bytes);
        let result = decoder.decode(&mut buf).unwrap().unwrap();
        assert_eq!(result.data.as_ref(), "!21\r\nSYNTAX invalid syntax\r\n".as_bytes());
        assert_eq!(result.res_is_ok, false);
        assert_eq!(result.is_done, true);
    }

    #[test]
    fn test_verbatim() {
        let bytes = "=15\r\ntxt:Some string\r\n".as_bytes();
        let mut decoder = RespPktDecoder::new();
        let mut buf = BytesMut::from(bytes);
        let result = decoder.decode(&mut buf).unwrap().unwrap();
        assert_eq!(result.data.as_ref(), "=15\r\ntxt:Some string\r\n".as_bytes());
        assert_eq!(result.res_is_ok, true);
        assert_eq!(result.is_done, true);
    }

    #[test]
    fn test_map() {
        let bytes = "%2\r\n+first\r\n:1\r\n+second\r\n:2\r\n".as_bytes();
        let mut decoder = RespPktDecoder::new();
        let mut buf = BytesMut::from(bytes);
        let result = decoder.decode(&mut buf).unwrap().unwrap();
        assert_eq!(result.data.as_ref(), bytes);
        assert_eq!(result.res_is_ok, true);
        assert_eq!(result.is_done, true);
    }

    #[test]
    fn test_set() {
        let bytes = "~3\r\n+hello\r\n+world\r\n+foo\r\n".as_bytes();
        let mut decoder = RespPktDecoder::new();
        let mut buf = BytesMut::from(bytes);
        let result = decoder.decode(&mut buf).unwrap().unwrap();
        assert_eq!(result.data.as_ref(), bytes);
        assert_eq!(result.res_is_ok, true);
        assert_eq!(result.is_done, true);
    }

    #[test]
    fn test_push() {
        let bytes = ">3\r\n+hello\r\n+world\r\n+foo\r\n".as_bytes();
        let mut decoder = RespPktDecoder::new();
        let mut buf = BytesMut::from(bytes);
        let result = decoder.decode(&mut buf).unwrap().unwrap();
        assert_eq!(result.data.as_ref(), bytes);
        assert_eq!(result.res_is_ok, true);
        assert_eq!(result.is_done, true);
    }

    #[test]
    fn test_stream_data(){
        
    }
}