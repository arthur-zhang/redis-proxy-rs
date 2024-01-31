use bytes::{Buf, BytesMut};
use tokio_util::codec::Decoder;

use crate::error::DecodeError;
use crate::tools::{CR, is_digit, LF};

#[derive(Debug)]
pub struct ResPartialDecoder {
    state: State,
    msg_type: MsgType,
    bytes_count: usize,
    has_token: bool,
    integer: u64,
    negative_: bool,

    v_len: usize,
}

impl ResPartialDecoder {
    pub fn reset(&mut self) {
        self.state = State::SW_START;
        self.bytes_count = 0;
        self.has_token = false;
        self.integer = 0;
        self.negative_ = false;
    }
}

#[derive(Debug)]
pub struct ResDecodedFrame {
    raw_bytes: bytes::Bytes,
}

impl Decoder for ResPartialDecoder {
    type Item = ResDecodedFrame;
    type Error = DecodeError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() { return Ok(None); }
        let mut p = src.as_ref();
        let mut prev_p = p;
        let mut done = false;
        while p.has_remaining() {
            let ch = p[0];
            match self.state {
                State::SW_START => {
                    match ch {
                        b'+' => {
                            // p = prev_p;
                            self.msg_type = MsgType::MSG_RSP_REDIS_STATUS;
                            self.state = State::SW_STATUS;
                            continue;
                        }
                        b'-' => {
                            self.msg_type = MsgType::MSG_RSP_REDIS_ERROR;
                            self.state = State::SW_ERROR;
                            continue;
                        }
                        b':' => {
                            self.msg_type = MsgType::MSG_RSP_REDIS_INTEGER;
                            self.integer = 0;
                            self.state = State::SW_INTEGER_START;
                        }
                        b'$' => {
                            todo!()
                            // self.state = State::SW_BULK;
                        }
                        b'*' => {
                            self.state = State::SW_MULTIBULK;
                            self.msg_type = MsgType::MSG_RSP_REDIS_BULK;
                            continue;
                        }
                        _ => {
                            self.state = State::SW_SIMPLE;
                            todo!()
                        }
                    }
                }
                State::SW_STATUS => {
                    self.state = State::SW_RUNTO_CRLF;
                }
                State::SW_ERROR => {
                    if !self.has_token {
                        if ch != b'-' { return Err(DecodeError::InvalidProtocol); }
                        self.has_token = true;
                    }
                    if ch == CR {
                        self.has_token = false;
                        self.state = State::SW_ALMOST_DONE;
                    }
                }
                State::SW_INTEGER_START => {
                    if ch == CR {
                        self.state = State::SW_ALMOST_DONE;
                    } else if ch == b'-' {
                        self.negative_ = true;
                    } else if is_digit(ch) {
                        self.integer = self.integer * 10 + (ch - b'0') as u64;
                    } else {
                        return Err(DecodeError::InvalidProtocol);
                    }
                }
                State::SW_SIMPLE => {}
                State::SW_BULK => {}
                State::SW_BULK_LF => {}
                State::SW_BULK_ARG => {}
                State::SW_BULK_ARG_LF => {}
                State::SW_MULTIBULK => {
                    if !self.has_token {
                        if ch != b'*' { return Err(DecodeError::InvalidProtocol); }
                        self.has_token = true;
                        self.v_len = 0;
                    } else if ch == b'-' {
                        // This is a null array (e.g. from BLPOP). Don't increment rnarg
                        self.v_len = 1;
                        self.state = State::SW_MULTIBULK_NARG_LF;
                    }  else if is_digit(ch){
                        self.v_len = self.v_len * 10 + (ch - b'0') as usize;
                    } else if ch == CR {
                        self.has_token = false;
                        self.state = State::SW_MULTIBULK_ARGN_LEN;
                    }
                }
                State::SW_MULTIBULK_NARG_LF => {}
                State::SW_MULTIBULK_ARGN_LEN => {}
                State::SW_MULTIBULK_ARGN_LEN_LF => {}
                State::SW_MULTIBULK_ARGN => {}
                State::SW_MULTIBULK_ARGN_LF => {}
                State::SW_RUNTO_CRLF => {
                    if ch == CR {
                        self.state = State::SW_ALMOST_DONE;
                    }
                }
                State::SW_ALMOST_DONE => {
                    if ch == LF {
                        self.state = State::SW_START;
                        p.advance(1);
                        done = true;
                        break;
                    }
                }
            }
            prev_p = p;
            p.advance(1);
        }

        if done {
            let consumed = p.as_ptr() as usize - src.as_ptr() as usize;
            let bytes = src.split_to(consumed).freeze();
            self.bytes_count += consumed;
            return Ok(Some(ResDecodedFrame { raw_bytes: bytes }));
        }
        Ok(None)
    }
}

#[derive(Debug)]
enum State {
    SW_START,
    SW_STATUS,
    SW_ERROR,
    SW_INTEGER_START,
    SW_SIMPLE,
    SW_BULK,
    SW_BULK_LF,
    SW_BULK_ARG,
    SW_BULK_ARG_LF,
    SW_MULTIBULK,
    SW_MULTIBULK_NARG_LF,
    SW_MULTIBULK_ARGN_LEN,
    SW_MULTIBULK_ARGN_LEN_LF,
    SW_MULTIBULK_ARGN,
    SW_MULTIBULK_ARGN_LF,
    SW_RUNTO_CRLF,
    SW_ALMOST_DONE,
}

#[derive(Debug)]
enum MsgType {
    MSG_RSP_REDIS_BULK,
    MSG_RSP_REDIS_ERROR,
    MSG_RSP_REDIS_ERROR_BUSY,
    MSG_RSP_REDIS_ERROR_BUSYKEY,
    MSG_RSP_REDIS_ERROR_ERR,
    MSG_RSP_REDIS_ERROR_EXECABORT,
    MSG_RSP_REDIS_ERROR_LOADING,
    MSG_RSP_REDIS_ERROR_MASTERDOWN,
    MSG_RSP_REDIS_ERROR_MISCONF,
    MSG_RSP_REDIS_ERROR_NOAUTH,
    MSG_RSP_REDIS_ERROR_NOREPLICAS,
    MSG_RSP_REDIS_ERROR_NOSCRIPT,
    MSG_RSP_REDIS_ERROR_OOM,
    MSG_RSP_REDIS_ERROR_READONLY,
    MSG_RSP_REDIS_ERROR_WRONGTYPE,
    MSG_RSP_REDIS_INTEGER,
    MSG_RSP_REDIS_MULTIBULK,
    MSG_RSP_REDIS_STATUS,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_resp_ok() {
        let mut decoder = ResPartialDecoder {
            state: State::SW_START,
            msg_type: MsgType::MSG_RSP_REDIS_STATUS,
            bytes_count: 0,
            has_token: false,
            integer: 0,
            negative_: false,
        };
        {
            let bytes = "+OK\r\n";
            let mut src = BytesMut::from(bytes);
            let result = decoder.decode(&mut src).unwrap().unwrap();
            println!("{:?}", result);
            println!("{:?}", decoder);
        }
        {
            decoder.reset();
            let bytes = "-ERR unknown command 'foobar'\r\n";
            let mut src = BytesMut::from(bytes);
            let result = decoder.decode(&mut src).unwrap().unwrap();
            println!("{:?}", result);
        }
        {
            decoder.reset();
            let bytes = ":1000\r\n";
            let mut src = BytesMut::from(bytes);
            let result = decoder.decode(&mut src).unwrap().unwrap();
            println!("{:?}", result);
            println!("{:?}", decoder);

            decoder.reset();
            let bytes = ":-3000\r\n";
            let mut src = BytesMut::from(bytes);
            let result = decoder.decode(&mut src).unwrap().unwrap();
            println!("{:?}", result);
            println!("{:?}", decoder);
        }
    }
}