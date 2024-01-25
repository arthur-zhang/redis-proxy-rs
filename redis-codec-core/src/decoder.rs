use bytes::{Buf, Bytes, BytesMut};
use tokio_util::codec::Decoder;

const CR: u8 = b'\r';
const LF: u8 = b'\n';

pub struct PartialDecoder {
    bulk_len: usize,
    rnarg: usize,
    poll_data: Option<Bytes>,
    state: State,
}

pub enum State {
    SW_START,
    SW_NARG,
    SW_NARG_LF,
}

impl Decoder for PartialDecoder {
    type Item = ();
    type Error = DecodeError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }

        let mut p = src.as_ref();
        while p.has_remaining() || self.poll_data.is_none() {
            let ch = p[0];
            // match self.state {
            //     State::SW_START => {
            //         if ch != b'*' {
            //             return Err(DecodeError::InvalidProtocol);
            //         }
            //         self.state = State::SW_NARG;
            //     }
            //     State::SW_NARG => {
            //         if is_digit(ch) {
            //             self.rnarg = self.rnarg * 10 + (ch - b'0') as usize;
            //         } else if ch == CR {
            //             self.bulk_len = self.rnarg;
            //             self.state = State::SW_NARG_LF;
            //         } else {
            //             return Err(DecodeError::InvalidProtocol);
            //         }
            //     }
            //     sta
            //     _ => {}
            // }
            p.advance(1);
        }
        return Ok(None);
    }
}

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