use std::mem;
use std::time::Instant;

use anyhow::bail;
use bytes::{Buf, Bytes, BytesMut};
use tokio_util::codec::Decoder;

use redis_proxy_common::ReqPkt;
use redis_proxy_common::tools::{CR, is_digit, LF};

enum State {
    ValueRoot,
    NArgInteger,
    NArgIntegerLF,
    BulkLenStart,
    BulkLen,
    BulkLenLF,
    BulkData,
}

pub struct ReqDecoder {
    state: State,
    bulk_size: u64,
    pending_integer: u64,
    bulk_read_index: u64,
    req_start: Instant,
    bulk_args_data: Vec<Bytes>,
}

impl ReqDecoder {
    pub fn new() -> Self {
        Self {
            state: State::ValueRoot,
            bulk_size: u64::MAX,
            pending_integer: 0,
            bulk_read_index: 0,
            req_start: Instant::now(),
            bulk_args_data: Vec::new(),
        }
    }
    pub fn req_start(&self) -> Instant {
        self.req_start
    }
}

impl Decoder for ReqDecoder {
    type Item = ReqPkt;
    type Error = anyhow::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }
        let start_len = src.len();
        while src.has_remaining() {
            let ch = src[0];
            match self.state {
                State::ValueRoot => {
                    if ch != b'*' {
                        bail!("Invalid request")
                    }
                    self.req_start = Instant::now();
                    self.bulk_size = u64::MAX;
                    self.bulk_read_index = 0;
                    self.pending_integer = 0;
                    self.bulk_args_data.clear();
                    self.state = State::NArgInteger;
                    src.advance(1);
                }

                State::NArgInteger => {
                    if ch == CR {
                        self.state = State::NArgIntegerLF;
                    } else if is_digit(ch) {
                        self.pending_integer = self.pending_integer * 10 + (ch - b'0') as u64;
                    } else {
                        return Err(anyhow::anyhow!("Invalid request"));
                    }
                    src.advance(1);
                }

                State::NArgIntegerLF => {
                    if ch != LF {
                        return Err(anyhow::anyhow!("Invalid request"));
                    }
                    self.bulk_size = self.pending_integer;
                    self.pending_integer = 0;
                    self.state = State::BulkLenStart;
                    src.advance(1);
                }
                State::BulkLenStart => {
                    if ch != b'$' {
                        return Err(anyhow::anyhow!("Invalid request"));
                    }
                    self.pending_integer = 0;
                    self.state = State::BulkLen;
                    src.advance(1);
                }
                State::BulkLen => {
                    if ch == CR {
                        self.state = State::BulkLenLF;
                    } else if is_digit(ch) {
                        self.pending_integer = self.pending_integer * 10 + (ch - b'0') as u64;
                    } else {
                        return Err(anyhow::anyhow!("Invalid request"));
                    }
                    src.advance(1);
                }
                State::BulkLenLF => {
                    if ch != LF {
                        return Err(anyhow::anyhow!("Invalid request"));
                    }
                    src.advance(1);
                    self.state = State::BulkData;
                }
                State::BulkData => {
                    src.reserve(self.pending_integer as usize + 2);
                    if src.len() < self.pending_integer as usize + 2 {
                        return Ok(None);
                    }
                    let data = src.split_to(self.pending_integer as usize + 2).freeze().split_to(self.pending_integer as usize);
                    self.bulk_args_data.push(data);

                    self.bulk_read_index += 1;
                    if self.bulk_read_index >= self.bulk_size {
                        self.state = State::ValueRoot;
                        break;
                    }
                    self.state = State::BulkLenStart;
                }
            }
        }
        if self.bulk_read_index < self.bulk_size {
            return Ok(None);
        }
        if self.bulk_args_data.is_empty() {
            bail!("Invalid request");
        }

        let bulk_args = mem::take(&mut self.bulk_args_data);
        let bytes_total = start_len.saturating_sub(src.len());
        let req_pkt = ReqPkt::new(bulk_args, bytes_total);
        if req_pkt.is_err() {
            bail!("Invalid request");
        }
        Ok(Some(req_pkt.unwrap()))
    }
}

#[cfg(test)]
mod tests {
    use std::vec;

    use super::*;

    #[test]
    fn test_req_decoder() {
        let mut pkts = vec![
            BytesMut::from("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"),
            BytesMut::from("*1\r\n$4\r\nPING\r\n"),
        ];

        for pkt in pkts {
            for i in 1..pkt.len() {
                let mut data = BytesMut::from(&pkt[0..=i]);
                let mut decoder = ReqDecoder::new();
                let mut pkt = decoder.decode(&mut data).unwrap();
                if let Some(pkt) = pkt {
                    for data in &pkt.bulk_args {
                        print!("{:?}\t", data);
                    }
                    println!("-------------some-------")
                } else {
                    println!("-------------none-------")
                }
            }
        }
    }

    #[test]
    fn test_bytes_mut() {
        let mut a = BytesMut::from("hello");
        let len1 = a.len();
        // a.advance(1);
        let _ = a.split_to(1);
        let len2 = a.len();
        println!("len1: {}, len2: {}", len1, len2);
    }
}