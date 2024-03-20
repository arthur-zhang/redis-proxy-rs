use std::mem;
use std::time::Instant;
use anyhow::bail;

use bytes::{Buf, Bytes, BytesMut};
use tokio_util::codec::Decoder;

use redis_proxy_common::ReqPkt;
use redis_proxy_common::tools::is_digit;

pub const CR: u8 = b'\r';
pub const LF: u8 = b'\n';

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
        while src.has_remaining() {
            match self.state {
                State::ValueRoot => {
                    if src[0] != b'*' {
                        return Err(anyhow::anyhow!("Invalid request"));
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
                    if src[0] == CR {
                        self.state = State::NArgIntegerLF;
                    } else if is_digit(src[0]) {
                        self.pending_integer = self.pending_integer * 10 + (src[0] - b'0') as u64;
                    } else {
                        return Err(anyhow::anyhow!("Invalid request"));
                    }
                    src.advance(1);
                }

                State::NArgIntegerLF => {
                    if src[0] != LF {
                        return Err(anyhow::anyhow!("Invalid request"));
                    }
                    self.bulk_size = self.pending_integer;
                    self.pending_integer = 0;
                    self.state = State::BulkLenStart;
                    src.advance(1);
                }
                State::BulkLenStart => {
                    if src[0] != b'$' {
                        return Err(anyhow::anyhow!("Invalid request"));
                    }
                    self.pending_integer = 0;
                    self.state = State::BulkLen;
                    src.advance(1);
                }
                State::BulkLen => {
                    if src[0] == CR {
                        self.state = State::BulkLenLF;
                    } else if is_digit(src[0]) {
                        self.pending_integer = self.pending_integer * 10 + (src[0] - b'0') as u64;
                    } else {
                        return Err(anyhow::anyhow!("Invalid request"));
                    }
                    src.advance(1);
                }
                State::BulkLenLF => {
                    if src[0] != LF {
                        return Err(anyhow::anyhow!("Invalid request"));
                    }
                    src.advance(1);
                    self.state = State::BulkData;
                }
                State::BulkData => {
                    // println!("pending_integer: {}, src: {:?}", self.pending_integer, src);
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
        return if self.bulk_read_index >= self.bulk_size {
            if self.bulk_args_data.is_empty() {
                bail!("Invalid request");
            }
            let bulk_args = mem::replace(&mut self.bulk_args_data, Vec::new());
            // todo
            let bytes_total = bulk_args.iter().fold(0, |acc, it| acc + it.len());
            Ok(Some(ReqPkt::new(bulk_args, bytes_total)))
        } else {
            Ok(None)
        }
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
}