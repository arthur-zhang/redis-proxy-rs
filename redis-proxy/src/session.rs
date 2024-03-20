use std::time::Instant;

use bytes::Bytes;
use smol_str::SmolStr;
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio_stream::StreamExt;
use tokio_util::codec::FramedRead;

use redis_codec_core::req_decoder_v2::ReqDecoder;
use redis_codec_core::resp_decoder::ResFramedData;
use redis_proxy_common::command::utils::{CMD_TYPE_AUTH, CMD_TYPE_SELECT, CMD_TYPE_UNKNOWN};
use redis_proxy_common::ReqPkt;

pub struct Session {
    downstream_reader: FramedRead<OwnedReadHalf, ReqDecoder>,
    downstream_writer: OwnedWriteHalf,
    pub password: Option<Vec<u8>>,
    pub db: u64,
    pub is_authed: bool,
    pub req_start: Instant,
    pub res_is_ok: bool,
    pub req_size: usize,
    pub res_size: usize,
}

impl Session {
    pub fn new(stream: TcpStream) -> Self {
        let (r, w) = stream.into_split();
        let r = FramedRead::new(r, ReqDecoder::new());

        Session {
            downstream_reader: r,
            downstream_writer: w,
            password: None,
            db: 0,
            is_authed: false,
            req_start: Instant::now(),
            res_is_ok: true,
            req_size: 0,
            res_size: 0,
        }
    }

    pub fn init_from_req(&mut self, req_pkt: &ReqPkt) {
        self.req_size = req_pkt.bytes_total;
        self.res_size = 0;
        self.req_start = self.downstream_reader.decoder().req_start();
        let cmd_type = req_pkt.cmd_type();

        if CMD_TYPE_SELECT.eq(cmd_type) {
            self.on_select_db(req_pkt);
        } else if CMD_TYPE_AUTH.eq(cmd_type) {
            self.on_auth(req_pkt);
        }
    }

    #[inline]
    pub fn cmd_type(&self) -> &SmolStr {
        // todo
        &CMD_TYPE_UNKNOWN
    }


    #[inline]
    pub async fn send_resp_to_downstream(&mut self, data: Bytes) -> anyhow::Result<()> {
        self.downstream_writer.write_all(&data).await?;
        Ok(())
    }

    #[inline]
    pub async fn read_downstream(&mut self) -> Option<anyhow::Result<ReqPkt>> {
        self.downstream_reader.next().await
    }

    #[inline]
    pub async fn write_downstream_batch(&mut self, bytes: Vec<ResFramedData>) -> anyhow::Result<()> {
        for data in bytes {
            self.downstream_writer.write(&data.data).await?;
        }
        self.downstream_writer.flush().await?;
        Ok(())
    }

    #[inline]
    pub async fn write_downstream(&mut self, data: &[u8]) -> anyhow::Result<()> {
        self.downstream_writer.write_all(data).await?;
        Ok(())
    }


}

fn convert_to_u64(vec: &[u8]) -> Option<u64> {
    let mut result = 0u64;
    for &byte in vec {
        if byte >= b'0' && byte <= b'9' {
            result = result * 10 + (byte - b'0') as u64;
        } else {
            return None;
        }
    }
    Some(result)
}

impl Session {
    fn on_select_db(&mut self, req_pkt: &ReqPkt) {
        if req_pkt.bulk_args.len() < 2 {
            // todo maybe throw error is better
            return;
        }
        let db = &req_pkt.bulk_args[1];
        let db = convert_to_u64(db.as_ref()).unwrap_or(0);
        self.db = db;
    }
    pub fn on_auth(&mut self, req_pkt: &ReqPkt) {
        if req_pkt.bulk_args.len() <= 2 {
            return;
        }
        let auth_password = req_pkt.bulk_args[1].as_ref().to_vec();
        self.password = Some(auth_password);
    }
}