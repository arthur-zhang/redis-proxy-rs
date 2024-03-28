use std::time::{Duration, Instant};

use bytes::Bytes;
use poolx::PoolConnection;
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio_stream::StreamExt;
use tokio_util::codec::FramedRead;

use redis_codec_core::req_decoder::ReqDecoder;
use redis_codec_core::resp_decoder::ResFramedData;
use redis_proxy_common::ReqPkt;

use crate::client_flags::SessionFlags;
use crate::handler::get_handler;
use crate::upstream_conn_pool::{AuthInfo, RedisConnection};

pub struct Session {
    downstream_reader: FramedRead<OwnedReadHalf, ReqDecoder>,
    downstream_writer: OwnedWriteHalf,
    flags: SessionFlags,
    pub upstream_conn: Option<PoolConnection<RedisConnection>>,
    pub dw_conn: Option<PoolConnection<RedisConnection>>,
    pub authed_info: Option<AuthInfo>,
    pub db: u64,
    pub req_start: Instant,
    pub(crate) upstream_start: Instant,
    pub upstream_elapsed: Duration,
    pub pool_acquire_elapsed: Duration,
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
            flags: SessionFlags::None,
            upstream_conn: None,
            dw_conn: None,
            authed_info: None,
            db: 0,
            req_start: Instant::now(),
            upstream_start: Instant::now(),
            upstream_elapsed: Default::default(),
            pool_acquire_elapsed: Default::default(),
            res_is_ok: true,
            req_size: 0,
            res_size: 0,
        }
    }

    pub fn init_from_req(&mut self, req_pkt: &ReqPkt) {
        self.req_size = req_pkt.bytes_total;
        self.res_size = 0;
        self.req_start = self.downstream_reader.decoder().req_start();
        let cmd_type = req_pkt.cmd_type;

        get_handler(cmd_type).map(|h| h.init_from_req(self, req_pkt));
    }

    #[inline]
    pub fn insert_client_flags(&mut self, flags: SessionFlags) {
        self.flags.insert(flags);
    }

    #[inline]
    pub fn remove_client_flags(&mut self, flags: SessionFlags) {
        self.flags.remove(flags);
    }

    #[inline]
    pub fn contains_client_flags(&self, flags: SessionFlags) -> bool {
        self.flags.contains(flags)
    }

    #[inline]
    pub async fn send_resp_to_downstream(&mut self, data: Bytes) -> anyhow::Result<()> {
        self.downstream_writer.write_all(&data).await?;
        Ok(())
    }

    #[inline]
    pub async fn read_req_pkt(&mut self) -> Option<anyhow::Result<ReqPkt>> {
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