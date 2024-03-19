use std::sync::Arc;
use std::time::Instant;

use anyhow::bail;
use bytes::Bytes;
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio_stream::StreamExt;
use tokio_util::codec::FramedRead;

use redis_codec_core::error::DecodeError;
use redis_codec_core::req_decoder::ReqPktDecoder;
use redis_proxy_common::cmd::CmdType;
use redis_proxy_common::ReqFrameData;

pub struct Session {
    downstream_reader: FramedRead<OwnedReadHalf, ReqPktDecoder>,
    downstream_writer: OwnedWriteHalf,
    pub header_frame: Option<Arc<ReqFrameData>>,
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
        let r = FramedRead::new(r, ReqPktDecoder::new());

        Session {
            downstream_reader: r,
            downstream_writer: w,
            header_frame: None,
            password: None,
            db: 0,
            is_authed: false,
            req_start: Instant::now(),
            res_is_ok: true,
            req_size: 0,
            res_size: 0,
        }
    }

    pub fn init_from_header_frame(&mut self, header_frame: Arc<ReqFrameData>) {
        self.req_size = header_frame.raw_bytes.len();
        self.res_size = 0;
        self.req_start = self.downstream_reader.decoder().req_start();
        let cmd_type = header_frame.cmd_type;
        self.header_frame = Some(header_frame);

        if cmd_type == CmdType::SELECT {
            self.on_select_db();
        } else if cmd_type == CmdType::AUTH {
            self.on_auth();
        }
    }

    #[inline]
    pub fn request_done(&self) -> bool {
        self.header_frame.as_ref().map(|it| it.end_of_body).unwrap_or(false)
    }

    #[inline]
    pub fn cmd_type(&self) -> CmdType {
        self.header_frame.as_ref().map(|it| it.cmd_type).unwrap_or(CmdType::UNKNOWN)
    }

    #[inline]
    pub fn header_frame_unchecked(&self) -> &ReqFrameData {
        self.header_frame.as_ref().unwrap()
    }

    #[inline]
    pub async fn send_resp_to_downstream(&mut self, data: Bytes) -> anyhow::Result<()> {
        self.downstream_writer.write_all(&data).await?;
        Ok(())
    }

    #[inline]
    pub async fn read_downstream(&mut self) -> Option<Result<ReqFrameData, DecodeError>> {
        self.downstream_reader.next().await
    }

    #[inline]
    pub async fn write_downstream_batch(&mut self, bytes: Vec<Bytes>) -> anyhow::Result<()> {
        for data in bytes {
            self.downstream_writer.write(&data).await?;
        }
        self.downstream_writer.flush().await?;
        Ok(())
    }

    #[inline]
    pub async fn write_downstream(&mut self, data: &[u8]) -> anyhow::Result<()> {
        self.downstream_writer.write_all(data).await?;
        Ok(())
    }

    #[inline]
    pub async fn send_response_and_drain_req(&mut self, data: &[u8]) -> anyhow::Result<()> {
        self.downstream_writer.write_all(data).await?;
        self.drain_req_until_done().await?;
        Ok(())
    }
}

impl Session {
    pub async fn drain_req_until_done(&mut self) -> anyhow::Result<Option<(Vec<Bytes>, usize)>> {
        if let Some(ref header_frame) = self.header_frame {
            if header_frame.end_of_body {
                return Ok(None);
            }
        }
        let mut resp = vec![];
        let mut total_size = 0;
        while let Some(Ok(req_frame_data)) = self.downstream_reader.next().await {
            let end_of_body = req_frame_data.end_of_body;
            total_size += req_frame_data.raw_bytes.len();
            resp.push(req_frame_data.raw_bytes);
            if end_of_body {
                return Ok(Some((resp, total_size)));
            }
        }
        bail!("drain req failed")
    }

    fn on_select_db(&mut self) {
        if let Some(ref header_frame) = self.header_frame {
            if let Some(args) = header_frame.args() {
                let db = std::str::from_utf8(args[0]).map(|it| it.parse::<u64>().unwrap_or(0)).unwrap_or(0);
                self.db = db;
            }
        }
    }
    pub fn on_auth(&mut self) {
        if let Some(ref header_frame) = self.header_frame {
            if let Some(args) = header_frame.args() {
                if args.len() > 0 {
                    let auth_password = args[0].to_vec();
                    self.password = Some(auth_password);
                }
            }
        }
    }
}