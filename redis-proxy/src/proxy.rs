use std::io::IoSlice;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{anyhow, bail};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::{SinkExt, StreamExt};
use log::{debug, error, info};
use poolx::PoolConnection;
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinHandle;
use tokio_util::codec::{Framed, FramedRead};

use redis_codec_core::req_decoder::ReqPktDecoder;
use redis_codec_core::resp_decoder::ResFramedData;
use redis_proxy_common::cmd::CmdType;
use redis_proxy_common::ReqFrameData;

use crate::peer;
use crate::router::Router;
use crate::server::ProxyChanData;
use crate::upstream_conn_pool::{AuthStatus, Pool, RedisConnection};

const TASK_BUFFER_SIZE: usize = 16;

pub struct RedisProxy<P> {
    pub inner: P,
    pub upstream_pool: Pool,
    pub mirror_pool: Pool,
}

impl<P> RedisProxy<P> {
    fn should_mirror(&self, header_frame: &ReqFrameData) -> bool {
        return if header_frame.cmd_type.is_connection_command() {
            true
        } else if header_frame.cmd_type.is_read_cmd() {
            false
        } else {
            // todo
            return true;
        };
    }
}

impl<P> RedisProxy<P> where P: Proxy + Send + Sync, <P as Proxy>::CTX: Send + Sync {
    pub async fn handle_new_request(&self, mut session: Session) -> Option<Session> {
        debug!("handle new request");

        self.inner.on_session_create().await.ok()?;
        let mut header_frame = session.downstream_reader.next().await?.ok()?;
        let header_frame = Arc::new(header_frame);
        let cmd_type = header_frame.cmd_type;
        session.init_from_header_frame(header_frame.clone());

        let mut ctx = self.inner.new_ctx();

        let mut response_sent =
            self.inner.request_filter(&mut session, &mut ctx).await.ok()?;
        if response_sent {
            session.drain_req_until_done().await.ok()?;
            return Some(session);
        }

        match self.handle_request_half(&mut session, header_frame, cmd_type, &mut ctx).await {
            Ok(_) => {
                self.inner.request_done(&mut session, None, &mut ctx).await;
            }
            Err(err) => {
                self.inner.request_done(&mut session, Some(&err), &mut ctx).await;
            }
        }
        Some(session)
    }
    pub async fn handle_request_half(&self, session: &mut Session, header_frame: Arc<ReqFrameData>,
                                     cmd_type: CmdType,
                                     ctx: &mut <P as Proxy>::CTX) -> anyhow::Result<()> {
        let conn =
            self.upstream_pool.acquire().await.map_err(|e| anyhow!("get connection from pool error: {:?}", e))?;

        if self.should_mirror(&header_frame) {
            let mirror_conn = self.mirror_pool.acquire().await.map_err(|e| anyhow!("get mirror connection from pool error: {:?}", e))?;
            self.handle_mirror(session, mirror_conn, conn, cmd_type, ctx).await
        } else {
            self.handle_non_mirror_request(session, &header_frame, cmd_type, ctx, conn).await
        }
    }

    async fn handle_non_mirror_request(&self, session: &mut Session, header_frame: &Arc<ReqFrameData>,
                                       cmd_type: CmdType, ctx: &mut <P as Proxy>::CTX,
                                       mut conn: PoolConnection<RedisConnection>) -> anyhow::Result<()> {
        let auth_status =
            conn.init_from_session(cmd_type, session.is_authed, &session.password, session.db).await?;

        if auth_status != AuthStatus::Authed {
            session.downstream_writer.write_all(b"-NOAUTH Authentication required.\r\n").await;
            return Ok(());
        }

        self.inner.proxy_upstream_filter(session, ctx).await?;
        self.inner.upstream_request_filter(session, &header_frame, ctx).await?;

        let (tx_upstream, rx_upstream) = mpsc::channel::<ProxyChanData>(TASK_BUFFER_SIZE);
        let (tx_downstream, rx_downstream) = mpsc::channel::<ProxyChanData>(TASK_BUFFER_SIZE);

        conn.w.write_all(&header_frame.raw_bytes).await.map_err(|e| anyhow!("send error :{:?}", e))?;

        // bi-directional proxy
        tokio::try_join!(
            self.proxy_handle_downstream(session, tx_downstream, rx_upstream, ctx),
            self.proxy_handle_upstream(conn, tx_upstream, rx_downstream, cmd_type)
        )?;
        self.inner.request_done(session, None, ctx).await;
        Ok(())
    }
    async fn handle_mirror(&self, session: &mut Session,
                           mut mirror_conn: PoolConnection<RedisConnection>,
                           mut conn: PoolConnection<RedisConnection>,
                           cmd_type: CmdType, ctx: &mut <P as Proxy>::CTX) -> anyhow::Result<()> {
        let (auth_status_upstream, auth_status_mirror) = tokio::try_join!(
                conn.init_from_session(cmd_type, session.is_authed, &session.password, session.db),
                mirror_conn.init_from_session(cmd_type, session.is_authed, &session.password, session.db))?;
        match (auth_status_upstream, auth_status_mirror) {
            (AuthStatus::Authed, AuthStatus::Authed) => {}
            (_, _) => {
                session.downstream_writer.write_all(b"-NOAUTH Authentication required.\r\n").await;
                return Ok(());
            }
        }

        let mut req_data_vec = vec![];

        let header_data = session.header_frame.as_ref().unwrap().raw_bytes.clone();
        req_data_vec.push(header_data);

        if !session.request_done() {
            while let Some(Ok(data)) = session.downstream_reader.next().await {
                let is_done = data.end_of_body;
                session.req_size += data.raw_bytes.len();
                req_data_vec.push(data.raw_bytes);
                if is_done {
                    break;
                }
            }
        }

        let t1 = tokio::spawn({
            let req_data = req_data_vec.clone();
            async move {
                conn.query_with_resp_vec(req_data).await
            }
        });
        let t2 = tokio::spawn({
            let req_data_io_slice = req_data_vec.clone();
            async move {
                mirror_conn.query_with_resp_vec(req_data_io_slice).await
            }
        });


        let res = tokio::try_join!(t1, t2).map_err(|e| anyhow!("join error: {:?}", e))?;

        // println!("res: {:?}", res);
        return match res {
            (Ok(res1), Ok(res2)) => {
                if cmd_type == CmdType::AUTH {
                    session.is_authed = res1.0;
                }
                if res1.0 == false {
                    for it in res1.1 {
                        let _ = session.downstream_writer.write_all(&it).await;
                    }
                } else if res2.0 == false {
                    for it in res2.1 {
                        let _ = session.downstream_writer.write_all(&it).await;
                    }
                } else {
                    for it in res1.1 {
                        let _ = session.downstream_writer.write_all(&it).await;
                    }
                }
                Ok(())
            }
            _ => {
                let _ = session.downstream_writer.write_all(b"-error").await;
                bail!("mirror error")
            }
        };
    }
    async fn proxy_handle_downstream(&self,
                                     session: &mut Session,
                                     tx_downstream: Sender<ProxyChanData>,
                                     mut rx_upstream: Receiver<ProxyChanData>,
                                     ctx: &mut <P as Proxy>::CTX) -> anyhow::Result<()> {
        let mut end_of_body = session.request_done();
        let mut response_done = false;
        while !end_of_body || !response_done {
            let tx_downstream_send_permit = tx_downstream.try_reserve();
            tokio::select! {
                data = session.downstream_reader.next(), if !end_of_body && tx_downstream_send_permit.is_ok() => {
                    match data {
                        Some(Ok(mut data)) => {
                            let is_done = data.end_of_body;
                            self.inner.upstream_request_filter(session, &mut data, ctx).await?;
                            session.req_size += data.raw_bytes.len();
                            tx_downstream_send_permit?.send(ProxyChanData::ReqFrameData(data));
                            end_of_body = is_done;
                        }
                        Some(Err(e)) => {
                            bail!("proxy_handle_downstream, framed next error: {:?}", e)
                        }
                        None => {
                            info!("proxy_handle_downstream, downstream eof");
                            // send_permit.unwrap().send(ProxyChanData::None);
                            return Ok(())
                        }
                    }
                }
                _ = tx_downstream.reserve(), if tx_downstream_send_permit.is_err() => {
                    debug!("waiting for permit: {:?}", tx_downstream_send_permit);
                }
                task = rx_upstream.recv(), if !response_done => {
                    match task {
                        Some(ProxyChanData::ResFrameData(res_framed_data)) => {
                            if res_framed_data.is_done {
                                session.res_is_ok = res_framed_data.res_is_ok;
                            }
                            session.res_size += res_framed_data.data.len();
                            session.downstream_writer.write_all(&res_framed_data.data).await?;

                            response_done = res_framed_data.is_done;

                            let cmd_type = session.cmd_type();
                            if cmd_type == CmdType::AUTH && res_framed_data.is_done {
                                session.is_authed = res_framed_data.res_is_ok;
                            }
                        }
                        Some(_) => {
                            todo ! ()
                        }

                        None => {
                            response_done = true;
                        }
                    }
                }

                else => {
                    break;
                }
            }
        }
        Ok(())
    }

    async fn proxy_handle_upstream(&self,
                                   mut conn: PoolConnection<RedisConnection>,
                                   tx_upstream: Sender<ProxyChanData>,
                                   mut rx_downstream: Receiver<ProxyChanData>,
                                   cmd_type: CmdType)
                                   -> anyhow::Result<()> {
        let mut request_done = false;
        let mut response_done = false;
        while !request_done || !response_done {
            tokio::select! {
                // get response data from upstream, send to downstream
                data = conn.r.next(), if !response_done => {
                    match data {
                        Some(Ok(data)) => {
                            let is_done = data.is_done;
                            if cmd_type == CmdType::AUTH {
                                conn.is_authed = data.res_is_ok;
                            }
                            tx_upstream.send(ProxyChanData::ResFrameData(data)).await?;
                            if is_done {
                                response_done = true;
                            }
                        }
                        Some(Err(err)) => {
                            bail!("read response from upstream failed: {:?}", err)
                        }
                        None => {
                            response_done = true;
                        }
                    }
                }
                task = rx_downstream.recv(), if !request_done => {
                    match task {
                        Some(ProxyChanData::ReqFrameData(frame_data)) => {
                            conn.w.write_all(&frame_data.raw_bytes).await?;
                            request_done = frame_data.end_of_body;
                        }

                        Some(a) =>{
                            error!("unexpected data: {:?}", a);
                            todo!()
                        }
                        _ => {
                            request_done = true;
                        }
                    }
                }
                else => {
                    break;
                }
            }
        }
        Ok(())
    }
}


pub struct Session {
    downstream_reader: FramedRead<OwnedReadHalf, ReqPktDecoder>,
    downstream_writer: OwnedWriteHalf,
    // underlying_stream: Framed<TcpStream, ReqPktDecoder>,
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

    pub fn request_done(&self) -> bool {
        self.header_frame.as_ref().map(|it| it.end_of_body).unwrap_or(false)
    }
    pub fn cmd_type(&self) -> CmdType {
        self.header_frame.as_ref().map(|it| it.cmd_type).unwrap_or(CmdType::UNKNOWN)
    }

    pub async fn send_resp_to_downstream(&mut self, data: Bytes) -> anyhow::Result<()> {
        self.downstream_writer.write_all(&data).await?;
        Ok(())
    }
}

impl Session {
    pub async fn drain_req_until_done(&mut self) -> anyhow::Result<()> {
        if let Some(ref header_frame) = self.header_frame {
            if header_frame.end_of_body {
                return Ok(());
            }
        }
        while let Some(Ok(req_frame_data)) = self.downstream_reader.next().await {
            if req_frame_data.end_of_body {
                return Ok(());
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

#[async_trait]
pub trait Proxy {
    type CTX;
    fn new_ctx(&self) -> Self::CTX { todo!() }

    async fn on_session_create(&self) -> anyhow::Result<()> {
        Ok(())
    }


    /// Define where the proxy should sent the request to.
    ///
    /// The returned [RedisPeer] contains the information regarding where and how this request should
    /// be forwarded to.
    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX,
    ) -> anyhow::Result<peer::RedisPeer> { todo!() }

    /// Handle the incoming request.
    ///
    /// In this phase, users can parse, validate, rate limit, perform access control and/or
    /// return a response for this request.
    ///
    /// If the user already sent a response to this request, a `Ok(true)` should be returned so that
    /// the proxy would exit. The proxy continues to the next phases when `Ok(false)` is returned.
    ///
    /// By default, this filter does nothing and returns `Ok(false)`.
    async fn request_filter(&self, _session: &mut Session, _ctx: &mut Self::CTX) -> anyhow::Result<bool> {
        Ok(false)
    }


    async fn request_data_filter(&self, _session: &mut Session, _ctx: &mut Self::CTX) -> anyhow::Result<()> {
        Ok(())
    }
    /// Decide if a request should continue to upstream
    ///
    /// returns: Ok(true) if the request should continue, Ok(false) if a response was written by the
    /// callback and the session should be finished, or an error
    ///
    /// This filter can be used for deferring checks like rate limiting or access control
    async fn proxy_upstream_filter(&self, _session: &mut Session,
                                   _ctx: &mut Self::CTX) -> anyhow::Result<bool> {
        Ok(false)
    }


    /// Modify the request before it is sent to the upstream
    ///
    /// Unlike [Self::request_filter()], this filter allows to change the request data to send
    /// to the upstream.
    async fn upstream_request_filter(&self, _session: &mut Session,
                                     _upstream_request: &ReqFrameData,
                                     _ctx: &mut Self::CTX) -> anyhow::Result<()> { Ok(()) }

    /// Modify the response header from the upstream
    ///
    /// The modification is before caching so any change here will be stored in cache if enabled.
    ///
    /// Responses served from cache won't trigger this filter.
    fn upstream_response_filter(
        &self,
        _session: &mut Session,
        _upstream_response: &mut ResFramedData,
        _ctx: &mut Self::CTX,
    ) {}

    /// Similar to [Self::upstream_response_filter()] but for response body
    ///
    /// This function will be called every time a piece of response body is received. The `body` is
    /// **not the entire response body**.
    fn upstream_response_body_filter(
        &self,
        _session: &mut Session,
        _body: &Option<Bytes>,
        _end_of_stream: bool,
        _ctx: &mut Self::CTX,
    ) {}

    async fn request_done(&self, _session: &mut Session, _e: Option<&anyhow::Error>, _ctx: &mut Self::CTX)
        where
            Self::CTX: Send + Sync {}
}


