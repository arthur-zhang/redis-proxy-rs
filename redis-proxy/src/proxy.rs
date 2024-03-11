use std::sync::Arc;
use std::time::Instant;

use anyhow::{anyhow, bail, Error};
use async_trait::async_trait;
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use futures::future::ok;
use futures::stream::TryNext;
use log::{debug, error, info};
use poolx::PoolConnection;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_util::codec::Framed;

use redis_codec_core::req_decoder::ReqPktDecoder;
use redis_codec_core::resp_decoder::ResFramedData;
use redis_proxy_common::cmd::CmdType;
use redis_proxy_common::ReqFrameData;

use crate::peer;
use crate::prometheus::METRICS;
use crate::server::{ProxyChanData, TASK_BUFFER_SIZE};
use crate::upstream_conn_pool::{Pool, RedisConnection};

pub struct RedisProxy<P> {
    pub inner: P,
    pub upstream_pool: Pool,
}

macro_rules! try_or_return {
    ($self:expr, $expr:expr) => {
        match $expr {
            Ok(val) => val,
            Err(_) => {
                return None;
            }
        }
    };
}


macro_rules! try_or_invoke_done {
    ($self:expr, $session:expr, $ctx:expr, $expr:expr) => {
        match $expr {
            Ok(val) => val,
            Err(err) => {
                $self.inner.request_done($session, Some(&err), $ctx).await;
                return None;
            }
        }
    };
}

impl<P> RedisProxy<P> where P: Proxy + Send + Sync, <P as Proxy>::CTX: Send + Sync {
    pub async fn handle_new_request(&self, mut session: Session, pool: Pool) -> Option<Session> {
        info!("handle new request");
        try_or_return!(self, self.inner.on_session_create().await);
        let mut req_frame = match session.underlying_stream.next().await {
            None => { return None; }
            Some(req_frame) => {
                try_or_return!(self, req_frame)
            }
        };
        session.req_size = req_frame.raw_bytes.len();
        session.res_size = 0;
        session.req_start = session.underlying_stream.codec().req_start();
        session.header_frame = Some(req_frame.clone());

        let cmd_type = req_frame.cmd_type;
        if cmd_type == CmdType::SELECT {
            session.on_select_db();
        } else if cmd_type == CmdType::AUTH {
            session.on_auth();
        }

        let mut ctx = self.inner.new_ctx();
        let response_sent = try_or_invoke_done!(self, &mut session, &mut ctx, self.inner.request_filter(&mut session, &mut ctx).await);
        if response_sent {
            if !req_frame.end_of_body {
                try_or_invoke_done!(self, &mut session, &mut ctx, session.drain_req_until_done().await);
            }
            return Some(session);
        }
        let mut conn = try_or_invoke_done!(self, &mut session, &mut ctx, pool.acquire().await.map_err(|e| anyhow!("get connection from pool error: {:?}", e)));
        debug!("get a connection : {:?}", conn.as_ref());
        let response_sent = try_or_invoke_done!(self, &mut session, &mut ctx, conn.init_from_session(&mut session).await);
        if response_sent {
            try_or_invoke_done!(self, &mut session, &mut ctx, session.drain_req_until_done().await);
            return Some(session);
        }

        try_or_invoke_done!(self, &mut session, &mut ctx, self.inner.proxy_upstream_filter(&mut session, &mut ctx).await);
        try_or_invoke_done!(self, &mut session, &mut ctx, self.inner.upstream_request_filter(&mut session, &mut req_frame, &mut ctx).await);

        let (tx_upstream, rx_upstream) = mpsc::channel::<ProxyChanData>(TASK_BUFFER_SIZE);
        let (tx_downstream, rx_downstream) = mpsc::channel::<ProxyChanData>(TASK_BUFFER_SIZE);

        try_or_invoke_done!(self, &mut session, &mut ctx, conn.w.write_all(&req_frame.raw_bytes).await.map_err(|e| anyhow!("send error :{:?}", e)));

        // bi-directional proxy
        try_or_invoke_done!(self, &mut session, &mut ctx,
            tokio::try_join!(
                self.proxy_handle_downstream(&mut session, tx_downstream, rx_upstream, &mut ctx),
                self.proxy_handle_upstream(conn, tx_upstream, rx_downstream, cmd_type)
        ));
        self.inner.request_done(&mut session, None, &mut ctx).await;
        debug!("request done");
        return Some(session);
    }
    async fn proxy_handle_downstream(&self,
                                     session: &mut Session,
                                     tx_downstream: Sender<ProxyChanData>,
                                     mut rx_upstream: Receiver<ProxyChanData>,
                                     ctx: &mut <P as Proxy>::CTX) -> anyhow::Result<()> {
        let mut end_of_body = session.request_done();
        let mut response_done = false;
        while !end_of_body || !response_done {
            let send_permit = tx_downstream.try_reserve();
            tokio::select! {
                data = session.underlying_stream.next(), if !end_of_body && send_permit.is_ok() => {
                    match data {
                        Some(Ok(mut data)) => {
                            let is_done = data.end_of_body;
                            self.inner.upstream_request_filter(session, &mut data, ctx).await?;
                            session.req_size += data.raw_bytes.len();
                            send_permit.unwrap().send(ProxyChanData::ReqFrameData(data));
                            end_of_body = is_done;
                        }
                        Some(Err(e)) => {
                            end_of_body = true;
                            bail!("proxy_handle_downstream, framed next error: {:?}", e)
                        }
                        None => {
                            info!("proxy_handle_downstream, downstream eof");
                            send_permit.unwrap().send(ProxyChanData::None);
                            end_of_body = true;
                            return Ok(())
                        }
                    }
                }
                task = rx_upstream.recv(), if !response_done => {
                    match task {
                        Some(ProxyChanData::ResFrameData(res_framed_data)) => {
                            if res_framed_data.is_done {
                                session.res_is_ok = res_framed_data.res_is_ok;
                            }
                            session.res_size += res_framed_data.data.len();
                            session.underlying_stream.send(res_framed_data.data).await?;

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
    pub underlying_stream: Framed<TcpStream, ReqPktDecoder>,
    pub header_frame: Option<ReqFrameData>,
    pub password: Option<Vec<u8>>,
    pub db: u64,
    pub is_authed: bool,
    pub req_start: Instant,
    pub res_is_ok: bool,
    pub req_size: usize,
    pub res_size: usize,
}

impl Session {
    pub fn new(underlying_stream: Framed<TcpStream, ReqPktDecoder>) -> Self {
        Session {
            underlying_stream,
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
    pub fn request_done(&self) -> bool {
        self.header_frame.as_ref().map(|it| it.end_of_body).unwrap_or(false)
    }
    pub fn cmd_type(&self) -> CmdType {
        self.header_frame.as_ref().map(|it| it.cmd_type).unwrap_or(CmdType::UNKNOWN)
    }
}

impl Session {
    pub async fn drain_req_until_done(&mut self) -> anyhow::Result<()> {
        if let Some(ref header_frame) = self.header_frame {
            if header_frame.end_of_body {
                return Ok(());
            }
        }
        while let Some(Ok(req_frame_data)) = self.underlying_stream.next().await {
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
        session: &mut Session,
        ctx: &mut Self::CTX,
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
    async fn request_filter(&self, session: &mut Session, ctx: &mut Self::CTX) -> anyhow::Result<bool> {
        Ok(false)
    }


    async fn request_data_filter(&self, session: &mut Session, ctx: &mut Self::CTX) -> anyhow::Result<()> {
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
                                     _upstream_request: &mut ReqFrameData,
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

    async fn request_done(&self, session: &mut Session, e: Option<&anyhow::Error>, ctx: &mut Self::CTX)
        where
            Self::CTX: Send + Sync {}
}


