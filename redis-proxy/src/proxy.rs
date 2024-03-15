use std::io::IoSlice;
use std::time::Instant;

use anyhow::{anyhow, bail};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::{SinkExt, StreamExt};
use log::{debug, error, info};
use poolx::PoolConnection;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinHandle;
use tokio_util::codec::Framed;

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

macro_rules! try_or_invoke_done {
    (|$self:expr, $session:expr, $ctx:expr, | $expr:expr) => {
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
    pub async fn handle_new_request(&self, mut session: Session) -> Option<Session> {
        debug!("handle new request");

        self.inner.on_session_create().await.ok()?;
        let mut header_frame = session.underlying_stream.next().await?.ok()?;
        let cmd_type = header_frame.cmd_type;
        session.init_from_header_frame(&header_frame);

        let mut ctx = self.inner.new_ctx();

        let mut response_sent = try_or_invoke_done! { |self, &mut session, &mut ctx,|
            self.inner.request_filter(&mut session, &mut ctx).await
        };
        if response_sent {
            try_or_invoke_done! {|self, &mut session, &mut ctx,|
                session.drain_req_until_done().await
            }
            return Some(session);
        }


        let mut conn = try_or_invoke_done!(|self, &mut session, &mut ctx,|
            self.upstream_pool.acquire().await.map_err(|e| anyhow!("get connection from pool error: {:?}", e))
        );

        let should_mirror = self.should_mirror(&header_frame);

        let mut mirror_conn = None;
        if should_mirror {
            let conn = try_or_invoke_done!(|self, &mut session, &mut ctx,|
                self.mirror_pool.acquire().await.map_err(|e| anyhow!("get mirror connection from pool error: {:?}", e)));
            mirror_conn = Some(conn);
        }

        // handle mirror request
        if let Some(mut mirror_conn) = mirror_conn {
            let resp = tokio::try_join!(
                conn.init_from_session(cmd_type, session.is_authed, &session.password, session.db),
                mirror_conn.init_from_session(cmd_type, session.is_authed, &session.password, session.db),
            );
            let (auth_status1, auth_status2) = try_or_invoke_done! {|self, &mut session, &mut ctx,| resp};
            match (auth_status1, auth_status2) {
                (AuthStatus::Authed, AuthStatus::Authed) => {}
                (_, _) => {
                    session.underlying_stream.send(Bytes::from_static(b"-NOAUTH Authentication required.\r\n")).await;
                    response_sent = true;
                }
            }
            let res = self.handle_mirror(session, mirror_conn, conn, cmd_type, &mut ctx).await;
            return res;
        }

        let auth_status = try_or_invoke_done!(|self, &mut session, &mut ctx,|
            conn.init_from_session(cmd_type, session.is_authed, &session.password, session.db).await);

        if auth_status != AuthStatus::Authed {
            session.underlying_stream.send(Bytes::from_static(b"-NOAUTH Authentication required.\r\n")).await;
            response_sent = true;
            return Some(session);
        }

        try_or_invoke_done!(|self, &mut session, &mut ctx,|
            self.inner.proxy_upstream_filter(&mut session, &mut ctx).await);
        try_or_invoke_done!(|self, &mut session, &mut ctx,|
            self.inner.upstream_request_filter(&mut session, &mut header_frame, &mut ctx).await);

        let (tx_upstream, rx_upstream) = mpsc::channel::<ProxyChanData>(TASK_BUFFER_SIZE);
        let (tx_downstream, rx_downstream) = mpsc::channel::<ProxyChanData>(TASK_BUFFER_SIZE);

        try_or_invoke_done!(|self, &mut session, &mut ctx,|
            conn.w.write_all(&header_frame.raw_bytes).await.map_err(|e| anyhow!("send error :{:?}", e)));

        // bi-directional proxy
        try_or_invoke_done!(|self, &mut session, &mut ctx,|
            tokio::try_join!(
                self.proxy_handle_downstream(&mut session, tx_downstream, rx_upstream, &mut ctx),
                self.proxy_handle_upstream(conn, tx_upstream, rx_downstream, cmd_type)
        ));
        self.inner.request_done(&mut session, None, &mut ctx).await;
        return Some(session);
    }
    async fn handle_mirror(&self, mut session: Session,
                           mut mirror_conn: PoolConnection<RedisConnection>,
                           mut conn: PoolConnection<RedisConnection>,
                           cmd_type: CmdType, ctx: &mut <P as Proxy>::CTX) -> Option<Session> {
        let mut req_data_vec = vec![];

        let header_data = session.header_frame.as_ref().unwrap().raw_bytes.clone();
        req_data_vec.push(header_data);
        // let mut req_data = BytesMut::from(session.header_frame.as_ref().unwrap().raw_bytes.as_ref());

        if !session.request_done() {
            while let Some(Ok(data)) = session.underlying_stream.next().await {
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


        let res = tokio::try_join!(t1, t2).map_err(|e| anyhow!("join error: {:?}", e));
        let res = try_or_invoke_done!(|self, &mut session, ctx,| res);

        // println!("res: {:?}", res);
        return match res {
            (Ok(res1), Ok(res2)) => {
                if cmd_type == CmdType::AUTH {
                    session.is_authed = res1.0;
                }
                if res1.0 == false {
                    for it in res1.1 {
                        let _ = session.underlying_stream.send(it).await;
                    }
                } else if res2.0 == false {
                    for it in res2.1 {
                        let _ = session.underlying_stream.send(it).await;
                    }
                } else {
                    for it in res1.1 {
                        let _ = session.underlying_stream.send(it).await;
                    }
                }
                Some(session)
            }
            _ => {
                let _ = session.underlying_stream.send(Bytes::from("-error")).await;
                None
            }
        }
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
                data = session.underlying_stream.next(), if !end_of_body && tx_downstream_send_permit.is_ok() => {
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

    pub fn init_from_header_frame(&mut self, header_frame: &ReqFrameData) {
        self.req_size = header_frame.raw_bytes.len();
        self.res_size = 0;
        self.req_start = self.underlying_stream.codec().req_start();
        self.header_frame = Some(header_frame.clone());

        let cmd_type = header_frame.cmd_type;
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

    async fn request_done(&self, _session: &mut Session, _e: Option<&anyhow::Error>, _ctx: &mut Self::CTX)
        where
            Self::CTX: Send + Sync {}
}


