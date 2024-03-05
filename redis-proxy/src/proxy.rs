use std::sync::Arc;
use std::time::Instant;

use anyhow::{anyhow, bail};
use async_trait::async_trait;
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use futures::future::ok;
use log::{error, info};
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
use crate::server::{ProxyChanData, TASK_BUFFER_SIZE};
use crate::tiny_client::TinyClient;
use crate::upstream_conn_pool::{Pool, RedisConnection};

pub struct RedisProxy<P> {
    pub inner: P,
    pub upstream_pool: Pool,
}

impl<P> RedisProxy<P> where P: Proxy + Send + Sync, <P as Proxy>::CTX: Send + Sync {
    pub async fn handle_new_request(&self, mut session: Session, pool: Pool) -> anyhow::Result<Option<Session>> {
        info!("handle_new_request..........");
        self.inner.on_session_create().await?;
        // read header frame
        let mut req_frame = match session.downstream_session.underlying_stream.next().await {
            None => { return Ok(None); }
            Some(req_frame) => {
                session.downstream_session.req_start = session.downstream_session.underlying_stream.codec().req_start();
                req_frame.map_err(|e| anyhow!("decode req frame error: {:?}", e))?
            }
        };
        error!("req_frame: {:?}", req_frame);

        session.downstream_session.header_frame = Some(req_frame.clone());

        if req_frame.cmd_type == CmdType::SELECT {
            on_select_db(&mut session)?;
        } else if req_frame.cmd_type == CmdType::AUTH {
            on_auth(&mut session)?;
        }


        let mut ctx = self.inner.new_ctx();
        let response_sent = self.inner.request_filter(&mut session, &mut ctx).await?;
        if response_sent {
            info!("response sent: true");
            if !req_frame.is_done {
                session.downstream_session.drain_req_until_done().await?;
            }
            self.inner.request_done(&mut session, None, &mut ctx).await;
            return Ok(Some(session));
        }

        let mut conn = pool.acquire().await.map_err(|e| anyhow!("get connection from pool error: {:?}", e))?;
        error!("get a connection : {:?}", conn.as_ref());

        let response_sent = conn.init_from_session(&mut session).await?;
        if response_sent {
            session.downstream_session.drain_req_until_done().await?;
            self.inner.request_done(&mut session, None, &mut ctx).await;
            return Ok(Some(session));
        }
        if let Err(err) = self.inner.proxy_upstream_filter(&mut session, &mut ctx).await {
            bail!("proxy_upstream_filter failed: {:?}", err)
        }
        if let Err(err) = self.inner.upstream_request_filter(&mut session, &mut req_frame, &mut ctx).await {
            bail!("upstream_request_filter failed: {:?}", err)
        }

        error!("client connection is none, reconnect to backend server, id: {}", conn.id);
        let (tx_upstream, rx_upstream) = mpsc::channel::<ProxyChanData>(TASK_BUFFER_SIZE);
        let (tx_downstream, rx_downstream) = mpsc::channel::<ProxyChanData>(TASK_BUFFER_SIZE);


        conn.w.write_all(&req_frame.raw_bytes).await?;

        // bi-directional proxy
        let res = tokio::try_join!(
            self.proxy_handle_downstream(&mut session, tx_downstream, rx_upstream, &mut ctx),
            self.proxy_handle_upstream(conn, tx_upstream, rx_downstream)
        );
        self.inner.request_done(&mut session, None, &mut ctx).await;
        match res {
            Ok(_) => {
                info!("proxy done");
            }
            Err(e) => {
                error!("proxy error: {:?}", e);
            }
        }
        Ok(Some(session))
    }
    async fn proxy_handle_downstream(&self,
                                     session: &mut Session,
                                     tx_downstream: Sender<ProxyChanData>,
                                     mut rx_upstream: Receiver<ProxyChanData>,
                                     ctx: &mut <P as Proxy>::CTX) -> anyhow::Result<()> {
        let mut end_of_body = session.request_done();
        let mut response_done = false;
        // let mut request_done = false;
        while !end_of_body || !response_done {
            info!("proxy_handle_downstream.... {}, {}", end_of_body, response_done);
            let send_permit = tx_downstream.try_reserve();
            tokio::select! {
                data = session.downstream_session.underlying_stream.next(), if !end_of_body && send_permit.is_ok() => {
                    info!("framed next...., request_done:{}, {:?}", end_of_body, data);
                    match data {
                        Some(Ok(mut data)) => {
                            let is_done = data.is_done;
                            self.inner.upstream_request_filter(session, &mut data, ctx).await?;
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
                _ = tx_downstream.reserve(), if send_permit.is_err() => {

                }
                task = rx_upstream.recv(), if !response_done => {
                    info!("rx_upstream recv...., response_done:{}, {:?}", response_done, task);

                    match task {
                        Some(ProxyChanData::ResFrameData(res_framed_data)) => {
                            session.downstream_session.underlying_stream.send(res_framed_data.data).await?;
                            response_done = res_framed_data.is_done;

                            let cmd_type = session.cmd_type();
                            if cmd_type == CmdType::AUTH && res_framed_data.is_done {
                                session.downstream_session.is_authed = !res_framed_data.is_error;
                            }
                        }
                        Some(_)=>{
                            todo!()
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
                                   mut rx_downstream: Receiver<ProxyChanData>)
                                   -> anyhow::Result<()> {
        let mut request_done = false;
        let mut response_done = false;
        while !request_done || !response_done {
            error!("in proxy_handle_upstream, request_done: {}, response_done: {}", request_done, response_done);
            tokio::select! {
                task = rx_downstream.recv(), if !request_done => {
                    match task {
                        Some(ProxyChanData::ReqFrameData(frame_data)) => {
                            conn.w.write_all(&frame_data.raw_bytes).await?;
                            request_done = frame_data.is_done;
                        }

                        Some(a)=>{
                            error!("unexpected data: {:?}", a);
                            todo!()
                        }
                        _ => {
                            request_done = true;
                        }
                    }
                }
                data = conn.r.next(), if !response_done => {
                    match data {
                        Some(Ok(data)) => {
                            let is_done = data.is_done;
                            tx_upstream.send(ProxyChanData::ResFrameData(data)).await?;
                            if is_done {
                                response_done = true;
                            }

                        }
                        Some(Err(err))=> {
                            todo!()
                        }

                        None => {
                            response_done = true;
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

fn on_select_db(session: &mut Session) -> anyhow::Result<()> {
    if let Some(ref header_frame) = session.downstream_session.header_frame {
        if let Some(args) = header_frame.args() {
            let db = std::str::from_utf8(args[0])?.parse::<u64>().unwrap_or(0);
            session.downstream_session.db = db;
        }
    }
    Ok(())
}

pub fn on_auth(session: &mut Session) -> anyhow::Result<()> {
    if let Some(ref header_frame) = session.downstream_session.header_frame {
        if let Some(args) = header_frame.args() {
            if args.len() > 0 {
                let auth_password = std::str::from_utf8(args[0]).unwrap_or("").to_owned();
                session.downstream_session.password = Some(auth_password);
            }
        }
    }
    Ok(())
}


pub struct Session {
    pub downstream_session: RedisSession,

}

impl Session {
    pub fn request_done(&self) -> bool {
        self.downstream_session.header_frame.as_ref().map(|it| it.is_done).unwrap_or(false)
    }
    pub fn cmd_type(&self) -> CmdType {
        self.downstream_session.header_frame.as_ref().map(|it| it.cmd_type).unwrap_or(CmdType::UNKNOWN)
    }
}

pub struct RedisSession {
    pub underlying_stream: Framed<TcpStream, ReqPktDecoder>,
    pub header_frame: Option<ReqFrameData>,
    pub password: Option<String>,
    pub db: u64,
    pub is_authed: bool,
    pub req_start: Instant,
    pub resp_is_ok: bool,
    pub req_end: Instant,

}

impl RedisSession {
    pub async fn drain_req_until_done(&mut self) -> anyhow::Result<()> {
        while let Some(Ok(req_frame_data)) = self.underlying_stream.next().await {
            if req_frame_data.is_done {
                return Ok(());
            }
        }
        return Ok(());
    }
}

// pub struct

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

    async fn request_done(&self, _session: &mut Session, _e: Option<&anyhow::Error>, _ctx: &mut Self::CTX)
        where
            Self::CTX: Send + Sync {}
}


