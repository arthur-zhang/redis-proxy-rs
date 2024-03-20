use std::io::IoSlice;
use std::sync::Arc;

use anyhow::{anyhow, bail};
use async_trait::async_trait;
use futures::StreamExt;
use log::{debug, error, info};
use poolx::PoolConnection;
use smol_str::SmolStr;
use tokio::io::AsyncWriteExt;
use tokio::net::unix::pipe::pipe;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};

use redis_codec_core::resp_decoder::ResFramedData;
use redis_proxy_common::{ReqFrameData, ReqPkt};
use redis_proxy_common::command::utils::{CMD_TYPE_ALL, CMD_TYPE_AUTH, is_connection_cmd, is_write_cmd};

use crate::double_writer::DoubleWriter;
use crate::peer;
use crate::server::ProxyChanData;
use crate::session::Session;
use crate::upstream_conn_pool::{AuthStatus, Pool, RedisConnection};

const TASK_BUFFER_SIZE: usize = 16;

pub struct RedisProxy<P> {
    pub inner: P,
    pub upstream_pool: Pool,
    pub double_writer: DoubleWriter,
}

impl<P> RedisProxy<P> {
    fn should_double_write(&self, header_frame: &ReqPkt) -> bool {
        // return if is_connection_cmd(&header_frame.cmd_type) {
        //     true
        // } else if is_write_cmd(&header_frame.cmd_type) {
        //     self.double_writer.should_double_write(header_frame)
        // } else {
        //     false
        // };
        // todo!()
        true
    }
}

impl<P> RedisProxy<P> where P: Proxy + Send + Sync, <P as Proxy>::CTX: Send + Sync {
    pub async fn handle_new_request(&self, mut session: Session) -> Option<Session> {
        debug!("handle new request");

        self.inner.on_session_create().await.ok()?;
        let req_pkt = session.read_req_pkt().await?.ok()?;
        session.init_from_req(&req_pkt);

        let mut ctx = self.inner.new_ctx();

        let response_sent =
            self.inner.request_filter(&mut session, &req_pkt, &mut ctx).await.ok()?;
        if response_sent {
            return Some(session);
        }

        match self.handle_request_inner(&mut session, &req_pkt, &req_pkt.cmd_type(), &mut ctx).await {
            Ok(_) => {
                self.inner.request_done(&mut session, None, &mut ctx).await;
            }
            Err(err) => {
                self.inner.request_done(&mut session, Some(&err), &mut ctx).await;
                return None;
            }
        }
        Some(session)
    }
    pub async fn handle_request_inner(&self, session: &mut Session,
                                      req_pkt: &ReqPkt,
                                      cmd_type: &SmolStr,
                                      ctx: &mut <P as Proxy>::CTX) -> anyhow::Result<()> {
        let conn =
            self.upstream_pool.acquire().await.map_err(|e| anyhow!("get connection from pool error: {:?}", e))?;

        if self.should_double_write(&req_pkt) {
            let dw_conn = self.double_writer.acquire_conn().await.map_err(|e| anyhow!("get double write connection from pool error: {:?}", e))?;
            self.handle_double_write(session, &req_pkt, dw_conn, conn, cmd_type, ctx).await
        } else {
            self.handle_normal_request(session, &req_pkt, cmd_type, ctx, conn).await
        }
    }

    async fn handle_normal_request(&self, session: &mut Session, req_pkt: &ReqPkt,
                                   cmd_type: &SmolStr, ctx: &mut <P as Proxy>::CTX,
                                   mut conn: PoolConnection<RedisConnection>) -> anyhow::Result<()> {
        let auth_status =
            conn.init_from_session(cmd_type, session.is_authed, &session.password, session.db).await?;

        if auth_status != AuthStatus::Authed {
            session.write_downstream(b"-NOAUTH Authentication required.\r\n").await?;
            return Ok(());
        }

        // self.inner.proxy_upstream_filter(session, ctx).await?;
        // self.inner.upstream_request_filter(session, req_pkt, ctx).await?;

        let (tx_upstream, rx_upstream) = mpsc::channel::<ResFramedData>(TASK_BUFFER_SIZE);

        conn.send_bytes_vectored(req_pkt).await?;

        // bi-directional proxy
        tokio::try_join!(
            self.proxy_handle_downstream(session, rx_upstream),
            self.proxy_handle_upstream(conn, tx_upstream, cmd_type)
        )?;
        Ok(())
    }
    async fn handle_double_write(&self, session: &mut Session,
                                 req_pkt: &ReqPkt,
                                 mut dw_conn: PoolConnection<RedisConnection>,
                                 mut conn: PoolConnection<RedisConnection>,
                                 cmd_type: &SmolStr, ctx: &mut <P as Proxy>::CTX) -> anyhow::Result<()> {
        let (auth_status_upstream, auth_status_dw) = tokio::try_join!(
                conn.init_from_session(cmd_type, session.is_authed, &session.password, session.db),
                dw_conn.init_from_session(cmd_type, session.is_authed, &session.password, session.db))?;
        match (auth_status_upstream, auth_status_dw) {
            (AuthStatus::Authed, AuthStatus::Authed) => {}
            (_, _) => {
                session.write_downstream(b"-NOAUTH Authentication required.\r\n").await?;
                return Ok(());
            }
        }


        let join_result = tokio::join!(conn.send_bytes_vectored_and_wait_resp(&req_pkt),  dw_conn.send_bytes_vectored_and_wait_resp(&req_pkt));

        return match join_result {
            (Ok((upstream_resp_ok, upstream_pkts, upstream_resp_size)),
                Ok((dw_resp_ok, dw_pkts, _))) => {
                if CMD_TYPE_AUTH.eq(cmd_type) {
                    session.is_authed = upstream_resp_ok;
                }
                session.res_size += upstream_resp_size;
                session.res_is_ok = upstream_resp_ok && dw_resp_ok;

                for pkt in &upstream_pkts {
                    self.inner.upstream_response_filter(session, &pkt, ctx);
                }

                if dw_resp_ok {
                    session.write_downstream_batch(upstream_pkts).await?;
                } else {
                    // if double write is not ok, send upstream response to downstream
                    session.write_downstream_batch(dw_pkts).await?;
                }
                Ok(())
            }
            _ => {
                let _ = session.write_downstream(b"-error").await;
                bail!("double write error")
            }
        };
    }

    async fn proxy_handle_downstream(&self, session: &mut Session, mut rx_upstream: Receiver<ResFramedData>) -> anyhow::Result<()> {
        while let Some(res_framed_data) = rx_upstream.recv().await {
            if res_framed_data.is_done {
                session.res_is_ok = res_framed_data.res_is_ok;
            }
            session.res_size += res_framed_data.data.len();
            let cmd_type = session.cmd_type();
            if CMD_TYPE_AUTH.eq(cmd_type) && res_framed_data.is_done {
                session.is_authed = res_framed_data.res_is_ok;
            }
           
            session.write_downstream(&res_framed_data.data).await?;

            if res_framed_data.is_done {
                break;
            }
        }
        Ok(())
    }

    async fn proxy_handle_upstream(&self,
                                   mut conn: PoolConnection<RedisConnection>,
                                   tx_upstream: Sender<ResFramedData>,
                                   cmd_type: &SmolStr)
                                   -> anyhow::Result<()> {
        while let Some(Ok(data)) = conn.r.next().await {
            let is_done = data.is_done;

            if CMD_TYPE_AUTH.eq(cmd_type) && is_done {
                conn.is_authed = data.res_is_ok;
            }
            tx_upstream.send(data).await?;
            if is_done {
                break;
            }
        }
        Ok(())
    }
}


#[async_trait]
pub trait Proxy {
    type CTX;
    fn new_ctx(&self) -> Self::CTX { unimplemented!("unexpected data") }

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
    ) -> anyhow::Result<peer::RedisPeer> { unimplemented!("unexpected data") }

    /// Handle the incoming request.
    ///
    /// In this phase, users can parse, validate, rate limit, perform access control and/or
    /// return a response for this request.
    ///
    /// If the user already sent a response to this request, a `Ok(true)` should be returned so that
    /// the proxy would exit. The proxy continues to the next phases when `Ok(false)` is returned.
    ///
    /// By default, this filter does nothing and returns `Ok(false)`.
    async fn request_filter(&self, _session: &mut Session, _req_pkt: &ReqPkt, _ctx: &mut Self::CTX) -> anyhow::Result<bool> {
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
        _upstream_response: &ResFramedData,
        _ctx: &mut Self::CTX,
    ) {}

    async fn request_done(&self, _session: &mut Session, _e: Option<&anyhow::Error>, _ctx: &mut Self::CTX)
        where
            Self::CTX: Send + Sync {}
}


