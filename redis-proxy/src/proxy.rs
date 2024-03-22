use std::collections::HashMap;
use std::time::Instant;

use anyhow::{anyhow, bail};
use async_trait::async_trait;
use futures::StreamExt;
use log::debug;
use poolx::PoolConnection;
use smol_str::SmolStr;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};

use redis_codec_core::resp_decoder::ResFramedData;
use redis_proxy_common::command::utils::{CMD_TYPE_AUTH, has_key, is_connection_cmd, is_readonly_cmd, is_write_cmd};
use redis_proxy_common::ReqPkt;

use crate::double_writer::DoubleWriter;
use crate::filter_trait::{Filter, FilterContext};
use crate::peer;
use crate::session::Session;
use crate::upstream_conn_pool::{AuthStatus, Pool, RedisConnection};

const TASK_BUFFER_SIZE: usize = 16;

pub struct RedisProxy<P> {
    pub filter: P,
    pub upstream_pool: Pool,
    pub double_writer: DoubleWriter,
}

impl<P> RedisProxy<P> {
    fn should_double_write(&self, header_frame: &ReqPkt) -> bool {
        if is_readonly_cmd(&header_frame.cmd_type) {
            return false;
        }
        if !has_key(&header_frame.cmd_type) {
            return true;
        }
        self.double_writer.should_double_write(header_frame)
    }
}

impl<P> RedisProxy<P> where P: Filter + Send + Sync {
    pub async fn handle_new_request(&self, mut session: Session) -> Option<Session> {
        debug!("handle new request");

        let req_pkt = session.read_req_pkt().await?.ok()?;
        session.init_from_req(&req_pkt);

        let mut ctx = FilterContext::new();

        let response_sent =
            self.filter.on_request(&mut session, &req_pkt, &mut ctx).await.ok()?;
        if response_sent {
            return Some(session);
        }
        session.upstream_start = Instant::now();

        match self.handle_request_inner(&mut session, &req_pkt, &req_pkt.cmd_type, &mut ctx).await {
            Ok(_) => {
                self.filter.on_request_done(&mut session, &req_pkt.cmd_type, None, &mut ctx).await;
            }
            Err(err) => {
                self.filter.on_request_done(&mut session, &req_pkt.cmd_type, Some(&err), &mut ctx).await;
                return None;
            }
        }
        Some(session)
    }
    pub async fn handle_request_inner(&self, session: &mut Session,
                                      req_pkt: &ReqPkt,
                                      cmd_type: &SmolStr,
                                      ctx: &mut FilterContext) -> anyhow::Result<()> {
        if self.should_double_write(&req_pkt) {
            let start = Instant::now();
            let (r1, r2) = tokio::join!(
                self.upstream_pool.acquire(),
                self.double_writer.acquire_conn(),
            );
            session.pool_acquire_elapsed = start.elapsed();
            let (conn, dw_conn) = match (r1, r2) {
                (Ok(c), Ok(d)) => (c, d),
                _ => bail!("get connection from pool error")
            };

            self.handle_double_write(session, &req_pkt, dw_conn, conn, cmd_type, ctx).await
        } else {
            let start = Instant::now();
            let conn =
                self.upstream_pool.acquire().await.map_err(|e| anyhow!("get connection from pool error: {:?}", e))?;
            session.pool_acquire_elapsed = start.elapsed();

            self.handle_normal_request(session, &req_pkt, cmd_type, ctx, conn).await
        }
    }

    async fn handle_normal_request(&self, session: &mut Session, req_pkt: &ReqPkt,
                                   cmd_type: &SmolStr, _ctx: &mut FilterContext,
                                   mut conn: PoolConnection<RedisConnection>) -> anyhow::Result<()> {
        let auth_status =
            conn.init_from_session(cmd_type, session.is_authed, &session.username, &session.password, session.db).await?;

        if auth_status != AuthStatus::Authed {
            session.write_downstream(b"-NOAUTH Authentication required.\r\n").await?;
            return Ok(());
        }

        let (tx_upstream, rx_upstream) = mpsc::channel::<ResFramedData>(TASK_BUFFER_SIZE);

        conn.send_bytes_vectored(req_pkt).await?;

        // bi-directional proxy
        tokio::try_join!(
            self.proxy_handle_downstream(session, cmd_type, rx_upstream),
            self.proxy_handle_upstream(conn, tx_upstream, cmd_type)
        )?;
        Ok(())
    }
    async fn handle_double_write(&self, session: &mut Session,
                                 req_pkt: &ReqPkt,
                                 mut dw_conn: PoolConnection<RedisConnection>,
                                 mut conn: PoolConnection<RedisConnection>,
                                 cmd_type: &SmolStr, ctx: &mut FilterContext) -> anyhow::Result<()> {
        let (auth_status_upstream, auth_status_dw) = tokio::try_join!(
                conn.init_from_session(cmd_type, session.is_authed, &session.username, &session.password, session.db),
                dw_conn.init_from_session(cmd_type, session.is_authed, &session.username, &session.password, session.db))?;
        match (auth_status_upstream, auth_status_dw) {
            (AuthStatus::Authed, AuthStatus::Authed) => {}
            (_, _) => {
                session.write_downstream(b"-NOAUTH Authentication required.\r\n").await?;
                return Ok(());
            }
        }

        session.upstream_start = Instant::now();
        let join_result = tokio::join!(
            conn.send_bytes_vectored_and_wait_resp(&req_pkt),
            dw_conn.send_bytes_vectored_and_wait_resp(&req_pkt)
        );

        session.upstream_elapsed = session.upstream_start.elapsed();

        return match join_result {
            (Ok((upstream_resp_ok, upstream_pkts, upstream_resp_size)),
                Ok((dw_resp_ok, dw_pkts, _))) => {
                if CMD_TYPE_AUTH.eq(cmd_type) {
                    session.is_authed = upstream_resp_ok;
                }
                session.res_size += upstream_resp_size;
                session.res_is_ok = upstream_resp_ok && dw_resp_ok;

                if dw_resp_ok {
                    session.write_downstream_batch(upstream_pkts).await?;
                } else {
                    // if double write is not ok, send upstream response to downstream
                    session.write_downstream_batch(dw_pkts).await?;
                }
                Ok(())
            }
            (r1, r2) => {
                let _ = session.write_downstream(b"-error").await;
                bail!("double write error: r1: {:?}, r2:{:?}", r1, r2)
            }
        };
    }

    async fn proxy_handle_downstream(&self, session: &mut Session, cmd_type: &SmolStr, mut rx_upstream: Receiver<ResFramedData>) -> anyhow::Result<()> {
        while let Some(res_framed_data) = rx_upstream.recv().await {
            if res_framed_data.is_done {
                session.upstream_elapsed = session.upstream_start.elapsed();
                session.res_is_ok = res_framed_data.res_is_ok;
            }
            session.res_size += res_framed_data.data.len();
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


