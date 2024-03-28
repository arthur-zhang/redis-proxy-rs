use std::time::Instant;

use anyhow::{anyhow, bail};
use futures::StreamExt;
use log::debug;
use poolx::PoolConnection;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};

use redis_codec_core::resp_decoder::ResFramedData;
use redis_command::{CommandFlags, Group};
use redis_command_gen::CmdType;
use redis_proxy_common::command::utils::{has_flag, has_key, is_group_of};
use redis_proxy_common::ReqPkt;

use crate::client_flags::SessionFlags;
use crate::double_writer::DoubleWriter;
use crate::filter_trait::{Filter, FilterContext};
use crate::handler::get_handler;
use crate::session::Session;
use crate::upstream_conn_pool::{AuthStatus, Pool, RedisConnection};

const TASK_BUFFER_SIZE: usize = 16;

pub struct RedisProxy<P> {
    pub filter: P,
    pub upstream_pool: Pool,
    pub double_writer: DoubleWriter,
}

impl<P> RedisProxy<P> {
    fn should_double_write(&self, session: &Session, header_frame: &&ReqPkt) -> bool {
        //is group of transaction
        if is_group_of(&header_frame.cmd_type, Group::Transactions) {
            return true;
        }
        if session.contains_client_flags(SessionFlags::InTrasactions)
            || session.contains_client_flags(SessionFlags::InWatching){
            return true;
        }
        // not a write command, ignore it.
        if !has_flag(&header_frame.cmd_type, CommandFlags::Write) {
            return false;
        }
        // write command and has no key in command
        // only FLUSHALL、FLUSHDB、FUNCTION DELETE、FUNCTION FLUSH、FUNCTION LOAD、FUNCTION RESTORE、SWAPDB
        // will double write anyway
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

        match self.handle_request_inner(&mut session, &req_pkt, &mut ctx).await {
            Ok(_) => {
                self.filter.on_request_done(&mut session, req_pkt.cmd_type, None, &mut ctx).await;
            }
            Err(err) => {
                self.filter.on_request_done(&mut session, req_pkt.cmd_type, Some(&err), &mut ctx).await;
                return None;
            }
        }
        Some(session)
    }
    pub async fn handle_request_inner(&self, session: &mut Session,
                                      req_pkt: &ReqPkt,
                                      ctx: &mut FilterContext) -> anyhow::Result<()> {
        let start = Instant::now();
        if self.should_double_write(session, &req_pkt) {
            match (&session.upstream_conn.is_none(), &session.dw_conn.is_none()) {
                (false, false) => {
                    // do nothing
                }
                (true, false) => {
                    bail!("should not happen!!! upstream connection is none, but dw_conn is not none")
                }
                (true, true) => {
                    let (r1, r2) = tokio::join!(
                        self.upstream_pool.acquire(),
                        self.double_writer.acquire_conn(),
                    );
                    session.pool_acquire_elapsed = start.elapsed();
                    match (r1, r2) {
                        (Ok(c), Ok(d)) => {
                            session.upstream_conn = Some(c);
                            session.dw_conn = Some(d);
                        }
                        _ => bail!("get connection from pool error")
                    };
                }
                (false, true) => {
                    let conn =
                        self.double_writer.acquire_conn().await.map_err(|e| anyhow!("get connection from dw pool error: {:?}", e))?;
                    session.pool_acquire_elapsed = start.elapsed();
                    session.dw_conn = Some(conn);
                }
            }

            let mut conn = session.upstream_conn.take().unwrap();
            let mut dw_conn = session.dw_conn.take().unwrap();
            self.handle_double_write(session, &req_pkt, &mut conn, &mut dw_conn, ctx).await?;
            session.upstream_conn = Some(conn);
            session.dw_conn = Some(dw_conn);
            Ok(())
        } else {
            if session.upstream_conn.is_none() {
                let conn =
                    self.upstream_pool.acquire().await.map_err(|e| anyhow!("get connection from upstream pool error: {:?}", e))?;
                session.pool_acquire_elapsed = start.elapsed();
                session.upstream_conn = Some(conn);
            }

            let mut conn = session.upstream_conn.take().unwrap();
            self.handle_normal_request(session, &req_pkt, &mut conn, ctx).await?;
            session.upstream_conn = Some(conn);
            Ok(())
        }
    }

    async fn handle_normal_request(&self, session: &mut Session, req_pkt: &ReqPkt,
                                   conn: &mut PoolConnection<RedisConnection>,
                                   _ctx: &mut FilterContext) -> anyhow::Result<()> {
        let auth_status =
            conn.init_from_session(req_pkt.cmd_type, &session.authed_info, session.db).await?;

        if auth_status != AuthStatus::Authed {
            session.write_downstream(b"-NOAUTH Authentication required.\r\n").await?;
            return Ok(());
        }

        let (tx_upstream, rx_upstream) = mpsc::channel::<ResFramedData>(TASK_BUFFER_SIZE);

        session.upstream_start = Instant::now();

        conn.send_bytes_vectored(req_pkt).await?;

        // bi-directional proxy
        tokio::try_join!(
            self.proxy_handle_downstream(session, req_pkt.cmd_type, rx_upstream),
            self.proxy_handle_upstream(conn, tx_upstream, req_pkt.cmd_type)
        )?;
        Ok(())
    }
    async fn handle_double_write(&self, session: &mut Session,
                                 req_pkt: &ReqPkt,
                                 conn: &mut PoolConnection<RedisConnection>,
                                 dw_conn: &mut PoolConnection<RedisConnection>,
                                 _ctx: &mut FilterContext) -> anyhow::Result<()> {
        let auth_status =
            conn.init_from_session(req_pkt.cmd_type, &session.authed_info, session.db).await?;

        if auth_status != AuthStatus::Authed {
            session.write_downstream(b"-NOAUTH Authentication required.\r\n").await?;
            return Ok(());
        }

        session.upstream_start = Instant::now();
        let join_result = tokio::join!(
            conn.send_bytes_vectored_and_wait_resp(&req_pkt),
            dw_conn.send_bytes_vectored_and_wait_resp(&req_pkt)
        );

        debug!("double write result: {:?}", join_result);

        session.upstream_elapsed = session.upstream_start.elapsed();

        return match join_result {
            (Ok((upstream_resp_ok, upstream_pkts, upstream_resp_size)),
                Ok((dw_resp_ok, dw_pkts, _))) => {

                get_handler(req_pkt.cmd_type).map(|h|
                    h.handler_session_after_resp(session, upstream_resp_ok));
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

    async fn proxy_handle_downstream(&self, session: &mut Session, cmd_type: CmdType, mut rx_upstream: Receiver<ResFramedData>) -> anyhow::Result<()> {
        while let Some(res_framed_data) = rx_upstream.recv().await {
            if res_framed_data.is_done {
                session.upstream_elapsed = session.upstream_start.elapsed();
                session.res_is_ok = res_framed_data.res_is_ok;
            }

            session.res_size += res_framed_data.data.len();

            if res_framed_data.is_done {
                get_handler(cmd_type).map(|h|
                    h.handler_session_after_resp(session, res_framed_data.res_is_ok));
            }

            session.write_downstream(&res_framed_data.data).await?;

            if res_framed_data.is_done {
                debug!("upstream result: {:?}", res_framed_data);
                break;
            }
        }
        Ok(())
    }

    async fn proxy_handle_upstream(&self, conn: &mut PoolConnection<RedisConnection>,
                                   tx_upstream: Sender<ResFramedData>,
                                   cmd_type: CmdType)
                                   -> anyhow::Result<()> {
        while let Some(Ok(data)) = conn.r.next().await {
            let is_done = data.is_done;

            if is_done {
                get_handler(cmd_type).map(|h|
                    h.handler_coon_after_resp(conn, data.res_is_ok));
            }
            tx_upstream.send(data).await?;
            if is_done {
                break;
            }
        }
        Ok(())
    }
}


