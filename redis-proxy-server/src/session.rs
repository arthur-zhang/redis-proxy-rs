use tokio::net::TcpStream;
use std::sync::Arc;
use tokio_util::codec::Framed;
use log::{error, info};
use anyhow::{anyhow, bail};
use bytes::Bytes;
use redis_codec_core::req_decoder::ReqPktDecoder;
use redis_proxy_common::cmd::CmdType;
use tokio::sync::mpsc;
use poolx::PoolConnection;
use redis_proxy_common::ReqFrameData;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_stream::StreamExt;
use futures::SinkExt;
use tokio::io::AsyncWriteExt;
use crate::config::Config;
use crate::filter_chain::TFilterChain;
use crate::server::{HttpTask, RedisService, TASK_BUFFER_SIZE};
use crate::traits::{FilterContext, FilterStatus};
use crate::upstream_conn_pool::{Pool, RedisConnection};

pub struct Session {
    pub filter_chains: TFilterChain,
    pub c2p_conn: Option<TcpStream>,
    pub config: Arc<Config>,
    pub pool: Pool,
}


impl Session {
    // 1. read from client
    // 2. write to backend upstream server
    // 3. read from backend upstream server
    // 4. write to client
    pub async fn handle(mut self) -> anyhow::Result<()> {
        let c2p_conn = self.c2p_conn.take().unwrap();
        let mut framed = Framed::with_capacity(c2p_conn, ReqPktDecoder::new(), 512);

        let mut ctx = FilterContext {
            db: 0,
            is_authed: false,
            cmd_type: CmdType::APPEND,
            password: None,
            attrs: Default::default(),
        };
        // self.filter_chains.on_session_create()?;
        loop {
            let res = self.process_req(&mut framed, &mut ctx).await;
            error!("process_req res: {:?}", res);
            // self.filter_chains.post_handle(&mut ctx)?;
            if let Err(e) = res {
                break;
            }
        }
        // self.filter_chains.on_session_close()?;

        Ok(())
    }

    async fn process_req(&mut self, framed: &mut Framed<TcpStream, ReqPktDecoder>, ctx: &mut FilterContext) -> anyhow::Result<()> {
        let pool = self.pool.clone();
        let req_frame = match framed.next().await {
            None => { return Ok(()); }
            Some(req_frame) => {
                req_frame.map_err(|e| anyhow!("decode req frame error: {:?}", e))?
            }
        };
        ctx.cmd_type = req_frame.cmd_type;
        if req_frame.cmd_type == CmdType::SELECT {
            RedisService::on_select_db(ctx, &req_frame)?;
        } else if req_frame.cmd_type == CmdType::AUTH {
            RedisService::on_auth(ctx, &req_frame)?;
        }
        let status = FilterStatus::Continue;
        if status == FilterStatus::Block {
            if req_frame.is_done {
                framed.send(Bytes::from_static(b"-ERR blocked\r\n")).await?;
            }
            return Ok(());
        }
        let mut conn = pool.acquire().await.map_err(|e| anyhow!("get connection from pool error: {:?}", e))?;
        error!("get a connection : {:?}", conn.as_ref());
        let response_sent = Self::auth_connection_if_needed(framed, ctx, &req_frame, &mut conn).await?;
        if response_sent {
            return Ok(());
        }

        conn.session_attr.password = ctx.password.clone();
        if conn.session_attr.db != ctx.db {
            info!("rebuild session from {} to {}", conn.session_attr.db, ctx.db);
            RedisService::rebuild_session(&mut conn, ctx.db).await?;
        }
        error!("client connection is none, reconnect to backend server, id: {}", conn.id);
        let (tx_upstream, rx_upstream) = mpsc::channel::<HttpTask>(TASK_BUFFER_SIZE);
        let (tx_downstream, rx_downstream) = mpsc::channel::<HttpTask>(TASK_BUFFER_SIZE);

        conn.w.write_all(&req_frame.raw_bytes).await?;

        // bi-directional proxy
        let res = tokio::try_join!(
            self.proxy_handle_downstream(framed, tx_downstream, rx_upstream),
            self.proxy_handle_upstream(conn, tx_upstream, rx_downstream)
        );

        error!(">>>>>>>>>>>>>>>>res: {:?}", res);

        Ok(())
    }

    async fn auth_connection_if_needed(
        framed: &mut Framed<TcpStream, ReqPktDecoder>,
        ctx: &mut FilterContext,
        req_frame: &ReqFrameData,
        mut conn: &mut PoolConnection<RedisConnection>) -> anyhow::Result<bool> {
        match (conn.is_authed, ctx.is_authed) {
            (true, true) | (false, false) => {}
            (false, true) => {
                // auth connection
                let authed = RedisService::auth_connection(&mut conn, ctx.password.as_ref().unwrap()).await?;
                if authed {
                    conn.is_authed = true;
                } else {
                    bail!("auth failed");
                }
            }
            (true, false) => {
                // connection is auth, but ctx is not auth, should return no auth
                if req_frame.is_done {
                    framed.send(Bytes::from_static(b"-NOAUTH Authentication required.\r\n")).await?;
                }
                return Ok(true);
            }
        }
        return Ok(false);
    }
    async fn proxy_handle_downstream(&self,
                                     framed: &mut Framed<TcpStream, ReqPktDecoder>,
                                     tx_downstream: Sender<HttpTask>,
                                     mut rx_upstream: Receiver<HttpTask>) -> anyhow::Result<()> {
        loop {
            tokio::select! {
                Some(Ok(data)) = framed.next() => {
                    tx_downstream.send(HttpTask::Data(data.raw_bytes)).await?;
                }
                Some(task) = rx_upstream.recv() => {
                    match task {
                        HttpTask::Data(data) => {
                            framed.send(data).await?;
                        }
                    }
                }
            }
        }
    }

    async fn proxy_handle_upstream(&self,
                                   mut conn: PoolConnection<RedisConnection>,
                                   tx_upstream: Sender<HttpTask>,
                                   mut rx_downstream: Receiver<HttpTask>)
                                   -> anyhow::Result<()> {
        loop {
            tokio::select! {
                Some(task) = rx_downstream.recv() => {
                    match task {
                        HttpTask::Data(data) => {
                            conn.w.write_all(&data).await?;
                        }
                    }
                }
                Some(Ok(data)) = conn.r.next() => {
                    tx_upstream.send(HttpTask::Data(data.data)).await?;
                }
            }
        }
    }
    fn truncate_str(s: &str, max_chars: usize) -> &str {
        if s.chars().count() <= max_chars {
            s
        } else {
            match s.char_indices().nth(max_chars) {
                Some((idx, _)) => &s[..idx],
                None => s,
            }
        }
    }
}
