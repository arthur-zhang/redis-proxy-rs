use anyhow::anyhow;
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use log::{error, info};
use poolx::PoolConnection;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_util::codec::Framed;

use redis_codec_core::req_decoder::ReqPktDecoder;
use redis_proxy_common::cmd::CmdType;
use redis_proxy_common::ReqFrameData;
use crate::server::{HttpTask, RedisService, TASK_BUFFER_SIZE};
use crate::traits::{FilterContext, FilterStatus};

use crate::upstream_conn_pool::{Pool, RedisConnection};

pub struct RedisProxy<P> where P: Proxy {
    pub inner: P,
    pub upstream_pool: Pool,
}

impl<P> RedisProxy<P> where P:Proxy {


    pub async fn handle_new_request(&self, mut downstream_session: RedisSession, pool: Pool) -> anyhow::Result<()> {
        let req_frame = match downstream_session.underlying_stream.next().await {
            None => { return Ok(()); }
            Some(req_frame) => {
                req_frame.map_err(|e| anyhow!("decode req frame error: {:?}", e))?
            }
        };

        let mut ctx = FilterContext {
            db: 0,
            is_authed: false,
            cmd_type: CmdType::APPEND,
            password: None,
            attrs: Default::default(),
        };

        ctx.cmd_type = req_frame.cmd_type;
        if req_frame.cmd_type == CmdType::SELECT {
            RedisService::on_select_db(&mut ctx, &req_frame)?;
        } else if req_frame.cmd_type == CmdType::AUTH {
            RedisService::on_auth(&mut ctx, &req_frame)?;
        }
        let status = FilterStatus::Continue;
        if status == FilterStatus::Block {
            if req_frame.is_done {
                downstream_session.underlying_stream.send(Bytes::from_static(b"-ERR blocked\r\n")).await?;
            }
            return Ok(());
        }
        let mut conn = pool.acquire().await.map_err(|e| anyhow!("get connection from pool error: {:?}", e))?;
        error!("get a connection : {:?}", conn.as_ref());
        // let response_sent = Self::auth_connection_if_needed(downstream_session.underlying_stream, ctx, &req_frame, &mut conn).await?;
        // if response_sent {
        //     return Ok(());
        // }

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
            self.proxy_handle_downstream(&mut downstream_session.underlying_stream, tx_downstream, rx_upstream),
            self.proxy_handle_upstream(conn, tx_upstream, rx_downstream)
        );
        Ok(())
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
}

pub struct RedisSession {
    pub underlying_stream: Framed<TcpStream, ReqPktDecoder>,
}

pub trait Proxy {}


pub struct MyProxy;

impl Proxy for MyProxy {}