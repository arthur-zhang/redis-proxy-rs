use std::fmt::format;
use std::net::SocketAddr;
use std::os::macos::raw::stat;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::{anyhow, bail};
use bytes::Bytes;
use futures::SinkExt;
use log::{debug, error, info};
use poolx::{PoolConnection, PoolOptions};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::{mpsc, oneshot};
use tokio::sync::mpsc::{Receiver, Sender, unbounded_channel};
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, FramedRead};

use redis_codec_core::req_decoder::ReqPktDecoder;
use redis_codec_core::resp_decoder::{ResFramedData, RespPktDecoder};
use redis_proxy_common::cmd::CmdType;
use redis_proxy_common::ReqFrameData;

use crate::blacklist_filter::BlackListFilter;
use crate::config::{Blacklist, Config, Mirror, TConfig};
use crate::filter_chain::{FilterChain, TFilterChain};
use crate::log_filter::LogFilter;
use crate::mirror_filter::MirrorFilter;
use crate::tiny_client::TinyClient;
use crate::traits::{Filter, FilterContext, FilterStatus, TFilterContext};
use crate::upstream_conn_pool::{Pool, RedisConnection, RedisConnectionOption};

pub struct ProxyServer {
    config: TConfig,
    filter_chain: TFilterChain,
}

impl ProxyServer {
    pub fn new(config: Config) -> anyhow::Result<Self> {
        let config = Arc::new(config);
        let filter_chain = Self::get_filters(config.clone())?;
        let filter_chains = FilterChain::new(filter_chain);
        Ok(ProxyServer { config, filter_chain: Arc::new(filter_chains) })
    }

    pub async fn start(self) -> anyhow::Result<()> {
        let listener = TcpListener::bind(&self.config.server.address).await.map_err(|e| {
            error!("bind error: {:?}", e);
            e
        })?;


        let conn_option = self.config.upstream.address.parse::<RedisConnectionOption>().unwrap();
        let pool: poolx::Pool<RedisConnection> = PoolOptions::new()
            .idle_timeout(std::time::Duration::from_secs(3))
            .min_connections(3)
            .max_connections(50000)
            .connect_lazy_with(conn_option);

        loop {
            tokio::spawn({
                let filter_chains = self.filter_chain.clone();
                let (c2p_conn, _) = listener.accept().await?;
                let config = self.config.clone();
                // one connection per task
                let pool = pool.clone();
                let session = Session { filter_chains, c2p_conn: Some(c2p_conn), config, pool };
                async move {
                    let _ = session.handle().await;
                    info!("session done")
                }
            });
        };
    }
}

const TASK_BUFFER_SIZE: usize = 4;

impl ProxyServer {
    fn get_filters(config: Arc<Config>) -> anyhow::Result<Vec<Box<dyn Filter>>> {
        let mut filters: Vec<Box<dyn Filter>> = vec![];
        let mut filter_chain_conf = config.filter_chain.clone();

        for filter_name in &config.filter_chain.filters {
            let filter: Box<dyn Filter> = match filter_name.as_str() {
                "blacklist" => {
                    match filter_chain_conf.blacklist.take() {
                        None => {
                            bail!("blacklist filter config is required")
                        }
                        Some(blacklist) => {
                            Box::new(BlackListFilter::new(blacklist.block_patterns, &blacklist.split_regex)?)
                        }
                    }
                }
                "log" => {
                    Box::new(LogFilter::new())
                }
                "mirror" => {
                    match filter_chain_conf.mirror.take() {
                        None => {
                            bail!("mirror filter config is required")
                        }
                        Some(mirror) => {
                            Box::new(MirrorFilter::new(mirror.address.as_str(), &mirror.mirror_patterns, mirror.split_regex.as_str(), mirror.queue_size)?)
                        }
                    }
                }
                _ => {
                    bail!("unknown filter: {}", filter_name)
                }
            };
            filters.push(filter);
        }

        Ok(filters)
    }
}

pub struct Session {
    filter_chains: TFilterChain,
    c2p_conn: Option<TcpStream>,
    config: Arc<Config>,
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

    async fn process_req(&mut self, framed: &mut Framed<TcpStream, ReqPktDecoder>, ctx:&mut FilterContext) -> anyhow::Result<()> {
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

pub enum HttpTask {
    Data(Bytes)
}


pub struct RedisService {}

impl RedisService {
    pub async fn rebuild_session(conn: &mut RedisConnection, db: u64) -> anyhow::Result<()> {
        let db_index = format!("{}", db);
        let cmd = format!("*2\r\n$6\r\nselect\r\n${}\r\n{}\r\n", db_index.len(), db_index);
        let ok = TinyClient::query(conn, cmd.as_bytes()).await?;
        conn.session_attr.db = db;
        if !ok {
            return Err(anyhow!("rebuild session failed"));
        }
        Ok(())
    }
    pub fn on_select_db(ctx: &mut FilterContext, data: &ReqFrameData) -> anyhow::Result<()> {
        if let Some(args) = data.args() {
            if args.len() > 0 {
                let db = std::str::from_utf8(args[0])?.parse::<u64>().unwrap_or(0);
                ctx.db = db;
            }
        }
        Ok(())
    }

    pub async fn auth_connection(conn: &mut RedisConnection, pass: &str) -> anyhow::Result<bool> {
        let cmd = format!("*2\r\n$4\r\nAUTH\r\n${}\r\n{}\r\n", pass.len(), pass);
        let (query_ok, resp_data) = TinyClient::query_with_resp(conn, cmd.as_bytes()).await?;
        if !query_ok || resp_data.len() == 0 {
            return Ok(false);
        }
        let mut data = bytes::BytesMut::with_capacity(resp_data.first().unwrap().len());
        for d in resp_data {
            data.extend_from_slice(&d);
        }
        return Ok(data.as_ref() == b"+OK\r\n");
    }
    pub fn on_auth(ctx: &mut FilterContext, data: &ReqFrameData) -> anyhow::Result<()> {
        if let Some(args) = data.args() {
            if args.len() > 0 {
                let auth_password = std::str::from_utf8(args[0]).unwrap_or("").to_owned();
                ctx.password = Some(auth_password);
            }
        }
        Ok(())
    }
}



