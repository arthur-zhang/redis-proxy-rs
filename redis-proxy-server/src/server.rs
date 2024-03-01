use std::fmt::format;
use std::net::SocketAddr;
use std::os::macos::raw::stat;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::{anyhow, bail};
use bytes::Bytes;
use futures::SinkExt;
use log::{debug, error, info};
use poolx::PoolOptions;
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
        let mut framed = Framed::with_capacity(c2p_conn, ReqPktDecoder::new(), 16384);
        let mut ctx = FilterContext { db: 0, is_authed: false, cmd_type: CmdType::UNKNOWN, password: None, attrs: Default::default(), framed, pool: self.pool.clone() };

        self.filter_chains.on_session_create(&mut ctx)?;
        loop {
            self.filter_chains.pre_handle(&mut ctx)?;
            let res = self.process_req(&mut ctx).await;
            self.filter_chains.post_handle(&mut ctx)?;
            if let Err(e) = res {
                break;
            }
        }
        self.filter_chains.on_session_close(&mut ctx)?;

        Ok(())
    }

    async fn process_req(&mut self, ctx: &mut FilterContext) -> anyhow::Result<()> {
        let pool = ctx.pool.clone();
        let mut client_conn = None;
        loop {
            match ctx.framed.next().await {
                Some(Ok(data)) => {
                    if data.is_first_frame {
                        self.filter_chains.pre_handle(ctx)?;
                        ctx.cmd_type = data.cmd_type;

                        if data.cmd_type == CmdType::SELECT {
                            RedisService::on_select_db(ctx, &data)?;
                        } else if data.cmd_type == CmdType::AUTH {
                            RedisService::on_auth(ctx, &data)?;
                        }

                        let cmd_type = data.cmd_type.clone();
                        ctx.set_attr_cmd_type(cmd_type);
                    }

                    let status = self.filter_chains.on_req_data(ctx, &data).await?;

                    if status == FilterStatus::Block {
                        if data.is_done {
                            ctx.framed.send(Bytes::from_static(b"-ERR blocked\r\n")).await?;
                        }
                        continue;
                    }
                    if client_conn.is_none() {
                        let mut conn = pool.acquire().await.map_err(|e| anyhow!("get connection from pool error: {:?}", e))?;

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
                                if data.is_done {
                                    ctx.framed.send(Bytes::from_static(b"-NOAUTH Authentication required.\r\n")).await?;
                                }
                                continue;
                            }
                        }
                        error!("get a connection : {}", conn.id);
                        conn.session_attr.password = ctx.password.clone();
                        if conn.session_attr.db != ctx.db {
                            info!("rebuild session from {} to {}", conn.session_attr.db, ctx.db);
                            RedisService::rebuild_session(&mut conn, ctx.db).await?;
                        }
                        error!("client connection is none, reconnect to backend server, id: {}", conn.id);
                        client_conn = Some(conn);
                    }

                    let mut is_err = client_conn.as_mut().unwrap().w.write_all(&data.raw_bytes).await.is_err();
                    if is_err {
                        drop(client_conn);
                        error!("write to backend server error1: {:?}", is_err);
                        // drop(ctx.client_conn.take().unwrap());
                        let mut conn = pool.acquire().await.map_err(|e| anyhow!("get connection from pool error: {:?}", e))?;
                        client_conn = Some(conn);
                        is_err = client_conn.as_mut().unwrap().w.write_all(&data.raw_bytes).await.is_err();
                        if is_err {
                            error!("write to backend server error2: {:?}", is_err);
                            bail!("write to backend server error2: {:?}", is_err);
                        }
                    };

                    if data.is_done {
                        break;
                    }
                }
                _ => {
                    error!("read from client error: None");
                    bail!("read from client error: None");
                }
            }
        }
        loop {
            let conn = client_conn.as_mut().unwrap();

            match conn.r.next().await {
                Some(Ok(it)) => {
                    ctx.framed.send(it.data).await?;
                    if it.is_done {
                        if ctx.cmd_type == CmdType::AUTH {
                            ctx.is_authed = !it.is_error
                        }
                        break;
                    }
                }
                _ => {
                    error!("read from backend server error: None");
                    let conn = client_conn.take().unwrap();
                    drop(conn);
                    bail!("read from backend server error: None");
                }
            }
        }
        Ok(())
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



