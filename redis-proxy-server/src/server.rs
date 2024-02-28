use std::fmt::format;
use std::os::macos::raw::stat;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::{anyhow, bail};
use bytes::Bytes;
use futures::SinkExt;
use log::{debug, error, info};
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
use crate::conn_pool::{ClientConnManager, Conn, Pool, SessionAttr};
use crate::filter_chain::{FilterChain, TFilterChain};
use crate::log_filter::LogFilter;
use crate::mirror_filter::MirrorFilter;
use crate::tiny_client::TinyClient;
use crate::traits::{Filter, FilterContext, FilterStatus, TFilterContext};

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

        let manager = ClientConnManager::new("127.0.0.1:6379".parse().unwrap());

        let pool = bb8::Pool::builder()
            .max_size(15)
            .build(manager)
            .await?;

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

// c->p->b session half
pub struct UpstreamSessionHalf {
    filter_chain: TFilterChain,
    filter_context: TFilterContext,
    req_pkt_reader: FramedRead<OwnedReadHalf, ReqPktDecoder>,
    c2p_shutdown_tx: oneshot::Sender<()>,
}


impl UpstreamSessionHalf {
    pub fn new(filter_chain: TFilterChain, filter_context: TFilterContext, c2p_r: OwnedReadHalf, c2p_shutdown_tx: oneshot::Sender<()>) -> Self {
        let mut req_pkt_reader = FramedRead::new(c2p_r, ReqPktDecoder::new());
        UpstreamSessionHalf {
            filter_chain,
            filter_context,
            req_pkt_reader,
            c2p_shutdown_tx,
        }
    }
    pub async fn handle(mut self) -> anyhow::Result<()> {
        let mut status = FilterStatus::Continue;

        while let Some(Ok(data)) = self.req_pkt_reader.next().await {
            if data.is_first_frame {
                let cmd_type = data.cmd_type.clone();
                self.filter_chain.pre_handle(&mut self.filter_context)?;
                self.filter_context.lock().unwrap().set_attr_cmd_type(cmd_type);
            }
            status = self.filter_chain.on_req_data(&mut self.filter_context, &data).await?;
        }
        let _ = self.c2p_shutdown_tx.send(());
        Ok(())
    }
}

// b->p->c session half
pub struct DownstreamSessionHalf {
    filter_chain: TFilterChain,
    filter_context: TFilterContext,
    res_pkt_reader: FramedRead<OwnedReadHalf, RespPktDecoder>,
    p2b_shutdown_tx: oneshot::Sender<()>,
}

impl DownstreamSessionHalf {
    pub fn new(filter_chain: TFilterChain, filter_context: TFilterContext, p2b_r: OwnedReadHalf, p2b_shutdown_tx: oneshot::Sender<()>) -> Self {
        let mut res_pkt_reader = FramedRead::new(p2b_r, RespPktDecoder::new());

        DownstreamSessionHalf {
            filter_chain,
            filter_context,
            res_pkt_reader,
            p2b_shutdown_tx,
        }
    }
    pub async fn handle(mut self) -> anyhow::Result<()> {
        // 3. read from backend upstream server
        while let Some(Ok(it)) = self.res_pkt_reader.next().await {
            debug!("resp>>>> is_done: {} , data: {:?}", it.is_done, std::str::from_utf8(it.data.as_ref())
                                        .map(|it| truncate_str(it, 100)));

            self.filter_chain.on_res_data(&mut self.filter_context, &it).await?;
            if it.is_done {
                self.filter_context.lock().unwrap().set_attr_res_is_error(it.is_error);
                self.filter_chain.post_handle(&mut self.filter_context)?;
            }
        }
        let _ = self.p2b_shutdown_tx.send(());
        Ok(())
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

        // c->p connection
        // let (c2p_r, mut p2c_w) = self.c2p_conn.into_split();
        // let (c2p_shutdown_tx, c2p_shutdown_rx) = oneshot::channel();

        // p->b connection
        // connect to backend upstream server


        // let mut conn = self.pool.get().await.map_err(|e| anyhow!("get connection from pool error: {:?}", e))?;


        // let conn = conn.as_mut();

        // let (b2p_r, mut p2b_w) = TcpStream::connect(&self.config.upstream.address).await?.into_split();
        // let (p2b_shutdown_tx, p2b_shutdown_rx) = oneshot::channel();
        // let io = TcpStream::connect(&self.config.upstream.address).await?;

        let c2p_conn = self.c2p_conn.take().unwrap();
        let mut framed = Framed::with_capacity(c2p_conn, ReqPktDecoder::new(), 16384);
        // let mut filter_context = Arc::new(Mutex::new(FilterContext{
        //     attrs: Default::default(),
        //     framed,
        //     pool: self.pool.clone(),
        //     client_conn: None,
        // }));
        // self.filter_chains.on_new_connection(&mut filter_context)?;
        // let mut req_pkt_reader = FramedRead::new(c2p_r, ReqPktDecoder::new());

        let filter_context = FilterContext { db: 0, attrs: Default::default(), framed, pool: self.pool.clone() };

        self.run(filter_context).await?;
        // let mut quit = false;
        // while !quit {
        //     loop {
        //         match framed.next().await {
        //             Some(Ok(data)) => {
        //                 if data.is_first_frame {
        //                     if data.cmd_type == CmdType::SELECT {
        //                         println!("eager>>>>>>: {:?}", data.eager_read_list);
        //                         if let Some(db) = data.eager_read_list.as_ref().and_then(|it| it.first()) {
        //                             let db = &data.raw_bytes[db.start..db.end];
        //                             let db = std::str::from_utf8(db).unwrap().parse::<u8>().unwrap();
        //                             println!("db: {}", db);
        //                             conn.db = db;
        //                         }
        //                         // continue;
        //                     }
        //                 }
        //
        //
        //                 conn.w.write_all(&data.raw_bytes).await?;
        //                 if data.is_done {
        //                     break;
        //                 }
        //             }
        //             _ => {
        //                 quit = true;
        //                 break;
        //             }
        //         }
        //     }
        //     if quit {
        //         break;
        //     }
        //
        //     // 3. read from backend upstream server
        //     loop {
        //         match conn.r.next().await {
        //             Some(Ok(it)) => {
        //                 p2c_w.write_all(&it.data).await?;
        //                 if it.is_done {
        //                     break;
        //                 }
        //             }
        //             _ => {
        //                 quit = true;
        //                 break;
        //             }
        //         }
        //     }
        // }

        Ok(())


        // // c->p->b
        // let t1 = tokio::spawn({
        //     let filter_chain = self.filter_chains.clone();
        //     let filter_context = filter_context.clone();
        //     let session_half = UpstreamSessionHalf::new(filter_chain, filter_context, c2p_r, c2p_shutdown_tx);
        //     async move {
        //         info!("....................");
        //         tokio::select! {
        //             res = session_half.handle() => {
        //                 if let Err(err) = res {
        //                     error!("session half error: c->p->b, {:?}", err);
        //                 }
        //             }
        //             _ = p2b_shutdown_rx => {
        //                 info!("p2b shutdown signal receive");
        //             }
        //         }
        //         info!("session half done: c->p->b")
        //     }
        // });
        //
        // let t2 = tokio::spawn({
        //     let filter_context = filter_context.clone();
        //     let filter_chain = self.filter_chains.clone();
        //     let session = DownstreamSessionHalf::new(
        //         filter_chain,
        //         filter_context,
        //         b2p_r,
        //         // p2c_w,
        //         p2b_shutdown_tx,
        //     );
        //     async move {
        //         tokio::select! {
        //             res = session.handle() => {
        //                 if let Err(err) = res {
        //                     error!("session half error: b->p->c, {:?}", err);
        //                 }
        //             }
        //             _ = c2p_shutdown_rx => {
        //                 info!("c2p shutdown signal receive");
        //             }
        //         }
        //         // let _ = session.handle().await;
        //         info!("session half done: b->p->c")
        //     }
        // });
        // tokio::join!(t1, t2);
        // Ok(())
    }

    async fn run(&mut self, mut ctx: FilterContext) -> anyhow::Result<()> {
        let pool = ctx.pool.clone();
        debug!("run session");
        let status = FilterStatus::Continue;
        let mut quit = false;
        let mut client_conn = None;
        while !quit {
            client_conn = None;
            // ctx.client_conn = None;
            loop {
                match ctx.framed.next().await {
                    Some(Ok(data)) => {
                        if data.is_first_frame {
                            if data.cmd_type == CmdType::SELECT {
                                RedisService::on_select_db(&mut ctx, &data).await?;
                            }
                        }
                        if status == FilterStatus::Block {
                            ctx.framed.send(Bytes::from_static(b"-ERR blocked\r\n")).await?;
                            continue;
                        }
                        if client_conn.is_none() {
                            let mut conn = pool.get().await.map_err(|e| anyhow!("get connection from pool error: {:?}", e))?;
                            if conn.session_attr.db != ctx.db {
                                info!("rebuild session from {} to {}", conn.session_attr.db, ctx.db);
                                RedisService::rebuild_session(&mut conn, SessionAttr { db: ctx.db }).await?;
                            }
                            error!("client connection is none, reconnect to backend server, id: {}", conn.id);
                            client_conn = Some(conn);
                        }

                        let mut is_err = client_conn.as_mut().unwrap().w.write_all(&data.raw_bytes).await.is_err();
                        if is_err {
                            drop(client_conn);
                            error!("write to backend server error1: {:?}", is_err);
                            // drop(ctx.client_conn.take().unwrap());
                            let mut conn = pool.get().await.map_err(|e| anyhow!("get connection from pool error: {:?}", e))?;
                            client_conn = Some(conn);
                            is_err = client_conn.as_mut().unwrap().w.write_all(&data.raw_bytes).await.is_err();
                            if is_err {
                                error!("write to backend server error2: {:?}", is_err);
                                quit = true;
                                break;
                            }
                        };

                        if data.is_done {
                            break;
                        }
                    }
                    _ => {
                        error!("read from client error: None");
                        quit = true;
                        break;
                    }
                }
            }
            println!(".2.....................");
            if quit {
                break;
            }
            loop {
                println!(".1.....................");
                let conn = client_conn.as_mut().unwrap();

                match conn.r.next().await {
                    Some(Ok(it)) => {
                        ctx.framed.send(it.data).await?;
                        if it.is_done {
                            break;
                        }
                    }
                    Some(Err(err)) => {
                        error!("read from backend server error: {:?}", err);
                        quit = true;
                        break;
                    }
                    None => {
                        error!("read from backend server error: None");
                        quit = true;
                        let conn = client_conn.take().unwrap();
                        drop(conn);
                        break;
                    }
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
    pub async fn rebuild_session(conn: &mut Conn, session_attr: SessionAttr) -> anyhow::Result<()> {
        let db_index = format!("{}", session_attr.db);
        let cmd = format!("*2\r\n$6\r\nselect\r\n${}\r\n{}\r\n", db_index.len(), db_index);
        let ok = TinyClient::query(conn, cmd.as_bytes()).await?;
        conn.session_attr.db = session_attr.db;
        if !ok {
            return Err(anyhow!("rebuild session failed"));
        }
        Ok(())
    }
    pub async fn on_select_db(ctx: &mut FilterContext, data: &ReqFrameData) -> anyhow::Result<()> {
        if let Some(db) = data.eager_read_list.as_ref().and_then(|it| it.first()) {
            let db = &data.raw_bytes[db.start..db.end];
            let db = std::str::from_utf8(db).unwrap_or("").parse::<u64>().unwrap_or(0);
            ctx.db = db;
        }
        Ok(())
    }
}



