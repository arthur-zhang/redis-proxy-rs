use std::os::macos::raw::stat;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::bail;
use bytes::Bytes;
use log::{debug, error, info};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::{mpsc, oneshot};
use tokio::sync::mpsc::{Receiver, Sender, unbounded_channel};
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tokio_util::codec::FramedRead;

use redis_codec_core::req_decoder::ReqPktDecoder;
use redis_codec_core::resp_decoder::{FramedData, RespPktDecoder};
use redis_proxy_common::cmd::CmdType;
use redis_proxy_common::DecodedFrame;
use redis_proxy_filter::traits::{Filter, FilterContext, FilterStatus, TFilterContext};

use crate::blacklist_filter::BlackListFilter;
use crate::config::{Blacklist, Config, Mirror, TConfig};
use crate::filter_chain::{FilterChain, TFilterChain};
use crate::log_filter::LogFilter;
use crate::mirror_filter::MirrorFilter;

pub struct ProxyServer {
    config: TConfig,
    filter_chain: TFilterChain,
}

impl ProxyServer {
    pub fn new(config: Config) -> Self {
        let config = Arc::new(config);
        // todo handle unwrap
        let filter_chain = Self::get_filters(config.clone()).unwrap();

        let filter_chains = FilterChain::new(filter_chain);
        ProxyServer { config, filter_chain: Arc::new(filter_chains) }
    }

    pub async fn start(self) -> anyhow::Result<()> {
        let listener = TcpListener::bind(&self.config.server.address).await.map_err(|e| {
            error!("bind error: {:?}", e);
            e
        })?;

        loop {
            tokio::spawn({
                let filter_chains = self.filter_chain.clone();
                let (c2p_conn, _) = listener.accept().await?;
                let config = self.config.clone();
                // one connection per task
                let session = Session { filter_chains, c2p_conn, config };
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
                            Box::new(BlackListFilter::new(blacklist.block_patterns))
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
                            Box::new(MirrorFilter::new(mirror.address.as_str()))
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
pub struct SessionHalfC2B {
    filter_chain: TFilterChain,
    filter_context: TFilterContext,
    req_pkt_reader: FramedRead<OwnedReadHalf, ReqPktDecoder>,
    p2b_w: OwnedWriteHalf,
    c2p_shutdown_tx: oneshot::Sender<()>,
}


impl SessionHalfC2B {
    pub fn new(filter_chain: TFilterChain, filter_context: TFilterContext, c2p_r: OwnedReadHalf, p2b_w: OwnedWriteHalf, c2p_shutdown_tx: oneshot::Sender<()>) -> Self {
        let mut req_pkt_reader = FramedRead::new(c2p_r, ReqPktDecoder::new());
        SessionHalfC2B {
            filter_chain,
            filter_context,
            req_pkt_reader,
            p2b_w,
            c2p_shutdown_tx,
        }
    }
    pub async fn handle(mut self) -> anyhow::Result<()> {
        let mut status = FilterStatus::Continue;

        while let Some(Ok(data)) = self.req_pkt_reader.next().await {
            if data.is_first_frame {
                let cmd_type = data.cmd_type.clone();
                self.filter_context.lock().unwrap().set_attr_cmd_type(cmd_type);
                self.filter_chain.pre_handle(&mut self.filter_context).await?;
            }
            status = self.filter_chain.on_data(&data, &mut self.filter_context).await?;
            if status == FilterStatus::StopIteration || status == FilterStatus::Block {
                break;
            }

            // 2. write to backend upstream server
            self.p2b_w.write_all(&data.raw_bytes).await?;
        }
        let _ = self.c2p_shutdown_tx.send(());
        Ok(())
    }
}

// b->p->c session half
pub struct SessionHalfB2C {
    filter_chain: TFilterChain,
    filter_context: TFilterContext,
    res_pkt_reader: FramedRead<OwnedReadHalf, RespPktDecoder>,
    c2p_w: OwnedWriteHalf,
    p2b_shutdown_tx: oneshot::Sender<()>,
}

impl SessionHalfB2C {
    pub fn new(filter_chain: TFilterChain, filter_context: TFilterContext, p2b_r: OwnedReadHalf, c2p_w: OwnedWriteHalf, p2b_shutdown_tx: oneshot::Sender<()>) -> Self {
        let mut res_pkt_reader = FramedRead::new(p2b_r, RespPktDecoder::new());

        SessionHalfB2C {
            filter_chain,
            filter_context,
            res_pkt_reader,
            c2p_w,
            p2b_shutdown_tx,
        }
    }
    pub async fn handle(mut self) -> anyhow::Result<()> {
        // 3. read from backend upstream server
        while let Some(Ok(it)) = self.res_pkt_reader.next().await {
            debug!("resp>>>> is_done: {} , data: {:?}", it.is_done, std::str::from_utf8(it.data.as_ref())
                                        .map(|it| truncate_str(it, 100)));

            let bytes = it.data;
            // 4. write to client
            match self.c2p_w.write_all(&bytes).await {
                Ok(_) => {}
                Err(err) => {
                    error!("error: {:?}", err);
                    break;
                }
            };

            if it.is_done {
                self.filter_context.lock().unwrap().set_attr_res_is_error(it.is_error);
                self.filter_chain.post_handle(&mut self.filter_context).await?;
            }
        }
        let _ = self.p2b_shutdown_tx.send(());
        Ok(())
    }
}

pub struct Session {
    filter_chains: TFilterChain,
    c2p_conn: TcpStream,
    config: Arc<Config>,
}


impl Session {
    // 1. read from client
    // 2. write to backend upstream server
    // 3. read from backend upstream server
    // 4. write to client
    pub async fn handle(mut self) -> anyhow::Result<()> {
        let filter_context = FilterContext::new();
        let mut filter_context = Arc::new(Mutex::new(filter_context));
        self.filter_chains.on_new_connection(&mut filter_context).await?;


        // c->p connection
        let (c2p_r, mut c2p_w) = self.c2p_conn.into_split();
        let (c2p_shutdown_tx, c2p_shutdown_rx) = tokio::sync::oneshot::channel();


        // p->b connection
        // connect to backend upstream server
        let (p2b_r, mut p2b_w) = TcpStream::connect(&self.config.upstream.address).await?.into_split();
        let (p2b_shutdown_tx, p2b_shutdown_rx) = tokio::sync::oneshot::channel();

        // c->p->b
        let t1 = tokio::spawn({
            let filter_chain = self.filter_chains.clone();
            let filter_context = filter_context.clone();
            let session_half = SessionHalfC2B::new(filter_chain, filter_context, c2p_r, p2b_w, c2p_shutdown_tx);
            async move {
                info!("....................");
                tokio::select! {
                    res = session_half.handle() => {
                        if let Err(err) = res {
                            error!("session half error: c->p->b, {:?}", err);
                        }
                    }
                    _ = p2b_shutdown_rx => {
                        info!("p2b shutdown signal receive");
                    }
                }
                info!("session half done: c->p->b")
            }
        });

        let t2 = tokio::spawn({
            let filter_context = filter_context.clone();
            let filter_chain = self.filter_chains.clone();
            let session = SessionHalfB2C::new(
                filter_chain,
                filter_context,
                p2b_r,
                c2p_w,
                p2b_shutdown_tx,
            );
            async move {
                tokio::select! {
                    res = session.handle() => {
                        if let Err(err) = res {
                            error!("session half error: b->p->c, {:?}", err);
                        }
                    }
                    _ = c2p_shutdown_rx => {
                        info!("c2p shutdown signal receive");
                    }
                }
                // let _ = session.handle().await;
                info!("session half done: b->p->c")
            }
        });
        tokio::join!(t1, t2);
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



