use std::os::macos::raw::stat;
use std::sync::Arc;
use std::time::Instant;

use anyhow::bail;
use bytes::Bytes;
use log::{debug, error, info};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender, unbounded_channel};
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tokio_util::codec::FramedRead;

use redis_codec_core::req_decoder::ReqPktDecoder;
use redis_codec_core::resp_decoder::RespPktDecoder;
use redis_proxy_common::DecodedFrame;
use redis_proxy_filter::traits::{Filter, FilterContext, FilterStatus};

use crate::blacklist_filter::BlackListFilter;
use crate::config::{Blacklist, Config, Mirror};
use crate::log_filter::LogFilter;
use crate::mirror_filter::MirrorFilter;

pub type FilterChainsPtr = Arc<FilterChains>;

pub struct ProxyServer {
    config: Arc<Config>,

    filter_chains: FilterChainsPtr,
}

impl ProxyServer {
    pub fn new(config: Config) -> Self {
        let config = Arc::new(config);
        // todo handle unwrap
        let filter_chain = Self::get_filters(config.clone()).unwrap();

        let filter_chains = FilterChains::new(filter_chain);
        ProxyServer { config, filter_chains: Arc::new(filter_chains) }
    }

    pub async fn handle_new_session(config: Arc<Config>, filter_chains: FilterChainsPtr, c2p_conn: TcpStream) -> anyhow::Result<()> {
        let mut filter_context = FilterContext::new();

        filter_chains.on_new_connection(&mut filter_context).await?;

        let (c2p_r, mut c2p_w) = c2p_conn.into_split();
        let mut req_pkt_reader = FramedRead::new(c2p_r, ReqPktDecoder::new());

        // connect to backend upstream server
        let (p2b_r, mut p2b_w) = TcpStream::connect(&config.upstream.address).await?.into_split();
        let mut res_pkt_reader = FramedRead::new(p2b_r, RespPktDecoder::new());

        loop {
            info!("in loop.........");
            // 1. read from client
            let mut status = FilterStatus::Continue;
            while let Some(Ok(data)) = req_pkt_reader.next().await {
                if data.frame_start {
                    filter_chains.pre_handle(&mut filter_context).await?;
                }
                status = filter_chains.on_data(&data, &mut filter_context).await?;
                if status == FilterStatus::StopIteration || status == FilterStatus::Block {
                    break;
                }

                // 2. write to backend upstream server
                p2b_w.write_all(&data.raw_bytes).await?;
                if data.is_done {
                    break;
                }
            }
            println!("status: {:?}", status);
            if status == FilterStatus::StopIteration {
                break;
            }
            if status == FilterStatus::Block {
                c2p_w.write_all(b"-ERR blocked\r\n").await?;
            } else {
                info!(">>>>>>>>>>>>.");
                // 3. read from backend upstream server
                while let Some(Ok(it)) = res_pkt_reader.next().await {
                    debug!("resp>>>> is_done: {} , data: {:?}", it.is_done, std::str::from_utf8(it.data.as_ref())
                                        .map(|it| truncate_str(it, 100)));

                    let bytes = it.data;
                    // 4. write to client
                    match c2p_w.write_all(&bytes).await {
                        Ok(_) => {}
                        Err(err) => {
                            error!("error: {:?}", err);
                            break;
                        }
                    };

                    if it.is_done {
                        filter_context.is_error = it.is_error;
                        break;
                    }
                }
            }

            filter_chains.post_handle(&mut filter_context).await?;
        }
        Ok(())
    }

    pub async fn start(self) -> anyhow::Result<()> {
        let listener = TcpListener::bind(&self.config.server.address).await.map_err(|e| {
            error!("bind error: {:?}", e);
            e
        })?;

        loop {
            tokio::spawn({
                let filter_chains = self.filter_chains.clone();
                let (c2p_conn, _) = listener.accept().await?;
                let config = self.config.clone();
                // one connection per task
                async move {
                    let _ = Self::handle_new_session(config, filter_chains, c2p_conn).await;
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


pub struct FilterChains {
    filters: Vec<Box<dyn Filter>>,
}

impl FilterChains {
    pub fn new(filters: Vec<Box<dyn Filter>>) -> Self {
        FilterChains {
            filters,
        }
    }
}


#[async_trait::async_trait]
impl Filter for FilterChains {
    async fn on_new_connection(&self, context: &mut FilterContext) -> anyhow::Result<()> {
        for filter in self.filters.iter() {
            filter.on_new_connection(context).await?;
        }
        Ok(())
    }

    async fn pre_handle(&self, context: &mut FilterContext) -> anyhow::Result<()> {
        for filter in self.filters.iter() {
            filter.pre_handle(context).await?;
        }
        Ok(())
    }

    async fn post_handle(&self, context: &mut FilterContext) -> anyhow::Result<()> {
        for filter in self.filters.iter() {
            filter.post_handle(context).await?;
        }
        Ok(())
    }

    async fn on_data(&self, data: &DecodedFrame, context: &mut FilterContext) -> anyhow::Result<FilterStatus> {
        for filter in self.filters.iter() {
            let status = filter.on_data(data, context).await?;
            if status != FilterStatus::Continue {
                return Ok(status);
            }
        }
        Ok(FilterStatus::Continue)
    }
}

