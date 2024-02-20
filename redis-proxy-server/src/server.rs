use std::time::Instant;

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
use redis_proxy_filter::traits::{Filter, FilterStatus};

use crate::blacklist_filter::BlackListFilter;
use crate::log_filter::LogFilter;
use crate::mirror_filter::MirrorFilter;

pub struct ProxyServer {}

impl ProxyServer {
    pub fn new() -> Self {
        ProxyServer {}
    }
    pub async fn start(&self) -> anyhow::Result<()> {
        let addr = "127.0.0.1:16379";
        let remote = "127.0.0.1:6379";
        let remote2 = "127.0.0.1:9001";

        let listener = TcpListener::bind(addr).await?;
        loop {
            let (mut c2p_conn, _) = listener.accept().await?;
            let _: JoinHandle<Result<(), anyhow::Error>> = tokio::spawn(async move {
                let (c2p_r, mut c2p_w) = c2p_conn.into_split();

                let (tx, rx): (Sender<Bytes>, Receiver<Bytes>) = mpsc::channel(100);

                let mirror_filter = MirrorFilter::new(remote2).await?;
                // let upstream_filter = UpstreamFilter::new(remote, tx.clone()).await?;
                let black_list_filter = BlackListFilter::new(vec!["a".into(), "b".into()], tx.clone());

                let log_filter = LogFilter::new();
                // let mut filter_chain: Vec<Box<dyn Filter>> = vec![
                //     Box::new(black_list_filter),
                //     Box::new(upstream_filter),
                //     Box::new(mirror_filter)];
                // filter_chain.clear();
                let mut filter_chain: Vec<Box<dyn Filter>> = vec![
                    // Box::new(black_list_filter),
                    Box::new(log_filter),
                    // Box::new(upstream_filter),
                    // Box::new(mirror_filter),
                ];

                tokio::spawn({
                    let read_half = c2p_r;
                    let mut p2b_conn = TcpStream::connect(&remote).await?;
                    let (p2b_r, mut p2b_w) = p2b_conn.into_split();
                    let mut resp_reader = FramedRead::new(p2b_r, RespPktDecoder::new());

                    async move {
                        let mut reader = FramedRead::new(read_half, ReqPktDecoder::new());
                        loop {
                            info!("in loop.........");

                            while let Some(Ok(data)) = reader.next().await {
                                if data.frame_start {
                                    for filter in filter_chain.iter_mut() {
                                        filter.pre_handle().await.unwrap();
                                    }
                                }
                                let mut status = FilterStatus::StopIteration;
                                for filter in filter_chain.iter_mut() {
                                    let res = filter.on_data(&data).await;
                                    match res {
                                        Ok(FilterStatus::Continue) => {
                                            status = FilterStatus::Continue;
                                        }
                                        Ok(FilterStatus::StopIteration) => {
                                            status = FilterStatus::StopIteration;
                                            break;
                                        }
                                        Err(e) => {
                                            status = FilterStatus::StopIteration;
                                            debug!("error: {:?}", e);
                                            break;
                                        }
                                    }
                                }
                                if status == FilterStatus::StopIteration {
                                    break;
                                }
                                p2b_w.write_all(&data.raw_bytes).await;
                                if data.is_done {
                                    break;
                                }
                            }

                            info!(">>>>>>>>>>>>.");
                            {
                                while let Some(Ok(it)) = resp_reader.next().await {
                                    debug!("resp>>>> is_done: {} , data: {:?}", it.is_done, std::str::from_utf8(it.data.as_ref())
                                        .map(|it| truncate_str(it, 100)));

                                    let bytes = it.data;
                                    match c2p_w.write_all(&bytes).await {
                                        Ok(_) => {}
                                        Err(err) => {
                                            error!("error: {:?}", err);
                                            break;
                                        }
                                    };
                                    if it.is_done {
                                        break;
                                    }
                                }
                            }


                            for filter in filter_chain.iter_mut() {
                                filter.post_handle().await.unwrap();
                            }
                        }
                    }
                });

                Ok(())
            });
        };
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



