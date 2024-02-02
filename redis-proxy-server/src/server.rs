use bytes::Bytes;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tokio_util::codec::FramedRead;

use redis_codec_core::req_decoder::ReqPktDecoder;
use redis_codec_core::resp_decoder::RespPktDecoder;
use redis_proxy_common::DecodedFrame;
use redis_proxy_filter::mirror::MirrorFilter;
use redis_proxy_filter::traits::{Filter, FilterStatus};

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
                let upstream_filter = UpstreamFilter::new(remote, tx.clone()).await?;
                let black_list_filter = BlackListFilter::new(vec!["a".into(), "b".into()], tx.clone());

                let mut filter_chain: Vec<Box<dyn Filter>> = vec![
                    Box::new(black_list_filter),
                    Box::new(upstream_filter), Box::new(mirror_filter)];

                tokio::spawn({
                    let mut rx = rx;
                    let mut write_half = c2p_w;
                    async move {
                        while let Some(it) = rx.recv().await {
                            let bytes = it;
                            write_half.write_all(&bytes).await.unwrap();
                        }
                    }
                });
                tokio::spawn({
                    let read_half = c2p_r;
                    async move {
                        let mut reader = FramedRead::new(read_half, ReqPktDecoder::new());
                        while let Some(Ok(data)) = reader.next().await {
                            for filter in filter_chain.iter_mut() {
                                let res = filter.on_data(&data).await;
                                match res {
                                    Ok(FilterStatus::Continue) => {}
                                    Ok(FilterStatus::StopIteration) => {
                                        break;
                                    }
                                    Err(e) => {
                                        println!("error: {:?}", e);
                                        break;
                                    }
                                }
                            }
                        }
                    }
                });

                Ok(())
            });
        };
    }
}

pub struct UpstreamFilter {
    // b2p_read_half: OwnedReadHalf,
    p2b_write_half: OwnedWriteHalf,
}

impl UpstreamFilter {
    async fn new(remote: &str, tx: Sender<Bytes>) -> anyhow::Result<Self> {
        let mut p2b_conn = TcpStream::connect(&remote).await?;

        let (p2b_r, mut p2b_w) = p2b_conn.into_split();


        tokio::spawn({
            let read_half = p2b_r;
            async move {
                let mut reader = FramedRead::new(read_half, RespPktDecoder::new());
                while let Some(Ok(it)) = reader.next().await {
                    tx.send(it.data).await.unwrap();
                }
            }
        });

        Ok(UpstreamFilter {
            // b2p_read_half: p2b_r,
            p2b_write_half: p2b_w,
        })
    }
}

#[async_trait::async_trait]
impl Filter for UpstreamFilter {
    async fn init(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn on_data(&mut self, data: &DecodedFrame) -> anyhow::Result<FilterStatus> {
        self.p2b_write_half.write_all(&data.raw_bytes).await?;
        Ok(FilterStatus::Continue)
    }
}

pub struct BlackListFilter {
    blocked: bool,
    black_list: Vec<String>,
    tx: Sender<Bytes>,
}

impl BlackListFilter {
    pub fn new(black_list: Vec<String>, tx: Sender<Bytes>) -> Self {
        BlackListFilter { blocked: false, black_list, tx }
    }
}

#[async_trait::async_trait]
impl Filter for BlackListFilter {
    async fn init(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn on_data(&mut self, data: &DecodedFrame) -> anyhow::Result<FilterStatus> {
        if self.blocked && !data.is_done {
            return Ok(FilterStatus::StopIteration);
        }

        let DecodedFrame { cmd_type, eager_read_list, raw_bytes, is_eager, is_done } = &data;
        if *is_eager {
            let key = eager_read_list.as_ref().and_then(|it| it.first().map(|it| &raw_bytes[it.start..it.end]));
            if let Some(key) = key {
                if self.black_list.contains(&std::str::from_utf8(key).unwrap().to_string()) {
                    self.blocked = true;
                    if *is_done {
                        self.blocked = false;
                    }
                    self.tx.send(Bytes::from_static(b"-ERR blocked\r\n")).await.unwrap();
                    return Ok(FilterStatus::StopIteration);
                }
            }
        }
        if data.is_done {
            self.blocked = false;
        }
        Ok(FilterStatus::Continue)
    }
}