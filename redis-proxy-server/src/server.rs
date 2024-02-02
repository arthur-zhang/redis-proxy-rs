use log::debug;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio_stream::StreamExt;
use tokio_util::codec::FramedRead;

use redis_codec_core::req_decoder::KeyAwareDecoder;
use redis_codec_core::resp_decoder::MyDecoder;
use redis_proxy_common::DecodedFrame;
use redis_proxy_filter::mirror::MirrorFilter;
use redis_proxy_filter::traits::Filter;

pub struct ProxyServer {}

// client->proxy->backend
pub struct UpstreamPair {
    c2p_read_half: OwnedReadHalf,
    p2b_write_half: OwnedWriteHalf,
    // remote2_read_half: OwnedReadHalf,
    // remote2_write_half: OwnedWriteHalf,
    // trie: Arc<PathTrie>,
    mirror_filter: MirrorFilter,
}

// backend->proxy->client
pub struct DownstreamPair {
    p2c_write_half: OwnedWriteHalf,
    b2p_read_half: OwnedReadHalf,
}

impl DownstreamPair {
    pub async fn pipe(mut self) {
        let mut reader = FramedRead::new(self.b2p_read_half, MyDecoder::new());
        while let Some(it) = reader.next().await {
            if let Ok(mut it) = it {
                // println!("is_done: {:?}", it.is_done);
                self.p2c_write_half.write_all(&it.data).await.unwrap();
            } else {
                break;
            }
        }
    }
}

impl UpstreamPair {
    pub async fn pipe(mut self) {
        let mut reader = FramedRead::new(self.c2p_read_half, KeyAwareDecoder::new());
        while let Some(it) = reader.next().await {
            match it {
                Ok(it) => {
                    self.mirror_filter.on_data(&reader.decoder(), &it).await.unwrap();
                    let DecodedFrame { raw_bytes, is_eager, is_done } = &it;
                    let raw_data = raw_bytes.as_ref();
                    self.p2b_write_half.write_all(raw_data).await.unwrap();

                    debug!("is_eager: {}, is_done: {}, raw_bytes:{:?}", is_eager, is_done, std::str::from_utf8(raw_bytes.as_ref()));
                }
                Err(_) => {
                    break;
                }
            }
        }
    }
}

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
            tokio::spawn(async move {
                let mut p2b_conn = TcpStream::connect(&remote).await.unwrap();

                let mirror_filter = MirrorFilter::new(remote2).await.unwrap();

                let (p2b_r, mut p2b_w) = p2b_conn.into_split();
                let (c2p_r, mut c2p_w) = c2p_conn.into_split();

                let downstream_pair = DownstreamPair {
                    p2c_write_half: c2p_w,
                    b2p_read_half: p2b_r,
                };
                let upstream_pair = UpstreamPair {
                    c2p_read_half: c2p_r,
                    p2b_write_half: p2b_w,
                    mirror_filter,
                };
                tokio::spawn(async move {
                    upstream_pair.pipe().await;
                });
                tokio::spawn(async move {
                    downstream_pair.pipe().await;
                });
            });
        }
    }
}