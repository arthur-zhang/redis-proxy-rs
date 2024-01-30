use std::sync::Arc;

use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_stream::StreamExt;
use tokio_util::codec::{BytesCodec, FramedRead};

use redis_codec_core::codec::{PartialDecoder, PartialResp};

use crate::path_trie::PathTrie;

pub struct ProxyServer {}

// client->proxy->backend
pub struct UpstreamPair {
    c2p_read_half: OwnedReadHalf,
    p2b_write_half: OwnedWriteHalf,
    remote2_read_half: OwnedReadHalf,
    remote2_write_half: OwnedWriteHalf,
    trie: Arc<PathTrie>,
}

// backend->proxy->client
pub struct DownstreamPair {
    p2c_write_half: OwnedWriteHalf,
    b2p_read_half: OwnedReadHalf,
}

impl DownstreamPair {
    pub async fn pipe(mut self) {
        let mut reader = FramedRead::new(self.b2p_read_half, BytesCodec::new());
        loop {
            while let Some(it) = reader.next().await {
                if let Ok(mut it) = it {
                    self.p2c_write_half.write_buf(&mut it).await.unwrap();
                }
            }
        }
    }
}

impl UpstreamPair {
    pub async fn pipe(mut self) {
        let (tx, mut rx): (UnboundedSender<Arc<bytes::Bytes>>, UnboundedReceiver<Arc<bytes::Bytes>>) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Some(it) = rx.recv().await {
                let bytes = it;
                self.remote2_write_half.write_all(&bytes).await.unwrap();
            }
        });
        tokio::spawn(async move {
            let mut reader = FramedRead::new(self.remote2_read_half, BytesCodec::new());
            while let Some(it) = reader.next().await {}
        });
        let mut reader = FramedRead::new(self.c2p_read_half, PartialDecoder::new());
        while let Some(it) = reader.next().await {
            match it {
                Ok(partial_resp) => {
                    match partial_resp {
                        PartialResp::Eager(resp) => {
                            reader.decoder_mut().set_key_match(false);
                            let d = reader.decoder();
                            let cmd = d.get_cmd();
                            let eager_read_list = d.get_eager_read_list();
                            let key = eager_read_list.first().map(|it| &resp[it.start..it.end]);
                            match key {
                                None => {
                                    self.p2b_write_half.write_all(&resp).await.unwrap();
                                }
                                Some(key) => {
                                    self.p2b_write_half.write_all(resp.as_ref()).await.unwrap();
                                    if self.trie.exists_path(key) {
                                        reader.decoder_mut().set_key_match(true);
                                        tx.send(Arc::new(resp)).unwrap();
                                    }
                                }
                            }
                        }
                        PartialResp::Lazy(resp) => {
                            self.p2b_write_half.write_all(resp.as_ref()).await.unwrap();
                            if reader.decoder().is_key_match() {
                                tx.send(Arc::new(resp)).unwrap();
                            }
                        }
                    }
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
        let trie = PathTrie::new(vec!["foo:uc:*:token".into(), "foo:care:score:*".into()], r"[:]")?;
        let trie = Arc::new(trie);

        let listener = TcpListener::bind(addr).await?;
        loop {
            let (mut c2p_conn, _) = listener.accept().await?;
            let mut p2b_conn = TcpStream::connect(&remote).await?;

            let mut remote2_conn = TcpStream::connect(&remote2).await?;
            let (remote2_r, remote2_w) = remote2_conn.into_split();

            let (p2b_r, mut p2b_w) = p2b_conn.into_split();
            let (c2p_r, mut c2p_w) = c2p_conn.into_split();

            let downstream_pair = DownstreamPair {
                p2c_write_half: c2p_w,
                b2p_read_half: p2b_r,
            };
            tokio::spawn({
                let trie = trie.clone();
                let upstream_pair = UpstreamPair {
                    c2p_read_half: c2p_r,
                    p2b_write_half: p2b_w,
                    remote2_read_half: remote2_r,
                    remote2_write_half: remote2_w,
                    trie,
                };
                async move {
                    upstream_pair.pipe().await;
                }
            });

            tokio::spawn(async move {
                downstream_pair.pipe().await;
            });
        }
    }
}