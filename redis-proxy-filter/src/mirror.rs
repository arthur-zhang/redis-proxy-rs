use std::ops::Range;

use tokio::io::AsyncWriteExt;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_stream::StreamExt;
use tokio_util::codec::{BytesCodec, FramedRead};

use redis_codec_core::cmd::CmdType;
use redis_codec_core::req_decoder::KeyAwareDecoder;
use redis_proxy_common::DecodedFrame;

use crate::path_trie::PathTrie;
use crate::traits::{Filter, FilterStatus};

pub struct MirrorFilter {
    path_trie: PathTrie,
    remote2_read_half: Option<OwnedReadHalf>,
    remote2_write_half: Option<OwnedWriteHalf>,

    tx: UnboundedSender<bytes::Bytes>,
    rx: Option<UnboundedReceiver<bytes::Bytes>>,
    should_mirror: bool,
}

impl MirrorFilter {
    pub async fn new(mirror: &str) -> anyhow::Result<Self> {
        let mut remote2_conn = TcpStream::connect(&mirror).await?;
        let (remote2_r, remote2_w) = remote2_conn.into_split();
        let (tx, mut rx): (UnboundedSender<bytes::Bytes>, UnboundedReceiver<bytes::Bytes>) = tokio::sync::mpsc::unbounded_channel();
        let trie = PathTrie::new(vec!["foo:uc:*:token".into(), "foo:care:score:*".into()], r"[:]")?;
        // let trie = Arc::new(trie);
        let mut ret = Self {
            path_trie: trie,
            remote2_read_half: Some(remote2_r),
            remote2_write_half: Some(remote2_w),
            tx,
            rx: Some(rx),
            should_mirror: false,
        };
        let _ = ret.init().await?;
        return Ok(ret);
    }

    fn should_mirror(&self, cmd: &CmdType, eager_read_list: &Vec<Range<usize>>, raw_data: &[u8]) -> bool {
        let key = eager_read_list.first().map(|it| &raw_data[it.start..it.end]);
        return match key {
            None => {
                false
            }
            Some(key) => {
                self.path_trie.exists_path(key)
            }
        };
    }
}


#[async_trait::async_trait]
impl Filter for MirrorFilter {
    async fn init(&mut self) -> anyhow::Result<()> {
        tokio::spawn({
            let mut rx = self.rx.take().unwrap();
            let mut write_half = self.remote2_write_half.take().unwrap();
            async move {
                while let Some(it) = rx.recv().await {
                    let bytes = it;
                    write_half.write_all(&bytes).await.unwrap();
                }
            }
        });
        tokio::spawn({
            let read_half = self.remote2_read_half.take().unwrap();
            async move {
                let mut reader = FramedRead::new(read_half, BytesCodec::new());
                while let Some(_) = reader.next().await {}
            }
        });

        Ok(())
    }


    async fn on_data(&mut self, decoder: &KeyAwareDecoder, data: &DecodedFrame) -> anyhow::Result<FilterStatus> {
        let raw_bytes = &data.raw_bytes;
        let raw_data = data.raw_bytes.as_ref();

        if data.is_eager {
            self.should_mirror = self.should_mirror(decoder.cmd_type(), &decoder.eager_read_list(), raw_data);
        }

        if self.should_mirror {
            self.tx.send(raw_bytes.clone()).unwrap();
        }

        if data.is_done {
            self.should_mirror = false;
        }
        Ok(FilterStatus::Continue)
    }
}