use std::ops::Range;

use tokio::io::AsyncWriteExt;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_stream::StreamExt;
use tokio_util::codec::{BytesCodec, FramedRead};

use redis_proxy_common::cmd::CmdType;
use redis_proxy_common::DecodedFrame;
use redis_proxy_filter::traits::{Filter, FilterContext, FilterStatus};

use crate::path_trie::PathTrie;

pub struct MirrorFilter {
    mirror_address: String,
    path_trie: PathTrie,
    remote2_read_half: Option<OwnedReadHalf>,
    remote2_write_half: Option<OwnedWriteHalf>,

    tx: Option< UnboundedSender<bytes::Bytes>>,
    rx: Option<UnboundedReceiver<bytes::Bytes>>,
    should_mirror: bool,
}

impl MirrorFilter {
    pub fn new(mirror: &str) -> Self {
        let trie = PathTrie::new(vec!["foo:uc:*:token".into(), "foo:care:score:*".into()], r"[:]").unwrap();
        let mut ret = Self {
            mirror_address: mirror.to_string(),
            path_trie: trie,
            remote2_read_half: None,
            remote2_write_half: None,
            tx:None,
            rx: None,
            should_mirror: false,
        };
        return ret;
    }

    fn should_mirror(&self, cmd: &Option<CmdType>, eager_read_list: &Option<Vec<Range<usize>>>, raw_data: &[u8]) -> bool {
        let key = eager_read_list.as_ref().map(|it| it.first().map(|it| &raw_data[it.start..it.end])).flatten();
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
    async fn init(&mut self, context: &mut FilterContext) -> anyhow::Result<()> {
        let mut remote2_conn = TcpStream::connect(&self.mirror_address).await?;
        let (remote2_r, remote2_w) = remote2_conn.into_split();
        let (tx, mut rx): (UnboundedSender<bytes::Bytes>, UnboundedReceiver<bytes::Bytes>) = tokio::sync::mpsc::unbounded_channel();
        let trie = PathTrie::new(vec!["foo:uc:*:token".into(), "foo:care:score:*".into()], r"[:]")?;
        self.remote2_read_half = Some(remote2_r);
        self.remote2_write_half = Some(remote2_w);
        self.tx = Some(tx);
        self.rx = Some(rx);


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

    async fn pre_handle(&mut self, context: &mut FilterContext) -> anyhow::Result<()> {
        Ok(())
    }

    async fn post_handle(&mut self, context: &mut FilterContext) -> anyhow::Result<()> {
        Ok(())
    }


    async fn on_data(&mut self, data: &DecodedFrame, context: &mut FilterContext) -> anyhow::Result<FilterStatus> {
        let raw_data = data.raw_bytes.as_ref();
        let DecodedFrame { frame_start, cmd_type, eager_read_list, raw_bytes, is_eager, is_done } = &data;
        if data.is_eager {
            self.should_mirror = self.should_mirror(cmd_type, &eager_read_list, raw_data);
        }

        if self.should_mirror {
            self.tx.as_ref().unwrap().send(raw_bytes.clone()).unwrap();
        }

        if data.is_done {
            self.should_mirror = false;
        }
        Ok(FilterStatus::Continue)
    }
}