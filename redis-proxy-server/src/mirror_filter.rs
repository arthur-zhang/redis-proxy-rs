use std::ops::Range;

use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_stream::StreamExt;
use tokio_util::codec::{BytesCodec, FramedRead};
use redis_proxy_common::cmd::CmdType;

use redis_proxy_common::DecodedFrame;
use redis_proxy_filter::traits::{ContextValue, Filter, FilterStatus, TFilterContext};

use crate::path_trie::PathTrie;

pub struct MirrorFilter {
    mirror_address: String,
    path_trie: PathTrie,
}

const SHOULD_MIRROR: &'static str = "mirror_filter_should_mirror";
const DATA_TX: &'static str = "mirror_filter_data_tx";

impl MirrorFilter {
    pub fn new(mirror: &str) -> Self {
        let trie = PathTrie::new(vec!["foo:uc:*:token".into(), "foo:care:score:*".into()], r"[:]").unwrap();
        let mut ret = Self {
            mirror_address: mirror.to_string(),
            path_trie: trie,
        };
        return ret;
    }

    fn should_mirror(&self, eager_read_list: &Option<Vec<Range<usize>>>, raw_data: &[u8]) -> bool {
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
    async fn on_new_connection(&self, context: &mut TFilterContext) -> anyhow::Result<()> {
        let mut remote2_conn = TcpStream::connect(&self.mirror_address).await?;
        let (remote2_r, remote2_w) = remote2_conn.into_split();
        let (tx, mut rx): (Sender<bytes::Bytes>, Receiver<bytes::Bytes>) = tokio::sync::mpsc::channel(10000);
        let trie = PathTrie::new(vec!["foo:uc:*:token".into(), "foo:care:score:*".into()], r"[:]")?;
        let remote2_write_half = remote2_w;

        context.lock().unwrap().set_attr(DATA_TX, ContextValue::ChanSender(tx));

        tokio::spawn({
            let mut write_half = remote2_write_half;
            async move {
                while let Some(it) = rx.recv().await {
                    let bytes = it;
                    write_half.write_all(&bytes).await.unwrap();
                }
            }
        });
        tokio::spawn({
            async move {
                let mut reader = FramedRead::new(remote2_r, BytesCodec::new());
                while let Some(_) = reader.next().await {}
            }
        });
        Ok(())
    }

    async fn pre_handle(&self, context: &mut TFilterContext) -> anyhow::Result<()> {
        context.lock().unwrap().remote_attr(SHOULD_MIRROR);
        Ok(())
    }

    async fn post_handle(&self, _context: &mut TFilterContext, resp_error: bool) -> anyhow::Result<()> {
        Ok(())
    }

    async fn on_data(&self, data: &DecodedFrame, context: &mut TFilterContext) -> anyhow::Result<FilterStatus> {
        let mut should_mirror = {
            let context = context.lock().unwrap();
            context.get_attr_as_bool(SHOULD_MIRROR).unwrap_or(false)
        };
        let raw_data = data.raw_bytes.as_ref();
        let DecodedFrame {
            is_first_frame,
            cmd_type,
            eager_read_list,
            raw_bytes,
            is_eager,
            is_done
        } = &data;

        if *is_first_frame {
            // always mirror connection commands, like AUTH, PING, etc
            if cmd_type.is_connection_command() {
                should_mirror = true;
            } else if data.cmd_type.is_read_cmd() {
                should_mirror = false;
            } else {
                if *is_eager {
                    should_mirror = self.should_mirror(&eager_read_list, raw_data);
                }
            }
            context.lock().unwrap().set_attr(SHOULD_MIRROR, ContextValue::Bool(should_mirror));
        }

        if should_mirror {
            // todo, handle block
            let tx = {
                let context = context.lock().unwrap();
                let tx = context.get_attr_as_sender(DATA_TX).ok_or(anyhow::anyhow!("data_tx not found"))?;
                tx.clone()
            };

            tx.send(raw_bytes.clone()).await.unwrap();
        }
        Ok(FilterStatus::Continue)
    }
}