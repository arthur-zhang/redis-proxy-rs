use std::ops::Range;

use log::info;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio_stream::StreamExt;
use tokio_util::codec::{BytesCodec, FramedRead};

use redis_codec_core::resp_decoder::ResFramedData;
use redis_proxy_common::ReqFrameData;
use redis_proxy_filter::traits::{Value, Filter, FilterStatus, TFilterContext};

use crate::path_trie::PathTrie;

pub struct MirrorFilter {
    mirror_address: String,
    path_trie: PathTrie,
    queue_size: usize,
}

const SHOULD_MIRROR: &'static str = "mirror_filter_should_mirror";
const DATA_TX: &'static str = "mirror_filter_data_tx";

impl MirrorFilter {
    pub fn new(mirror: &str, mirror_patterns: &Vec<String>, split_regex: &str, queue_size: usize) -> anyhow::Result<Self> {
        let trie = PathTrie::new(mirror_patterns, split_regex)?;
        Ok(Self {
            mirror_address: mirror.to_string(),
            path_trie: trie,
            queue_size,
        })
    }

    fn should_mirror(&self, eager_read_list: &Option<Vec<Range<usize>>>, raw_data: &[u8]) -> bool {
        let key = eager_read_list.as_ref().map(|it| {
            it.first().map(|it| &raw_data[it.start..it.end])
        }).flatten();
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


impl Filter for MirrorFilter {
    fn on_new_connection(&self, context: &mut TFilterContext) -> anyhow::Result<()> {
        let (tx, mut rx) = tokio::sync::mpsc::channel(self.queue_size);
        context.lock().unwrap().set_attr(DATA_TX, Value::ChanSender(tx));

        tokio::spawn({
            let mirror_address = self.mirror_address.clone();
            async move {
                let (r, mut w) = TcpStream::connect(&mirror_address).await?.into_split();

                tokio::spawn({
                    async move {
                        while let Some(it) = rx.recv().await {
                            let bytes = it;
                            let _ = w.write_all(&bytes).await;
                        }
                        info!("mirror filter done: half1")
                    }
                });
                tokio::spawn({
                    async move {
                        let mut reader = FramedRead::new(r, BytesCodec::new());
                        while let Some(_) = reader.next().await {}
                        info!("mirror filter done: half2")
                    }
                });
                return Ok::<(), anyhow::Error>(());
            }
        });
        Ok(())
    }

    fn pre_handle(&self, context: &mut TFilterContext) -> anyhow::Result<()> {
        context.lock().unwrap().remote_attr(SHOULD_MIRROR);
        Ok(())
    }

    fn on_req_data(&self, context: &mut TFilterContext, data: &ReqFrameData) -> anyhow::Result<FilterStatus> {
        let mut should_mirror = {
            let context = context.lock().unwrap();
            context.get_attr_as_bool(SHOULD_MIRROR).unwrap_or(false)
        };
        let raw_data = data.raw_bytes.as_ref();
        let ReqFrameData {
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
            context.lock().unwrap().set_attr(SHOULD_MIRROR, Value::Bool(should_mirror));
        }

        if should_mirror {
            // todo, handle block
            let tx = {
                let context = context.lock().unwrap();
                let tx = context.get_attr_as_sender(DATA_TX).ok_or(anyhow::anyhow!("data_tx not found"))?;
                tx.clone()
            };

            let _ = tx.try_send(raw_bytes.clone());
        }
        Ok(FilterStatus::Continue)
    }
}