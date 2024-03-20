use anyhow::bail;
use async_trait::async_trait;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_stream::StreamExt;

use redis_proxy::config::GenericUpstream;
use redis_proxy::double_writer::DoubleWriter;
use redis_proxy::etcd_client::EtcdClient;
use redis_proxy::proxy::Proxy;
use redis_proxy::session::Session;
use redis_proxy_common::command::utils::{is_connection_cmd, is_write_cmd};
use redis_proxy_common::ReqFrameData;

use crate::filter_trait::{FilterContext, Value};

pub struct MirrorFilter {
    double_writer: DoubleWriter,
}

const DATA_TX: &'static str = "mirror_filter_data_tx";
const SHOULD_MIRROR: &'static str = "mirror_filter_should_mirror";

impl MirrorFilter {
    pub async fn new(splitter: char, mirror_conf: &GenericUpstream, etcd_client: Option<EtcdClient>) -> anyhow::Result<Self> {
        let double_writer = DoubleWriter::new(
            splitter, String::from("mirror"), mirror_conf, etcd_client).await?;
        Ok(Self { double_writer })
    }
}

#[async_trait]
impl Proxy for MirrorFilter {
    type CTX = FilterContext;

    async fn proxy_upstream_filter(&self, session: &mut Session, ctx: &mut Self::CTX) -> anyhow::Result<bool> {
        let data = session.header_frame.as_ref().unwrap();
        let should_mirror = if is_connection_cmd(&data.cmd_type) {
            true
        } else if is_write_cmd(&data.cmd_type) {
            self.double_writer.should_double_write(data)
        } else {
            false
        };
        if !should_mirror {
            return Ok(false);
        }
        let mut conn = self.double_writer.acquire_conn().await?;
        conn.init_from_session(session.cmd_type(), session.is_authed, &session.password, session.db).await?;

        let (tx, mut rx): (Sender<bytes::Bytes>, Receiver<bytes::Bytes>) = tokio::sync::mpsc::channel(100);
        ctx.attrs.insert(DATA_TX.to_string(), Value::ChanSender(tx));

        ctx.set_attr(SHOULD_MIRROR, Value::Bool(should_mirror));
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    res = conn.r.next() => {
                        match res {
                            Some(Ok(it)) => {
                                // continue
                                if it.is_done {
                                    return Ok::<_, anyhow::Error>(());
                                }
                             }
                            Some(Err(e)) => {
                                bail!("read error: {:?}", e)
                            }
                            None => {
                                return Ok::<_, anyhow::Error>(());
                            }
                        }
                    }
                    req = rx.recv() => {
                        match req {
                            Some(data) => {
                                conn.w.write_all(&data).await?;
                            }
                            None => {
                                return Ok::<_, anyhow::Error>(());
                            }
                        }
                    }
                }
            }
        });
        Ok(false)
    }
    async fn upstream_request_filter(&self, _session: &mut Session, upstream_request: &ReqFrameData, ctx: &mut Self::CTX) -> anyhow::Result<()> {
        let should_mirror = ctx.get_attr_as_bool(SHOULD_MIRROR).unwrap_or(false);
        if !should_mirror {
            return Ok(());
        }
        let tx = ctx.get_attr_as_sender(DATA_TX).unwrap();
        tx.send(upstream_request.raw_bytes.clone()).await?;
        Ok(())
    }
}