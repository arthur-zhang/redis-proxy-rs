use anyhow::bail;
use async_trait::async_trait;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_stream::StreamExt;

use redis_proxy::peer::RedisPeer;
use redis_proxy::proxy::{Proxy, Session};
use redis_proxy::upstream_conn_pool::Pool;
use redis_proxy_common::ReqFrameData;

use crate::filter_trait::{FilterContext, Value};

pub struct Mirror {

    pool: Pool,
}

const DATA_TX: &'static str = "mirror_filter_data_tx";

impl Mirror {
    pub fn new(pool: Pool) -> Self {
        Mirror {
            pool,
        }
    }
}

#[async_trait]
impl Proxy for Mirror {
    type CTX = FilterContext;

    fn new_ctx(&self) -> Self::CTX {
        todo!()
    }
    async fn proxy_upstream_filter(&self, session: &mut Session, ctx: &mut Self::CTX) -> anyhow::Result<()>
    {
        let should_mirror = true;
        if should_mirror {
            let (tx, mut rx): (Sender<bytes::Bytes>, Receiver<bytes::Bytes>) = tokio::sync::mpsc::channel(100);
            let mut conn = self.pool.acquire().await?;
            conn.init_from_session(session).await?;
            ctx.attrs.insert(DATA_TX.to_string(), Value::ChanSender(tx));

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
        } else {
            ctx.remote_attr(DATA_TX);
        }
        Ok(())
    }
    async fn upstream_request_filter(&self, _session: &mut Session, _upstream_request: &mut ReqFrameData, _ctx: &mut Self::CTX) -> anyhow::Result<()> {
        let tx = _ctx.get_attr_as_sender(DATA_TX).unwrap();
        tx.send(_upstream_request.raw_bytes.clone()).await?;
        Ok(())
    }
}