use anyhow::bail;
use bytes::Bytes;
use tokio::io::AsyncWriteExt;
use tokio_stream::StreamExt;

use crate::upstream_conn_pool::RedisConnection;

pub struct TinyClient {}


impl TinyClient {
    pub async fn query(conn: &mut RedisConnection, data: &[u8]) -> anyhow::Result<bool> {
        conn.w.write_all(data.as_ref()).await?;
        while let Some(it) = conn.r.next().await {
            match it {
                Ok(it) => {
                    if it.is_done {
                        return Ok(!it.is_error);
                    }
                }
                Err(_) => {
                    bail!("read error");
                }
            }
        }
        bail!("read eof");
    }

    pub async fn query_with_resp(conn: &mut RedisConnection, data: &[u8]) -> anyhow::Result<(bool, Vec<Bytes>)> {
        let mut bytes = vec![];
        conn.w.write_all(data.as_ref()).await?;
        while let Some(it) = conn.r.next().await {
            match it {
                Ok(it) => {
                    bytes.push(it.data);
                    if it.is_done {
                        return Ok((!it.is_error, bytes));
                    }
                }
                Err(_) => {
                    bail!("read error");
                }
            }
        }
        bail!("read eof");
    }
}
