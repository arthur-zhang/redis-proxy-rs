use anyhow::bail;
use tokio::io::AsyncWriteExt;
use tokio_stream::StreamExt;

use crate::conn_pool::Conn;

pub struct TinyClient {}


impl TinyClient {
    pub async fn query(conn: &mut Conn, data: &[u8]) -> anyhow::Result<bool> {
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
}
