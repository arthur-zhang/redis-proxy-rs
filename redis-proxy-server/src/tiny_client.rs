use tokio::io::AsyncWriteExt;
use tokio_stream::StreamExt;

use crate::conn_pool::Conn;

pub struct TinyClient<'a> {
    _phantom: std::marker::PhantomData<&'a ()>,
}


impl<'a> TinyClient<'a> {
    pub async fn query(conn: &mut Conn, data: &[u8]) -> anyhow::Result<bool> {
        conn.w.write_all(data.as_ref()).await?;
        let mut ok = false;
        while let Some(Ok(it)) = conn.r.next().await {
            if it.is_done {
                ok = !it.is_error;
                break;
            }
        }

        Ok(ok)
    }
}
