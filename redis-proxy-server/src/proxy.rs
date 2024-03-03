use tokio::net::TcpStream;

use crate::upstream_conn_pool::Pool;

pub struct RedisProxy<P> {
    inner: P,
    upstream_pool: Pool,
}

impl<P> RedisProxy<P> {
    async fn handle_new_request(&self, mut downstream_session: RedisSession) {

    }
}

pub struct RedisSession {
    underlying_stream: TcpStream,
    buf: bytes::Bytes,
}

pub trait Proxy {}
