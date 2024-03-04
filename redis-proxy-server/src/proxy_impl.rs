use async_trait::async_trait;
use bytes::Bytes;
use futures::SinkExt;

use redis_proxy::proxy::{Proxy, Session};
use redis_proxy_common::cmd::CmdType;

use crate::filter_chain::FilterChain;
use crate::traits::FilterContext;

pub struct MyProxy {
    pub filter_chain: FilterChain,
}

#[async_trait]
impl Proxy for MyProxy {
    type CTX = FilterContext;

    fn new_ctx(&self) -> Self::CTX {
        FilterContext {
            db: 0,
            is_authed: false,
            cmd_type: CmdType::UNKNOWN,
            password: None,
            attrs: Default::default(),
        }
    }

    async fn request_filter(&self, session: &mut Session, ctx: &mut Self::CTX) -> anyhow::Result<bool> {
        let cmd_type = session.cmd_type();
        if cmd_type == CmdType::SET {
            session.downstream_session.underlying_stream.send(Bytes::from_static(b"-nimei\r\n")).await?;
            return Ok(true);
        }
        Ok(false)
    }
}