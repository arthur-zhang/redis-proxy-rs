use redis_codec_core::req_decoder::KeyAwareDecoder;
use redis_proxy_common::DecodedFrame;

#[async_trait::async_trait]
pub trait Filter: Send + Sync {
    async fn init(&mut self) -> anyhow::Result<()>;
    async fn on_data(&mut self, decoder: &KeyAwareDecoder, data: &DecodedFrame) -> anyhow::Result<()>;
}


