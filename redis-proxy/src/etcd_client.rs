use etcd_client::{Client, ConnectOptions};

use crate::config::EtcdConfig;

#[derive(Clone)]
pub struct EtcdClient {
    pub client: Client,
    pub prefix: String,
    pub interval: u64,
}

impl EtcdClient {
    pub async fn new(etcd_config: EtcdConfig) -> anyhow::Result<Self> {
        let etcd_client = Client::connect(
            etcd_config.endpoints,
            Some(ConnectOptions::new()
                .with_timeout(std::time::Duration::from_millis(etcd_config.timeout))
                .with_user(etcd_config.username, etcd_config.password)
            )
        ).await?;
        
        Ok(Self{
            client: etcd_client,
            prefix: etcd_config.prefix,
            interval: etcd_config.interval,
        })
    }
}