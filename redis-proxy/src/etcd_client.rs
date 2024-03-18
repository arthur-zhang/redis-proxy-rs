use anyhow::anyhow;
use etcd_client::{Client, ConnectOptions, EventType, GetOptions, KeyValue, WatchOptions};
use log::{debug, error, info, warn};

use crate::config::EtcdConfig;
use crate::router::etcd::RouteArray;
use crate::router::Route;

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

    pub fn list_watch_routes(&self, route_name: String, routes: RouteArray, update_tx: tokio::sync::mpsc::Sender<()>) {
        let watch_prefix = format!("{}/{}/", self.prefix, route_name);
        let clone_self = self.clone();
        tokio::spawn({
            async move {
                loop {
                    let routes = routes.clone();
                    let revision = clone_self.list_routes(&watch_prefix, routes.clone()).await;
                    if revision.is_err() {
                        error!("etcd router {} list error: {:?}, will retry later.", watch_prefix, revision.err());
                        tokio::time::sleep(std::time::Duration::from_millis(clone_self.interval)).await;
                        continue
                    }
                    let update_result = update_tx.send(()).await;
                    if update_result.is_err() {
                        error!("etcd router {} update_tx send error: {:?}, will retry later.", watch_prefix, update_result.err());
                        tokio::time::sleep(std::time::Duration::from_millis(clone_self.interval)).await;
                        continue
                    }
                    if let Err(err) = clone_self.watch_routes(&watch_prefix, revision.unwrap(), routes, update_tx.clone()).await {
                        error!("etcd router {} watch error: {:?}, will retry later.", watch_prefix, err);
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(clone_self.interval)).await;
                }
            }
        });
    }

    async fn list_routes(&self, route_keys: &str, routes: RouteArray) -> anyhow::Result<i64> {
        let mut etcd_client = self.client.clone();
        let list_resp = etcd_client.get(
            route_keys,
            Some(GetOptions::new().with_prefix())
        ).await?;
        
        list_resp.kvs().iter().for_each(|kv| {
            let (key, route) = Self::decode_kv(kv);
            if key.is_ok() && route.is_ok() {
                routes.insert(key.unwrap(), route.unwrap());
            }
        });

        if let Some(header) = list_resp.header() {
            return Ok(header.revision())
        }

        Err(anyhow!("etcd list header none"))
    }

    async fn watch_routes(
        &self,
        route_keys: &str,
        revision: i64,
        routes: RouteArray,
        update_tx: tokio::sync::mpsc::Sender<()>
    ) -> anyhow::Result<()> {
        let mut etcd_client = self.client.clone();
        let (_, mut watch_stream) = etcd_client.watch(
            route_keys, Some(WatchOptions::new().with_prefix().with_start_revision(revision))).await?;

        while let Some(resp) = watch_stream.message().await? {
            debug!("etcd router {} watch get resp: {:?}", route_keys, resp);
            if resp.created() {
                continue
            }
            if resp.canceled() {
                info!("etcd router {} watch canceled: {}, will relist and watch later", route_keys, resp.cancel_reason());
                break;
            }
            for event in resp.events() {
                debug!("etcd router {} watch get event: {:?}", route_keys, event);
                let event_type = event.event_type();
                match event_type {
                    EventType::Put => {
                        if let Some(kv) = event.kv() {
                            let (key, route) = Self::decode_kv(kv);
                            if key.is_ok() && route.is_ok() {
                                routes.insert(key.unwrap(), route.unwrap());
                            }
                        }
                    }
                    EventType::Delete => {
                        if let Some(kv) = event.kv() {
                            let (key, route) = Self::decode_kv(kv);
                            if key.is_ok() && route.is_ok() {
                                routes.remove(&key.unwrap());
                            }
                        }
                    }
                }
            }
            update_tx.send(()).await?;
        }
        Ok(())
    }

    fn decode_kv(kv: &KeyValue) -> (anyhow::Result<String>, anyhow::Result<Route>) {
        let key = kv.key_str();
        let value = kv.value_str();
        if key.is_err() || value.is_err(){
            warn!("etcd key or value is not correct, will ignore it!");
            return (Err(anyhow!("etcd key or value is not correct")), Err(anyhow!("etcd key or value is not correct")))
        }

        let key = key.unwrap();
        let key_split: Vec<&str> = key.split('/').collect::<Vec<_>>();
        if key_split.len() != 4 { // pattern must be /prefix/router_name/route_id
            return (Err(anyhow!("etcd key is not correct")), Err(anyhow!("etcd key is not correct")))
        }
        let key = key_split[3].to_string();

        let value = value.unwrap();
        let route: serde_json::error::Result<Route> = serde_json::from_str(value);

        if route.is_err() {
            warn!("etcd value is not correct, will ignore it: key: {}, value: {}", key, value);
            return (Ok(key), Err(anyhow!("etcd value is not correct")))
        }
        (Ok(key), Ok(route.unwrap()))
    }
}