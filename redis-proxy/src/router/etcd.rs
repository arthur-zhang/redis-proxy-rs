use std::sync::Arc;

use anyhow::anyhow;
use arc_swap::ArcSwap;
use dashmap::DashMap;
use etcd_client::{Client, EventType, GetOptions, KeyValue, WatchOptions};
use log::{debug, error, info, warn};

use redis_proxy_common::cmd::CmdType;

use crate::etcd_client::EtcdClient;
use crate::router::{InnerRouter, Route, Router};

pub type RouteArray = Arc<DashMap<String, Route>>;

pub struct EtcdRouter {
    pub name: String,
    pub routes: RouteArray,
    pub inner: ArcSwap<InnerRouter>,
    pub route_splitter: char,
}

impl Router for EtcdRouter {
    fn match_route(&self, key: &[u8], cmd_type: CmdType) -> bool {
        self.inner.load_full().get(key, cmd_type).is_some()
    }
}

impl EtcdRouter {
    pub async fn new(etcd_client: EtcdClient, name: String, splitter: char) -> anyhow::Result<Arc<Self>> {
        let routes = Arc::new(DashMap::new());
        let router = ArcSwap::new(Arc::new(InnerRouter::default()));
        let (update_tx, mut update_rx) = tokio::sync::mpsc::channel::<()>(1);

        let router = Arc::new(EtcdRouter {
            name,
            routes: routes.clone(),
            inner: router,
            route_splitter: splitter,
        });
        
        let prefix = format!("{}/{}/", etcd_client.prefix, router.name);
        Self::list_watch(
            etcd_client.client.clone(),
            prefix,
            etcd_client.interval,
            routes,
            update_tx
        );

        tokio::spawn({
            let router = router.clone();
            async move {
                loop {
                    let _ = update_rx.recv().await;
                    router.update_router();
                }
            }
        });

        Ok(router)
    }

    fn update_router(&self) {
        let mut routes = Vec::new();
        self.routes.iter().for_each(|it| {
            routes.push(it.value().clone());
        });
        let router = InnerRouter::new(routes, self.route_splitter);
        self.inner.store(Arc::new(router));
        info!("etcd router {} updated!", self.name);
    }

    fn list_watch(
        etcd_client: Client,
        prefix: String,
        interval: u64,
        routes: RouteArray,
        update_tx: tokio::sync::mpsc::Sender<()>)
    {
        tokio::spawn({
            async move {
                loop {
                    let routes = routes.clone();
                    let revision = Self::list(etcd_client.clone(), &prefix, routes.clone()).await;
                    if revision.is_err() {
                        error!("etcd router {} list error: {:?}, will retry later.", prefix, revision.err());
                        tokio::time::sleep(std::time::Duration::from_millis(interval)).await;
                        continue
                    }
                    let update_result = update_tx.send(()).await;
                    if update_result.is_err() {
                        error!("etcd router {} update_tx send error: {:?}, will retry later.", prefix, update_result.err());
                        tokio::time::sleep(std::time::Duration::from_millis(interval)).await;
                        continue
                    }
                    if let Err(err) = Self::watch(etcd_client.clone(), &prefix, revision.unwrap(), routes, update_tx.clone()).await {
                        error!("etcd router {} watch error: {:?}, will retry later.", prefix, err);
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(interval)).await;
                }
            }
        });
    }

    async fn list(mut etcd_client: Client, route_keys: &str, routes: RouteArray) -> anyhow::Result<i64> {
        let list_resp = etcd_client.get(
            route_keys,
            Some(GetOptions::new().with_prefix())
        ).await?;

        //let mut new_routes = HashMap::new();
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

    async fn watch(
        mut etcd_client: Client,
        route_keys: &str,
        revision: i64,
        routes: RouteArray,
        update_tx: tokio::sync::mpsc::Sender<()>
    ) -> anyhow::Result<()> {
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