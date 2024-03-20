use std::sync::Arc;

use arc_swap::ArcSwap;
use dashmap::DashMap;
use log::info;
use smol_str::SmolStr;

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
    fn match_route(&self, key: &[u8], cmd_type: &SmolStr) -> bool {
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
        
        etcd_client.list_watch_routes(router.name.clone(), routes.clone(), update_tx.clone());
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
}