use std::collections::HashMap;
use std::sync::Arc;

use anyhow::anyhow;
use arc_swap::ArcSwap;
use dashmap::DashMap;
use etcd_client::{Client, ConnectOptions, EventType, GetOptions, KeyValue, WatchOptions};
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};

use redis_proxy_common::cmd::CmdType;

use crate::config::EtcdConfig;

pub type RouteArray = Arc<DashMap<String, Route>>;

pub struct RouterManager {
    pub name: String,
    pub routes: RouteArray,
    pub router: ArcSwap<Router>,
    pub route_splitter: char,
}

impl RouterManager {
    pub async fn new(etcd_config: EtcdConfig, name: String) -> anyhow::Result<Arc<Self>> {
        let routes = Arc::new(DashMap::new());
        let router = ArcSwap::new(Arc::new(Router::default()));
        let (update_tx, mut update_rx) = tokio::sync::mpsc::channel::<()>(1);

        let router_manager = Arc::new(RouterManager {
            name,
            routes: routes.clone(),
            router,
            route_splitter: etcd_config.key_splitter,
        });

        let etcd_client = Client::connect(
            etcd_config.endpoints,
            Some(ConnectOptions::new()
                .with_timeout(std::time::Duration::from_millis(etcd_config.timeout))
                .with_user(etcd_config.username, etcd_config.password)
            )
        ).await?;

        let prefix = format!("{}/{}/", etcd_config.prefix, router_manager.name);
        Self::list_watch(
            etcd_client,
            prefix,
            etcd_config.interval,
            routes,
            update_tx
        );

        tokio::spawn({
            let router_manager = router_manager.clone();
            async move {
                loop {
                    let _ = update_rx.recv().await;
                    router_manager.update_router();
                }
            }
        });

        Ok(router_manager)
    }

    pub fn get_router(&self) -> Arc<Router> {
        self.router.load_full()
    }

    fn update_router(&self) {
        let mut routes = Vec::new();
        self.routes.iter().for_each(|it| {
            routes.push(it.value().clone());
        });
        let router = Router::new(routes, self.route_splitter);
        self.router.store(Arc::new(router));
        info!("router {} updated!", self.name);
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

        Err(anyhow!("list header none"))
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

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Route {
    pub id: String,
    pub name: String,
    pub keys: Vec<String>,
    pub commands: Vec<String>,
    pub enable: bool,
}

impl Route {
    pub fn validate(&self) -> anyhow::Result<()> {
        for key in &self.keys {
            if key.is_empty() {
                error!("empty key in route: {:?}", self.id);
                return Err(anyhow::anyhow!("empty key in route: {:?}", self.id));
            }
            //todo validate key regex
        }
        for cmd in &self.commands {
            let cmd_type = CmdType::from(cmd.as_bytes());
            if cmd_type == CmdType::UNKNOWN {
                error!("unknown command type: {}, route id: {:?}", cmd, self.id);
                return Err(anyhow::anyhow!("unknown command type: {}, route id: {:?}", cmd, self.id));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct Router {
    root: Node,
    splitter: char,
}

impl Router {
    pub fn new(routes: Vec<Route>, splitter: char) -> Self {
        let root = Node::default();
        let mut router = Router { root, splitter };

        for route in routes {
            router.push(route);
        }
        router
    }

    pub fn push(&mut self, route: Route) {
        if !route.enable || route.validate().is_err() {
            return
        }
        for key in &route.keys {
            let parts = self.get_seg_parts(key.as_ref());
            self.root.push(&parts, route.clone());
        }
    }

    pub fn get(&self, s: &[u8], cmd_type: CmdType) -> Option<&Route> {
        if s.is_empty() {
            return None;
        }
        let s = std::str::from_utf8(s);
        if s.is_err() {
            return None;
        }
        let seg_parts = self.get_seg_parts(s.unwrap());
        self.root.get(&seg_parts, 0, cmd_type)
    }

    #[inline]
    fn get_seg_parts<'a>(&self, path: &'a str) -> Vec<&'a str> {
        path.split(self.splitter).filter(|it| !it.is_empty()).collect::<Vec<_>>()
    }

    pub fn _dump(&self) {
        self.root._dump(0);
    }
}

#[derive(Debug, Default)]
pub struct Node {
    name: String,
    command_router: HashMap<CmdType, Route>,
    children: HashMap<String, Node>,
}

impl Node {
    pub fn new(name: &str) -> Self {
        Node {
            name: name.to_string(),
            command_router: HashMap::new(),
            children: HashMap::new(),
        }
    }

    pub fn push(&mut self, parts: &[&str], route: Route) {
        if self.name.as_str() == "**" || parts.is_empty() {
            return;
        }
        let part = *parts.first().unwrap();
        let child = self.children.get_mut(part);
        if child.is_none() {
            self.children.insert(part.to_string(), Node::new(part));
        }
        let matched_child = self.children.get_mut(part).expect("should not happen");

        if parts.len() == 1 || part == "**" { // it means it is the last part of the route
            let mut command_router = HashMap::new();
            for cmd in &route.commands {
                let cmd_type = CmdType::from(cmd.as_bytes());
                if cmd_type == CmdType::UNKNOWN {
                    continue;
                }
                command_router.insert(cmd_type, route.clone());
            }
            if command_router.is_empty() {
                command_router.insert(CmdType::UNKNOWN, route.clone());
            }
            for (cmd_type, route) in command_router {
                matched_child.command_router.insert(cmd_type, route);
            }
        }
        matched_child.push(&parts[1..], route);
    }

    fn get(&self, parts: &[&str], level: usize, cmd_type: CmdType) -> Option<&Route> {
        if level >= parts.len() || self.name == "**" {
            return self.command_router.get(&cmd_type)
                .or_else(|| self.command_router.get(&CmdType::UNKNOWN));
        }

        if self.children.is_empty() {
            return None;
        }

        let part = parts[level];
        let mut route = self.children.get(part).and_then(|it| it.get(parts, level + 1, cmd_type));

        if route.is_none() {
            route = self.children.get("*").and_then(|it| it.get(parts, level + 1, cmd_type));
        }

        if route.is_none() {
            route = self.children.get("**").and_then(|it| it.get(parts, level + 1, cmd_type));
        }

        route
    }

    pub fn _dump(&self, level: usize) {
        if level == 0 {
            println!(".")
        } else {
            let mut indent = String::new();
            for _ in 1..level {
                indent.push('\t');
            }
            indent.push_str("└──");
            indent.push_str(&self.name);
            indent.push_str(" ==> ");
            for (cmd_type, route) in &self.command_router {
                indent.push('(');
                indent.push_str(&format!("{:?}", cmd_type));
                indent.push_str(" => ");
                indent.push_str(&format!("{:?}", route.id));
                indent.push(')');
            }
            println!("{}", indent)
        }
        for (_, child) in self.children.iter() {
            child._dump(level + 1);
        }
    }
}

#[cfg(test)]
mod tests {
    use redis_proxy_common::cmd::CmdType;

    use crate::router::{Route, Router};

    #[test]
    fn test_all() {
        let route_1 = Route {
            id: "a".to_string(), name: "a".to_string(), keys: vec!["account:login".to_string()],
            commands: vec!["get".to_string()], enable: true,
        };
        let route_2 = Route {
            id: "b".to_string(), name: "b".to_string(), keys: vec!["account:login:*".to_string()],
            commands: vec!["get".to_string()], enable: true,
        };
        let route_3 = Route {
            id: "bb".to_string(), name: "bb".to_string(), keys: vec!["account:login:**".to_string()],
            commands: vec!["get".to_string(), "hget".to_string()], enable: true,
        };
        let route_4 = Route {
            id: "c".to_string(), name: "c".to_string(), keys: vec!["account:login:userId".to_string()],
            commands: vec!["get".to_string()], enable: true,
        };
        let route_5 = Route {
            id: "d".to_string(), name: "d".to_string(), keys: vec!["account:*:userId".to_string()],
            commands: vec!["get".to_string(), "set".to_string(), "hget".to_string()], enable: true,
        };
        let routes = vec![
            route_1, route_2, route_3, route_4, route_5
        ];
        let router = Router::new(routes, ':');
        router._dump();

        let route = router.get("account:login".as_bytes(), CmdType::GET);
        assert_eq!(route.unwrap().id, "a");

        let route = router.get("account:login:whatever".as_bytes(), CmdType::GET);
        assert_eq!(route.unwrap().id, "b");

        let route = router.get("account:login:userId".as_bytes(), CmdType::GET);
        assert_eq!(route.unwrap().id, "c");

        let route = router.get("account:login:userId".as_bytes(), CmdType::SET);
        assert_eq!(route.unwrap().id, "d");

        let route = router.get("account:whatever:userId".as_bytes(), CmdType::GET);
        assert_eq!(route.unwrap().id, "d");

        let route = router.get("account:login:userId".as_bytes(), CmdType::HGET);
        assert_eq!(route.unwrap().id, "bb");

        let route = router.get("account:login:whatever:whatever".as_bytes(), CmdType::GET);
        assert_eq!(route.unwrap().id, "bb");

        let route = router.get("account:whatever:whatever".as_bytes(), CmdType::GET);
        assert!(route.is_none());

        let route = router.get("account:login".as_bytes(), CmdType::HGET);
        assert!(route.is_none());
    }
}