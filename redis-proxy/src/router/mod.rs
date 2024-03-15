use std::collections::HashMap;
use std::sync::Arc;

use log::{error, info};
use serde::{Deserialize, Serialize};

use redis_proxy_common::cmd::CmdType;

use crate::config::{ConfigCenter, EtcdConfig, LocalRoute};

pub mod etcd;
pub mod local;

pub trait Router: Send + Sync {
    fn match_route(&self, key: &[u8], cmd_type: CmdType) -> bool;
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
    pub fn from_local_route(local_routes: Vec<LocalRoute>) -> Vec<Route> {
        let mut index = 0;
        local_routes.into_iter().map(|it| {
            let route = Route {
                id: format!("local_route_{}", index),
                name: format!("local_route_{}", index),
                keys: it.keys,
                commands: it.commands,
                enable: true,
            };
            index += 1;
            route
        }).collect()
    }
    
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
pub struct InnerRouter {
    root: Node,
    splitter: char,
}

impl InnerRouter {
    pub fn new(routes: Vec<Route>, splitter: char) -> Self {
        let root = Node::default();
        let mut router = InnerRouter { root, splitter };

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

pub async fn create_router(
    splitter: char,
    router_name: String,
    config_center: ConfigCenter, 
    local_routes: Option<Vec<LocalRoute>>,
    etcd_config: Option<EtcdConfig>
) -> anyhow::Result<Arc<dyn Router>>{
    match config_center {
        ConfigCenter::Etcd => {
            if etcd_config.is_none() {
                anyhow::bail!("etcd config is none");
            }
            info!("create router {} as etcd router", router_name);
            Ok(etcd::EtcdRouter::new(etcd_config.unwrap(), router_name, splitter).await?)
        }
        ConfigCenter::Local => {
            if local_routes.is_none() {
                anyhow::bail!("local routes is none");
            }
            info!("create router {} as local router", router_name);
            Ok(Arc::new(local::LocalRouter::new(local_routes.unwrap(), router_name, splitter)))
        }
    }
}

#[cfg(test)]
mod tests {
    use redis_proxy_common::cmd::CmdType;

    use crate::router::{InnerRouter, Route};

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
        let router = InnerRouter::new(routes, ':');
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