use std::collections::HashMap;

use log::error;
use serde::{Deserialize, Serialize};

use redis_proxy_common::cmd::CmdType;

pub type FilterConfig = Vec<u8>;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Route {
    pub id: Option<String>,
    pub keys: Vec<String>,
    pub commands: Vec<String>,
    pub filters: HashMap<String, Option<FilterConfig>>,
    pub enable: bool,
}

impl Route {
    pub fn validate(&self) -> anyhow::Result<()> {
        for key in &self.keys {
            if key.is_empty() {
                error!("empty key in route: {:?}", self.id);
                return Err(anyhow::anyhow!("empty key in route: {:?}", self.id));
            }
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

#[derive(Debug)]
pub struct Router {
    root: Node,
    splitter: char,
}

impl Router {
    pub fn init(routes: &Vec<Route>, splitter: char) -> anyhow::Result<Self> {
        let root = Node::default();
        let mut router = Router { root, splitter };

        for route in routes {
            if !route.enable || route.validate().is_err() {
                continue
            }
            for key in &route.keys {
                let parts = router.get_seg_parts(key.as_ref());
                router.push(&parts, route);
            }
        }
        Ok(router)
    }

    pub fn push(&mut self, segs: &[&str], route: &Route) {
        if segs.is_empty() {
            return;
        }

        self.root.push(segs, route);
    }

    pub fn get(&self, s: &[u8], cmd_type: CmdType) -> Option<&Route> {
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

    pub fn push(&mut self, parts: &[&str], route: &Route) {
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
        let routes = vec![
            Route {
                id: Some("a".to_string()), keys: vec!["account:login".to_string()],
                commands: vec!["get".to_string()], filters: Default::default(), enable: true,
            },
            Route {
                id: Some("b".to_string()), keys: vec!["account:login:*".to_string()],
                commands: vec!["get".to_string()], filters: Default::default(), enable: true,
            },
            Route {
                id: Some("bb".to_string()), keys: vec!["account:login:**".to_string()],
                commands: vec!["get".to_string(), "hget".to_string()], filters: Default::default(), enable: true,
            },
            Route {
                id: Some("c".to_string()), keys: vec!["account:login:userId".to_string()],
                commands: vec!["get".to_string()], filters: Default::default(), enable: true,
            },
            Route {
                id: Some("d".to_string()), keys: vec!["account:*:userId".to_string()],
                commands: vec!["get".to_string(), "set".to_string(), "hget".to_string()], filters: Default::default(), enable: true,
            },
        ];
        let router = Router::init(&routes, ':').unwrap();
        router._dump();

        let route = router.get("account:login".as_bytes(), CmdType::GET);
        assert_eq!(route.unwrap().id, Some("a".to_string()));

        let route = router.get("account:login:whatever".as_bytes(), CmdType::GET);
        assert_eq!(route.unwrap().id, Some("b".to_string()));

        let route = router.get("account:login:userId".as_bytes(), CmdType::GET);
        assert_eq!(route.unwrap().id, Some("c".to_string()));

        let route = router.get("account:login:userId".as_bytes(), CmdType::SET);
        assert_eq!(route.unwrap().id, Some("d".to_string()));

        let route = router.get("account:whatever:userId".as_bytes(), CmdType::GET);
        assert_eq!(route.unwrap().id, Some("d".to_string()));

        let route = router.get("account:login:userId".as_bytes(), CmdType::HGET);
        assert_eq!(route.unwrap().id, Some("bb".to_string()));

        let route = router.get("account:login:whatever:whatever".as_bytes(), CmdType::GET);
        assert_eq!(route.unwrap().id, Some("bb".to_string()));

        let route = router.get("account:whatever:whatever".as_bytes(), CmdType::GET);
        assert!(route.is_none());

        let route = router.get("account:login".as_bytes(), CmdType::HGET);
        assert!(route.is_none());
    }
}