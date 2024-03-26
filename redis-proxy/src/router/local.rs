use redis_command_gen::CmdType;

use crate::config::LocalRoute;
use crate::router::{InnerRouter, Route, Router};

pub struct LocalRouter {
    pub name: String,
    pub inner: InnerRouter,
}

impl LocalRouter {
    pub fn new(local_routes: Vec<LocalRoute>, name: String, splitter: char) -> Self {
        let routes = Route::from_local_route(local_routes);
        let inner = InnerRouter::new(routes, splitter);
        LocalRouter { name, inner }
    }
}

impl Router for LocalRouter {
    fn match_route(&self, key: &[u8], cmd_type: CmdType) -> bool {
        self.inner.get(key, cmd_type).is_some()
    }
}