use std::collections::HashMap;

use lazy_static::lazy_static;

use redis_command_gen::CmdType;
use redis_proxy_common::ReqPkt;

use crate::handler::auth::Auth;
use crate::handler::select::Select;
use crate::session::Session;
use crate::upstream_conn_pool::RedisConnection;

mod auth;
mod multi;
mod select;

lazy_static! {
    static ref HANDLER_MAP: HashMap<CmdType, Box<dyn CommandHandler>> = {
        let mut handlers: HashMap<CmdType, Box<dyn CommandHandler>> = HashMap::new();
        handlers.insert(CmdType::AUTH, Box::new(Auth));
        handlers.insert(CmdType::SELECT, Box::new(Select));
        handlers
    };
}

pub fn get_handler(cmd_type: CmdType) -> Option<&'static Box<dyn CommandHandler>> {
    HANDLER_MAP.get(&cmd_type)
}

pub trait CommandHandler: Send + Sync {
    fn init_from_req(&self, _session: &mut Session, _req_pkt: &ReqPkt) {
        //tod nothing
    }
    fn handler_session_after_resp(&self, _session: &mut Session, _upstream_resp_ok: bool) {
        //do nothing
    }
    fn handler_coon_after_resp(&self, _conn: &mut RedisConnection, _upstream_resp_ok: bool) {
        // do nothing
    }
}