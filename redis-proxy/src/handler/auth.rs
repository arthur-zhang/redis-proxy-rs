use redis_proxy_common::ReqPkt;

use crate::handler::CommandHandler;
use crate::session::Session;
use crate::upstream_conn_pool::{AuthInfo, RedisConnection};

pub struct Auth;

impl CommandHandler for Auth {
    fn init_from_req(&self, session: &mut Session, req_pkt: &ReqPkt) {
        if req_pkt.bulk_args.len() <= 1 {
            return;
        }
        session.authed_info = Some(AuthInfo {
            username: if req_pkt.bulk_args.len() > 2 {
                Some(req_pkt.bulk_args[1].as_ref().to_vec())
            } else {
                None
            },
            password: req_pkt.bulk_args[req_pkt.bulk_args.len() - 1].as_ref().to_vec(),
        });
    }

    fn handler_session_after_resp(&self, session: &mut Session, upstream_resp_ok: bool) {
        if !upstream_resp_ok {
            session.authed_info = None;
        }
    }

    fn handler_coon_after_resp(&self, conn: &mut RedisConnection, upstream_resp_ok: bool) {
        conn.update_authed_info(upstream_resp_ok);
    }
}