use redis_proxy_common::ReqPkt;

use crate::handler::CommandHandler;
use crate::session::Session;

pub struct Select;

impl CommandHandler for Select {
    fn init_from_req(&self, session: &mut Session, req_pkt: &ReqPkt) {
        if req_pkt.bulk_args.len() <= 1 {
            return;
        }
        let db = &req_pkt.bulk_args[1];
        let db = convert_to_u64(db.as_ref()).unwrap_or(0);
        session.db = db;
    }
}

fn convert_to_u64(vec: &[u8]) -> Option<u64> {
    let mut result = 0u64;
    for &byte in vec {
        if byte >= b'0' && byte <= b'9' {
            result = result * 10 + (byte - b'0') as u64;
        } else {
            return None;
        }
    }
    Some(result)
}