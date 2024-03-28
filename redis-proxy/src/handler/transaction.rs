use crate::client_flags::SessionFlags;
use crate::handler::CommandHandler;
use crate::session::Session;

pub struct Multi;
impl CommandHandler for Multi {
    fn handler_session_after_resp(&self, session: &mut Session, upstream_resp_ok: bool) {
        if upstream_resp_ok {
            session.insert_client_flags(SessionFlags::InTrasactions)
        }
    }
}

pub struct Exec;
impl CommandHandler for Exec {
    fn handler_session_after_resp(&self, session: &mut Session, upstream_resp_ok: bool) {
        if upstream_resp_ok {
            session.remove_client_flags(SessionFlags::InTrasactions)
        }
    }
}

pub struct Discard;
impl CommandHandler for Discard {
    fn handler_session_after_resp(&self, session: &mut Session, upstream_resp_ok: bool) {
        if upstream_resp_ok {
            session.remove_client_flags(SessionFlags::InTrasactions)
        }
    }
}