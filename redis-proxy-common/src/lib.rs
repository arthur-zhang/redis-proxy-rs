use bytes::Bytes;
use smol_str::SmolStr;

use crate::command::holder::MULTIPART_COMMANDS;
use crate::command::utils::get_cmd_key_bulks;

pub mod tools;
pub mod command;

pub struct ReqPkt {
    pub cmd_type: SmolStr,
    pub bulk_args: Vec<Bytes>,
    pub bytes_total: usize,
}

impl ReqPkt {
    pub fn new(bulk_args: Vec<Bytes>, bytes_total: usize) -> Self {
        let cmd = unsafe { 
            std::str::from_utf8_unchecked(&bulk_args[0]) 
        };
        let mut cmd_type: SmolStr = cmd.into();
        if MULTIPART_COMMANDS.contains_key(&cmd_type) && bulk_args.len() > 1 {
            let sub_cmd = unsafe {
                std::str::from_utf8_unchecked(&bulk_args[1])
            };
            let sub_cmd: SmolStr = sub_cmd.into();

            cmd_type = format!("{} {}", cmd, sub_cmd).into();
            
        }
        return ReqPkt {
            cmd_type,
            bulk_args,
            bytes_total
        }
    }

    pub fn keys(&self)-> Option<Vec<&[u8]>> {
        get_cmd_key_bulks(&self.cmd_type, &self.bulk_args)
    }
}