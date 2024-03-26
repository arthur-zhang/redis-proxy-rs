use std::str::FromStr;

use bytes::Bytes;
use smol_str::SmolStr;

use redis_command_gen::{CmdType, MULTIPART_COMMANDS};

// use crate::command::holder::MULTIPART_COMMANDS;
use crate::command::utils::get_cmd_key_bulks;

pub mod tools;
pub mod command;

#[derive(Debug)]
pub struct ReqPkt {
    pub cmd_type: CmdType,
    pub bulk_args: Vec<Bytes>,
    pub bytes_total: usize,
}

impl ReqPkt {
    pub fn new(bulk_args: Vec<Bytes>, bytes_total: usize) -> Self {
        let cmd_type_str = bulk_args[0].iter().map(|it| it.to_ascii_uppercase() as char).collect::<SmolStr>();
        // todo, add unknown
        let mut cmd_type = CmdType::from_str(&cmd_type_str).unwrap();
        if MULTIPART_COMMANDS.contains_key(&cmd_type) && bulk_args.len() > 1 {
            let cmd_with_sub_cmd: SmolStr = cmd_type_str.chars()
                .chain([' ' as char])
                .chain(bulk_args[1].iter().map(|it| it.to_ascii_lowercase() as char))
                .collect();
            cmd_type = CmdType::from_str(&cmd_with_sub_cmd).unwrap();
        }
        return ReqPkt {
            cmd_type,
            bulk_args,
            bytes_total,
        };
    }

    pub fn keys(&self) -> Option<Vec<&[u8]>> {
        get_cmd_key_bulks(&self.cmd_type, &self.bulk_args)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_smol_concat() {
        let a: SmolStr = "HELLO".chars().map(|it| (it as u8).to_ascii_lowercase() as char).collect();
        let b = "WORLD";
        let c = format!("{} {}", a, b.to_ascii_lowercase());
        assert_eq!(c, "hello world");
        let c: SmolStr = a.chars().chain([' ' as char]).chain(b.chars().map(|it| (it as u8).to_ascii_lowercase() as char)).collect();
        assert_eq!(c, "hello world");
    }
}