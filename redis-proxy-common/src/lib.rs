use bytes::Bytes;
use smol_str::SmolStr;

use crate::command::holder::MULTIPART_COMMANDS;

pub mod tools;
pub mod command;

pub struct ReqPkt {
    pub cmd_type: SmolStr,
    pub bulk_args: Vec<Bytes>,
    pub bytes_total: usize,
}

impl ReqPkt {
    pub fn new(bulk_args: Vec<Bytes>, bytes_total: usize) -> Self {
        let mut cmd_type = bulk_args[0].iter().map(|it| it.to_ascii_lowercase() as char).collect::<SmolStr>();
        if MULTIPART_COMMANDS.contains_key(&cmd_type) && bulk_args.len() > 1 {
            cmd_type = cmd_type.chars()
                .chain([' ' as char])
                .chain(bulk_args[1].iter().map(|it| it.to_ascii_lowercase() as char))
                .collect();
        }
        return ReqPkt {
            cmd_type,
            bulk_args,
            bytes_total,
        };
    }

    pub fn cmd_type(&self) -> &SmolStr {
        &self.cmd_type
    }

    pub fn keys(&self) -> Option<Vec<&[u8]>> {
        None
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