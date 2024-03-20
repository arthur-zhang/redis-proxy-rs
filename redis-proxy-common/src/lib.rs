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
        let cmd = unsafe { 
            std::str::from_utf8_unchecked(&bulk_args[0]) 
        };
        let mut cmd_type: SmolStr = cmd.into();
        if MULTIPART_COMMANDS.contains_key(&cmd_type) && bulk_args.len() > 1 {
            let sub_cmd = unsafe {
                std::str::from_utf8_unchecked(&bulk_args[1])
            };
            cmd_type = SmolStr::from_iter(cmd_type.chars().chain([' ' as char]).chain(sub_cmd.chars()));
        }
        return ReqPkt {
            cmd_type,
            bulk_args,
            bytes_total
        }
    }
    
    pub fn cmd_type(&self) -> &SmolStr {
        &self.cmd_type
    }
    
    pub fn keys(&self)-> Option<Vec<&[u8]>> {
        
        None
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_smol_concat() {
        let a: SmolStr = "hello".into();
        let b= "world";
        let c = format!("{} {}", a, b);
        assert_eq!(c, "hello world");
        let c = a.chars().chain([' ' as char]).chain(b.chars());
        let d  = SmolStr::from_iter(c);
        assert_eq!(d, "hello world");
    }
}