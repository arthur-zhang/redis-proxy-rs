use smol_str::SmolStr;

use crate::command::RedisCmdDescribeEntity;

pub fn is_read_command(cmd: &SmolStr) -> bool {
    todo!()
}

pub fn should_read_more(cmd: &SmolStr) -> bool {
    todo!()
}

pub fn get_command_info(cmd: &SmolStr) -> &RedisCmdDescribeEntity {
    todo!()
}