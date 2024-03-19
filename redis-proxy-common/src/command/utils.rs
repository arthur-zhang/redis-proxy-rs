use crate::command::holder::COMMANDS_INFO;
use crate::command::RedisCmdDescribeEntity;

pub fn is_read_command(cmd: &str) -> bool {
    todo!()
}

pub fn should_read_more(cmd: &str) -> bool {
    todo!()
}

pub fn get_command_info(cmd: &str) -> &RedisCmdDescribeEntity {
    COMMANDS_INFO.get(cmd).expect("command not found")
}