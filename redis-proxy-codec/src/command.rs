use arrayvec::ArrayVec;
use log::error;

use crate::codec::IndexedResp;

#[derive(Debug)]
pub struct Command {
    request: IndexedResp,
    info: CommandInfo,
}

impl Command {
    pub fn new(packet: IndexedResp) -> Self {
        let info = CommandInfo::new(&packet);
        Self { request: packet, info }
    }

    pub fn get_command_name(&self) -> Option<&str> {
        self.request.get_command_name()
    }
    pub fn get_command_len(&self) -> Option<usize> {
        self.request.get_array_len()
    }

    pub fn into_packet(self) -> IndexedResp {
        self.request
    }
    pub fn get_command_element(&self, index: usize) -> Option<&[u8]> {
        self.request.get_array_element(index)
    }
    pub fn get_key(&self) -> Option<&[u8]> {
        match self.info.data_cmd_type {
            DataCmdType::Eval | DataCmdType::Evalsha => self.request.get_array_element(3),
            _ => self.request.get_array_element(1),
        }
    }
}

#[derive(Debug)]
struct CommandInfo {
    cmd_type: CmdType,
    data_cmd_type: DataCmdType,
}

impl CommandInfo {
    pub fn new(packet: &IndexedResp) -> Self {
        let cmd_type = CmdType::from_packet(packet);
        let data_cmd_type = DataCmdType::from_packet(packet);

        Self {
            cmd_type,
            data_cmd_type,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum CmdType {
    Ping,
    Info,
    Auth,
    Quit,
    Echo,
    Select,
    Others,
    Invalid,
    Cluster,
    Config,
    Command,
    Asking,
    Hello,
}

const MAX_COMMAND_NAME_LENGTH: usize = 64;

#[inline]
pub fn byte_to_uppercase(b: u8) -> u8 {
    const DELTA: u8 = b'a' - b'A';
    if (b'a'..=b'z').contains(&b) {
        b - DELTA
    } else {
        b
    }
}

impl CmdType {
    fn from_cmd_name(cmd_name: &[u8]) -> Self {
        let mut stack_cmd_name = ArrayVec::<u8, MAX_COMMAND_NAME_LENGTH>::new();
        for b in cmd_name {
            if let Err(err) = stack_cmd_name.try_push(byte_to_uppercase(*b)) {
                error!("Unexpected long command name: {:?} {:?}", cmd_name, err);
                return CmdType::Others;
            }
        }
        // The underlying `deref` will take the real length intead of the whole MAX_COMMAND_NAME_LENGTH array;
        let cmd_name: &[u8] = &stack_cmd_name;

        match cmd_name {
            b"PING" => CmdType::Ping,
            b"INFO" => CmdType::Info,
            b"AUTH" => CmdType::Auth,
            b"QUIT" => CmdType::Quit,
            b"ECHO" => CmdType::Echo,
            b"SELECT" => CmdType::Select,
            b"CLUSTER" => CmdType::Cluster,
            b"CONFIG" => CmdType::Config,
            b"COMMAND" => CmdType::Command,
            b"ASKING" => CmdType::Asking,
            b"HELLO" => CmdType::Hello,
            _ => CmdType::Others,
        }
    }

    pub fn from_packet(packet: &IndexedResp) -> Self {
        let cmd_name = match packet.get_array_element(0) {
            Some(cmd_name) => cmd_name,
            None => return CmdType::Invalid,
        };

        CmdType::from_cmd_name(cmd_name)
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum DataCmdType {
    // String commands
    Append,
    Bitcount,
    Bitfield,
    Bitop,
    Bitpos,
    Decr,
    Decrby,
    Get,
    Getbit,
    Getrange,
    Getset,
    Incr,
    Incrby,
    Incrbyfloat,
    Mget,
    Mset,
    Msetnx,
    Psetex,
    Set,
    Setbit,
    Setex,
    Setnx,
    Setrange,
    Strlen,
    Eval,
    Evalsha,
    Del,
    Exists,
    // List commands
    Blpop,
    Brpop,
    Brpoplpush,
    Lpop,
    Rpop,
    Rpoplpush,
    Lrem,
    Ltrim,
    // Hash commands
    Hdel,
    // Set commands
    Smove,
    Spop,
    Srem,
    // Sorted Set commands
    Zpopmax,
    Zpopmin,
    Zrem,
    Zremrangebylex,
    Zremrangebyrank,
    Zremrangebyscore,
    Bzpopmin,
    Bzpopmax,
    // Key commands
    Expire,
    Expireat,
    Pexpire,
    Pexpireat,
    Move,
    Rename,
    Renamenx,
    Unlink,
    Others,
}

impl DataCmdType {
    fn is_blocking_cmd(self) -> bool {
        matches!(
            self,
            Self::Bzpopmin | Self::Bzpopmax | Self::Blpop | Self::Brpop | Self::Brpoplpush
        )
    }
    fn from_cmd_name(cmd_name: &[u8]) -> Self {
        let mut stack_cmd_name = ArrayVec::<u8, MAX_COMMAND_NAME_LENGTH>::new();
        for b in cmd_name {
            if let Err(err) = stack_cmd_name.try_push(byte_to_uppercase(*b)) {
                error!(
                    "Unexpected long data command name: {:?} {:?}",
                    cmd_name, err
                );
                return DataCmdType::Others;
            }
        }
        // The underlying `deref` will take the real length intead of the whole MAX_COMMAND_NAME_LENGTH array;
        let cmd_name: &[u8] = &stack_cmd_name;

        match cmd_name {
            b"APPEND" => DataCmdType::Append,
            b"BITCOUNT" => DataCmdType::Bitcount,
            b"BITFIELD" => DataCmdType::Bitfield,
            b"BITOP" => DataCmdType::Bitop,
            b"BITPOS" => DataCmdType::Bitpos,
            b"DECR" => DataCmdType::Decr,
            b"DECRBY" => DataCmdType::Decrby,
            b"GET" => DataCmdType::Get,
            b"GETBIT" => DataCmdType::Getbit,
            b"GETRANGE" => DataCmdType::Getrange,
            b"GETSET" => DataCmdType::Getset,
            b"INCR" => DataCmdType::Incr,
            b"INCRBY" => DataCmdType::Incrby,
            b"INCRBYFLOAT" => DataCmdType::Incrbyfloat,
            b"MGET" => DataCmdType::Mget,
            b"MSET" => DataCmdType::Mset,
            b"MSETNX" => DataCmdType::Msetnx,
            b"PSETEX" => DataCmdType::Psetex,
            b"SET" => DataCmdType::Set,
            b"SETBIT" => DataCmdType::Setbit,
            b"SETEX" => DataCmdType::Setex,
            b"SETNX" => DataCmdType::Setnx,
            b"SETRANGE" => DataCmdType::Setrange,
            b"STRLEN" => DataCmdType::Strlen,
            b"EVAL" => DataCmdType::Eval,
            b"EVALSHA" => DataCmdType::Evalsha,
            b"DEL" => DataCmdType::Del,
            b"EXISTS" => DataCmdType::Exists,
            b"BLPOP" => DataCmdType::Blpop,
            b"BRPOP" => DataCmdType::Brpop,
            b"BRPOPLPUSH" => DataCmdType::Brpoplpush,
            b"EXPIRE" => DataCmdType::Expire,
            b"EXPIREAT" => DataCmdType::Expireat,
            b"PEXPIRE" => DataCmdType::Pexpire,
            b"PEXPIREAT" => DataCmdType::Pexpireat,
            b"HDEL" => DataCmdType::Hdel,
            b"LPOP" => DataCmdType::Lpop,
            b"RPOP" => DataCmdType::Rpop,
            b"RPOPLPUSH" => DataCmdType::Rpoplpush,
            b"LREM" => DataCmdType::Lrem,
            b"LTRIM" => DataCmdType::Ltrim,
            b"MOVE" => DataCmdType::Move,
            b"RENAME" => DataCmdType::Rename,
            b"RENAMENX" => DataCmdType::Renamenx,
            b"SMOVE" => DataCmdType::Smove,
            b"SPOP" => DataCmdType::Spop,
            b"SREM" => DataCmdType::Srem,
            b"UNLINK" => DataCmdType::Unlink,
            b"ZPOPMAX" => DataCmdType::Zpopmax,
            b"ZPOPMIN" => DataCmdType::Zpopmin,
            b"BZPOPMAX" => DataCmdType::Bzpopmax,
            b"BZPOPMIN" => DataCmdType::Bzpopmin,
            b"ZREM" => DataCmdType::Zrem,
            b"ZREMRANGEBYLEX" => DataCmdType::Zremrangebylex,
            b"ZREMRANGEBYRANK" => DataCmdType::Zremrangebyrank,
            b"ZREMRANGEBYSCORE" => DataCmdType::Zremrangebyscore,
            _ => DataCmdType::Others,
        }
    }

    pub fn from_packet(packet: &IndexedResp) -> Self {
        let cmd_name = match packet.get_array_element(0) {
            Some(cmd_name) => cmd_name,
            None => return DataCmdType::Others,
        };

        DataCmdType::from_cmd_name(cmd_name)
    }
}