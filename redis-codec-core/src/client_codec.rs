// use std::io::Read;
//
// use bytes::{Buf, Bytes, BytesMut};
// use log::debug;
// use tokio_util::codec::Decoder;
//
// pub struct IndexedBytes {
//     pub start: usize,
//     pub size: usize,
//     pub bytes: bytes::Bytes,
// }
//
// pub enum MsgType {
//     UNKNOWN,
//     PING,
//     QUIT,
//     COMMAND,
//
//     LOLWUT,
//
//     EVAL,
//     EVALSHA,
//
//     MGET,
//     DEL,
//     UNLINK,
//     TOUCH,
//
//     SORT,
//     COPY,
//     BITCOUNT,
//     BITPOS,
//     BITFIELD,
//     EXISTS,
//     GETEX,
//     SET,
//     HDEL,
//     HMGET,
//     HMSET,
//     HSCAN,
//     HSET,
//     HRANDFIELD,
//     LPUSH,
//     LPUSHX,
//     RPUSH,
//     RPUSHX,
//     LPOP,
//     RPOP,
//     LPOS,
//     SADD,
//     SDIFF,
//     SDIFFSTORE,
//     SINTER,
//     SINTERSTORE,
//     SREM,
//     SUNION,
//     SUNIONSTORE,
//     SRANDMEMBER,
//     SSCAN,
//     SPOP,
//     SMISMEMBER,
//     PFADD,
//     PFMERGE,
//     PFCOUNT,
//     ZADD,
//     ZDIFF,
//     ZDIFFSTORE,
//     ZINTER,
//     ZINTERSTORE,
//     ZMSCORE,
//     ZPOPMAX,
//     ZPOPMIN,
//     ZRANDMEMBER,
//     ZRANGE,
//     ZRANGEBYLEX,
//     ZRANGEBYSCORE,
//     ZRANGESTORE,
//     ZREM,
//     ZREVRANGE,
//     ZREVRANGEBYLEX,
//     ZREVRANGEBYSCORE,
//     ZSCAN,
//     ZUNION,
//     ZUNIONSTORE,
//     GEODIST,
//     GEOPOS,
//     GEOHASH,
//     GEOADD,
//     GEORADIUS,
//     GEORADIUSBYMEMBER,
//     GEOSEARCH,
//     GEOSEARCHSTORE,
//     RESTORE,
//
//     LINSERT,
//     LMOVE,
//
//     GETRANGE,
//     PSETEX,
//     SETBIT,
//     SETEX,
//     SETRANGE,
//
//     HINCRBY,
//     HINCRBYFLOAT,
//     HSETNX,
//
//     LRANGE,
//     LREM,
//     LSET,
//     LTRIM,
//
//     SMOVE,
//
//     ZCOUNT,
//     ZLEXCOUNT,
//     ZINCRBY,
//     ZREMRANGEBYLEX,
//     ZREMRANGEBYRANK,
//     ZREMRANGEBYSCORE,
//
//     EXPIRE,
//     EXPIREAT,
//     PEXPIRE,
//     PEXPIREAT,
//     MOVE,
//
//     APPEND,
//     DECRBY,
//     GETBIT,
//     GETSET,
//     INCRBY,
//     INCRBYFLOAT,
//     SETNX,
//
//     HEXISTS,
//     HGET,
//     HSTRLEN,
//
//     LINDEX,
//     RPOPLPUSH,
//
//     SISMEMBER,
//
//     ZRANK,
//     ZREVRANK,
//     ZSCORE,
//
//     PERSIST,
//     PTTL,
//     TTL,
//     TYPE,
//     DUMP,
//
//     DECR,
//     GET,
//     GETDEL,
//     INCR,
//     STRLEN,
//
//     HGETALL,
//     HKEYS,
//     HLEN,
//     HVALS,
//
//     LLEN,
//
//     SCARD,
//     SMEMBERS,
//
//     ZCARD,
//     AUTH,
// }
//
// impl MsgType {
//     // Return true, if the redis command accepts no key
//     pub fn redis_argz(&self) -> bool {
//         match self {
//             MsgType::PING | MsgType::QUIT | MsgType::COMMAND => true,
//             _ => false,
//         }
//     }
//
//     pub fn redis_nokey(&self) -> bool {
//         return match self {
//             MsgType::LOLWUT => true,
//             _ => false,
//         };
//     }
//
//     // Return true, if the redis command accepts exactly 0 arguments, otherwise return false
//     pub fn redis_arg0(&self) -> bool {
//         match self {
//             MsgType::PERSIST |
//             MsgType::PTTL |
//             MsgType::TTL |
//             MsgType::TYPE |
//             MsgType::DUMP |
//
//             MsgType::DECR |
//             MsgType::GET |
//             MsgType::GETDEL |
//             MsgType::INCR |
//             MsgType::STRLEN |
//
//             MsgType::HGETALL |
//             MsgType::HKEYS |
//             MsgType::HLEN |
//             MsgType::HVALS |
//
//             MsgType::LLEN |
//
//             MsgType::SCARD |
//             MsgType::SMEMBERS |
//
//             MsgType::ZCARD |
//             MsgType::AUTH => true,
//             _ => false,
//         }
//     }
//     // Return true, if the redis command accepts exactly 1 argument, otherwise return false
//     pub fn redis_arg1(&self) -> bool {
//         match self {
//             MsgType::EXPIRE |
//             MsgType::EXPIREAT |
//             MsgType::PEXPIRE |
//             MsgType::PEXPIREAT |
//             MsgType::MOVE |
//
//             MsgType::APPEND |
//             MsgType::DECRBY |
//             MsgType::GETBIT |
//             MsgType::GETSET |
//             MsgType::INCRBY |
//             MsgType::INCRBYFLOAT |
//             MsgType::SETNX |
//
//             MsgType::HEXISTS |
//             MsgType::HGET |
//             MsgType::HSTRLEN |
//
//             MsgType::LINDEX |
//             MsgType::RPOPLPUSH |
//
//             MsgType::SISMEMBER |
//
//             MsgType::ZRANK |
//             MsgType::ZREVRANK |
//             MsgType::ZSCORE => {
//                 true
//             }
//             _ => false
//         }
//     }
//     // Return true, if the redis command accepts exactly 2 arguments, otherwise return false
//     // SETEX mykey 10 "Hello"
//     pub fn redis_arg2(&self) -> bool {
//         match self {
//             MsgType::GETRANGE |
//             MsgType::PSETEX |
//             MsgType::SETBIT |
//             MsgType::SETEX |
//             MsgType::SETRANGE |
//
//             MsgType::HINCRBY |
//             MsgType::HINCRBYFLOAT |
//             MsgType::HSETNX |
//
//             MsgType::LRANGE |
//             MsgType::LREM |
//             MsgType::LSET |
//             MsgType::LTRIM |
//
//             MsgType::SMOVE |
//             MsgType::ZCOUNT |
//             MsgType::ZLEXCOUNT |
//             MsgType::ZINCRBY |
//             MsgType::ZREMRANGEBYLEX |
//             MsgType::ZREMRANGEBYRANK |
//             MsgType::ZREMRANGEBYSCORE => true,
//             _ => false,
//         }
//     }
//     // Return true, if the redis command accepts exactly 3 arguments, otherwise return false
//     pub fn redis_arg3(&self) -> bool {
//         match self {
//             MsgType::LINSERT | MsgType::LMOVE => true,
//             _ => false,
//         }
//     }
//     // Return true, if the redis command operates on one key and accepts 0 or more arguments, otherwise
//     // return false
//     pub fn redis_argn(&self) -> bool {
//         match self {
//             MsgType::SORT |
//             MsgType::COPY |
//             MsgType::BITCOUNT |
//             MsgType::BITPOS |
//             MsgType::BITFIELD |
//             MsgType::EXISTS |
//             MsgType::GETEX |
//             MsgType::SET |
//             MsgType::HDEL |
//             MsgType::HMGET |
//             MsgType::HMSET |
//             MsgType::HSCAN |
//             MsgType::HSET |
//             MsgType::HRANDFIELD |
//             MsgType::LPUSH |
//             MsgType::LPUSHX |
//             MsgType::RPUSH |
//             MsgType::RPUSHX |
//             MsgType::LPOP |
//             MsgType::RPOP |
//             MsgType::LPOS |
//             MsgType::SADD |
//             MsgType::SDIFF |
//             MsgType::SDIFFSTORE |
//             MsgType::SINTER |
//             MsgType::SINTERSTORE |
//             MsgType::SREM |
//             MsgType::SUNION |
//             MsgType::SUNIONSTORE |
//             MsgType::SRANDMEMBER |
//             MsgType::SSCAN |
//             MsgType::SPOP |
//             MsgType::SMISMEMBER |
//             MsgType::PFADD |
//             MsgType::PFMERGE |
//             MsgType::PFCOUNT |
//             MsgType::ZADD |
//             MsgType::ZDIFF |
//             MsgType::ZDIFFSTORE |
//             MsgType::ZINTER |
//             MsgType::ZINTERSTORE |
//             MsgType::ZMSCORE |
//             MsgType::ZPOPMAX |
//             MsgType::ZPOPMIN |
//             MsgType::ZRANDMEMBER |
//             MsgType::ZRANGE |
//             MsgType::ZRANGEBYLEX |
//             MsgType::ZRANGEBYSCORE |
//             MsgType::ZRANGESTORE |
//             MsgType::ZREM |
//             MsgType::ZREVRANGE |
//             MsgType::ZREVRANGEBYLEX |
//             MsgType::ZREVRANGEBYSCORE |
//             MsgType::ZSCAN |
//             MsgType::ZUNION |
//             MsgType::ZUNIONSTORE |
//             MsgType::GEODIST |
//             MsgType::GEOPOS |
//             MsgType::GEOHASH |
//             MsgType::GEOADD |
//             MsgType::GEORADIUS |
//             MsgType::GEORADIUSBYMEMBER |
//             MsgType::GEOSEARCH |
//             MsgType::GEOSEARCHSTORE |
//             MsgType::RESTORE => true,
//             _ => false,
//         }
//     }
//
//
//     // Return true, if the redis command is a vector command accepting one or
//     // more keys, otherwise return false
//     pub fn redis_argx(&self) -> bool {
//         match self {
//             MsgType::MGET | MsgType::DEL | MsgType::UNLINK | MsgType::TOUCH => true,
//             _ => false,
//         }
//     }
//     // Return true, if the redis command is a vector command accepting one or
//     // more key-value pairs, otherwise return false
//     pub fn redis_argkvx(&self) -> bool {
//         match self {
//             MsgType::HMSET => true,
//             _ => false,
//         }
//     }
//     pub fn redis_argeval(&self) -> bool {
//         match self {
//             MsgType::EVAL | MsgType::EVALSHA => true,
//             _ => false,
//         }
//     }
// }
//
// pub struct ClientCodec {
//     token: Option<bool>,
//     state: State,
//     narg: usize,
//     rnarg: usize,
//     rlen: usize,
//     keys: Vec<Bytes>,
//     type_: MsgType,
//     buf_read: usize,
// }
//
// impl ClientCodec {
//     pub fn new() -> Self {
//         Self {
//             token: None,
//             state: State::SW_START,
//             narg: 0,
//             rnarg: 0,
//             rlen: 0,
//             keys: vec![],
//             type_: MsgType::UNKNOWN,
//             buf_read: 0,
//         }
//     }
// }
//
// pub enum State {
//     SW_START,
//     SW_NARG,
//     SW_NARG_LF,
//     SW_REQ_TYPE_LEN,
//     SW_REQ_TYPE_LEN_LF,
//     SW_REQ_TYPE,
//     SW_REQ_TYPE_LF,
//     SW_KEY_LEN,
//     SW_KEY_LEN_LF,
//     SW_KEY,
//     SW_KEY_LF,
//     SW_ARG1_LEN,
//     SW_ARG1_LEN_LF,
//     SW_ARG1,
//     SW_ARG1_LF,
//     SW_ARG2_LEN,
//     SW_ARG2_LEN_LF,
//     SW_ARG2,
//     SW_ARG2_LF,
//     SW_ARG3_LEN,
//     SW_ARG3_LEN_LF,
//     SW_ARG3,
//     SW_ARG3_LF,
//     SW_ARGN_LEN,
//     SW_ARGN_LEN_LF,
//     SW_ARGN,
//     SW_ARGN_LF,
//     SW_SENTINEL,
// }
//
// #[derive(Debug)]
// pub enum Pkt {
//     Key((Bytes, Vec<Bytes>)),
//     Raw(bytes::Bytes),
// }
//
// #[derive(Debug, PartialEq)]
// pub enum DecodeError {
//     InvalidProtocol,
//     NotEnoughData,
//     UnexpectedErr,
//     IOError,
// }
//
// impl From<std::io::Error> for DecodeError {
//     fn from(e: std::io::Error) -> Self {
//         DecodeError::IOError
//     }
// }
//
// impl From<anyhow::Error> for DecodeError {
//     fn from(e: anyhow::Error) -> Self {
//         DecodeError::UnexpectedErr
//     }
// }
//
// impl Decoder for ClientCodec {
//     type Item = Pkt;
//     type Error = DecodeError;
//
//     fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
//         if src.len() == 0 {
//             return Ok(None);
//         }
//         let bytes1 = src.freeze();
//         let buf = src.as_ref();
//
//         let mut p = buf;
//         let mut m = buf;
//
//         while p.len() > 0 {
//             println!(">>>> {:?}", p);
//             let ch = p[0];
//             match self.state {
//                 State::SW_START => {
//                     debug!("SW_START {:?}", p);
//                     if ch != b'*' {
//                         return Err(DecodeError::InvalidProtocol);
//                     }
//                     self.token = Some(true);
//                     self.narg = 0;
//                     self.state = State::SW_NARG;
//                 }
//                 State::SW_NARG => {
//                     debug!("SW_NARG, {:?}", p);
//                     if is_digit(ch) {
//                         self.rnarg = self.rnarg * 10 + (ch - b'0') as usize;
//                     } else if ch == CR {
//                         if self.rnarg == 0 {
//                             return Err(DecodeError::InvalidProtocol);
//                         }
//                         self.narg = self.rnarg;
//                         self.token = None;
//                         self.state = State::SW_NARG_LF;
//                     } else {
//                         return Err(DecodeError::InvalidProtocol);
//                     }
//                 }
//                 State::SW_NARG_LF => {
//                     debug!("SW_NARG_LF, {:?}", p);
//                     if ch != LF {
//                         return Err(DecodeError::InvalidProtocol);
//                     }
//                     self.state = State::SW_REQ_TYPE_LEN;
//                 }
//                 State::SW_REQ_TYPE_LEN => {
//                     debug!("SW_REQ_TYPE_LEN, {:?}", p);
//                     if self.token.is_none() {
//                         if ch != b'$' {
//                             return Err(DecodeError::InvalidProtocol);
//                         }
//                         self.token = Some(true);
//                         self.rlen = 0;
//                     } else if is_digit(ch) {
//                         self.rlen = self.rlen * 10 + (ch - b'0') as usize;
//                     } else if ch == CR {
//                         if self.rlen == 0 || self.rnarg == 0 {
//                             return Err(DecodeError::InvalidProtocol);
//                         }
//                         self.rnarg -= 1;
//                         self.token = None;
//                         self.state = State::SW_REQ_TYPE_LEN_LF;
//                     } else {
//                         return Err(DecodeError::InvalidProtocol);
//                     }
//                 }
//                 State::SW_REQ_TYPE_LEN_LF => {
//                     debug!("SW_REQ_TYPE_LEN_LF, {:?}", p);
//                     if ch != LF {
//                         return Err(DecodeError::InvalidProtocol);
//                     }
//                     self.state = State::SW_REQ_TYPE;
//                 }
//                 State::SW_REQ_TYPE => {
//                     debug!("SW_REQ_TYPE, {:?}", p);
//                     if self.token.is_none() {
//                         self.token = Some(true);
//                     }
//                     if p.len() < self.rlen {
//                         return Ok(None);
//                     }
//                     m = p;
//                     p.advance(self.rlen);
//                     self.type_ = MsgType::UNKNOWN;
//                     let m = &m[0..self.rlen];
//                     self.rlen = 0;
//
//                     if m == b"SET" {
//                         self.type_ = MsgType::SET;
//                     } else if m == b"DEL" {
//                         self.type_ = MsgType::DEL;
//                     }
//
//                     self.state = State::SW_REQ_TYPE_LF;
//                 }
//                 State::SW_REQ_TYPE_LF => {
//                     debug!("SW_REQ_TYPE_LF, {:?}", p);
//                     if ch != LF {
//                         return Err(DecodeError::InvalidProtocol);
//                     }
//                     if self.type_.redis_argz() {
//                         break;
//                     } else if self.type_.redis_nokey() {
//                         if self.narg == 1 {
//                             break;
//                         }
//                         self.state = State::SW_ARGN_LEN;
//                     } else if self.narg == 1 {
//                         return Err(DecodeError::InvalidProtocol);
//                     } else if self.type_.redis_argeval() {
//                         self.state = State::SW_ARG1_LEN;
//                     } else {
//                         self.state = State::SW_KEY_LEN;
//                     }
//                 }
//                 State::SW_KEY_LEN => {
//                     debug!("SW_KEY_LEN, {:?}", p);
//                     if self.token.is_none() {
//                         if ch != b'$' {
//                             return Err(DecodeError::InvalidProtocol);
//                         }
//                         self.token = Some(true);
//                         self.rlen = 0;
//                     } else if is_digit(ch) {
//                         self.rlen = self.rlen * 10 + (ch - b'0') as usize;
//                     } else if ch == CR {
//                         self.rnarg -= 1;
//                         self.token = None;
//                         self.state = State::SW_KEY_LEN_LF;
//                     }
//                 }
//                 State::SW_KEY_LEN_LF => {
//                     debug!("SW_KEY_LEN_LF, {:?}", p);
//                     if ch != LF {
//                         return Err(DecodeError::InvalidProtocol);
//                     }
//                     self.state = State::SW_KEY;
//                 }
//                 State::SW_KEY => {
//                     debug!("SW_KEY, {:?}", p);
//                     if self.token.is_none() {
//                         self.token = Some(true);
//                     }
//                     if p.len() < self.rlen {
//                         return Ok(None);
//                     }
//
//
//                     p.advance(self.rlen);
//                     m = self.token.take().unwrap();
//
//                     let key_bytes = m.split_to(self.rlen).freeze();
//                     self.keys.push(key_bytes);
//                     self.rlen = 0;
//                     self.state = State::SW_KEY_LF;
//                 }
//                 State::SW_KEY_LF => {
//                     debug!("SW_KEY_LF, {:?}", p);
//                     if ch != LF {
//                         return Err(DecodeError::InvalidProtocol);
//                     }
//                     if self.type_.redis_arg0() { // get a
//                         // key reading is done
//                         if self.rnarg != 0 {
//                             return Err(DecodeError::InvalidProtocol);
//                         }
//                         break;
//                     } else if self.type_.redis_arg1() { // INCRBY a 10
//                         // key reading is done
//                         if self.rnarg != 1 {
//                             return Err(DecodeError::InvalidProtocol);
//                         }
//                         self.state = State::SW_ARG1_LEN;
//                     } else if self.type_.redis_arg2() { // SETEX mykey 10 "Hello"
//                         // key reading is done
//                         if self.rnarg != 2 {
//                             return Err(DecodeError::InvalidProtocol);
//                         }
//                         self.state = State::SW_ARG1_LEN;
//                     } else if self.type_.redis_arg3() {
//                         if self.rnarg != 3 {
//                             return Err(DecodeError::InvalidProtocol);
//                         }
//                         self.state = State::SW_ARG1_LEN;
//                     } else if self.type_.redis_argn() { // set a b
//
//
//                         // key reading is done
//                         if self.rnarg == 0 {
//                             break;
//                         }
//                         self.state = State::SW_ARG1_LEN;
//                     } else if self.type_.redis_argx() { // del a b c d
//                         if self.rnarg == 0 {
//                             // key reading is done
//                             break;
//                         }
//                         // continue reading keys
//                         self.state = State::SW_KEY_LEN;
//                     } else if self.type_.redis_argkvx() {
//                         // todo
//                         self.state = State::SW_ARG1_LEN;
//                     } else if self.type_.redis_argeval() {
//                         // todo
//                         if self.rnarg == 0 {
//                             break;
//                         }
//                         self.state = State::SW_ARGN_LEN;
//                     } else {
//                         return Err(DecodeError::InvalidProtocol);
//                     }
//                 }
//                 State::SW_ARG1_LEN => {
//                     debug!("SW_ARG1_LEN, {:?}", p);
//                     if self.token.is_none() {
//                         if ch != b'$' {
//                             return Err(DecodeError::InvalidProtocol);
//                         }
//                         self.rlen = 0;
//                         self.token = Some(p.clone());
//                     } else if is_digit(ch) {
//                         self.rlen = self.rlen * 10 + (ch - b'0') as usize;
//                     } else if ch == CR {
//                         self.rnarg -= 1;
//                         self.token = None;
//                         self.state = State::SW_ARG1_LEN_LF;
//                     }
//                 }
//                 State::SW_ARG1_LEN_LF => {
//                     debug!("SW_ARG1_LEN_LF, {:?}", p);
//                     if ch != LF {
//                         return Err(DecodeError::InvalidProtocol);
//                     }
//                     self.state = State::SW_ARG1;
//                 }
//                 State::SW_ARG1 => {
//                     debug!("SW_ARG1, {:?}", p);
//                     if p.len() < self.rlen {
//                         src.split_to()
//                         return Err(DecodeError::NotEnoughData);
//                     }
//                     p.advance(self.rlen);
//                     self.rlen = 0;
//                     self.state = State::SW_ARG1_LF;
//                 }
//                 State::SW_ARG1_LF => {
//                     debug!("SW_ARG1_LF, {:?}", p);
//                     if ch != LF {
//                         return Err(DecodeError::InvalidProtocol);
//                     }
//
//                     if self.type_.redis_arg1() {
//                         break;
//                     } else if self.type_.redis_arg2() {
//                         self.state = State::SW_ARG2_LEN;
//                     } else if self.type_.redis_arg3() {
//                         self.state = State::SW_ARG2_LEN;
//                     } else if self.type_.redis_argn() {
//                         if self.rnarg == 0 {
//                             break;
//                         }
//                         self.state = State::SW_ARGN_LEN;
//                     } else if self.type_.redis_argeval() {
//                         self.state = State::SW_ARG2_LEN;
//                     } else if self.type_.redis_argkvx() {
//                         if self.rnarg == 0 { break; }
//                         self.state = State::SW_KEY_LEN;
//                     } else {
//                         return Err(DecodeError::InvalidProtocol);
//                     }
//                 }
//                 _ => {}
//             }
//             p.advance(1);
//         }
//         self.state = State::SW_START;
//         self.token = None;
//         println!(">>>> {:?}", self.keys);
//         Ok(None)
//     }
// }
//
// impl ClientCodec {}
//
// const CR: u8 = b'\r';
// const LF: u8 = b'\n';
//
// #[inline]
// fn is_digit(b: u8) -> bool {
//     b >= b'0' && b <= b'9'
// }
//
// #[cfg(test)]
// mod tests {
//     use super::*;
//
//     #[test]
//     fn test_bytes() {
//         let mut bytes = BytesMut::from("*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n");
//         let bytes = bytes.split();
//         let mut bytes = bytes.freeze();
//         let b2 = bytes.clone();
//         bytes.advance(1);
//         println!("{:?}", bytes);
//         println!("{:?}", b2);
//     }
//
//     #[test]
//     fn test_fn() {
//         env_logger::init();
//
//         let mut bytes = BytesMut::from("*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$1024\r\n");
//         // let mut bytes = BytesMut::from("*3\r\n$3\r\nDEL\r\n$5\r\naaaaa\r\n$5\r\nbbbbb\r\n");
//         println!("len: {} ,cap: {}", bytes.len(), bytes.capacity());
//         let mut c = ClientCodec::new();
//         let a = c.decode(&mut bytes);
//         println!(">>>>>>>>>>>{:?}", a);
//         bytes.extend_from_slice(generate_a_string().as_bytes());
//         let a = c.decode(&mut bytes);
//         println!(">>>>>>>>>>>{:?}", a);
//     }
//
//     fn generate_a_string() -> String {
//         std::iter::repeat('a').take(1024).collect()
//     }
// }