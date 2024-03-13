use enum_iterator::Sequence;

use crate::cmd::KeyInfo::NoKey;
use crate::tools::{str10icmp, str11icmp, str12icmp, str13icmp, str14icmp, str15icmp, str16icmp, str17icmp, str3icmp, str4icmp, str5icmp, str6icmp, str7icmp, str8icmp, str9icmp};

#[derive(Debug)]
pub enum KeyInfo {
    NoKey,
    OneKey,
    MultiKey,
    Whatever,
    Special,
}

#[derive(Debug, Clone, Copy, Sequence, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub enum CmdType {
    APPEND,
    AUTH,
    BGREWRITEAOF,
    BGSAVE,

    BITCOUNT,
    BITFIELD,
    // operation destkey key [key ...], BITOP OR destkey srckey1 srckey2 srckey3 ... srckeyN
    BITOP,
    BITPOS,
    BLPOP,
    BRPOP,
    BRPOPLPUSH,
    BZPOPMAX,
    BZPOPMIN,
    CLIENT,
    CLUSTER,
    COMMAND,
    CONFIG,
    DBSIZE,
    DEBUG,
    DECR,
    DECRBY,
    DEL,
    DISCARD,
    DUMP,
    ECHO,
    EVAL,
    EVALSHA,
    EXEC,
    EXISTS,
    EXPIRE,
    EXPIREAT,
    FLUSHALL,
    FLUSHDB,
    GEOADD,
    GEODIST,
    GEOHASH,
    GEOPOS,
    GEORADIUS,
    GEORADIUSBYMEMBER,
    GET,
    GETBIT,
    GETRANGE,
    GETSET,
    HDEL,
    HEXISTS,
    HGET,
    HGETALL,
    HINCRBY,
    HINCRBYFLOAT,
    HKEYS,
    HLEN,
    HMGET,
    HMSET,
    HSCAN,
    HSET,
    HSETNX,
    HSTRLEN,
    HVALS,
    INCR,
    INCRBY,
    INCRBYFLOAT,
    INFO,
    KEYS,
    LASTSAVE,
    LINDEX,
    LINSERT,
    LLEN,
    LOLWUT,
    LPOP,
    LPUSH,
    LPUSHX,
    LRANGE,
    LREM,
    LSET,
    LTRIM,
    MEMORY,
    MGET,
    MIGRATE,
    MONITOR,
    MOVE,
    MSET,
    MSETNX,
    MULTI,
    OBJECT,
    PERSIST,
    PEXPIRE,
    PEXPIREAT,
    PFADD,
    PFCOUNT,
    PFMERGE,
    PING,
    PSETEX,
    PSUBSCRIBE,
    PTTL,
    PUBLISH,
    PUBSUB,
    PUNSUBSCRIBE,
    QUIT,
    RANDOMKEY,
    READONLY,
    READWRITE,
    RENAME,
    RENAMENX,
    REPLICAOF,
    RESTORE,
    ROLE,
    RPOP,
    RPOPLPUSH,
    RPUSH,
    RPUSHX,
    SADD,
    SAVE,
    SCAN,
    SCARD,
    SCRIPT,
    SDIFF,
    SDIFFSTORE,
    SELECT,
    SET,
    SETBIT,
    SETEX,
    SETNX,
    SETRANGE,
    SHUTDOWN,
    SINTER,
    SINTERSTORE,
    SISMEMBER,
    SLAVEOF,
    SLOWLOG,
    SMEMBERS,
    SMOVE,
    SORT,
    SPOP,
    SRANDMEMBER,
    SREM,
    SSCAN,
    STRLEN,
    SUBSCRIBE,
    SUNION,
    SUNIONSTORE,
    SWAPDB,
    SYNC,
    TIME,
    TOUCH,
    TTL,
    TYPE,
    UNKNOWN,
    UNLINK,

    UNSUBSCRIBE,
    UNWATCH,
    WAIT,
    WATCH,
    // not implemented
    // XACK,
    // XADD,
    // XCLAIM,
    // XDEL,
    // XGROUP,
    // XINFO,
    // XLEN,
    // XPENDING,
    // XRANGE,
    // XREAD,
    // XREADGROUP,
    // XREVRANGE,
    // XTRIM,
    ZADD,
    ZCARD,
    ZCOUNT,
    ZINCRBY,
    ZINTERSTORE,
    ZLEXCOUNT,
    ZPOPMAX,
    ZPOPMIN,
    ZRANGE,
    ZRANGEBYLEX,
    ZRANGEBYSCORE,
    ZRANK,
    ZREM,
    ZREMRANGEBYLEX,
    ZREMRANGEBYRANK,
    ZREMRANGEBYSCORE,
    ZREVRANGE,
    ZREVRANGEBYLEX,
    ZREVRANGEBYSCORE,
    ZREVRANK,
    ZSCAN,
    ZSCORE,
    ZUNIONSTORE,
}

impl CmdType {
    pub fn is_read_cmd(&self) -> bool {
        return match self {
            CmdType::APPEND
            | CmdType::BITFIELD
            | CmdType::DECR
            | CmdType::DECRBY
            | CmdType::DEL
            | CmdType::EXPIRE
            | CmdType::EXPIREAT
            | CmdType::EVAL
            | CmdType::EVALSHA
            | CmdType::GEOADD
            | CmdType::HDEL
            | CmdType::HINCRBY
            | CmdType::HINCRBYFLOAT
            | CmdType::HMSET
            | CmdType::HSET
            | CmdType::HSETNX
            | CmdType::INCR
            | CmdType::INCRBY
            | CmdType::INCRBYFLOAT
            | CmdType::LINSERT
            | CmdType::LPOP
            | CmdType::LPUSH
            | CmdType::LPUSHX
            | CmdType::LREM
            | CmdType::LSET
            | CmdType::LTRIM
            | CmdType::MSET
            | CmdType::PERSIST
            | CmdType::PEXPIRE
            | CmdType::PEXPIREAT
            | CmdType::PFADD
            | CmdType::PSETEX
            | CmdType::RESTORE
            | CmdType::RPOP
            | CmdType::RPUSH
            | CmdType::RPUSHX
            | CmdType::SADD
            | CmdType::SET
            | CmdType::SETBIT
            | CmdType::SETEX
            | CmdType::SETNX
            | CmdType::SETRANGE
            | CmdType::SPOP
            | CmdType::SREM
            | CmdType::ZADD
            | CmdType::ZINCRBY
            | CmdType::TOUCH
            | CmdType::ZPOPMIN
            | CmdType::ZPOPMAX
            | CmdType::ZREM
            | CmdType::ZREMRANGEBYLEX
            | CmdType::ZREMRANGEBYRANK
            | CmdType::ZREMRANGEBYSCORE
            | CmdType::UNLINK => false,
            _ => true,
        };
    }

    pub fn is_connection_command(&self) -> bool {
        return match self {
            CmdType::AUTH |
            CmdType::ECHO |
            CmdType::PING |
            CmdType::QUIT |
            CmdType::SELECT |
            CmdType::SWAPDB => true,
            _ => false,
        };
    }

    pub fn should_lazy_read(&self) -> bool {
        return match self {
            CmdType::APPEND
            | CmdType::GETSET
            | CmdType::HMSET
            | CmdType::HSET
            | CmdType::HSETNX
            | CmdType::LPUSH
            | CmdType::LPUSHX
            | CmdType::LREM
            | CmdType::LSET
            | CmdType::MSET
            | CmdType::MSETNX
            | CmdType::PSETEX
            | CmdType::RESTORE
            | CmdType::RPUSH
            | CmdType::RPUSHX
            | CmdType::SET
            | CmdType::SETBIT
            | CmdType::SETEX
            | CmdType::SETNX
            | CmdType::SETRANGE => true,
            _ => false,
        };
    }

    pub fn redis_key_info(&self) -> KeyInfo {
        match self {
            // connection
            CmdType::AUTH
            | CmdType::ECHO
            | CmdType::PING
            | CmdType::QUIT
            | CmdType::SELECT
            | CmdType::SWAPDB
            | CmdType::BGREWRITEAOF
            | CmdType::BGSAVE
            | CmdType::CLIENT
            | CmdType::CLUSTER
            | CmdType::COMMAND
            | CmdType::CONFIG
            | CmdType::DBSIZE
            | CmdType::DEBUG
            | CmdType::DISCARD
            | CmdType::FLUSHALL
            | CmdType::FLUSHDB
            | CmdType::INFO
            | CmdType::LASTSAVE
            | CmdType::MEMORY
            | CmdType::MIGRATE
            | CmdType::MONITOR
            | CmdType::MULTI
            | CmdType::OBJECT
            | CmdType::PSUBSCRIBE
            | CmdType::REPLICAOF
            | CmdType::PUBLISH
            | CmdType::PUBSUB
            | CmdType::PUNSUBSCRIBE
            | CmdType::RANDOMKEY
            | CmdType::READONLY
            | CmdType::READWRITE
            | CmdType::ROLE
            | CmdType::SAVE
            | CmdType::SCAN
            | CmdType::SCRIPT
            | CmdType::SHUTDOWN
            | CmdType::SLAVEOF
            | CmdType::SLOWLOG
            | CmdType::SUBSCRIBE
            | CmdType::SYNC
            | CmdType::TIME
            | CmdType::UNSUBSCRIBE
            | CmdType::UNWATCH
            | CmdType::WAIT
            | CmdType::LOLWUT => NoKey,

            CmdType::APPEND
            | CmdType::BITCOUNT
            | CmdType::BITFIELD
            | CmdType::BITPOS
            | CmdType::DECR
            | CmdType::DECRBY
            | CmdType::DUMP
            | CmdType::EXPIRE
            | CmdType::EXPIREAT
            | CmdType::GET
            | CmdType::GETBIT
            | CmdType::GETRANGE
            | CmdType::GETSET
            | CmdType::HDEL
            | CmdType::HEXISTS
            | CmdType::HGET
            | CmdType::HGETALL
            | CmdType::HINCRBY
            | CmdType::HINCRBYFLOAT
            | CmdType::HKEYS
            | CmdType::HLEN
            | CmdType::HMGET
            | CmdType::HMSET
            | CmdType::HSCAN
            | CmdType::HSET
            | CmdType::HSETNX
            | CmdType::HSTRLEN
            | CmdType::HVALS
            | CmdType::INCR
            | CmdType::INCRBY
            | CmdType::INCRBYFLOAT
            | CmdType::LINDEX
            | CmdType::LINSERT
            | CmdType::LLEN
            | CmdType::LPOP
            | CmdType::LPUSH
            | CmdType::LPUSHX
            | CmdType::LRANGE
            | CmdType::LREM
            | CmdType::LSET
            | CmdType::LTRIM
            | CmdType::PERSIST
            | CmdType::PEXPIRE
            | CmdType::PEXPIREAT
            | CmdType::PFADD
            | CmdType::PSETEX
            | CmdType::PTTL
            | CmdType::RESTORE
            | CmdType::RPOP
            | CmdType::RPUSH
            | CmdType::RPUSHX
            | CmdType::SADD
            | CmdType::SCARD
            | CmdType::SET
            | CmdType::SETBIT
            | CmdType::SETEX
            | CmdType::SETNX
            | CmdType::SETRANGE
            | CmdType::SISMEMBER
            | CmdType::SMEMBERS
            | CmdType::SORT
            | CmdType::SPOP
            | CmdType::SRANDMEMBER
            | CmdType::SREM
            | CmdType::SSCAN
            | CmdType::STRLEN
            | CmdType::TTL
            | CmdType::TYPE
            | CmdType::ZADD
            | CmdType::ZCARD
            | CmdType::ZCOUNT
            | CmdType::ZINCRBY
            | CmdType::ZLEXCOUNT
            | CmdType::ZPOPMAX
            | CmdType::ZPOPMIN
            | CmdType::ZRANGE
            | CmdType::ZRANGEBYLEX
            | CmdType::ZRANGEBYSCORE
            | CmdType::ZRANK
            | CmdType::ZREM
            | CmdType::ZREMRANGEBYLEX
            | CmdType::ZREMRANGEBYRANK
            | CmdType::ZREMRANGEBYSCORE
            | CmdType::ZREVRANGE
            | CmdType::ZREVRANGEBYLEX
            | CmdType::ZREVRANGEBYSCORE
            | CmdType::ZREVRANK
            | CmdType::ZSCAN
            | CmdType::ZSCORE
            | CmdType::MOVE => {
                return KeyInfo::OneKey;
            }

            CmdType::DEL
            | CmdType::EXISTS
            | CmdType::MGET
            | CmdType::PFCOUNT
            | CmdType::PFMERGE
            | CmdType::RENAME
            | CmdType::RENAMENX
            | CmdType::SDIFF
            | CmdType::SDIFFSTORE
            | CmdType::SINTER
            | CmdType::SINTERSTORE
            | CmdType::TOUCH
            | CmdType::UNLINK
            | CmdType::WATCH
            | CmdType::SUNION
            | CmdType::SUNIONSTORE => {
                return KeyInfo::MultiKey;
            }

            CmdType::BLPOP
            | CmdType::BRPOP
            | CmdType::BZPOPMAX
            | CmdType::BZPOPMIN
            | CmdType::BITOP
            | CmdType::MSET
            | CmdType::MSETNX
            | CmdType::SMOVE
            | CmdType::ZINTERSTORE
            | CmdType::ZUNIONSTORE => {
                return KeyInfo::Special;
            }

            CmdType::BRPOPLPUSH => KeyInfo::Whatever,
            CmdType::KEYS => {
                return KeyInfo::Whatever;
            }

            CmdType::EVAL | CmdType::EVALSHA | CmdType::EXEC => {
                return KeyInfo::Whatever;
            }
            CmdType::GEOADD
            | CmdType::GEODIST
            | CmdType::GEOHASH
            | CmdType::GEOPOS
            | CmdType::GEORADIUS
            | CmdType::GEORADIUSBYMEMBER => {
                return KeyInfo::Whatever;
            }
            CmdType::RPOPLPUSH => {
                return KeyInfo::Whatever;
            }

            CmdType::UNKNOWN => {
                return KeyInfo::NoKey;
            }
        }
    }
}

impl From<&[u8]> for CmdType {
    fn from(cmd: &[u8]) -> Self {
        let len = cmd.len();
        return match len {
            3 => {
                if str3icmp(cmd, b'g', b'e', b't') {
                    return CmdType::GET;
                }
                if str3icmp(cmd, b's', b'e', b't') {
                    return CmdType::SET;
                }
                if str3icmp(cmd, b'd', b'e', b'l') {
                    return CmdType::DEL;
                }
                if str3icmp(cmd, b't', b't', b'l') {
                    return CmdType::TTL;
                }
                CmdType::UNKNOWN
            }
            4 => {
                if str4icmp(cmd, b'd', b'e', b'c', b'r') {
                    return CmdType::DECR;
                }
                if str4icmp(cmd, b'd', b'u', b'm', b'p') {
                    return CmdType::DUMP;
                }
                if str4icmp(cmd, b'e', b'c', b'h', b'o') {
                    return CmdType::ECHO;
                }
                if str4icmp(cmd, b'e', b'v', b'a', b'l') {
                    return CmdType::EVAL;
                }
                if str4icmp(cmd, b'e', b'x', b'e', b'c') {
                    return CmdType::EXEC;
                }
                if str4icmp(cmd, b'h', b'd', b'e', b'l') {
                    return CmdType::HDEL;
                }
                if str4icmp(cmd, b'h', b'g', b'e', b't') {
                    return CmdType::HGET;
                }
                if str4icmp(cmd, b'h', b'l', b'e', b'n') {
                    return CmdType::HLEN;
                }
                if str4icmp(cmd, b'h', b's', b'e', b't') {
                    return CmdType::HSET;
                }
                if str4icmp(cmd, b'i', b'n', b'c', b'r') {
                    return CmdType::INCR;
                }
                if str4icmp(cmd, b'i', b'n', b'f', b'o') {
                    return CmdType::INFO;
                }
                if str4icmp(cmd, b'k', b'e', b'y', b's') {
                    return CmdType::KEYS;
                }
                if str4icmp(cmd, b'l', b'l', b'e', b'n') {
                    return CmdType::LLEN;
                }
                if str4icmp(cmd, b'l', b'p', b'o', b'p') {
                    return CmdType::LPOP;
                }
                if str4icmp(cmd, b'l', b'r', b'e', b'm') {
                    return CmdType::LREM;
                }
                if str4icmp(cmd, b'l', b's', b'e', b't') {
                    return CmdType::LSET;
                }
                if str4icmp(cmd, b'm', b'g', b'e', b't') {
                    return CmdType::MGET;
                }
                if str4icmp(cmd, b'm', b'o', b'v', b'e') {
                    return CmdType::MOVE;
                }
                if str4icmp(cmd, b'm', b's', b'e', b't') {
                    return CmdType::MSET;
                }
                if str4icmp(cmd, b'p', b'i', b'n', b'g') {
                    return CmdType::PING;
                }
                if str4icmp(cmd, b'p', b't', b't', b'l') {
                    return CmdType::PTTL;
                }
                if str4icmp(cmd, b'q', b'u', b'i', b't') {
                    return CmdType::QUIT;
                }
                if str4icmp(cmd, b'r', b'o', b'l', b'e') {
                    return CmdType::ROLE;
                }
                if str4icmp(cmd, b'r', b'p', b'o', b'p') {
                    return CmdType::RPOP;
                }
                if str4icmp(cmd, b's', b'a', b'd', b'd') {
                    return CmdType::SADD;
                }
                if str4icmp(cmd, b's', b'a', b'v', b'e') {
                    return CmdType::SAVE;
                }
                if str4icmp(cmd, b's', b'c', b'a', b'n') {
                    return CmdType::SCAN;
                }
                if str4icmp(cmd, b's', b'o', b'r', b't') {
                    return CmdType::SORT;
                }
                if str4icmp(cmd, b's', b'p', b'o', b'p') {
                    return CmdType::SPOP;
                }
                if str4icmp(cmd, b's', b'r', b'e', b'm') {
                    return CmdType::SREM;
                }
                if str4icmp(cmd, b's', b'y', b'n', b'c') {
                    return CmdType::SYNC;
                }
                if str4icmp(cmd, b't', b'i', b'm', b'e') {
                    return CmdType::TIME;
                }
                if str4icmp(cmd, b't', b'y', b'p', b'e') {
                    return CmdType::TYPE;
                }
                if str4icmp(cmd, b'w', b'a', b'i', b't') {
                    return CmdType::WAIT;
                }
                if str4icmp(cmd, b'z', b'a', b'd', b'd') {
                    return CmdType::ZADD;
                }
                if str4icmp(cmd, b'z', b'r', b'e', b'm') {
                    return CmdType::ZREM;
                }
                if str4icmp(cmd, b'a', b'u', b't', b'h') {
                    return CmdType::AUTH;
                }
                CmdType::UNKNOWN
            }
            5 => {
                if str5icmp(cmd, b'b', b'i', b't', b'o', b'p') {
                    return CmdType::BITOP;
                }
                if str5icmp(cmd, b'b', b'l', b'p', b'o', b'p') {
                    return CmdType::BLPOP;
                }
                if str5icmp(cmd, b'b', b'r', b'p', b'o', b'p') {
                    return CmdType::BRPOP;
                }
                if str5icmp(cmd, b'd', b'e', b'b', b'u', b'g') {
                    return CmdType::DEBUG;
                }
                if str5icmp(cmd, b'h', b'k', b'e', b'y', b's') {
                    return CmdType::HKEYS;
                }
                if str5icmp(cmd, b'h', b'm', b'g', b'e', b't') {
                    return CmdType::HMGET;
                }
                if str5icmp(cmd, b'h', b'm', b's', b'e', b't') {
                    return CmdType::HMSET;
                }
                if str5icmp(cmd, b'h', b's', b'c', b'a', b'n') {
                    return CmdType::HSCAN;
                }
                if str5icmp(cmd, b'h', b'v', b'a', b'l', b's') {
                    return CmdType::HVALS;
                }
                if str5icmp(cmd, b'l', b'p', b'u', b's', b'h') {
                    return CmdType::LPUSH;
                }
                if str5icmp(cmd, b'l', b't', b'r', b'i', b'm') {
                    return CmdType::LTRIM;
                }
                if str5icmp(cmd, b'm', b'u', b'l', b't', b'i') {
                    return CmdType::MULTI;
                }
                if str5icmp(cmd, b'p', b'f', b'a', b'd', b'd') {
                    return CmdType::PFADD;
                }
                if str5icmp(cmd, b'r', b'p', b'u', b's', b'h') {
                    return CmdType::RPUSH;
                }
                if str5icmp(cmd, b's', b'c', b'a', b'r', b'd') {
                    return CmdType::SCARD;
                }
                if str5icmp(cmd, b's', b'd', b'i', b'f', b'f') {
                    return CmdType::SDIFF;
                }
                if str5icmp(cmd, b's', b'e', b't', b'e', b'x') {
                    return CmdType::SETEX;
                }
                if str5icmp(cmd, b's', b'e', b't', b'n', b'x') {
                    return CmdType::SETNX;
                }
                if str5icmp(cmd, b's', b'm', b'o', b'v', b'e') {
                    return CmdType::SMOVE;
                }
                if str5icmp(cmd, b's', b's', b'c', b'a', b'n') {
                    return CmdType::SSCAN;
                }
                if str5icmp(cmd, b't', b'o', b'u', b'c', b'h') {
                    return CmdType::TOUCH;
                }
                if str5icmp(cmd, b'w', b'a', b't', b'c', b'h') {
                    return CmdType::WATCH;
                }
                if str5icmp(cmd, b'z', b'c', b'a', b'r', b'd') {
                    return CmdType::ZCARD;
                }
                if str5icmp(cmd, b'z', b'r', b'a', b'n', b'k') {
                    return CmdType::ZRANK;
                }
                if str5icmp(cmd, b'z', b's', b'c', b'a', b'n') {
                    return CmdType::ZSCAN;
                }
                CmdType::UNKNOWN
            }
            6 => {
                if str6icmp(cmd, b'a', b'p', b'p', b'e', b'n', b'd') {
                    return CmdType::APPEND;
                }
                if str6icmp(cmd, b'b', b'g', b's', b'a', b'v', b'e') {
                    return CmdType::BGSAVE;
                }
                if str6icmp(cmd, b'b', b'i', b't', b'p', b'o', b's') {
                    return CmdType::BITPOS;
                }
                if str6icmp(cmd, b'c', b'l', b'i', b'e', b'n', b't') {
                    return CmdType::CLIENT;
                }
                if str6icmp(cmd, b'c', b'o', b'n', b'f', b'i', b'g') {
                    return CmdType::CONFIG;
                }
                if str6icmp(cmd, b'd', b'b', b's', b'i', b'z', b'e') {
                    return CmdType::DBSIZE;
                }
                if str6icmp(cmd, b'd', b'e', b'c', b'r', b'b', b'y') {
                    return CmdType::DECRBY;
                }
                if str6icmp(cmd, b'e', b'x', b'i', b's', b't', b's') {
                    return CmdType::EXISTS;
                }
                if str6icmp(cmd, b'e', b'x', b'p', b'i', b'r', b'e') {
                    return CmdType::EXPIRE;
                }
                if str6icmp(cmd, b'g', b'e', b'o', b'a', b'd', b'd') {
                    return CmdType::GEOADD;
                }
                if str6icmp(cmd, b'g', b'e', b'o', b'p', b'o', b's') {
                    return CmdType::GEOPOS;
                }
                if str6icmp(cmd, b'g', b'e', b't', b'b', b'i', b't') {
                    return CmdType::GETBIT;
                }
                if str6icmp(cmd, b'g', b'e', b't', b's', b'e', b't') {
                    return CmdType::GETSET;
                }
                if str6icmp(cmd, b'h', b's', b'e', b't', b'n', b'x') {
                    return CmdType::HSETNX;
                }
                if str6icmp(cmd, b'i', b'n', b'c', b'r', b'b', b'y') {
                    return CmdType::INCRBY;
                }
                if str6icmp(cmd, b'l', b'i', b'n', b'd', b'e', b'x') {
                    return CmdType::LINDEX;
                }
                if str6icmp(cmd, b'l', b'o', b'l', b'w', b'u', b't') {
                    return CmdType::LOLWUT;
                }
                if str6icmp(cmd, b'l', b'p', b'u', b's', b'h', b'x') {
                    return CmdType::LPUSHX;
                }
                if str6icmp(cmd, b'l', b'r', b'a', b'n', b'g', b'e') {
                    return CmdType::LRANGE;
                }
                if str6icmp(cmd, b'm', b'e', b'm', b'o', b'r', b'y') {
                    return CmdType::MEMORY;
                }
                if str6icmp(cmd, b'm', b's', b'e', b't', b'n', b'x') {
                    return CmdType::MSETNX;
                }
                if str6icmp(cmd, b'o', b'b', b'j', b'e', b'c', b't') {
                    return CmdType::OBJECT;
                }
                if str6icmp(cmd, b'p', b's', b'e', b't', b'e', b'x') {
                    return CmdType::PSETEX;
                }
                if str6icmp(cmd, b'p', b'u', b'b', b's', b'u', b'b') {
                    return CmdType::PUBSUB;
                }
                if str6icmp(cmd, b'r', b'e', b'n', b'a', b'm', b'e') {
                    return CmdType::RENAME;
                }
                if str6icmp(cmd, b'r', b'p', b'u', b's', b'h', b'x') {
                    return CmdType::RPUSHX;
                }
                if str6icmp(cmd, b's', b'c', b'r', b'i', b'p', b't') {
                    return CmdType::SCRIPT;
                }
                if str6icmp(cmd, b's', b'e', b'l', b'e', b'c', b't') {
                    return CmdType::SELECT;
                }
                if str6icmp(cmd, b's', b'e', b't', b'b', b'i', b't') {
                    return CmdType::SETBIT;
                }
                if str6icmp(cmd, b's', b'i', b'n', b't', b'e', b'r') {
                    return CmdType::SINTER;
                }
                if str6icmp(cmd, b's', b't', b'r', b'l', b'e', b'n') {
                    return CmdType::STRLEN;
                }
                if str6icmp(cmd, b's', b'u', b'n', b'i', b'o', b'n') {
                    return CmdType::SUNION;
                }
                if str6icmp(cmd, b's', b'w', b'a', b'p', b'd', b'b') {
                    return CmdType::SWAPDB;
                }
                if str6icmp(cmd, b'u', b'n', b'l', b'i', b'n', b'k') {
                    return CmdType::UNLINK;
                }
                if str6icmp(cmd, b'z', b'c', b'o', b'u', b'n', b't') {
                    return CmdType::ZCOUNT;
                }
                if str6icmp(cmd, b'z', b'r', b'a', b'n', b'g', b'e') {
                    return CmdType::ZRANGE;
                }
                if str6icmp(cmd, b'z', b's', b'c', b'o', b'r', b'e') {
                    return CmdType::ZSCORE;
                }
                CmdType::UNKNOWN
            }
            7 => {
                if str7icmp(cmd, b'c', b'l', b'u', b's', b't', b'e', b'r') {
                    return CmdType::CLUSTER;
                }
                if str7icmp(cmd, b'c', b'o', b'm', b'm', b'a', b'n', b'd') {
                    return CmdType::COMMAND;
                }
                if str7icmp(cmd, b'd', b'i', b's', b'c', b'a', b'r', b'd') {
                    return CmdType::DISCARD;
                }
                if str7icmp(cmd, b'e', b'v', b'a', b'l', b's', b'h', b'a') {
                    return CmdType::EVALSHA;
                }
                if str7icmp(cmd, b'f', b'l', b'u', b's', b'h', b'd', b'b') {
                    return CmdType::FLUSHDB;
                }
                if str7icmp(cmd, b'g', b'e', b'o', b'd', b'i', b's', b't') {
                    return CmdType::GEODIST;
                }
                if str7icmp(cmd, b'g', b'e', b'o', b'h', b'a', b's', b'h') {
                    return CmdType::GEOHASH;
                }
                if str7icmp(cmd, b'h', b'e', b'x', b'i', b's', b't', b's') {
                    return CmdType::HEXISTS;
                }
                if str7icmp(cmd, b'h', b'g', b'e', b't', b'a', b'l', b'l') {
                    return CmdType::HGETALL;
                }
                if str7icmp(cmd, b'h', b'i', b'n', b'c', b'r', b'b', b'y') {
                    return CmdType::HINCRBY;
                }
                if str7icmp(cmd, b'h', b's', b't', b'r', b'l', b'e', b'n') {
                    return CmdType::HSTRLEN;
                }
                if str7icmp(cmd, b'l', b'i', b'n', b's', b'e', b'r', b't') {
                    return CmdType::LINSERT;
                }
                if str7icmp(cmd, b'm', b'i', b'g', b'r', b'a', b't', b'e') {
                    return CmdType::MIGRATE;
                }
                if str7icmp(cmd, b'm', b'o', b'n', b'i', b't', b'o', b'r') {
                    return CmdType::MONITOR;
                }
                if str7icmp(cmd, b'p', b'e', b'r', b's', b'i', b's', b't') {
                    return CmdType::PERSIST;
                }
                if str7icmp(cmd, b'p', b'e', b'x', b'p', b'i', b'r', b'e') {
                    return CmdType::PEXPIRE;
                }
                if str7icmp(cmd, b'p', b'f', b'c', b'o', b'u', b'n', b't') {
                    return CmdType::PFCOUNT;
                }
                if str7icmp(cmd, b'p', b'f', b'm', b'e', b'r', b'g', b'e') {
                    return CmdType::PFMERGE;
                }
                if str7icmp(cmd, b'p', b'u', b'b', b'l', b'i', b's', b'h') {
                    return CmdType::PUBLISH;
                }
                if str7icmp(cmd, b'r', b'e', b's', b't', b'o', b'r', b'e') {
                    return CmdType::RESTORE;
                }
                if str7icmp(cmd, b's', b'l', b'a', b'v', b'e', b'o', b'f') {
                    return CmdType::SLAVEOF;
                }
                if str7icmp(cmd, b's', b'l', b'o', b'w', b'l', b'o', b'g') {
                    return CmdType::SLOWLOG;
                }
                if str7icmp(cmd, b'u', b'n', b'k', b'n', b'o', b'w', b'n') {
                    return CmdType::UNKNOWN;
                }
                if str7icmp(cmd, b'u', b'n', b'w', b'a', b't', b'c', b'h') {
                    return CmdType::UNWATCH;
                }
                if str7icmp(cmd, b'z', b'i', b'n', b'c', b'r', b'b', b'y') {
                    return CmdType::ZINCRBY;
                }
                if str7icmp(cmd, b'z', b'p', b'o', b'p', b'm', b'a', b'x') {
                    return CmdType::ZPOPMAX;
                }
                if str7icmp(cmd, b'z', b'p', b'o', b'p', b'm', b'i', b'n') {
                    return CmdType::ZPOPMIN;
                }
                CmdType::UNKNOWN
            }
            8 => {
                if str8icmp(cmd, b'b', b'i', b't', b'c', b'o', b'u', b'n', b't') {
                    return CmdType::BITCOUNT;
                }
                if str8icmp(cmd, b'b', b'i', b't', b'f', b'i', b'e', b'l', b'd') {
                    return CmdType::BITFIELD;
                }
                if str8icmp(cmd, b'b', b'z', b'p', b'o', b'p', b'm', b'a', b'x') {
                    return CmdType::BZPOPMAX;
                }
                if str8icmp(cmd, b'b', b'z', b'p', b'o', b'p', b'm', b'i', b'n') {
                    return CmdType::BZPOPMIN;
                }
                if str8icmp(cmd, b'e', b'x', b'p', b'i', b'r', b'e', b'a', b't') {
                    return CmdType::EXPIREAT;
                }
                if str8icmp(cmd, b'f', b'l', b'u', b's', b'h', b'a', b'l', b'l') {
                    return CmdType::FLUSHALL;
                }
                if str8icmp(cmd, b'g', b'e', b't', b'r', b'a', b'n', b'g', b'e') {
                    return CmdType::GETRANGE;
                }
                if str8icmp(cmd, b'l', b'a', b's', b't', b's', b'a', b'v', b'e') {
                    return CmdType::LASTSAVE;
                }
                if str8icmp(cmd, b'r', b'e', b'a', b'd', b'o', b'n', b'l', b'y') {
                    return CmdType::READONLY;
                }
                if str8icmp(cmd, b'r', b'e', b'n', b'a', b'm', b'e', b'n', b'x') {
                    return CmdType::RENAMENX;
                }
                if str8icmp(cmd, b's', b'e', b't', b'r', b'a', b'n', b'g', b'e') {
                    return CmdType::SETRANGE;
                }
                if str8icmp(cmd, b's', b'h', b'u', b't', b'd', b'o', b'w', b'n') {
                    return CmdType::SHUTDOWN;
                }
                if str8icmp(cmd, b's', b'm', b'e', b'm', b'b', b'e', b'r', b's') {
                    return CmdType::SMEMBERS;
                }
                if str8icmp(cmd, b'z', b'r', b'e', b'v', b'r', b'a', b'n', b'k') {
                    return CmdType::ZREVRANK;
                }
                CmdType::UNKNOWN
            }
            9 => {
                if str9icmp(cmd, b'g', b'e', b'o', b'r', b'a', b'd', b'i', b'u', b's') {
                    return CmdType::GEORADIUS;
                }
                if str9icmp(cmd, b'p', b'e', b'x', b'p', b'i', b'r', b'e', b'a', b't') {
                    return CmdType::PEXPIREAT;
                }
                if str9icmp(cmd, b'r', b'a', b'n', b'd', b'o', b'm', b'k', b'e', b'y') {
                    return CmdType::RANDOMKEY;
                }
                if str9icmp(cmd, b'r', b'e', b'a', b'd', b'w', b'r', b'i', b't', b'e') {
                    return CmdType::READWRITE;
                }
                if str9icmp(cmd, b'r', b'e', b'p', b'l', b'i', b'c', b'a', b'o', b'f') {
                    return CmdType::REPLICAOF;
                }
                if str9icmp(cmd, b'r', b'p', b'o', b'p', b'l', b'p', b'u', b's', b'h') {
                    return CmdType::RPOPLPUSH;
                }
                if str9icmp(cmd, b's', b'i', b's', b'm', b'e', b'm', b'b', b'e', b'r') {
                    return CmdType::SISMEMBER;
                }
                if str9icmp(cmd, b's', b'u', b'b', b's', b'c', b'r', b'i', b'b', b'e') {
                    return CmdType::SUBSCRIBE;
                }
                if str9icmp(cmd, b'z', b'l', b'e', b'x', b'c', b'o', b'u', b'n', b't') {
                    return CmdType::ZLEXCOUNT;
                }
                if str9icmp(cmd, b'z', b'r', b'e', b'v', b'r', b'a', b'n', b'g', b'e') {
                    return CmdType::ZREVRANGE;
                }
                CmdType::UNKNOWN
            }
            10 => {
                if str10icmp(cmd, b'b', b'r', b'p', b'o', b'p', b'l', b'p', b'u', b's', b'h') {
                    return CmdType::BRPOPLPUSH;
                }
                if str10icmp(cmd, b'p', b's', b'u', b'b', b's', b'c', b'r', b'i', b'b', b'e') {
                    return CmdType::PSUBSCRIBE;
                }
                if str10icmp(cmd, b's', b'd', b'i', b'f', b'f', b's', b't', b'o', b'r', b'e') {
                    return CmdType::SDIFFSTORE;
                }
                CmdType::UNKNOWN
            }
            11 => {
                if str11icmp(cmd, b'i', b'n', b'c', b'r', b'b', b'y', b'f', b'l', b'o', b'a', b't') {
                    return CmdType::INCRBYFLOAT;
                }
                if str11icmp(cmd, b's', b'i', b'n', b't', b'e', b'r', b's', b't', b'o', b'r', b'e') {
                    return CmdType::SINTERSTORE;
                }
                if str11icmp(cmd, b's', b'r', b'a', b'n', b'd', b'm', b'e', b'm', b'b', b'e', b'r') {
                    return CmdType::SRANDMEMBER;
                }
                if str11icmp(cmd, b's', b'u', b'n', b'i', b'o', b'n', b's', b't', b'o', b'r', b'e') {
                    return CmdType::SUNIONSTORE;
                }
                if str11icmp(cmd, b'u', b'n', b's', b'u', b'b', b's', b'c', b'r', b'i', b'b', b'e') {
                    return CmdType::UNSUBSCRIBE;
                }
                if str11icmp(cmd, b'z', b'i', b'n', b't', b'e', b'r', b's', b't', b'o', b'r', b'e') {
                    return CmdType::ZINTERSTORE;
                }
                if str11icmp(cmd, b'z', b'r', b'a', b'n', b'g', b'e', b'b', b'y', b'l', b'e', b'x') {
                    return CmdType::ZRANGEBYLEX;
                }
                if str11icmp(cmd, b'z', b'u', b'n', b'i', b'o', b'n', b's', b't', b'o', b'r', b'e') {
                    return CmdType::ZUNIONSTORE;
                }
                CmdType::UNKNOWN
            }
            12 => {
                if str12icmp(cmd, b'b', b'g', b'r', b'e', b'w', b'r', b'i', b't', b'e', b'a', b'o', b'f') {
                    return CmdType::BGREWRITEAOF;
                }
                if str12icmp(cmd, b'h', b'i', b'n', b'c', b'r', b'b', b'y', b'f', b'l', b'o', b'a', b't') {
                    return CmdType::HINCRBYFLOAT;
                }
                if str12icmp(cmd, b'p', b'u', b'n', b's', b'u', b'b', b's', b'c', b'r', b'i', b'b', b'e') {
                    return CmdType::PUNSUBSCRIBE;
                }
                CmdType::UNKNOWN
            }
            13 => {
                if str13icmp(cmd, b'z', b'r', b'a', b'n', b'g', b'e', b'b', b'y', b's', b'c', b'o', b'r', b'e') {
                    return CmdType::ZRANGEBYSCORE;
                }
                CmdType::UNKNOWN
            }
            14 => {
                if str14icmp(cmd, b'z', b'r', b'e', b'm', b'r', b'a', b'n', b'g', b'e', b'b', b'y', b'l', b'e', b'x') {
                    return CmdType::ZREMRANGEBYLEX;
                }
                if str14icmp(cmd, b'z', b'r', b'e', b'v', b'r', b'a', b'n', b'g', b'e', b'b', b'y', b'l', b'e', b'x') {
                    return CmdType::ZREVRANGEBYLEX;
                }
                CmdType::UNKNOWN
            }
            15 => {
                if str15icmp(cmd, b'z', b'r', b'e', b'm', b'r', b'a', b'n', b'g', b'e', b'b', b'y', b'r', b'a', b'n', b'k') {
                    return CmdType::ZREMRANGEBYRANK;
                }
                CmdType::UNKNOWN
            }
            16 => {
                if str16icmp(cmd, b'z', b'r', b'e', b'm', b'r', b'a', b'n', b'g', b'e', b'b', b'y', b's', b'c', b'o', b'r', b'e') {
                    return CmdType::ZREMRANGEBYSCORE;
                }
                if str16icmp(cmd, b'z', b'r', b'e', b'v', b'r', b'a', b'n', b'g', b'e', b'b', b'y', b's', b'c', b'o', b'r', b'e') {
                    return CmdType::ZREVRANGEBYSCORE;
                }
                CmdType::UNKNOWN
            }
            17 => {
                if str17icmp(cmd, b'g', b'e', b'o', b'r', b'a', b'd', b'i', b'u', b's', b'b', b'y', b'm', b'e', b'm', b'b', b'e', b'r') {
                    return CmdType::GEORADIUSBYMEMBER;
                }
                CmdType::UNKNOWN
            }
            _ => { CmdType::UNKNOWN }
        };
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use enum_iterator::all;

    use super::*;

    #[test]
    fn test_cmd_type_iterator() {
        let mut iter = all::<CmdType>().collect::<Vec<CmdType>>();
        let name_list = iter.iter().map(|x| format!("{:?}", x)).collect::<Vec<String>>();
        let max_len = name_list.iter().max_by(|a, b| a.len().cmp(&b.len())).map(|it| it.len());
        let mut map = HashMap::new();
        for i in 0..18 {
            map.insert(i, vec![]);
        }
        println!("{:?}", max_len);
        for cmd_name in name_list {
            let entry = map.get_mut(&cmd_name.len()).unwrap();
            entry.push(cmd_name);
        }
        let mut str = "match len {\n".to_string();
        for len in 3usize..=17 {
            let list = map.get(&len).unwrap();
            if list.is_empty() {
                continue;
            }

            str.push_str(&format!("{}=> ", len));
            str.push_str("{\n");
            for cmd_name in list {
                str.push_str("if str");
                str.push_str(&format!("{}", len));
                str.push_str("icmp(cmd, ");
                let part = cmd_name.chars().map(|it| it.to_ascii_lowercase()).map(|it| format!("b'{}'", it)).collect::<Vec<String>>().join(", ");
                str.push_str(&part);

                str.push_str("){");
                str.push_str("\n return CmdType::");
                str.push_str(&cmd_name);
                str.push_str(";\n");
                str.push_str("}\n");
            }

            str.push_str("return CmdType::UNKNOWN;\n");
            str.push_str("}\n");
        }
        println!("{}", str);
        // for x in iter {
        //     println!("{:?}", x);
        // }
    }

    #[test]
    fn test_cmd_parse() {
        let cmd = "command";
        let cmd_type = CmdType::from(cmd.as_bytes());
        println!("{:?}", cmd_type);
    }
}
