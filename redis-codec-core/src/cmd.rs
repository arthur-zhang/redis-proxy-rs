use crate::tools::{str3icmp, str4icmp};

pub enum CmdType {
    UNKNOWN,

    PING,
    QUIT,
    COMMAND,

    LOLWUT,

    EVAL,
    EVALSHA,

    MGET,
    DEL,
    UNLINK,
    TOUCH,

    SORT,
    COPY,
    BITCOUNT,
    BITPOS,
    BITFIELD,
    EXISTS,
    GETEX,
    SET,
    HDEL,
    HMGET,
    HMSET,
    HSCAN,
    HSET,
    HRANDFIELD,
    LPUSH,
    LPUSHX,
    RPUSH,
    RPUSHX,
    LPOP,
    RPOP,
    LPOS,
    SADD,
    SDIFF,
    SDIFFSTORE,
    SINTER,
    SINTERSTORE,
    SREM,
    SUNION,
    SUNIONSTORE,
    SRANDMEMBER,
    SSCAN,
    SPOP,
    SMISMEMBER,
    PFADD,
    PFMERGE,
    PFCOUNT,
    ZADD,
    ZDIFF,
    ZDIFFSTORE,
    ZINTER,
    ZINTERSTORE,
    ZMSCORE,
    ZPOPMAX,
    ZPOPMIN,
    ZRANDMEMBER,
    ZRANGE,
    ZRANGEBYLEX,
    ZRANGEBYSCORE,
    ZRANGESTORE,
    ZREM,
    ZREVRANGE,
    ZREVRANGEBYLEX,
    ZREVRANGEBYSCORE,
    ZSCAN,
    ZUNION,
    ZUNIONSTORE,
    GEODIST,
    GEOPOS,
    GEOHASH,
    GEOADD,
    GEORADIUS,
    GEORADIUSBYMEMBER,
    GEOSEARCH,
    GEOSEARCHSTORE,
    RESTORE,

    LINSERT,
    LMOVE,

    GETRANGE,
    PSETEX,
    SETBIT,
    SETEX,
    SETRANGE,

    HINCRBY,
    HINCRBYFLOAT,
    HSETNX,

    LRANGE,
    LREM,
    LSET,
    LTRIM,

    SMOVE,

    ZCOUNT,
    ZLEXCOUNT,
    ZINCRBY,
    ZREMRANGEBYLEX,
    ZREMRANGEBYRANK,
    ZREMRANGEBYSCORE,

    EXPIRE,
    EXPIREAT,
    PEXPIRE,
    PEXPIREAT,
    MOVE,

    APPEND,
    DECRBY,
    GETBIT,
    GETSET,
    INCRBY,
    INCRBYFLOAT,
    SETNX,

    HEXISTS,
    HGET,
    HSTRLEN,

    LINDEX,
    RPOPLPUSH,

    SISMEMBER,

    ZRANK,
    ZREVRANK,
    ZSCORE,

    PERSIST,
    PTTL,
    TTL,
    TYPE,
    DUMP,

    DECR,
    GET,
    GETDEL,
    INCR,
    STRLEN,

    HGETALL,
    HKEYS,
    HLEN,
    HVALS,

    LLEN,

    SCARD,
    SMEMBERS,

    ZCARD,
    AUTH,
}

impl CmdType {
    // Return true, if the redis command accepts no key
    pub fn redis_argz(&self) -> bool {
        match self {
            CmdType::PING | CmdType::QUIT | CmdType::COMMAND => true,
            _ => false,
        }
    }

    pub fn redis_nokey(&self) -> bool {
        return match self {
            CmdType::LOLWUT => true,
            _ => false,
        };
    }
    pub fn redis_key_count(&self) -> u8 {
        match self {
            CmdType::LOLWUT |
            CmdType::PING |
            CmdType::QUIT |
            CmdType::COMMAND => 0,

            CmdType::PERSIST |
            CmdType::PTTL |
            CmdType::TTL |
            CmdType::TYPE |
            CmdType::DUMP |

            CmdType::DECR |
            CmdType::GET |
            CmdType::GETDEL |
            CmdType::INCR |
            CmdType::STRLEN |

            CmdType::HGETALL |
            CmdType::HKEYS |
            CmdType::HLEN |
            CmdType::HVALS |

            CmdType::LLEN |

            CmdType::SCARD |
            CmdType::SMEMBERS |

            CmdType::ZCARD |
            CmdType::AUTH => 1,
            CmdType::EXPIRE |
            CmdType::EXPIREAT |
            CmdType::PEXPIRE |
            CmdType::PEXPIREAT |
            CmdType::MOVE |

            CmdType::APPEND |
            CmdType::DECRBY |
            CmdType::GETBIT |
            CmdType::GETSET |
            CmdType::INCRBY |
            CmdType::INCRBYFLOAT |
            CmdType::SETNX |

            CmdType::HEXISTS |
            CmdType::HGET |
            CmdType::HSTRLEN |

            CmdType::LINDEX |
            CmdType::RPOPLPUSH |

            CmdType::SISMEMBER |

            CmdType::ZRANK |
            CmdType::ZREVRANK |
            CmdType::ZSCORE => 1,

            CmdType::GETRANGE |
            CmdType::PSETEX |
            CmdType::SETBIT |
            CmdType::SETEX |
            CmdType::SETRANGE |

            CmdType::HINCRBY |
            CmdType::HINCRBYFLOAT |
            CmdType::HSETNX |

            CmdType::LRANGE |
            CmdType::LREM |
            CmdType::LSET |
            CmdType::LTRIM |

            CmdType::SMOVE |
            CmdType::ZCOUNT |
            CmdType::ZLEXCOUNT |
            CmdType::ZINCRBY |
            CmdType::ZREMRANGEBYLEX |
            CmdType::ZREMRANGEBYRANK |
            CmdType::ZREMRANGEBYSCORE => 1,

            CmdType::LINSERT | CmdType::LMOVE => 1,

            CmdType::SORT |
            CmdType::COPY |
            CmdType::BITCOUNT |
            CmdType::BITPOS |
            CmdType::BITFIELD |
            CmdType::EXISTS |
            CmdType::GETEX |
            CmdType::SET |
            CmdType::HDEL |
            CmdType::HMGET |
            CmdType::HMSET |
            CmdType::HSCAN |
            CmdType::HSET |
            CmdType::HRANDFIELD |
            CmdType::LPUSH |
            CmdType::LPUSHX |
            CmdType::RPUSH |
            CmdType::RPUSHX |
            CmdType::LPOP |
            CmdType::RPOP |
            CmdType::LPOS |
            CmdType::SADD |
            CmdType::SDIFF |
            CmdType::SDIFFSTORE |
            CmdType::SINTER |
            CmdType::SINTERSTORE |
            CmdType::SREM |
            CmdType::SUNION |
            CmdType::SUNIONSTORE |
            CmdType::SRANDMEMBER |
            CmdType::SSCAN |
            CmdType::SPOP |
            CmdType::SMISMEMBER |
            CmdType::PFADD |
            CmdType::PFMERGE |
            CmdType::PFCOUNT |
            CmdType::ZADD |
            CmdType::ZDIFF |
            CmdType::ZDIFFSTORE |
            CmdType::ZINTER |
            CmdType::ZINTERSTORE |
            CmdType::ZMSCORE |
            CmdType::ZPOPMAX |
            CmdType::ZPOPMIN |
            CmdType::ZRANDMEMBER |
            CmdType::ZRANGE |
            CmdType::ZRANGEBYLEX |
            CmdType::ZRANGEBYSCORE |
            CmdType::ZRANGESTORE |
            CmdType::ZREM |
            CmdType::ZREVRANGE |
            CmdType::ZREVRANGEBYLEX |
            CmdType::ZREVRANGEBYSCORE |
            CmdType::ZSCAN |
            CmdType::ZUNION |
            CmdType::ZUNIONSTORE |
            CmdType::GEODIST |
            CmdType::GEOPOS |
            CmdType::GEOHASH |
            CmdType::GEOADD |
            CmdType::GEORADIUS |
            CmdType::GEORADIUSBYMEMBER |
            CmdType::GEOSEARCH |
            CmdType::GEOSEARCHSTORE |
            CmdType::RESTORE => 1,

            CmdType::MGET | CmdType::DEL | CmdType::UNLINK | CmdType::TOUCH => 2,

            CmdType::HMSET => 2,
            CmdType::EVAL | CmdType::EVALSHA => 2,
            CmdType::UNKNOWN => 2
        }
    }
    pub fn redis_arg0(&self) -> bool {
        match self {
            CmdType::PERSIST |
            CmdType::PTTL |
            CmdType::TTL |
            CmdType::TYPE |
            CmdType::DUMP |

            CmdType::DECR |
            CmdType::GET |
            CmdType::GETDEL |
            CmdType::INCR |
            CmdType::STRLEN |

            CmdType::HGETALL |
            CmdType::HKEYS |
            CmdType::HLEN |
            CmdType::HVALS |

            CmdType::LLEN |

            CmdType::SCARD |
            CmdType::SMEMBERS |

            CmdType::ZCARD |
            CmdType::AUTH => true,
            _ => false,
        }
    }
    // Return true, if the redis command accepts exactly 1 argument, otherwise return false
    pub fn redis_arg1(&self) -> bool {
        match self {
            CmdType::EXPIRE |
            CmdType::EXPIREAT |
            CmdType::PEXPIRE |
            CmdType::PEXPIREAT |
            CmdType::MOVE |

            CmdType::APPEND |
            CmdType::DECRBY |
            CmdType::GETBIT |
            CmdType::GETSET |
            CmdType::INCRBY |
            CmdType::INCRBYFLOAT |
            CmdType::SETNX |

            CmdType::HEXISTS |
            CmdType::HGET |
            CmdType::HSTRLEN |

            CmdType::LINDEX |
            CmdType::RPOPLPUSH |

            CmdType::SISMEMBER |

            CmdType::ZRANK |
            CmdType::ZREVRANK |
            CmdType::ZSCORE => {
                true
            }
            _ => false
        }
    }
    // Return true, if the redis command accepts exactly 2 arguments, otherwise return false
    // SETEX mykey 10 "Hello"
    pub fn redis_arg2(&self) -> bool {
        match self {
            CmdType::GETRANGE |
            CmdType::PSETEX |
            CmdType::SETBIT |
            CmdType::SETEX |
            CmdType::SETRANGE |

            CmdType::HINCRBY |
            CmdType::HINCRBYFLOAT |
            CmdType::HSETNX |

            CmdType::LRANGE |
            CmdType::LREM |
            CmdType::LSET |
            CmdType::LTRIM |

            CmdType::SMOVE |
            CmdType::ZCOUNT |
            CmdType::ZLEXCOUNT |
            CmdType::ZINCRBY |
            CmdType::ZREMRANGEBYLEX |
            CmdType::ZREMRANGEBYRANK |
            CmdType::ZREMRANGEBYSCORE => true,
            _ => false,
        }
    }
    // Return true, if the redis command accepts exactly 3 arguments, otherwise return false
    pub fn redis_arg3(&self) -> bool {
        match self {
            CmdType::LINSERT | CmdType::LMOVE => true,
            _ => false,
        }
    }
    // Return true, if the redis command operates on one key and accepts 0 or more arguments, otherwise
    // return false
    pub fn redis_argn(&self) -> bool {
        match self {
            CmdType::SORT |
            CmdType::COPY |
            CmdType::BITCOUNT |
            CmdType::BITPOS |
            CmdType::BITFIELD |
            CmdType::EXISTS |
            CmdType::GETEX |
            CmdType::SET |
            CmdType::HDEL |
            CmdType::HMGET |
            CmdType::HMSET |
            CmdType::HSCAN |
            CmdType::HSET |
            CmdType::HRANDFIELD |
            CmdType::LPUSH |
            CmdType::LPUSHX |
            CmdType::RPUSH |
            CmdType::RPUSHX |
            CmdType::LPOP |
            CmdType::RPOP |
            CmdType::LPOS |
            CmdType::SADD |
            CmdType::SDIFF |
            CmdType::SDIFFSTORE |
            CmdType::SINTER |
            CmdType::SINTERSTORE |
            CmdType::SREM |
            CmdType::SUNION |
            CmdType::SUNIONSTORE |
            CmdType::SRANDMEMBER |
            CmdType::SSCAN |
            CmdType::SPOP |
            CmdType::SMISMEMBER |
            CmdType::PFADD |
            CmdType::PFMERGE |
            CmdType::PFCOUNT |
            CmdType::ZADD |
            CmdType::ZDIFF |
            CmdType::ZDIFFSTORE |
            CmdType::ZINTER |
            CmdType::ZINTERSTORE |
            CmdType::ZMSCORE |
            CmdType::ZPOPMAX |
            CmdType::ZPOPMIN |
            CmdType::ZRANDMEMBER |
            CmdType::ZRANGE |
            CmdType::ZRANGEBYLEX |
            CmdType::ZRANGEBYSCORE |
            CmdType::ZRANGESTORE |
            CmdType::ZREM |
            CmdType::ZREVRANGE |
            CmdType::ZREVRANGEBYLEX |
            CmdType::ZREVRANGEBYSCORE |
            CmdType::ZSCAN |
            CmdType::ZUNION |
            CmdType::ZUNIONSTORE |
            CmdType::GEODIST |
            CmdType::GEOPOS |
            CmdType::GEOHASH |
            CmdType::GEOADD |
            CmdType::GEORADIUS |
            CmdType::GEORADIUSBYMEMBER |
            CmdType::GEOSEARCH |
            CmdType::GEOSEARCHSTORE |
            CmdType::RESTORE => true,
            _ => false,
        }
    }


    // Return true, if the redis command is a vector command accepting one or
    // more keys, otherwise return false
    pub fn redis_argx(&self) -> bool {
        match self {
            CmdType::MGET | CmdType::DEL | CmdType::UNLINK | CmdType::TOUCH => true,
            _ => false,
        }
    }
    // Return true, if the redis command is a vector command accepting one or
    // more key-value pairs, otherwise return false
    pub fn redis_argkvx(&self) -> bool {
        match self {
            CmdType::HMSET => true,
            _ => false,
        }
    }
    pub fn redis_argeval(&self) -> bool {
        match self {
            CmdType::EVAL | CmdType::EVALSHA => true,
            _ => false,
        }
    }
}

impl From<&[u8]> for CmdType {
    fn from(cmd: &[u8]) -> Self {
        let len = cmd.len();
        match len {
            3 => {
                if str3icmp(cmd, b'g', b'e', b't') {
                    return CmdType::GET;
                }
                if str3icmp(cmd, b's', b'e', b't') {
                    return CmdType::SET;
                }
                return CmdType::UNKNOWN;
            }
            4 => {
                if str4icmp(cmd, b'p', b'i', b'n', b'g') {
                    return CmdType::PING;
                }
            }

            _ => {
                return CmdType::UNKNOWN;
            }
        }
        return CmdType::UNKNOWN;
    }
}