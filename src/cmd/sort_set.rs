use super::{check_cmd, next_bytes, next_float, next_string};
use crate::db::Database;
use crate::frame::Frame;
use crate::RedisErr;

#[derive(Debug)]
pub struct ZAdd {
    key: String,
    nx: bool,
    xx: bool,
    lt: bool,
    gt: bool,
    ch: bool,
    incr: bool,
    zset: Vec<(f64, Vec<u8>)>,
}

impl ZAdd {
    fn new(
        key: String,
        nx: bool,
        xx: bool,
        lt: bool,
        gt: bool,
        ch: bool,
        incr: bool,
        zset: Vec<(f64, Vec<u8>)>,
    ) -> Self {
        Self {
            key,
            zset,
            nx,
            xx,
            lt,
            gt,
            ch,
            incr,
        }
    }

    // ZADD key [NX|XX] [CH] [INCR] score member [score member ...]
    pub fn from_frames(frames: Vec<Frame>) -> Result<Self, RedisErr> {
        if frames.len() < 3 {
            return Err(RedisErr::WrongNumberOfArguments);
        }

        let mut iter = frames.into_iter();
        check_cmd(&mut iter, b"ZADD")?;
        let key = next_string(&mut iter)?;

        // [NX|XX] [CH] [INCR]
        let mut nx = false;
        let mut xx = false;
        let mut lt = false;
        let mut gt = false;
        let mut ch = false;
        let mut incr = false;
        let score = loop {
            match next_string(&mut iter)?.to_uppercase().as_str() {
                "NX" => {
                    nx = true;
                    continue;
                }
                "XX" => {
                    xx = true;
                    continue;
                }
                "LT" => {
                    lt = true;
                    continue;
                }
                "GT" => {
                    gt = true;
                    continue;
                }
                "CH" => {
                    ch = true;
                    continue;
                }
                "INCR" => {
                    incr = true;
                    continue;
                }
                s => break s.parse::<f64>().map_err(|_| RedisErr::SyntaxError)?,
            };
        };

        // conflict options
        if nx && xx {
            return Err(RedisErr::SyntaxError);
        }
        if lt && gt {
            return Err(RedisErr::SyntaxError);
        }

        let member = next_bytes(&mut iter)?; // member

        let mut zset = vec![(score, member)];
        while iter.len() > 0 {
            let score = next_float(&mut iter)?; // score
            let member = next_bytes(&mut iter)?; // member
            zset.push((score, member));
        }
        Ok(Self::new(key, nx, xx, lt, gt, ch, incr, zset))
    }

    pub fn apply(self, db: &mut Database) -> Frame {
        match db.zadd(
            &self.key, self.nx, self.xx, self.lt, self.gt, self.ch, self.incr, self.zset,
        ) {
            Ok(len) => Frame::Integer(len as i64),
            Err(e) => match e {
                RedisErr::WrongType => Frame::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                ),
                RedisErr::OutOfMemory => Frame::Error("Out of memory".to_string()),
                _ => unreachable!("unexpect zadd error: {:?}", e),
            },
        }
    }
}

#[derive(Debug)]
pub struct ZCard {
    key: String,
}

impl ZCard {
    fn new(key: String) -> Self {
        Self { key }
    }

    pub fn from_frames(frames: Vec<Frame>) -> Result<Self, RedisErr> {
        if frames.len() != 2 {
            return Err(RedisErr::WrongNumberOfArguments);
        }
        let mut iter = frames.into_iter();
        check_cmd(&mut iter, b"ZCARD")?;
        let key = next_string(&mut iter)?; // key
        Ok(Self::new(key))
    }

    pub fn apply(self, db: &Database) -> Frame {
        match db.zcard(&self.key) {
            Ok(len) => Frame::Integer(len as i64),
            Err(e) => match e {
                RedisErr::KeyNotFound => Frame::Integer(0),
                RedisErr::WrongType => Frame::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                ),
                _ => unreachable!("unexpect zcard error: {:?}", e),
            },
        }
    }
}

#[derive(Debug)]
pub struct ZRem {
    key: String,
    members: Vec<Vec<u8>>,
}

impl ZRem {
    fn new(key: String, members: Vec<Vec<u8>>) -> Self {
        Self { key, members }
    }

    pub fn from_frames(frames: Vec<Frame>) -> Result<Self, RedisErr> {
        if frames.len() < 3 {
            return Err(RedisErr::WrongNumberOfArguments);
        }
        let mut iter = frames.into_iter();
        check_cmd(&mut iter, b"ZREM")?;
        let key = next_string(&mut iter)?; // key
        let mut members = vec![];
        while iter.len() > 0 {
            let member = next_bytes(&mut iter)?; // member
            members.push(member);
        }
        Ok(Self::new(key, members))
    }

    pub fn apply(self, db: &mut Database) -> Frame {
        match db.zrem(&self.key, self.members) {
            Ok(len) => Frame::Integer(len as i64),
            Err(e) => match e {
                RedisErr::KeyNotFound => Frame::Integer(0),
                RedisErr::WrongType => Frame::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                ),
                _ => unreachable!("unexpect zrem error: {:?}", e),
            },
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_zadd() {
        let mut db = Database::new();
        let cmd = ZAdd::from_frames(vec![
            Frame::BulkString(b"zadd".to_vec()),
            Frame::BulkString(b"key".to_vec()),
            Frame::BulkString(b"1".to_vec()),
            Frame::BulkString(b"one".to_vec()),
        ])
        .unwrap();
        let result = cmd.apply(&mut db);
        assert_eq!(result, Frame::Integer(1));
    }

    #[test]
    fn test_zcard() {
        let mut db = Database::new();
        let cmd = ZCard::from_frames(vec![
            Frame::BulkString(b"zcard".to_vec()),
            Frame::BulkString(b"key".to_vec()),
        ])
        .unwrap();
        let result = cmd.apply(&mut db);
        assert_eq!(result, Frame::Integer(0));
    }

    #[test]
    fn test_zrem() {
        let mut db = Database::new();
        let cmd = ZRem::from_frames(vec![
            Frame::BulkString(b"zrem".to_vec()),
            Frame::BulkString(b"key".to_vec()),
            Frame::BulkString(b"one".to_vec()),
        ])
        .unwrap();
        let result = cmd.apply(&mut db);
        assert_eq!(result, Frame::Integer(0));
    }
}
