//! Sort Set commands

use super::*;
use crate::db::DB;
use crate::frame::Frame;
use crate::Result;

use marco::Applyer;

#[derive(Debug, Applyer)]
pub struct ZAdd {
    key: String,
    nx: bool,
    xx: bool,
    lt: bool,
    gt: bool,
    ch: bool,
    incr: bool,
    zset: Vec<(f64, Bytes)>,
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
        zset: Vec<(f64, Bytes)>,
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
    pub fn from_frames(frames: Vec<Frame>) -> Result<Self> {
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

    pub fn apply(self, db: &mut DB) -> Frame {
        let db = db;
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

#[derive(Debug, Applyer)]
pub struct ZCard {
    key: String,
}

impl ZCard {
    fn new(key: String) -> Self {
        Self { key }
    }

    pub fn from_frames(frames: Vec<Frame>) -> Result<Self> {
        if frames.len() != 2 {
            return Err(RedisErr::WrongNumberOfArguments);
        }
        let mut iter = frames.into_iter();
        check_cmd(&mut iter, b"ZCARD")?;
        let key = next_string(&mut iter)?; // key
        Ok(Self::new(key))
    }

    pub fn apply(self, db: &mut DB) -> Frame {
        let db = db;
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

#[derive(Debug, Applyer)]
pub struct ZRem {
    key: String,
    members: Vec<Bytes>,
}

impl ZRem {
    fn new(key: String, members: Vec<Bytes>) -> Self {
        Self { key, members }
    }

    pub fn from_frames(frames: Vec<Frame>) -> Result<Self> {
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

    pub fn apply(self, db: &mut DB) -> Frame {
        let db = db;
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
        let mut db = DB::new();
        let cmd = ZAdd::from_frames(vec![
            Frame::BulkString(Bytes::from_static(b"zadd")),
            Frame::BulkString(Bytes::from_static(b"key")),
            Frame::BulkString(Bytes::from_static(b"1")),
            Frame::BulkString(Bytes::from_static(b"one")),
        ])
        .unwrap();
        let result = cmd.apply(&mut db);
        assert_eq!(result, Frame::Integer(1));
    }

    #[test]
    fn test_zcard() {
        let mut db = DB::new();
        let cmd = ZCard::from_frames(vec![
            Frame::BulkString(Bytes::from_static(b"zcard")),
            Frame::BulkString(Bytes::from_static(b"key")),
        ])
        .unwrap();
        let result = cmd.apply(&mut db);
        assert_eq!(result, Frame::Integer(0));
    }

    #[test]
    fn test_zrem() {
        let mut db = DB::new();
        let cmd = ZRem::from_frames(vec![
            Frame::BulkString(Bytes::from_static(b"zrem")),
            Frame::BulkString(Bytes::from_static(b"key")),
            Frame::BulkString(Bytes::from_static(b"one")),
        ])
        .unwrap();
        let result = cmd.apply(&mut db);
        assert_eq!(result, Frame::Integer(0));
    }
}
