//! KV commands

use super::*;

use crate::frame::Frame;
use crate::Result;
use crate::{db::DB, RedisErr};

use marco::Applyer;

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Debug, Applyer)]
pub struct Get {
    key: String,
}

impl Get {
    pub fn new(key: String) -> Self {
        Self { key }
    }

    pub fn from_frames(frames: Vec<Frame>) -> Result<Self> {
        let mut iter = frames.into_iter();
        check_cmd(&mut iter, b"GET")?;
        let key = next_string(&mut iter)?;
        Ok(Self::new(key))
    }

    pub fn apply(self, db: &mut DB) -> Frame {
        match db.get(&self.key) {
            Ok(value) => Frame::BulkString(value),
            Err(e) => match e {
                RedisErr::KeyNotFound => Frame::Nil,
                RedisErr::WrongType => Frame::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                ),
                _ => unreachable!("unexpect get error: {:?}", e),
            },
        }
    }
}

#[derive(Debug, Applyer)]
pub struct MGet {
    key: Vec<String>,
}

impl MGet {
    pub fn new(key: Vec<String>) -> Self {
        Self { key }
    }

    pub fn from_frames(frames: Vec<Frame>) -> Result<Self> {
        let mut iter = frames.into_iter();
        check_cmd(&mut iter, b"MGET").unwrap();
        let mut key = Vec::new();
        while iter.len() > 0 {
            key.push(next_string(&mut iter).unwrap());
        }
        if key.is_empty() {
            return Err(RedisErr::SyntaxError);
        }
        Ok(Self::new(key))
    }

    pub fn apply(self, db: &mut DB) -> Frame {
        let mut result = Vec::new();
        for k in self.key {
            match db.get(&k) {
                Ok(value) => result.push(Frame::BulkString(value)),
                Err(e) => match e {
                    RedisErr::KeyNotFound => result.push(Frame::Nil),
                    RedisErr::WrongType => result.push(Frame::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    )),
                    _ => unreachable!("unexpect get error: {:?}", e),
                },
            }
        }
        Frame::Array(result)
    }
}

#[derive(Debug, Applyer)]
pub struct Set {
    key: String,
    value: Bytes,
    nx: bool,
    xx: bool,
    get: bool,
    ex: Option<Duration>,
    exat: Option<Instant>,
    keepttl: bool,
}

impl Set {
    fn new(
        key: String,
        value: Bytes,
        nx: bool,
        xx: bool,
        get: bool,
        ex: Option<Duration>,
        exat: Option<Instant>,
        keepttl: bool,
    ) -> Self {
        Self {
            key,
            value,
            nx,
            xx,
            get,
            ex,
            exat,
            keepttl,
        }
    }

    // SET key value [NX] [XX] [GET] [EX <seconds>] [PX <milliseconds>] [KEEPTTL]
    pub fn from_frames(frames: Vec<Frame>) -> Result<Self> {
        let mut iter = frames.into_iter();
        check_cmd(&mut iter, b"SET")?;

        let key = next_string(&mut iter)?; // key
        let value = next_bytes(&mut iter)?; // value
        let (mut nx, mut xx, mut get, mut keepttl) = (false, false, false, false);
        let (mut ex, mut exat) = (None, None);
        let now = SystemTime::now();
        while iter.len() > 0 {
            let next_opt = next_string(&mut iter)?;
            match next_opt.to_ascii_uppercase().as_str() {
                "NX" => nx = true,
                "XX" => xx = true,
                "GET" => get = true,
                "KEEPTTL" => keepttl = true,
                "EX" => {
                    ex = Some(Duration::from_secs(next_integer(&mut iter)? as u64));
                }
                "PX" => {
                    ex = Some(Duration::from_millis(next_integer(&mut iter)? as u64));
                }
                "EXAT" => {
                    let exat_ts = UNIX_EPOCH + Duration::from_secs(next_integer(&mut iter)? as u64);
                    if exat_ts < now {
                        return Err(RedisErr::SyntaxError);
                    }
                    exat = Some(Instant::now() + exat_ts.duration_since(now).unwrap());
                }
                "PXAT" => {
                    let exat_ts =
                        UNIX_EPOCH + Duration::from_millis(next_integer(&mut iter)? as u64);
                    if exat_ts < now {
                        return Err(RedisErr::SyntaxError);
                    }
                    exat = Some(Instant::now() + exat_ts.duration_since(now).unwrap());
                }
                _ => {
                    return Err(RedisErr::SyntaxError);
                }
            }
        }

        if nx && xx {
            return Err(RedisErr::SyntaxError);
        }
        if ex.is_some() && exat.is_some() {
            return Err(RedisErr::SyntaxError);
        }
        if keepttl && (ex.is_some() || exat.is_some()) {
            return Err(RedisErr::SyntaxError);
        }

        Ok(Self::new(key, value, nx, xx, get, ex, exat, keepttl))
    }

    pub fn apply(self, db: &mut DB) -> Frame {
        let expire_at = match (self.ex, self.exat) {
            (Some(d), None) => Some(Instant::now() + d),
            (None, Some(t)) => Some(t),
            _ => None,
        };
        match db.set(
            self.key,
            self.value,
            self.nx,
            self.xx,
            self.get,
            self.keepttl,
            expire_at,
        ) {
            Ok(Some(value)) => Frame::BulkString(value),
            Ok(None) => Frame::SimpleString("OK".to_string()),
            Err(e) => match e {
                RedisErr::NoAction => Frame::Nil,
                RedisErr::WrongType => Frame::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                ),
                RedisErr::OutOfMemory => Frame::Error("Out of memory".to_string()),
                _ => unreachable!("unexpect set error: {:?}", e),
            },
        }
    }
}

#[derive(Debug, Applyer)]
pub struct MSet {
    pairs: Vec<(String, Bytes)>,
}

impl MSet {
    pub fn new(pairs: Vec<(String, Bytes)>) -> Self {
        Self { pairs }
    }

    pub fn from_frames(frames: Vec<Frame>) -> Result<Self> {
        let mut iter = frames.into_iter();
        check_cmd(&mut iter, b"MSET")?;

        let mut pairs = vec![];
        while iter.len() > 0 {
            let key = next_string(&mut iter)?;
            let value = next_bytes(&mut iter)?;
            pairs.push((key, value));
        }
        if pairs.is_empty() {
            return Err(RedisErr::SyntaxError);
        }
        Ok(Self::new(pairs))
    }

    pub fn apply(self, db: &mut DB) -> Frame {
        for (key, value) in self.pairs {
            match db.set(key, value, false, false, false, false, None) {
                Ok(_) => {}
                Err(e) => match e {
                    RedisErr::OutOfMemory => return Frame::Error("Out of memory".to_string()),
                    _ => unreachable!("unexpect set error: {:?}", e),
                },
            }
        }
        Frame::SimpleString("Ok".to_string())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_get() {
        let mut db = DB::new();
        let cmd = Get::from_frames(vec![
            Frame::BulkString(Bytes::from_static(b"get")),
            Frame::BulkString(Bytes::from_static(b"key")),
        ])
        .unwrap();
        let result = cmd.apply(&mut db);
        assert_eq!(result, Frame::Nil);
    }

    #[test]
    fn test_mget() {
        let mut db = DB::new();
        let cmd = MGet::from_frames(vec![
            Frame::BulkString(Bytes::from_static(b"mget")),
            Frame::BulkString(Bytes::from_static(b"key1")),
            Frame::BulkString(Bytes::from_static(b"key2")),
        ])
        .unwrap();

        let result = cmd.apply(&mut db);
        assert_eq!(result, Frame::Array(vec![Frame::Nil, Frame::Nil]));
    }

    #[test]
    fn test_mset() {
        let mut db = DB::new();
        let cmd = MSet::from_frames(vec![
            Frame::BulkString(Bytes::from_static(b"mset")),
            Frame::BulkString(Bytes::from_static(b"key1")),
            Frame::BulkString(Bytes::from_static(b"value1")),
            Frame::BulkString(Bytes::from_static(b"key2")),
            Frame::BulkString(Bytes::from_static(b"value2")),
        ])
        .unwrap();

        let result = cmd.apply(&mut db);
        assert_eq!(result, Frame::SimpleString("Ok".to_string()));
    }

    #[test]
    fn test_set() {
        let mut db = DB::new();
        let cmd = Set::from_frames(vec![
            Frame::SimpleString("set".to_string()),
            Frame::SimpleString("key".to_string()),
            Frame::BulkString(Bytes::from_static(b"value")),
        ])
        .unwrap();
        let result = cmd.apply(&mut db);
        assert_eq!(result, Frame::SimpleString("OK".to_string()));
    }
}
