use crate::cmd::check_cmd;
use crate::cmd::{next_bytes, next_integer, next_string, CommandErr};
use crate::db::Database;
use crate::frame::Frame;

use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Debug)]
pub struct Get {
    key: String,
}

impl Get {
    #[allow(dead_code)]
    pub fn new(key: String) -> Self {
        Self { key }
    }

    pub fn from_frames(frames: Vec<Frame>) -> Result<Self, CommandErr> {
        let mut iter = frames.into_iter();
        check_cmd(&mut iter, b"GET")?;
        let key = next_string(&mut iter)?;
        Ok(Self { key })
    }

    #[allow(dead_code)]
    fn key(&self) -> &str {
        &self.key
    }

    pub fn apply(self, db: &mut Database) -> Frame {
        match db.get(&self.key) {
            Ok(value) => Frame::BulkString(value),
            Err(e) => match e {
                crate::db::ExecuteError::KeyNotFound => Frame::Nil,
                crate::db::ExecuteError::WrongType => Frame::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                ),
                _ => unreachable!("unexpect get error: {:?}", e),
            },
        }
    }
}

#[derive(Debug)]
pub struct MGet {
    key: Vec<String>,
}

impl MGet {
    pub fn new(key: Vec<String>) -> Self {
        Self { key }
    }

    pub fn from_frames(frames: Vec<Frame>) -> Result<Self, CommandErr> {
        let mut iter = frames.into_iter();
        check_cmd(&mut iter, b"MGET").unwrap();
        let mut key = Vec::new();
        while iter.len() > 0 {
            key.push(next_string(&mut iter).unwrap());
        }
        if key.len() == 0 {
            return Err(CommandErr::SyntaxError);
        }
        Ok(Self::new(key))
    }

    pub fn apply(self, db: &mut Database) -> Frame {
        let mut result = Vec::new();
        for k in self.key {
            match db.get(&k) {
                Ok(value) => result.push(Frame::BulkString(value)),
                Err(e) => match e {
                    crate::db::ExecuteError::KeyNotFound => result.push(Frame::Nil),
                    crate::db::ExecuteError::WrongType => result.push(Frame::Error(
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

#[derive(Debug)]
pub struct Set {
    key: String,
    value: Vec<u8>,
    nx: bool,
    xx: bool,
    get: bool,
    ex: Option<Duration>,
    exat: Option<SystemTime>,
    keepttl: bool,
}

impl Set {
    #[allow(dead_code)]
    fn new(
        key: String,
        value: Vec<u8>,
        nx: bool,
        xx: bool,
        get: bool,
        ex: Option<Duration>,
        exat: Option<SystemTime>,
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
    pub fn from_frames(frames: Vec<Frame>) -> Result<Self, CommandErr> {
        let mut iter = frames.into_iter();
        check_cmd(&mut iter, b"SET")?;

        let key = next_string(&mut iter)?; // key
        let value = next_bytes(&mut iter)?; // value
        let (mut nx, mut xx, mut get, mut keepttl) = (false, false, false, false);
        let (mut ex, mut exat) = (None, None);
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
                    exat = Some(UNIX_EPOCH + Duration::from_secs(next_integer(&mut iter)? as u64));
                }
                "PXAT" => {
                    exat =
                        Some(UNIX_EPOCH + Duration::from_millis(next_integer(&mut iter)? as u64));
                }
                _ => {
                    return Err(CommandErr::SyntaxError);
                }
            }
        }

        if nx && xx {
            return Err(CommandErr::SyntaxError);
        }
        if ex.is_some() && exat.is_some() {
            return Err(CommandErr::SyntaxError);
        }
        if keepttl && (ex.is_some() || exat.is_some()) {
            return Err(CommandErr::SyntaxError);
        }

        Ok(Self::new(key, value, nx, xx, get, ex, exat, keepttl))
    }

    #[allow(dead_code)]
    fn key(&self) -> &str {
        &self.key
    }

    #[allow(dead_code)]
    fn value(&self) -> &Vec<u8> {
        &self.value
    }

    pub fn apply(self, db: &mut Database) -> Frame {
        let expire_at = match (self.ex, self.exat) {
            (Some(d), None) => Some(SystemTime::now() + d),
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
                crate::db::ExecuteError::NoAction => Frame::Nil,
                crate::db::ExecuteError::WrongType => Frame::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                ),
                crate::db::ExecuteError::OutOfMemory => Frame::Error("Out of memory".to_string()),
                _ => unreachable!("unexpect set error: {:?}", e),
            },
        }
    }
}

#[derive(Debug)]
pub struct MSet {
    pairs: Vec<(String, Vec<u8>)>,
}

impl MSet {
    pub fn new(pairs: Vec<(String, Vec<u8>)>) -> Self {
        Self { pairs }
    }

    pub fn from_frames(frames: Vec<Frame>) -> Result<Self, CommandErr> {
        let mut iter = frames.into_iter();
        check_cmd(&mut iter, b"MSET")?;

        let mut pairs = vec![];
        while iter.len() > 0 {
            let key = next_string(&mut iter)?;
            let value = next_bytes(&mut iter)?;
            pairs.push((key, value));
        }
        if pairs.len() == 0 {
            return Err(CommandErr::SyntaxError);
        }
        Ok(Self::new(pairs))
    }

    pub fn apply(self, db: &mut Database) -> Frame {
        for (key, value) in self.pairs {
            match db.set(key, value, false, false, false, false, None) {
                Ok(_) => {}
                Err(e) => match e {
                    crate::db::ExecuteError::OutOfMemory => {
                        return Frame::Error("Out of memory".to_string())
                    }
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
        let mut db = Database::new();
        let cmd = Get::from_frames(vec![
            Frame::BulkString(b"get".to_vec()),
            Frame::BulkString(b"key".to_vec()),
        ])
        .unwrap();
        let result = cmd.apply(&mut db);
        assert_eq!(result, Frame::Nil);
    }

    #[test]
    fn test_mget() {
        let mut db = Database::new();
        let cmd = MGet::from_frames(vec![
            Frame::BulkString(b"mget".to_vec()),
            Frame::BulkString(b"key1".to_vec()),
            Frame::BulkString(b"key2".to_vec()),
        ])
        .unwrap();

        let result = cmd.apply(&mut db);
        assert_eq!(result, Frame::Array(vec![Frame::Nil, Frame::Nil]));
    }

    #[test]
    fn test_mset() {
        let mut db = Database::new();
        let cmd = MSet::from_frames(vec![
            Frame::BulkString(b"mset".to_vec()),
            Frame::BulkString(b"key1".to_vec()),
            Frame::BulkString(b"value1".to_vec()),
            Frame::BulkString(b"key2".to_vec()),
            Frame::BulkString(b"value2".to_vec()),
        ])
        .unwrap();

        let result = cmd.apply(&mut db);
        assert_eq!(result, Frame::SimpleString("Ok".to_string()));
    }

    #[test]
    fn test_set() {
        let mut db = Database::new();
        let cmd = Set::from_frames(vec![
            Frame::SimpleString("set".to_string()),
            Frame::SimpleString("key".to_string()),
            Frame::BulkString(b"value".to_vec()),
        ])
        .unwrap();
        let result = cmd.apply(&mut db);
        assert_eq!(result, Frame::SimpleString("OK".to_string()));
    }
}
