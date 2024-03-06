//! List commands

use super::*;

use crate::db::DB;
use crate::frame::Frame;
use crate::{RedisErr, Result};

use marco::Applyer;

#[derive(Debug, Applyer)]
pub struct LPush {
    key: String,
    values: Vec<Bytes>,
}

impl LPush {
    fn new(key: String, values: Vec<Bytes>) -> Self {
        Self { key, values }
    }

    pub fn from_frames(frames: Vec<Frame>) -> Result<Self> {
        let mut iter = frames.into_iter();
        check_cmd(&mut iter, b"LPUSH")?;

        let key = next_string(&mut iter)?; // key
        let mut value = Vec::new();
        while iter.len() > 0 {
            value.push(next_bytes(&mut iter)?);
        }
        Ok(Self::new(key, value))
    }

    pub fn apply(self, db: &mut DB) -> Frame {
        match db.lpush(&self.key, self.values) {
            Ok(len) => Frame::Integer(len as i64),
            Err(e) => match e {
                RedisErr::WrongType => Frame::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                ),
                RedisErr::OutOfMemory => Frame::Error("Out of memory".to_string()),
                _ => unreachable!("unexpect lpush error: {:?}", e),
            },
        }
    }
}

#[derive(Debug, Applyer)]
pub struct LRange {
    key: String,
    start: i64,
    stop: i64,
}

impl LRange {
    fn new(key: String, start: i64, stop: i64) -> Self {
        Self { key, start, stop }
    }

    pub fn from_frames(frames: Vec<Frame>) -> Result<Self> {
        let mut iter = frames.into_iter();
        check_cmd(&mut iter, b"LRANGE")?;
        let key = next_string(&mut iter)?; // key
        let start = next_integer(&mut iter)?; // start
        let stop = next_integer(&mut iter)?; // stop
        Ok(Self::new(key, start, stop))
    }

    pub fn apply(self, db: &mut DB) -> Frame {
        match db.lrange(&self.key, self.start, self.stop) {
            Ok(values) => Frame::Array(
                values
                    .into_iter()
                    .map(Frame::BulkString)
                    .collect::<Vec<_>>(),
            ),
            Err(e) => match e {
                RedisErr::KeyNotFound => Frame::Array(vec![]),
                RedisErr::WrongType => Frame::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                ),
                _ => unreachable!("unexpect lrange error: {:?}", e),
            },
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_lpush() {
        let mut db = DB::new();
        let cmd = LPush::from_frames(vec![
            Frame::BulkString(Bytes::from_static(b"lpush")),
            Frame::BulkString(Bytes::from_static(b"key")),
            Frame::BulkString(Bytes::from_static(b"1")),
            Frame::BulkString(Bytes::from_static(b"2")),
            Frame::BulkString(Bytes::from_static(b"3")),
        ])
        .unwrap();
        let result = cmd.apply(&mut db);
        assert_eq!(result, Frame::Integer(3));
    }

    #[test]
    fn test_lrange() {
        let mut db = DB::new();
        let cmd = LRange::from_frames(vec![
            Frame::BulkString(Bytes::from_static(b"lrange")),
            Frame::BulkString(Bytes::from_static(b"key")),
            Frame::Integer(0),
            Frame::Integer(-1),
        ])
        .unwrap();
        let result = cmd.apply(&mut db);
        assert_eq!(result, Frame::Array(vec![]));
    }
}
