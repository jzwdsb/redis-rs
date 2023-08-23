use super::*;
use crate::db::Database;
use crate::frame::Frame;
use crate::RedisErr;

use marco::Applyer;

#[derive(Debug, Applyer)]
pub struct LPush {
    key: String,
    values: Vec<Vec<u8>>,
}

impl LPush {
    fn new(key: String, values: Vec<Vec<u8>>) -> Self {
        Self { key, values }
    }

    pub fn from_frames(frames: Vec<Frame>) -> Result<Self, RedisErr> {
        let mut iter = frames.into_iter();
        check_cmd(&mut iter, b"LPUSH")?;

        let key = next_string(&mut iter)?; // key
        let mut value = Vec::new();
        while iter.len() > 0 {
            value.push(next_bytes(&mut iter)?);
        }
        Ok(Self::new(key, value))
    }

    #[allow(dead_code)]
    fn key(&self) -> &str {
        &self.key
    }

    #[allow(dead_code)]
    fn value(&self) -> &Vec<Vec<u8>> {
        &self.values
    }

    pub fn apply(self, db: &mut Database) -> Frame {
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
    #[allow(dead_code)]
    fn new(key: String, start: i64, stop: i64) -> Self {
        Self { key, start, stop }
    }

    pub fn from_frames(frames: Vec<Frame>) -> Result<Self, RedisErr> {
        let mut iter = frames.into_iter();
        check_cmd(&mut iter, b"LRANGE")?;
        let key = next_string(&mut iter)?; // key
        let start = next_integer(&mut iter)?; // start
        let stop = next_integer(&mut iter)?; // stop
        Ok(Self { key, start, stop })
    }

    #[allow(dead_code)]
    fn key(&self) -> &str {
        &self.key
    }

    #[allow(dead_code)]
    fn start(&self) -> i64 {
        self.start
    }

    #[allow(dead_code)]
    fn stop(&self) -> i64 {
        self.stop
    }

    pub fn apply(self, db: &mut Database) -> Frame {
        match db.lrange(&self.key, self.start, self.stop) {
            Ok(values) => Frame::Array(
                values
                    .into_iter()
                    .map(|v| Frame::BulkString(v))
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
        let mut db = Database::new();
        let cmd = LPush::from_frames(vec![
            Frame::BulkString(b"lpush".to_vec()),
            Frame::BulkString(b"key".to_vec()),
            Frame::BulkString(b"1".to_vec()),
            Frame::BulkString(b"2".to_vec()),
            Frame::BulkString(b"3".to_vec()),
        ])
        .unwrap();
        let result = cmd.apply(&mut db);
        assert_eq!(result, Frame::Integer(3));
    }

    #[test]
    fn test_lrange() {
        let mut db = Database::new();
        let cmd = LRange::from_frames(vec![
            Frame::BulkString(b"lrange".to_vec()),
            Frame::BulkString(b"key".to_vec()),
            Frame::Integer(0),
            Frame::Integer(-1),
        ])
        .unwrap();
        let result = cmd.apply(&mut db);
        assert_eq!(result, Frame::Array(vec![]));
    }
}
