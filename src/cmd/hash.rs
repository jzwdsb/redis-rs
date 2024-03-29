//! Hash commands

use super::*;

use crate::db::DB;
use crate::frame::Frame;
use crate::{RedisErr, Result};

use marco::Applyer;

use bytes::Bytes;

#[derive(Debug, Applyer)]
pub struct HSet {
    key: String,
    field_values: Vec<(String, Bytes)>,
}

impl HSet {
    fn new(key: String, field_values: Vec<(String, Bytes)>) -> Self {
        Self { key, field_values }
    }

    pub fn from_frames(frames: Vec<Frame>) -> Result<Self> {
        let mut iter = frames.into_iter();
        check_cmd(&mut iter, b"HSET")?;

        let key = next_string(&mut iter)?; // key
        let mut field_values = Vec::new();
        while iter.len() > 0 {
            let field = next_string(&mut iter)?; // field
            let value = next_bytes(&mut iter)?; // value
            field_values.push((field, value));
        }
        if field_values.is_empty() {
            return Err(RedisErr::WrongNumberOfArguments);
        }
        Ok(Self::new(key, field_values))
    }

    pub fn apply(self, db: &mut DB) -> Frame {
        match db.hset(self.key, self.field_values.clone()) {
            Ok(len) => Frame::Integer(len as i64),
            Err(e) => match e {
                RedisErr::WrongType => Frame::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                ),
                _ => unreachable!("unexpect hset error: {:?}", e),
            },
        }
    }
}

#[derive(Debug, Applyer)]
pub struct HGet {
    key: String,
    field: String,
}

impl HGet {
    fn new(key: String, field: String) -> Self {
        Self { key, field }
    }

    pub fn from_frames(frames: Vec<Frame>) -> Result<Self> {
        let mut iter = frames.into_iter();
        check_cmd(&mut iter, b"HGET")?;

        let key = next_string(&mut iter)?; // key
        let field = next_string(&mut iter)?; // field
        Ok(Self::new(key, field))
    }

    pub fn apply(self, db: &mut DB) -> Frame {
        match db.hget(&self.key, &self.field) {
            Ok(value) => match value {
                Some(v) => Frame::BulkString(v),
                None => Frame::Nil,
            },
            Err(e) => match e {
                RedisErr::KeyNotFound => Frame::Nil,
                RedisErr::WrongType => Frame::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                ),
                _ => unreachable!("unexpect hget error: {:?}", e),
            },
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_hset() {
        let mut db = DB::new();
        let cmd = HSet::from_frames(vec![
            Frame::BulkString(Bytes::from_static(b"hset")),
            Frame::BulkString(Bytes::from_static(b"key")),
            Frame::BulkString(Bytes::from_static(b"field")),
            Frame::BulkString(Bytes::from_static(b"value")),
        ])
        .unwrap();
        let result = cmd.apply(&mut db);
        assert_eq!(result, Frame::Integer(1));
    }

    #[test]
    fn test_hget() {
        let mut db = DB::new();
        let cmd = HGet::from_frames(vec![
            Frame::BulkString(Bytes::from_static(b"hget")),
            Frame::BulkString(Bytes::from_static(b"key")),
            Frame::BulkString(Bytes::from_static(b"field")),
        ])
        .unwrap();
        let result = cmd.apply(&mut db);
        assert_eq!(result, Frame::Nil);
    }
}
