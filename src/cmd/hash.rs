use super::*;
use crate::db::Database;
use crate::frame::Frame;
use crate::RedisErr;

use marco::{Applyer, CommandParser};

#[derive(Debug, Applyer, CommandParser)]
pub struct HSet {
    key: String,
    field_values: Vec<(String, Vec<u8>)>,
}

impl HSet {
    fn new(key: String, field_values: Vec<(String, Vec<u8>)>) -> Self {
        Self { key, field_values }
    }

    pub fn from_frames(frames: Vec<Frame>) -> Result<Self, RedisErr> {
        let mut iter = frames.into_iter();
        check_cmd(&mut iter, b"HSET")?;

        let key = next_string(&mut iter)?; // key
        let mut field_values = Vec::new();
        while iter.len() > 0 {
            let field = next_string(&mut iter)?; // field
            let value = next_bytes(&mut iter)?; // value
            field_values.push((field, value));
        }
        if field_values.len() == 0 {
            return Err(RedisErr::WrongNumberOfArguments);
        }
        Ok(Self::new(key, field_values))
    }

    pub fn apply(self: Box<Self>, db: &mut Database) -> Frame {
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

#[derive(Debug, Applyer, CommandParser)]
pub struct HGet {
    key: String,
    field: String,
}

impl HGet {
    fn new(key: String, field: String) -> Self {
        Self { key, field }
    }

    pub fn from_frames(frames: Vec<Frame>) -> Result<Self, RedisErr> {
        let mut iter = frames.into_iter();
        check_cmd(&mut iter, b"HGET")?;

        let key = next_string(&mut iter)?; // key
        let field = next_string(&mut iter)?; // field
        Ok(Self::new(key, field))
    }

    pub fn apply(self: Box<Self>, db: &mut Database) -> Frame {
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
        let mut db = Database::new();
        let cmd = HSet::from_frames(vec![
            Frame::BulkString(b"hset".to_vec()),
            Frame::BulkString(b"key".to_vec()),
            Frame::BulkString(b"field".to_vec()),
            Frame::BulkString(b"value".to_vec()),
        ])
        .unwrap();
        let result = Box::new(cmd).apply(&mut db);
        assert_eq!(result, Frame::Integer(1));
    }

    #[test]
    fn test_hget() {
        let mut db: Database = Database::new();
        let cmd = HGet::from_frames(vec![
            Frame::BulkString(b"hget".to_vec()),
            Frame::BulkString(b"key".to_vec()),
            Frame::BulkString(b"field".to_vec()),
        ])
        .unwrap();
        let result = Box::new(cmd).apply(&mut db);
        assert_eq!(result, Frame::Nil);
    }
}
