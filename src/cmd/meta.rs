//! Meta commands

use super::*;
use crate::db::DB;
use crate::frame::Frame;
use crate::Result;

use marco::Applyer;

use std::time::{Duration, Instant};

#[derive(Debug, Applyer)]
pub struct Type {
    key: String,
}

impl Type {
    fn new(key: String) -> Self {
        Self { key }
    }

    pub fn from_frames(frames: Vec<Frame>) -> Result<Self> {
        if frames.len() != 2 {
            return Err(RedisErr::WrongNumberOfArguments);
        }
        let mut iter = frames.into_iter();
        check_cmd(&mut iter, b"TYPE")?;
        let key = next_string(&mut iter)?; // key
        Ok(Self::new(key))
    }

    pub fn apply(self, db: &mut DB) -> Frame {
        match db.get_type(&self.key) {
            Some(t) => Frame::SimpleString(t.to_string()),
            None => Frame::SimpleString("none".to_string()),
        }
    }
}

#[derive(Debug, Applyer)]
pub struct Del {
    key: String,
}

impl Del {
    fn new(key: String) -> Self {
        Self { key }
    }

    pub fn from_frames(frames: Vec<Frame>) -> Result<Self> {
        let mut iter = frames.into_iter();
        check_cmd(&mut iter, b"DEL")?;
        let key = next_string(&mut iter)?; // key
        Ok(Self::new(key))
    }

    pub fn apply(self, db: &mut DB) -> Frame {
        let db = db;
        match db.del(&self.key) {
            Some(_) => Frame::Integer(1),
            None => Frame::Integer(0),
        }
    }
}

#[derive(Debug, Applyer)]
pub struct Expire {
    key: String,
    expire: Duration,
}

impl Expire {
    fn new(key: String, expire: Duration) -> Self {
        Self { key, expire }
    }

    pub fn from_frames(frames: Vec<Frame>) -> Result<Self> {
        let mut iter = frames.into_iter();
        check_cmd(&mut iter, b"EXPIRE")?;
        let key = next_string(&mut iter)?; // key
        let expire = Duration::from_secs(next_integer(&mut iter)? as u64); // expire
        Ok(Self::new(key, expire))
    }

    pub fn apply(self, db: &mut DB) -> Frame {
        let db = db;
        let expire_at = Instant::now() + self.expire;
        match db.expire(&self.key, expire_at) {
            Ok(()) => Frame::Integer(1),
            Err(e) => match e {
                RedisErr::KeyNotFound => Frame::Integer(0),
                _ => unreachable!("unexpect expire error: {:?}", e),
            },
        }
    }
}

#[derive(Debug)]
enum ObjectOption {
    Encoding,
    Idletime,
    Refcount,
    Frequency,
}

#[derive(Debug, Applyer)]
pub struct Object {
    key: String,
    option: ObjectOption,
}

impl Object {
    fn new(key: String, opt: ObjectOption) -> Self {
        Self { key, option: opt }
    }

    pub fn from_frames(frames: Vec<Frame>) -> Result<Self> {
        if frames.len() != 3 {
            return Err(RedisErr::WrongNumberOfArguments);
        }
        let mut iter = frames.into_iter();
        check_cmd(&mut iter, b"OBJECT")?;
        let option = match next_string(&mut iter)?.to_uppercase().as_str() {
            "ENCODING" => ObjectOption::Encoding,
            "IDLETIME" => ObjectOption::Idletime,
            "REFCOUNT" => ObjectOption::Refcount,
            "FREQUENCY" => ObjectOption::Frequency,
            _ => return Err(RedisErr::UnknownCommand),
        };
        let key = next_string(&mut iter)?; // key
        Ok(Self::new(key, option))
    }

    pub fn apply(self, db: &mut DB) -> Frame {
        match self.option {
            ObjectOption::Encoding => {
                // only raw encoding is supported for now
                Frame::SimpleString("raw".to_string())
            }
            ObjectOption::Idletime => match db.get_object_last_touch(&self.key) {
                Some(last_touch) => Frame::Integer(last_touch.elapsed().as_secs() as i64),
                None => Frame::Nil,
            },
            // don't support refcount
            ObjectOption::Refcount => Frame::Integer(0),
            // don't support frequency
            ObjectOption::Frequency => Frame::Integer(0),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_del() {
        let mut db = DB::new();
        let cmd = Del::from_frames(vec![
            Frame::BulkString(Bytes::from_static(b"del")),
            Frame::BulkString(Bytes::from_static(b"key")),
        ])
        .unwrap();
        let result = cmd.apply(&mut db);
        assert_eq!(result, Frame::Integer(0));
    }

    #[test]
    fn test_expire() {
        let mut db = DB::new();
        let cmd = Expire::from_frames(vec![
            Frame::BulkString(Bytes::from_static(b"expire")),
            Frame::BulkString(Bytes::from_static(b"key")),
            Frame::Integer(10),
        ])
        .unwrap();
        let result = cmd.apply(&mut db);
        assert_eq!(result, Frame::Integer(0));
    }

    #[test]
    fn test_type() {
        let mut db = DB::new();
        let cmd = Type::from_frames(vec![
            Frame::BulkString(Bytes::from_static(b"type")),
            Frame::BulkString(Bytes::from_static(b"key")),
        ])
        .unwrap();
        let result = cmd.apply(&mut db);
        assert_eq!(result, Frame::SimpleString("none".to_string()));
    }
}
