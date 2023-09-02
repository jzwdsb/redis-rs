use marco::Applyer;

use crate::{db::Database, frame::Frame, value::Value, RedisErr};

use super::*;

use bloomfilter::Bloom;

#[derive(Debug, Applyer)]
pub struct BFAdd {
    key: String,
    value: String,
}

impl BFAdd {
    pub fn new(key: String, value: String) -> Self {
        Self { key, value }
    }

    pub fn from_frames(frames: Vec<Frame>) -> Result<Self, RedisErr> {
        let mut iter = frames.into_iter();
        check_cmd(&mut iter, b"BF.ADD")?;
        let key = next_string(&mut iter)?;
        let value = next_string(&mut iter)?;
        Ok(Self::new(key, value))
    }

    pub fn apply(self, db: &mut Database) -> Frame {
        match db.get_value_mut(&self.key) {
            Some(value) => match value {
                Value::BloomFilter(bf) => {
                    bf.set(&self.value);
                    Frame::Integer(1)
                }
                _ => Frame::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                ),
            },
            None => {
                // TODO: 1024, 1024 should be configurable or calculated
                let mut bf = Bloom::new(1024, 1024);
                bf.set(&self.value);
                db.set_value(&self.key, Value::BloomFilter(bf), None);
                Frame::Integer(1)
            }
        }
    }
}

#[derive(Debug, Applyer)]
pub struct BFExists {
    key: String,
    value: String,
}

impl BFExists {
    pub fn new(key: String, value: String) -> Self {
        Self { key, value }
    }

    pub fn from_frames(frames: Vec<Frame>) -> Result<Self, RedisErr> {
        let mut iter = frames.into_iter();
        check_cmd(&mut iter, b"BF.EXISTS")?;
        let key = next_string(&mut iter)?;
        let value = next_string(&mut iter)?;
        Ok(Self::new(key, value))
    }

    pub fn apply(self, db: &mut Database) -> Frame {
        match db.get_value_mut(&self.key) {
            Some(value) => match value {
                Value::BloomFilter(bf) => {
                    if bf.check(&self.value) {
                        Frame::Integer(1)
                    } else {
                        Frame::Integer(0)
                    }
                }
                _ => Frame::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                ),
            },
            None => Frame::Integer(0),
        }
    }
}
