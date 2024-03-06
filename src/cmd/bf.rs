//! Bloom Filter commands

use super::*;

use crate::{db::DB, frame::Frame};

use marco::Applyer;

#[derive(Debug, Applyer)]
pub struct BFAdd {
    key: String,
    value: String,
}

impl BFAdd {
    pub fn new(key: String, value: String) -> Self {
        Self { key, value }
    }

    pub fn from_frames(frames: Vec<Frame>) -> Result<Self> {
        let mut iter = frames.into_iter();
        check_cmd(&mut iter, b"BF.ADD")?;
        let key = next_string(&mut iter)?;
        let value = next_string(&mut iter)?;
        Ok(Self::new(key, value))
    }

    pub fn apply(self, db: &mut DB) -> Frame {
        match db.bf_add(self.key, self.value) {
            Ok(()) => Frame::Integer(1),
            Err(_) => Frame::Integer(0),
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

    pub fn from_frames(frames: Vec<Frame>) -> Result<Self> {
        let mut iter = frames.into_iter();
        check_cmd(&mut iter, b"BF.EXISTS")?;
        let key = next_string(&mut iter)?;
        let value = next_string(&mut iter)?;
        Ok(Self::new(key, value))
    }

    pub fn apply(self, db: &mut DB) -> Frame {
        match db.bf_exists(&self.key, &self.value) {
            Ok(true) => Frame::Integer(1),
            Ok(false) => Frame::Integer(0),
            Err(_) => Frame::Error("ERR".to_string()),
        }
    }
}
