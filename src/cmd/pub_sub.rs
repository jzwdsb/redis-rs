//! support pub/sub comamnd
use super::*;
use crate::db::Database;
use crate::frame::Frame;
use crate::RedisErr;

use marco::Applyer;


#[derive(Debug, Applyer)]
struct Publish {
    channel: String,
    message: String,
}

impl Publish {
    fn new (channel: String, message: String) -> Self {
        Self { channel, message }
    }

    fn from_frames(frames: Vec<Frame>) -> Result<Self, RedisErr> {
        let mut iter = frames.into_iter();
        check_cmd(&mut iter, b"PUBLISH")?;
        let channel = next_string(&mut iter)?; // channel
        let message = next_string(&mut iter)?; // message
        Ok(Self::new(channel, message))
    }

    fn apply(self, db: &Database) -> Frame {
        todo!("publish")
    }
}

#[derive(Debug, Applyer)]
struct Subscribe {
    channels: Vec<String>,
}

impl Subscribe {
    fn new(channels: Vec<String>) -> Self {
        Self { channels }
    }

    fn from_frames(frames: Vec<Frame>) -> Result<Self, RedisErr> {
        let mut iter = frames.into_iter();
        check_cmd(&mut iter, b"SUBSCRIBE")?;
        let mut channels = Vec::new();
        while let Some(frame) = iter.next() {
            let channel = next_string(&mut iter)?;
            channels.push(channel);
        }
        Ok(Self::new(channels))
    }

    fn apply(self, db: &Database) -> Frame {
        todo!("subscribe")
    }
}