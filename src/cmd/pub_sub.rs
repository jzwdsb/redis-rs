//! support pub/sub comamnd

use super::*;
use crate::connection::Connection;
use crate::db::Database;
use crate::frame::Frame;
use crate::{connection, RedisErr};

use std::cell::RefCell;
use std::rc::Rc;

#[derive(Debug)]
pub struct Publish {
    channel: String,
    message: String,
}

impl Publish {
    fn new(channel: String, message: String) -> Self {
        Self { channel, message }
    }

    pub fn from_frames(frames: Vec<Frame>) -> Result<Self, RedisErr> {
        let mut iter = frames.into_iter();
        check_cmd(&mut iter, b"PUBLISH")?;
        let channel = next_string(&mut iter)?; // channel
        let message = next_string(&mut iter)?; // message
        Ok(Self::new(channel, message))
    }

    pub fn apply(self, db: &mut Database) -> Frame {
        match db.publish(&self.channel, &self.message) {
            Ok(count) => Frame::Integer(count as i64),
            Err(_) => Frame::Integer(0),
        }
    }
}

#[derive(Debug)]
pub struct Subscribe {
    conn: Rc<RefCell<connection::Connection>>,
    channels: Vec<String>,
}

impl Subscribe {
    fn new(channels: Vec<String>, conn: Rc<RefCell<connection::Connection>>) -> Self {
        Self { channels, conn }
    }

    pub fn from_frames(
        frames: Vec<Frame>,
        conn: Rc<RefCell<Connection>>,
    ) -> Result<Self, RedisErr> {
        let mut iter = frames.into_iter();
        check_cmd(&mut iter, b"SUBSCRIBE")?;
        let mut channels = Vec::new();
        while let Some(_) = iter.next() {
            let channel = next_string(&mut iter)?;
            channels.push(channel);
        }
        Ok(Self::new(channels, conn))
    }

    pub fn apply(self, db: &mut Database) -> Frame {
        let cnt = self.channels.len();
        for channel in self.channels {
            db.add_subscripter(&channel, self.conn.clone())
        }
        Frame::Array(vec![Frame::SimpleString("subscribe".to_string()); cnt])
    }
}

#[derive(Debug)]
pub struct Unsubscribe {
    conn_id: usize,
    channels: Vec<String>,
}

impl Unsubscribe {
    fn new(conn_id: usize, channels: Vec<String>) -> Self {
        Self { conn_id, channels }
    }

    pub fn from_frames(frames: Vec<Frame>, conn_id: usize) -> Result<Self, RedisErr> {
        let mut iter = frames.into_iter();
        check_cmd(&mut iter, b"UNSUBSCRIBE")?;
        let mut channels = Vec::new();
        while let Some(_) = iter.next() {
            let channel = next_string(&mut iter)?;
            channels.push(channel);
        }
        Ok(Self::new(conn_id, channels))
    }

    pub fn apply(self, db: &mut Database) -> Frame {
        let cnt = self.channels.len();
        for channel in self.channels {
            db.remove_subscripter(&channel, self.conn_id);
        }
        Frame::Array(vec![Frame::SimpleString("unsubscribe".to_string()); cnt])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_frames() {
        let cmd = Publish::from_frames(vec![
            Frame::BulkString(b"PUBLISH".to_vec()),
            Frame::BulkString(b"channel".to_vec()),
            Frame::BulkString(b"message".to_vec()),
        ]);
        assert_eq!(
            cmd.unwrap().channel,
            Publish::new("channel".to_string(), "message".to_string()).channel
        );
    }
}
