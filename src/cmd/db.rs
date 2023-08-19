use super::*;
use crate::db::Database;
use crate::frame::Frame;
use crate::RedisErr;

use marco::{Applyer, CommandParser};

#[derive(Debug, Applyer, CommandParser)]
pub struct Ping {
    message: Option<Vec<u8>>,
}

impl Ping {
    pub fn new(message: Option<Vec<u8>>) -> Self {
        Self { message }
    }

    pub fn from_frames(frames: Vec<Frame>) -> Result<Self, RedisErr> {
        if frames.len() > 2 {
            return Err(RedisErr::WrongNumberOfArguments);
        }
        let mut iter = frames.into_iter();
        check_cmd(&mut iter, b"PING")?;
        let message = if iter.len() == 1 {
            Some(next_bytes(&mut iter)?)
        } else {
            None
        };
        Ok(Self::new(message))
    }

    pub fn apply(self, _db: &mut Database) -> Frame {
        if let Some(message) = self.message {
            Frame::BulkString(message)
        } else {
            Frame::SimpleString("PONG".to_string())
        }
    }
}

#[derive(Debug, Applyer, CommandParser)]
pub struct Flush {}

impl Flush {
    pub fn new() -> Self {
        Self {}
    }

    pub fn from_frames(frames: Vec<Frame>) -> Result<Self, RedisErr> {
        if frames.len() != 1 {
            return Err(RedisErr::WrongNumberOfArguments);
        }
        check_cmd(&mut frames.into_iter(), b"FLUSH")?;
        Ok(Self::new())
    }

    pub fn apply(self, db: &mut Database) -> Frame {
        db.flush();
        Frame::SimpleString("OK".to_string())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_flush() {
        let mut db = Database::new();
        let cmd = Flush::from_frames(vec![Frame::BulkString(b"flush".to_vec())]);
        assert_eq!(cmd.is_ok(), true);
        let cmd: Flush = cmd.unwrap();

        let result = cmd.apply(&mut db);
        assert_eq!(result, Frame::SimpleString("OK".to_string()));
    }
}
