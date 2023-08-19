use super::*;
use crate::db::Database;
use crate::frame::Frame;
use crate::RedisErr;

use marco::{Applyer, CommandParser};

#[derive(Debug, Applyer, CommandParser)]
pub struct Quit {}

impl Quit {
    pub fn new() -> Quit {
        Quit {}
    }

    pub fn from_frames(frames: Vec<Frame>) -> Result<Quit, RedisErr> {
        let mut iter = frames.into_iter();
        check_cmd(&mut iter, b"QUIT")?;

        Ok(Quit::new())
    }

    pub fn apply(self: Box<Self>, _db: &mut Database) -> Frame {
        Frame::SimpleString("OK".to_string())
    }
}
