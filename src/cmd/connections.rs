use super::check_cmd;

use crate::{db::Database, frame::Frame, RedisErr};

#[derive(Debug)]
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

    pub fn apply(&self, _db: &mut Database) -> Frame {
        Frame::SimpleString("OK".to_string())
    }
}
