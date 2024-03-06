//! Connection related commands

use super::*;
use crate::db::DB;
use crate::frame::Frame;
use crate::Result;

use marco::Applyer;

#[derive(Debug, Applyer)]
pub struct Quit {}

impl Quit {
    pub fn new() -> Quit {
        Quit {}
    }

    pub fn from_frames(frames: Vec<Frame>) -> Result<Quit> {
        let mut iter = frames.into_iter();
        check_cmd(&mut iter, b"QUIT")?;

        Ok(Quit::new())
    }

    pub fn apply(self, _db: &mut DB) -> Frame {
        Frame::SimpleString("OK".to_string())
    }
}
