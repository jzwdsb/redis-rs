use crate::cmd::check_cmd;
use crate::cmd::CommandErr;
use crate::db::Database;
use crate::frame::Frame;

#[derive(Debug)]
pub struct Flush {}

impl Flush {
    fn new() -> Self {
        Self {}
    }

    pub fn from_frames(frames: Vec<Frame>) -> Result<Self, CommandErr> {
        if frames.len() != 1 {
            return Err(CommandErr::WrongNumberOfArguments);
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
