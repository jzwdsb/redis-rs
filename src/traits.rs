use crate::{db::Database, frame::Frame};

trait Command {
    fn apply(self, db: &mut Database) -> Frame;
}
