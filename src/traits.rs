use crate::{db::Database, frame::Frame};

trait DCommand {
    fn apply(self, db: &mut Database) -> Frame;
}


