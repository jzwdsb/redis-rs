#![allow(dead_code)]
pub trait Logger {
    fn debug(&self, msg: &str);
    fn info(&self, msg: &str);
    fn error(&self, msg: &str);
    fn fatal(&self, msg: &str);
}
