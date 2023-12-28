use crate::connection::{AsyncConnection, FrameWriter};
use crate::frame::Frame::BulkString;

use std::cell::RefCell;
use std::rc::Rc;

pub struct Sender {
    receiver: Vec<Receiver>,
}

impl Sender {
    pub fn new() -> Self {
        Self {
            receiver: Vec::new(),
        }
    }

    pub fn send(&mut self, msg: String) -> usize {
        let mut count = 0;

        count
    }

    pub fn add_receiver(&mut self, receiver: Receiver) {
        self.receiver.push(receiver);
    }

    pub fn remove_receiver(&mut self, ids: Vec<usize>) {
        
    }
}

#[derive(Clone)]
pub struct Receiver {
    dest: Rc<RefCell<AsyncConnection>>,
}

impl Receiver {
    pub fn new(dest: Rc<RefCell<AsyncConnection>>) -> Self {
        Self { dest: dest }
    }
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use super::*;
}
