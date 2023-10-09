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
        for receiver in self.receiver.iter_mut() {
            match receiver
                .dest
                .borrow_mut()
                .write_frame(BulkString(msg.as_bytes().to_vec()))
            {
                Ok(_) => count += 1,
                Err(_) => {}
            };
        }

        count
    }

    pub fn add_receiver(&mut self, receiver: Receiver) {
        self.receiver.push(receiver);
    }

    pub fn remove_receiver(&mut self, ids: Vec<usize>) {
        let mut new_receiver = Vec::new();
        for receiver in self.receiver.iter() {
            if !ids.contains(&receiver.dest.borrow().id()) {
                new_receiver.push(receiver.clone());
            }
        }
        self.receiver = new_receiver;
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
