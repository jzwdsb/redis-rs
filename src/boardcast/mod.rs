use crate::connection::Connection;

use std::cell::RefCell;
use std::io::Write;
use std::rc::Rc;

pub struct Sender {
    receiver: Vec<Receiver>,
}

impl Sender {
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self {
            receiver: Vec::new(),
        }
    }

    #[allow(dead_code)]
    pub fn send(&mut self, msg: String) -> usize {
        let mut count = 0;
        for receiver in self.receiver.iter_mut() {
            match receiver.write(msg.as_bytes()) {
                Ok(_) => count += 1,
                Err(_) => {}
            }
        }

        count
    }

    #[allow(dead_code)]
    pub fn add_receiver(&mut self, receiver: Receiver) {
        self.receiver.push(receiver);
    }
}

pub struct Receiver {
    dest: Rc<RefCell<Connection>>,
}

impl Write for Receiver {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.dest.borrow_mut().write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.dest.borrow_mut().flush()
    }
}

impl Receiver {
    #[allow(dead_code)]
    pub fn new(dest: Rc<RefCell<Connection>>) -> Self {
        Self { dest: dest }
    }
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use super::*;
}
