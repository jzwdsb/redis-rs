use std::io::Write;

struct Sender {
    receiver: Vec<Box<dyn Write>>,
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
        self.receiver.push(Box::new(receiver));
    }
}

struct Receiver {
    dest: Box<dyn Write>,
}

impl Write for Receiver {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.dest.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.dest.flush()
    }
}

impl Receiver {
    #[allow(dead_code)]
    pub fn new(dest: impl Write + 'static) -> Self {
        Self {
            dest: Box::new(dest),
        }
    }


}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use super::*;
    struct MockStream {}
    impl Write for MockStream {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl Read for MockStream {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            println!("read {}", buf.len());
            Ok(buf.len())
        }
    }

    #[test]
    fn test_send() {
        let mut sender = Sender::new();
        let mut receiver = Receiver::new(mockStream {});
        sender.add_receiver(receiver);
        assert_eq!(sender.send("hello".to_string()), 1);
    }
}
