use crate::command::Command;
use crate::data::Value;
use crate::db::DB;
use crate::err::Err;
use crate::protocol::Protocol;

use std::io::{Read, Write};
use std::net::TcpListener;

pub struct ServerBuilder {
    addr: String,
    port: u16,
}

impl ServerBuilder {
    pub fn new() -> Self {
        Self {
            addr: String::from(""),
            port: 0,
        }
    }
    pub fn addr(mut self, addr: &str) -> Self {
        self.addr = String::from(addr);
        self
    }
    pub fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }
    pub fn build(self) -> Server {
        Server::new(self.addr, self.port)
    }
}

pub struct Server {
    db: DB,
    pub port: u16,
    pub addr: String,
    pub shutdown: bool,
}

impl Server {
    pub fn new(addr: String, port: u16) -> Self {
        Self {
            db: DB::new(),
            port,
            addr,
            shutdown: false,
        }
    }

    pub fn run(&mut self) -> Result<(), Err> {
        let listener = TcpListener::bind(format!("{}:{}", self.addr, self.port)).unwrap();
        println!("Server is running on {}:{}", self.addr, self.port);
        // listener.set_nonblocking(true)?;

        loop {
            if self.shutdown {
                break;
            }

            match listener.accept() {
                Ok((stream, addr)) => {
                    println!("New connection: {}", addr);
                    self.handle_connection(stream);
                    println!("Connection closed: {}", addr)
                }
                Err(e) => {
                    std::thread::sleep(std::time::Duration::from_secs(1));
                    println!("Error: {}", e);
                }
            }
        }
        Ok(())
    }

    // parse command from stream
    // execute the command
    // write the response to the stream
    fn handle_connection(&mut self, stream: impl Read + Write) {
        //
        let mut stream = stream;

        loop {
            let protocol = crate::protocol::Protocol::from_stream(&mut stream);
            if protocol.is_err() {
                println!("connection closed: {:?}", protocol.unwrap_err());
                drop(stream);
                break;
            }
            let protocol = protocol.unwrap();

            let command = Command::from_protocol(protocol);
            if command.is_err() {
                println!("Error: {:?}", command);
                stream.write_all(b"-ERR Syntax error\r\n").unwrap();
                drop(stream);
                break;
            }

            let command = command.unwrap();
            let response = self.execute(command);

            let resp_bytes = response.serialize_response();
            println!("Response: {:?}", resp_bytes);
            stream.write_all(resp_bytes.as_slice()).unwrap();
        }
    }

    fn execute(&mut self, command: Command) -> Protocol {
        match command {
            Command::Get(key) => match self.db.get(&key) {
                Ok(value) => match value {
                    Value::KV(v) => Protocol::SimpleString(String::from_utf8(v).unwrap()),
                    Value::Nil => Protocol::SimpleString(String::from("(nil)")),
                    _ => Protocol::Errors(String::from("ERR")),
                },
                Err(e) => Protocol::Errors(e.into()),
            },
            Command::Set(key, value, expire_at) => {
                let res = self.db.set(key, value, expire_at);
                if res.is_ok() {
                    Protocol::SimpleString(String::from("OK"))
                } else {
                    Protocol::Errors(String::from("ERR"))
                }
            }
            Command::Del(key) => {
                self.db.del(&key);
                Protocol::SimpleString(String::from("OK"))
            }
            Command::Expire(key, expire_at) => {
                self.db.expire(key, expire_at);
                Protocol::SimpleString(String::from("OK"))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestStream {
        pub data: Vec<u8>,
        pub closed: bool,
    }

    impl From<Vec<u8>> for TestStream {
        fn from(data: Vec<u8>) -> Self {
            Self {
                data: data,
                closed: false,
            }
        }
    }

    impl std::io::Read for TestStream {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            if self.closed == false {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Stream is closed",
                ));
            }
            let len = std::cmp::min(buf.len(), self.data.len());
            buf[..len].copy_from_slice(&self.data[..len]);
            self.data = self.data[len..].to_vec();
            Ok(len)
        }
    }

    impl std::io::Write for TestStream {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.data.extend_from_slice(buf);
            self.closed = true;
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn test_handle_connection() {
        let mut server = Server::new(String::from("127.0.0.1"), 6379);
        let stream: TestStream = "$7\r\nSET a b\r\n".as_bytes().to_vec().into();
        server.handle_connection(stream);
    }
}
