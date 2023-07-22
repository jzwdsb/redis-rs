use crate::command::Command;
use crate::data::Value;
use crate::db::DB;
use crate::err::Err;
use crate::protocol::{Protocol, ProtocolError};

use std::collections::HashMap;
use std::error::Error;
use std::io::{ErrorKind, Read, Write};

use mio::net::{TcpListener, TcpStream};
use mio::{Events, Interest, Poll, Token};

// Server is the main struct of the server
// data retrieval and storage are done through the server
// this process can divide into different layers
// 1. transport layer: handle the connection
// 2. protocol layer: parse the command from the stream
// 3. command layer: execute the command
// 4. storage layer: store the data
// 5. response layer: represent the response
// 6. protocol layer: serialize the response
// 7. transport layer: write the response to the stream
// the data flow is like this:
// client -> transport -> protocol -> request  -> storage
//                                                 |
//                                                 v
// client <- transport <- protocol <- response <- storage
pub struct Server {
    db: DB,
    shutdown: bool,
    poll: Poll,
    listener: TcpListener,
    events: Events,
    sockets: HashMap<Token, TcpStream>,
    token_count: Token,
    requests: HashMap<Token, Vec<u8>>,
    response: HashMap<Token, Vec<u8>>,
}

impl Server {
    pub fn new(addr: String, port: u16, max_client: usize) -> Result<Self, Box<dyn Error>> {
        let addr = format!("{}:{}", addr, port).parse()?;
        let db = DB::new();
        let mut listener = TcpListener::bind(addr)?;
        let poll = Poll::new()?;
        poll.registry()
            .register(
                &mut listener,
                Token(0), // server is token 0
                Interest::READABLE,
            )
            .unwrap();
        Ok(Self {
            db: db,
            shutdown: false,
            poll: poll,
            listener: listener,
            events: Events::with_capacity(max_client),
            sockets: HashMap::new(),
            token_count: Token(1),
            response: HashMap::new(),
            requests: HashMap::new(),
        })
    }

    pub fn run(&mut self) -> Result<(), Err> {
        while !self.shutdown {}

        Ok(())
    }

    pub fn handle_event(&mut self) {
        self.poll.poll(&mut self.events, None).unwrap();
        for event in self.events.iter() {
            match event.token() {
                // handle new connection
                Token(0) => match self.listener.accept() {
                    Ok((mut stream, _)) => {
                        let token = self.token_count;
                        self.poll
                            .registry()
                            .register(&mut stream, token, Interest::READABLE)
                            .unwrap();
                        self.sockets.insert(token, stream);
                        self.token_count.0 += 1;
                    }
                    Err(ref e) if e.kind() == ErrorKind::WouldBlock => break,
                    Err(e) => {
                        println!("Failed to accept new connection: {}", e);
                    }
                },
                token if event.is_readable() => {
                    let mut buf = [0; 1024];
                    let mut stream = self.sockets.remove(&token).unwrap();
                    let mut req_bytes = self.requests.remove(&token).unwrap_or(Vec::new());
                    loop {
                        match stream.read(&mut buf) {
                            Ok(0) => {
                                self.poll
                                    .registry()
                                    .deregister(&mut stream)
                                    .unwrap();
                            }
                            Ok(n) => {
                                req_bytes.extend_from_slice(&buf[..n]);
                            }
                            Err(ref e) if e.kind() == ErrorKind::WouldBlock => break,
                            Err(e) => {
                                println!("Failed to read from socket: {}", e);
                                break;
                            }
                        }
                    }
                    let mut protocol = Protocol::from_bytes(&req_bytes);
                    match protocol {
                        Ok(p) => {
                            // TODO: handle the protocol
                            // let command = Command::from_protocol(p);
                            // match command {
                            //     Ok(c) => {
                            //         let response = self.execute(c);
                            //         let res_bytes = response.serialize();
                            //         self.response.insert(token, res_bytes);
                            //         self.poll
                            //             .registry()
                            //             .reregister(&mut stream, token, Interest::WRITABLE)
                            //             .unwrap();
                            //     }
                            //     Err(e) => {
                                    
                            //     }
                            // }
                        }
                        Err(ProtocolError::Incomplete) => {
                            self.requests.insert(token, req_bytes);
                            self.poll
                                .registry()
                                .reregister(&mut stream, token, Interest::READABLE)
                                .unwrap();
                        }
                        Err(ProtocolError::Malformed) => {
                            println!("Malformed request");
                        }
                    }
                    
                }
                token if event.is_writable() => {
                    let mut stream = self.sockets.remove(&token).unwrap();
                    let res_bytes = self.response.remove(&token).unwrap();
                    let mut n = 0;
                    loop {
                        match stream.write(&res_bytes[n..]) {
                            Ok(0) => {
                                self.poll
                                    .registry()
                                    .deregister(&mut stream)
                                    .unwrap();
                                break;
                            }
                            Ok(len) => {
                                n += len;
                                if n == res_bytes.len() {
                                    break;
                                }
                            }
                            Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
                                self.response.insert(token, res_bytes[n..].to_vec());
                                self.poll
                                    .registry()
                                    .reregister(&mut stream, token, Interest::WRITABLE)
                                    .unwrap();
                                break;
                            },
                            Err(e) => {
                                println!("Failed to write to socket: {}", e);
                                break;
                            }
                        }
                    }
                },
                _ => unreachable!(),
            }
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
}
