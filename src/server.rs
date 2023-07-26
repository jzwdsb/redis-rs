//! Redis Server implementation
//! use mio to achieve non-blocking IO, multiplexing and event driven
//! an event loop is used to handle all the IO events

use crate::cmd::Command;
use crate::db::Database;
use crate::err::Err;
use crate::frame::{Frame, FrameError};
use crate::helper::{read_request, write_response};

use std::collections::HashMap;
use std::error::Error;
use std::io::ErrorKind;

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
    db: Database,
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
    pub fn new(addr: &str, port: u16, max_client: usize) -> Result<Self, Box<dyn Error>> {
        let addr = format!("{}:{}", addr, port).parse()?;
        let db = Database::new();
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
        while !self.shutdown {
            self.handle_event();
        }

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
                    let req_bytes = read_request(&mut self.sockets.get_mut(&token).unwrap());
                    let req_bytes = match req_bytes {
                        Ok(req_bytes) => req_bytes,
                        Err(e) => {
                            println!("Failed to read request: {}", e);
                            continue;
                        }
                    };

                    let protocol = Frame::from_bytes(&req_bytes);
                    let protocol = match protocol {
                        Ok(protocol) => protocol,
                        Err(e) if e == FrameError::Incomplete => {
                            // incomplete frame, wait for next read
                            self.requests.insert(token, req_bytes);
                            continue;
                        }
                        Err(e) => {
                            println!("Failed to parse protocol: {}", e);
                            continue;
                        }
                    };

                    let command = Command::from_frame(protocol);
                    let command = match command {
                        Ok(command) => command,
                        Err(e) => {
                            println!("Failed to parse command: {}", e);
                            continue;
                        }
                    };
                    let resp = Command::apply(&mut self.db, command);
                    self.response.insert(token, resp.serialize());
                }
                token if event.is_writable() => {
                    if self.response.contains_key(&token) {
                        let resp_bytes = self.response.remove(&token).unwrap();
                        let sent = write_response(
                            self.sockets.get_mut(&token).unwrap(),
                            resp_bytes.as_slice(),
                        );
                        match sent {
                            Ok(len) => {
                                if len == resp_bytes.len() {
                                    self.poll
                                        .registry()
                                        .reregister(
                                            self.sockets.get_mut(&token).unwrap(),
                                            token,
                                            Interest::READABLE,
                                        )
                                        .unwrap();
                                } else {
                                    self.response.insert(token, resp_bytes[len..].to_vec());
                                }
                            }
                            Err(e) => {
                                println!("Failed to send response: {}", e);
                            }
                        }
                    }
                }
                _ => unreachable!(),
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
