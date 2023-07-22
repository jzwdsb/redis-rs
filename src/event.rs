/*
    Event Driven .
*/

use std::collections::HashMap;
use std::error::Error;
use std::io::{ErrorKind, Read, Write};
use std::time::Duration;
use std::net::SocketAddr;

use mio::{Events, Interest, Poll, Token};

use mio::net::{TcpListener, TcpStream};

use crate::err::Err;

pub(crate) struct EventLoop {
    poll: Poll,
    listener: TcpListener,
    events: Events,
    sockets: HashMap<Token, TcpStream>,
    token_count: Token,
    requests: HashMap<Token, Vec<u8>>,
    response: HashMap<Token, Vec<u8>>,
}


impl EventLoop {
    pub fn new(addr: SocketAddr, max_event_size: usize) -> Result<Self, Box<dyn Error>> {
        let mut listener = TcpListener::bind(addr)?;
        let poll = Poll::new()?;
        poll.registry()
            .register(
                &mut listener,
                Token(0), // server is token 0
                Interest::READABLE | Interest::WRITABLE,
            )
            .unwrap();
        Ok(EventLoop {
            poll: poll,
            listener: listener,
            events: Events::with_capacity(max_event_size),
            sockets: HashMap::new(),
            token_count: Token(1),
            response: HashMap::new(),
            requests: HashMap::new(),
        })
    }

    // connection to the server first
    // request and response are both in the same connection and act like ping-pong
    // server wait for the request, handle the request and send the response.
    // client send the request and wait for the response.
    // for the server, the connection is readable when waitting for the request
    // and writable when sending the response.

    pub fn event_loop(&mut self) {
        loop {
            self.handle_event(|data| Ok(*data));
        }
    }

    pub fn handle_event(&mut self, handle: fn(&Vec<u8>) -> Result<Vec<u8>, Err> ) {
        self.poll.poll(&mut self.events, Some(Duration::from_millis(1000))).unwrap();
        
        // check whether is there is incoming request or new connection
        for event in self.events.iter() {
            match event.token() {
                Token(0) => {
                    match self.listener.accept() {
                        Ok((mut stream, addr)) => {
                            println!("New connection: {}", addr);
                            self.poll
                                .registry()
                                .register(&mut stream, self.token_count, Interest::READABLE)
                                .unwrap();
                            self.sockets.insert(self.token_count, stream);
                            self.token_count.0 += 1;
                        }
                        // no connection is ready to read
                        Err(ref e) if e.kind() == ErrorKind::WouldBlock => break,
                        Err(e) => println!("Error: {}", e),
                    }
                }
                // a connection is ready to read
                token if event.is_readable() => {
                    // new request comes in
                    // read and parse the request into a request object
                    // return an error if the request is malformed or incomplete when timeout.
                    let mut buffer = [0; 1024];
                    let mut req_bytes = self.requests.remove(&token).unwrap_or(Vec::new());
                    loop {
                        let read = self.sockets.get_mut(&token).unwrap().read(&mut buffer);
                        match read {
                            Ok(0) => {
                                // readable but EOF, remove it from the poll.
                                println!("meet EOF on token {:?}", token);
                                let mut stream = self.sockets.remove(&token).unwrap();
                                self.poll.registry().deregister(&mut stream).unwrap();
                            }
                            Ok(len) => {
                                // readable and not EOF, save the request to the requests map.
                                req_bytes.extend_from_slice(&buffer[0..len]);
                            }
                            Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
                                // but no data to read, break the loop.
                                break;
                            }
                            Err(e) => panic!("err: {:?}", e),
                        }
                    }
                    // handle the request
                    let res = handle(&req_bytes);
                    if res.is_err() {
                        // request incomplete, save it to the requests map.
                        self.requests.insert(token, req_bytes);
                        continue;
                    }
                    // request complete, send the response to the client.
                    let res = res.unwrap();
                    self.response.insert(token, res);
                    self.poll
                        .registry()
                        .reregister(self.sockets.get_mut(&token).unwrap(), token, Interest::WRITABLE)
                        .unwrap();
                }
                // a connection is ready to write
                token if event.is_writable() => {
                    // check if there is any data to write
                    if self.response.contains_key(&token) {
                        let stream = self.sockets.get_mut(&token).unwrap();
                        let response = self.response.get_mut(&token).unwrap();
                        stream.write(response).unwrap();
                        self.response.remove(&token);
                        // reregister the stream to the poll with readable interest
                        self.poll
                            .registry()
                            .reregister(stream, token, Interest::READABLE)
                            .unwrap();
                    }
                }
                _ => unreachable!(),
            }
        }
    }

    // get request and check whether the request is complete
    pub fn get_all_requests(&self) -> Vec<(Token, &Vec<u8>)> {
        let mut requests = Vec::new();
        for (token, request) in self.requests.iter() {
            requests.push((token.clone(), request));
        }
        requests
    }

    // remove the request from the requests map if the request is complete and can be handled
    pub fn remove_request(&mut self, token: Token) {
        self.requests.remove(&token);
    }

    // send response to the client
    pub fn send_response(&mut self, token: Token, response: Vec<u8>) {
        self.response.insert(token, response);
        self.poll
            .registry()
            .reregister(self.sockets.get_mut(&token).unwrap(), token, Interest::WRITABLE)
            .unwrap();
    }

}
