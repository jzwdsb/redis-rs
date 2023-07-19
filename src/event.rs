/*
    Event Driven .
*/

use std::collections::HashMap;
use std::io::{ErrorKind, Read, Write};
use std::net::Shutdown;
use std::time::Duration;

use mio::{Events, Interest, Poll, Token};

use mio::net::{TcpListener, TcpStream};

struct EventLoop {
    poll: Poll,
    listener: TcpListener,
    events: Events,
    sockets: HashMap<Token, TcpStream>,
    token_count: Token,
    response: HashMap<Token, Vec<u8>>,
}

const SERVER_TOKEN: Token = Token(0);

impl EventLoop {
    fn new(addr: &str, max_event_size: usize) -> Self {
        let addr = addr.parse().unwrap();
        let mut listener: TcpListener = TcpListener::bind(addr).unwrap();
        let poll = Poll::new().unwrap();
        poll.registry()
            .register(
                &mut listener,
                SERVER_TOKEN,
                Interest::READABLE | Interest::WRITABLE,
            )
            .unwrap();
        EventLoop {
            poll: poll,
            listener: listener,
            events: Events::with_capacity(max_event_size),
            token_count: Token(1),
            sockets: HashMap::new(),
            response: HashMap::new(),
        }
    }

    fn event_loop(&mut self) {
        loop {
            let mut buffer = [0; 1024];
            self.poll.poll(&mut self.events, None).unwrap();
            for event in self.events.iter() {
                match event.token() {
                    Token(0) => {
                        let (mut stream, addr) = self.listener.accept().unwrap();
                        println!("New connection: {}", addr);
                        self.poll
                            .registry()
                            .register(
                                &mut stream,
                                self.token_count,
                                Interest::READABLE | Interest::WRITABLE,
                            )
                            .unwrap();
                        self.sockets.insert(self.token_count, stream);
                        self.token_count.0 += 1;
                    }
                    // a connection is ready to read
                    token if event.is_readable() => {
                        // new request comes in
                        // read and parse the request into a request object
                        // return an error if the request is malformed or incomplete when timeout.
                        if !self.sockets.contains_key(&token) {
                            print!("socket not found for token {:?}", token);
                            continue;
                        }
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
                                    // readable and not EOF, echo the request.
                                    let mut stream = self.sockets.get_mut(&token).unwrap();
                                    // FIXME: not writable
                                    stream.write(&buffer[0..len]).unwrap();
                                }
                                Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
                                    // readable but no data to read, break the loop.
                                    break;
                                }
                                Err(e) => panic!("err: {:?}", e),
                            }
                        }
                    }
                    // a connection is ready to write
                    token if event.is_writable() => {
                        // check if there is any data to write
                        if self.response.contains_key(&token) {
                            let stream = self.sockets.get_mut(&token).unwrap();
                            let response = self.response.get_mut(&token).unwrap();
                            stream.write(response).unwrap();
                            self.response.remove(&token);
                        }
                    }
                    _ => unreachable!(),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::thread;

    /*
    use mutiple threads to test the event loop.
    one thead act like the server, and the others act like the client.
    the server simply echo the request.

     */
    #[test]
    fn test_event_loop() {
        // start server
        let mut event_loop = EventLoop::new("127.0.0.1:8080", 1024);
    
        let server = thread::spawn(move || event_loop.event_loop());

        let client = thread::spawn(move || {
            let mut stream = TcpStream::connect("127.0.0.1:8080".parse().unwrap()).unwrap();
            let mut buffer = [0; 1024];
            thread::sleep(std::time::Duration::from_secs(1));
            stream.write(b"hello world").unwrap();
            stream.flush().unwrap();
            stream.read(&mut buffer).unwrap();
            assert_eq!(&buffer[0..11], b"hello world");
        });

        let res = server.join();
        if res.is_err() {
            println!("server error: {:?}", res.err());
        }
        client.join().unwrap();
    }
}
