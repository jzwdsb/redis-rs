//! Redis Server implementation
//! use mio to achieve non-blocking IO, multiplexing and event driven
//! an event loop is used to handle all the IO events

use crate::cmd::{Command, Parser};
use crate::connection::{AsyncConnection, FrameReader, FrameWriter};
use crate::db::Database;
use crate::frame::Frame;
use crate::RedisErr;

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::time::{Duration, SystemTime};

use mio::net::TcpListener;
use mio::{Events, Interest, Poll, Token};

#[allow(unused_imports)]
use log::{debug, error, info, trace, warn};

pub struct ServerBuilder {
    addr: String,
    port: u16,
    max_client: usize,

    handler_num: usize,
}

impl ServerBuilder {
    pub fn new(/* ... */) -> Self {
        Self {
            addr: "0.0.0.0".to_string(),
            port: 6379,
            max_client: 1024,
            handler_num: 1,
        }
    }

    pub fn addr(mut self, addr: &str) -> Self {
        self.addr = addr.to_string();
        self
    }

    pub fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    pub fn max_client(mut self, max_client: usize) -> Self {
        self.max_client = max_client;
        self
    }

    pub fn handler_num(mut self, handler_num: usize) -> Self {
        self.handler_num = handler_num;
        self
    }

    pub fn build(self) -> Result<Server, RedisErr> {
        Server::new(&self.addr, self.port, self.max_client)
    }
}

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
    sockets: HashMap<Token, Rc<RefCell<AsyncConnection>>>,
    token_count: Token,
    wait_duration: Duration,
    command_parser: Parser,
    response: HashMap<Token, Frame>,
}

impl Server {
    pub fn new(addr: &str, port: u16, max_client: usize) -> Result<Self, RedisErr> {
        let addr: std::net::SocketAddr = format!("{}:{}", addr, port).parse()?;
        let db = Database::new();
        let mut listener = TcpListener::bind(addr)?;
        let poll = Poll::new()?;
        poll.registry().register(
            &mut listener,
            Token(0), // server is token 0
            Interest::READABLE,
        )?;
        Ok(Self {
            db: db,
            shutdown: false,
            poll: poll,
            listener: listener,
            events: Events::with_capacity(max_client),
            sockets: HashMap::new(),
            token_count: Token(1),
            wait_duration: Duration::from_millis(100),
            command_parser: crate::cmd::Parser::new(),
            response: HashMap::new(),
        })
    }

    pub fn run(&mut self) -> Result<(), RedisErr> {
        while !self.shutdown {
            let (reads, writes) = self.collect_events();
            let resp_cnt = self.handle_writes(writes);
            let req_cnt = self.handle_reads(reads);

            if resp_cnt == 0 && req_cnt == 0 {
                self.idle();
            }
        }

        Ok(())
    }

    fn collect_events(&mut self) -> (Vec<Token>, HashSet<Token>) {
        self.poll(Some(self.wait_duration)).unwrap();
        let (mut reads, mut writes) = (Vec::new(), HashSet::new());
        for event in self.events.iter() {
            if event.is_readable() {
                reads.push(event.token());
            } else if event.is_writable() {
                writes.insert(event.token());
            }
        }
        (reads, writes)
    }

    fn handle_reads(&mut self, reads: Vec<Token>) -> usize {
        let count = reads.len();
        for read in reads {
            match read {
                // handle new connection
                Token(0) => self.accept_new_connection(),
                token => match self.handle_request(token) {
                    Ok(resp) => {
                        if let Some(resp) = resp {
                            self.set_resp(token, resp);
                            self.invert_interest(token, Interest::WRITABLE);
                        }
                    }
                    Err(e) => {
                        self.handle_error(token, e);
                    }
                },
            }
        }
        count
    }

    fn handle_error(&mut self, token: Token, err: RedisErr) {
        match err {
            RedisErr::ConnectionAborted => self.drop_conncetion(&token),
            _ => {
                error!("Error: {:?}", err);
            }
        }
    }

    fn handle_writes(&mut self, writes: HashSet<Token>) -> usize {
        if self.response.is_empty() || writes.is_empty() {
            return 0;
        }

        let can_write = {
            let mut can_write = Vec::new();
            for token in self.response.keys().into_iter() {
                if writes.contains(&token) {
                    can_write.push(token.clone());
                }
            }
            can_write
        };
        let count = can_write.len();
        for token in can_write {
            let resp = self.response.remove(&token).unwrap();
            self.send_response(token, resp);
            self.invert_interest(token, Interest::READABLE);
        }
        count
    }

    fn handle_request(&mut self, token: Token) -> Result<Option<Frame>, RedisErr> {
        let conn = self.sockets.get(&token).unwrap().clone();
        let req_data = conn.borrow_mut().read_frame()?;

        let command = self.decode_frame(req_data, conn.clone())?;

        let resp = self.apply_command(&token, command);

        trace!("Apply command from token {:?}: {:?}", token, resp);

        Ok(resp)
    }

    fn decode_frame(
        &mut self,
        frame: Frame,
        conn: Rc<RefCell<AsyncConnection>>,
    ) -> Result<Command, RedisErr> {
        match self.command_parser.parse(frame, conn) {
            Ok(command) => Ok(command),
            Err(e) => {
                debug!("Failed to parse command: {}", e);
                Err(e)
            }
        }
    }

    fn apply_command(&mut self, token: &Token, command: Command) -> Option<Frame> {
        if let Command::Quit(_) = command {
            self.drop_conncetion(token);
            return None;
        }
        Some(command.apply(&mut self.db))
    }

    fn send_response(&mut self, token: Token, data: Frame) {
        let write_res = self
            .sockets
            .get(&token)
            .unwrap()
            .clone()
            .borrow_mut()
            .write_frame(data);
        match write_res {
            Ok(_) => {}
            Err(e) => self.handle_error(token, e),
        }
    }

    fn accept_new_connection(&mut self) {
        match self.listener.accept() {
            Ok((mut stream, addr)) => {
                trace!("Accept new connection, addr: {}", addr);
                let token = self.token_count;
                self.poll
                    .registry()
                    .register(&mut stream, token, Interest::READABLE)
                    .unwrap();
                let stream = Box::new(stream);
                self.sockets.insert(
                    token,
                    Rc::new(RefCell::new(AsyncConnection::new(token.0, stream))),
                );
                self.token_count.0 += 1;
            }
            Err(e) => {
                warn!("Failed to accept new connection: {}", e);
            }
        }
    }

    #[allow(dead_code)]
    fn shutdown(&mut self) {
        self.shutdown = true;
    }

    fn invert_interest(&mut self, token: Token, interest: Interest) {
        let mut conn = self.sockets.get(&token).unwrap().borrow_mut();

        self.poll
            .registry()
            .reregister(conn.source(), token, interest)
            .unwrap();
    }

    fn set_resp(&mut self, token: Token, resp: Frame) {
        self.response.insert(token, resp);
    }

    fn drop_conncetion(&mut self, token: &Token) {
        let connection = self.sockets.remove(&token);
        if connection.is_none() {
            return;
        }
        let mut connection = match Rc::try_unwrap(connection.unwrap()) {
            Ok(conn) => conn,
            Err(_) => {
                return;
            }
        };
        self.poll
            .registry()
            .deregister(connection.get_mut().source())
            .unwrap();
    }

    fn poll(&mut self, timeout: Option<Duration>) -> Result<(), RedisErr> {
        self.poll
            .poll(&mut self.events, timeout)
            .map_err(|_| RedisErr::PollError)
    }

    fn idle(&mut self) {
        let start_time = SystemTime::now();

        // check expired key
        for (key, expire_at) in self.db.expire_items() {
            if start_time.elapsed().unwrap() > Duration::from_millis(100) {
                return;
            }
            if expire_at.cmp(&start_time) == std::cmp::Ordering::Less {
                self.db.remove(&key);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
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
