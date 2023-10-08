use std::io::{self, Error, ErrorKind, Read, Write};
use std::fmt::Debug;
use bytes::{Buf, BytesMut};

use mio::event::Source;

use crate::frame::Frame;
use crate::RedisErr;


pub trait AsyncConnectionLike: Source + Read + Write + Debug {}
impl AsyncConnectionLike for mio::net::TcpStream {}

pub trait SyncConnectionLike: Read + Write + Debug {}

#[derive(Debug)]
pub struct Connection {
    id: usize,
    stream: Box<dyn AsyncConnectionLike>,
    read_buffer: BytesMut,
    write_buffer: BytesMut,
}

impl Connection {
    pub fn new(id: usize, stream: Box<dyn AsyncConnectionLike>) -> Self {
        Self {
            id,
            stream: stream,
            read_buffer: BytesMut::with_capacity(4096),
            write_buffer: BytesMut::with_capacity(4096),
        }
    }

    pub fn read_frame(&mut self) -> Result<Frame, RedisErr> {
        match read_request(&mut self.stream) {
            Ok(data) => {
                self.read_buffer.extend_from_slice(&data);
                match self.parse_frame() {
                    Ok(frame) => {
                        self.read_buffer.advance(frame.len());
                        return Ok(frame);
                    }
                    Err(e) => Err(e),
                }
            }
            Err(e) => {
                return Err(e.into());
            }
        }
    }

    fn parse_frame(&mut self) -> Result<Frame, RedisErr> {
        match Frame::from_bytes(&self.read_buffer) {
            Ok(frame) => {
                return Ok(frame);
            }
            Err(e) => return Err(e),
        }
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn write_frame(&mut self, frame: Frame) -> Result<(), RedisErr> {
        self.write_buffer
            .extend_from_slice(frame.serialize().as_slice());
        match write_response(&mut self.stream, &self.write_buffer) {
            Ok(len) => {
                self.write_buffer.advance(len);
                return Ok(());
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                return Ok(());
            }
            Err(e) => {
                return Err(e.into());
            }
        }
    }

    pub fn source(&mut self) -> &mut Box<dyn AsyncConnectionLike> {
        &mut self.stream
    }
}

fn read_request(stream: &mut impl Read) -> Result<Vec<u8>, Error> {
    let mut buffer = [0; 1024];
    let mut req_bytes = Vec::new();
    loop {
        match stream.read(&mut buffer) {
            Ok(0) => {
                return Err(Error::new(
                    ErrorKind::ConnectionAborted,
                    "connection aborted",
                ));
            }
            Ok(n) => {
                req_bytes.extend_from_slice(&buffer[0..n]);
            }
            Err(e) if e.kind() == ErrorKind::WouldBlock => {
                break;
            }
            Err(e) => {
                return Err(e);
            }
        }
    }
    Ok(req_bytes)
}

impl Write for Connection {
    fn write(&mut self, buf: &[u8]) -> Result<usize, Error> {
        match self.write_frame(Frame::BulkString(buf.to_vec())) {
            Ok(_) => Ok(buf.len()),
            Err(e) => Err(io::Error::new(io::ErrorKind::Other, e)),
        }
    }

    fn flush(&mut self) -> Result<(), Error> {
        self.stream.flush()
    }
}

fn write_response(stream: &mut impl Write, response: &[u8]) -> Result<usize, Error> {
    let mut response = response;
    let mut cnt = 0;
    loop {
        match stream.write(&response) {
            Ok(len) => {
                response = &response[len..];
                cnt += len;
                if response.len() == 0 {
                    break;
                }
            }
            Err(e) if e.kind() == ErrorKind::WouldBlock => {
                break;
            }
            Err(e) => {
                return Err(e);
            }
        }
    }
    Ok(cnt)
}
