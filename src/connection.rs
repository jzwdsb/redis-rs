use std::io::{Error, ErrorKind, Read, Write};

use bytes::{Buf, BytesMut};
use mio::net::TcpStream;

use mio::event::Source;

use crate::frame::Frame;
use crate::RedisErr;

pub struct Connection {
    stream: TcpStream,
    read_buffer: BytesMut,
    write_buffer: BytesMut,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Self {
        Self {
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
}

impl Source for Connection {
    fn register(
        &mut self,
        registry: &mio::Registry,
        token: mio::Token,
        interests: mio::Interest,
    ) -> std::io::Result<()> {
        return registry.register(&mut self.stream, token, interests);
    }

    fn reregister(
        &mut self,
        registry: &mio::Registry,
        token: mio::Token,
        interests: mio::Interest,
    ) -> std::io::Result<()> {
        return registry.reregister(&mut self.stream, token, interests);
    }

    fn deregister(&mut self, registry: &mio::Registry) -> std::io::Result<()> {
        return registry.deregister(&mut self.stream);
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
