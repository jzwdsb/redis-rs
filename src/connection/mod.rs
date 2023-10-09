use bytes::{Buf, BytesMut};
use std::fmt::Debug;
use std::io::{Error, ErrorKind, Read, Write};

use mio::event::Source;

use crate::frame::Frame;
use crate::RedisErr;

pub trait AsyncConnectionLike: Source + Read + Write + Debug {}
impl AsyncConnectionLike for mio::net::TcpStream {}

pub trait SyncConnectionLike: Read + Write + Debug {}
impl SyncConnectionLike for std::net::TcpStream {}

pub trait FrameReader {
    fn read_frame(&mut self) -> Result<Frame, RedisErr>;
}

pub trait FrameWriter {
    fn write_frame(&mut self, frame: Frame) -> Result<(), RedisErr>;
}

#[derive(Debug)]
pub struct AsyncConnection {
    id: usize,
    stream: Box<dyn AsyncConnectionLike>,
    read_buffer: BytesMut,
    write_buffer: BytesMut,
}

impl AsyncConnection {
    pub fn new(id: usize, stream: Box<dyn AsyncConnectionLike>) -> Self {
        Self {
            id,
            stream: stream,
            read_buffer: BytesMut::with_capacity(4096),
            write_buffer: BytesMut::with_capacity(4096),
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

    pub fn source(&mut self) -> &mut Box<dyn AsyncConnectionLike> {
        &mut self.stream
    }

    
}

impl FrameWriter for AsyncConnection {
    fn write_frame(&mut self, frame: Frame) -> Result<(), RedisErr> {
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


impl FrameReader for AsyncConnection {
    fn read_frame(&mut self) -> Result<Frame, RedisErr> {
        match read_request(&mut self.stream) {
            Ok(data) => {
                self.read_buffer.extend_from_slice(&data);
                match self.parse_frame() {
                    Ok(frame) => {
                        self.read_buffer.clear();
                        return Ok(frame);
                    }
                    Err(e) => return Err(e),
                }
            }
            Err(e) => {
                return Err(e.into());
            }
        }
    }
}

pub struct SyncConnection {
    id: usize,
    stream: Box<dyn SyncConnectionLike>,
}

impl SyncConnection {
    pub fn new(id: usize, stream: Box<dyn SyncConnectionLike>) -> Self {
        Self { id, stream }
    }

    #[allow(dead_code)]
    pub fn id(&self) -> usize {
        self.id
    }
}

impl FrameReader for SyncConnection {
    fn read_frame(&mut self) -> Result<Frame, RedisErr> {
        let mut buffer = vec![];
        loop {
            match read_request(&mut self.stream) {
                Ok(data) => {
                    buffer.extend_from_slice(&data);
                    match Frame::from_bytes(&buffer) {
                        Ok(frame) => {
                            return Ok(frame);
                        }
                        Err(e) => match e {
                            RedisErr::FrameIncomplete => {
                                continue;
                            }
                            _ => {
                                return Err(e);
                            }
                        },
                    }
                }
                Err(e) => {
                    return Err(e.into());
                }
            }
        }
    }
}

impl FrameWriter for SyncConnection {
    fn write_frame(&mut self, frame: Frame) -> Result<(), RedisErr> {
        match write_response(&mut self.stream, &frame.serialize()) {
            Ok(_) => {
                return Ok(());
            }
            Err(e) => {
                return Err(e.into());
            }
        }
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
