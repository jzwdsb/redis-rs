use bytes::BytesMut;
use std::fmt::Debug;
use std::io::{Error, ErrorKind, Read, Write};

use crate::frame::Frame;
use crate::RedisErr;

use super::SyncConnectionLike;

use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;

#[derive(Debug)]
pub struct AsyncConnection {
    id: usize,
    stream: BufWriter<TcpStream>,
    read_buffer: BytesMut,
}

impl AsyncConnection {
    pub fn new(id: usize, stream: TcpStream) -> Self {
        Self {
            id,
            stream: BufWriter::new(stream),
            read_buffer: BytesMut::with_capacity(4096),
        }
    }

    pub fn id(&self) -> usize {
        self.id
    }

    fn parse_frame(&mut self) -> Result<Option<Frame>, RedisErr> {
        match Frame::from_bytes(&self.read_buffer) {
            Ok(frame) => {
                return Ok(Some(frame));
            }
            Err(RedisErr::FrameIncomplete) => return Ok(None),
            Err(e) => return Err(e),
        }
    }

    pub async fn read_frame(&mut self) -> Result<Option<Frame>, RedisErr> {
        loop {
            if let Some(frame) = self.parse_frame()? {
                self.read_buffer.clear();
                return Ok(Some(frame));
            }
            
            if self.stream.read_buf(&mut self.read_buffer).await? == 0 {
                if self.read_buffer.is_empty() {
                    return Ok(None);
                } else {
                    return Err(RedisErr::ConnectionAborted);
                }
            }
        }
    }
    pub async fn write_frame(&mut self, frame: Frame) -> Result<(), RedisErr> {
        let data = frame.serialize();
        self.stream.write_all(&mut data.as_slice()).await?;

        Ok(())
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

impl SyncConnection {
    fn read_frame(&mut self) -> Result<Frame, RedisErr> {
        let mut buffer = vec![];
        loop {
            let mut data = vec![0; 1024];
            let len = self.stream.read(&mut data)?;
            data.truncate(len);
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
    }

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
