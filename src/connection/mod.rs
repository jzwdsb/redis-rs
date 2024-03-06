use crate::frame::Frame;
use crate::{RedisErr, Result};

use std::fmt::Debug;
use std::io::{ErrorKind, Read, Write};

use bytes::BytesMut;
use log::trace;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;

pub trait SyncConnectionLike: Read + Write + Debug {}
impl SyncConnectionLike for std::net::TcpStream {}

#[derive(Debug)]
pub struct AsyncConnection {
    stream: BufWriter<TcpStream>,
    read_buffer: BytesMut,
}

impl AsyncConnection {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream: BufWriter::new(stream),
            read_buffer: BytesMut::with_capacity(4096),
        }
    }

    fn parse_frame(&mut self) -> Result<Option<Frame>> {
        match Frame::from_bytes(&self.read_buffer) {
            Ok(frame) => Ok(Some(frame)),
            Err(RedisErr::FrameIncomplete) => Ok(None),
            Err(e) => Err(e),
        }
    }

    pub async fn read_frame(&mut self) -> Result<Frame> {
        loop {
            if let Some(frame) = self.parse_frame()? {
                self.read_buffer.clear();
                return Ok(frame);
            }

            if self.stream.read_buf(&mut self.read_buffer).await? == 0 {
                return Err(RedisErr::ConnectionAborted);
            }
        }
    }
    pub async fn write_frame(&mut self, frame: Frame) -> Result<()> {
        let data = frame.serialize();
        trace!(
            "writing frame {}",
            String::from_utf8_lossy(data.to_vec().as_slice())
        );
        self.stream.write_all(&data).await?;

        // flush the stream so the client can see the response immediately
        self.stream.flush().await?;
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
    pub fn read_frame(&mut self) -> Result<Frame> {
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

    pub fn write_frame(&mut self, frame: Frame) -> Result<()> {
        match write_response(&mut self.stream, &frame.serialize()) {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }
}

fn write_response(stream: &mut impl Write, response: &[u8]) -> Result<usize> {
    let mut response = response;
    let mut cnt = 0;
    loop {
        match stream.write(response) {
            Ok(len) => {
                response = &response[len..];
                cnt += len;
                if response.is_empty() {
                    break;
                }
            }
            Err(e) if e.kind() == ErrorKind::WouldBlock => {
                break;
            }
            Err(e) => {
                return Err(e.into());
            }
        }
    }
    Ok(cnt)
}
