//! Async client implementation
//! 
//! This module contains the implementation of an asynchronous client for Redis.
//! 
//! The client is implemented using the [tokio](https://docs.rs/tokio/1.9.0/tokio/) crate.
//! 



use std::error::Error;

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use crate::frame::Frame;

// use tokio as asynchronous runtime
pub struct AsyncConnection {
    stream: TcpStream,
}

impl AsyncConnection {
    fn new(stream: TcpStream) -> Self {
        Self { stream }
    }

    async fn read_frame(&mut self) -> Result<Frame, Box<dyn Error>> {
        let mut buffer = vec![];
        loop {
            let mut buf = vec![0; 4096];
            let n = self.stream.read(&mut buf).await?;
            buffer.extend_from_slice(&buf[..n]);
            match Frame::from_bytes(&buffer) {
                Ok(frame) => return Ok(frame),
                Err(e) => match e {
                    crate::RedisErr::FrameIncomplete => continue,
                    _ => return Err(Box::new(e)),
                },
            }
        }
    }

    async fn write_frame(&mut self, frame: Frame) -> Result<(), Box<dyn Error>> {
        let data = frame.serialize();
        self.stream.write_all(&data).await?;
        Ok(())
    }

    pub async fn get(&mut self, key: &str) -> Result<Vec<u8>, Box<dyn Error>> {
        let cmd = Frame::Array(
            [
                Frame::BulkString("GET".as_bytes().to_vec()),
                Frame::BulkString(key.as_bytes().to_vec()),
            ]
            .to_vec(),
        );
        self.write_frame(cmd).await?;
        let result = self.read_frame().await?;
        match result {
            Frame::BulkString(data) => Ok(data),
            Frame::Nil => Ok(vec![]),
            _ => Err("Invalid response".into()),
        }
    }

    pub async fn set(&mut self, key: &str, value: &str) -> Result<(), Box<dyn Error>> {
        let cmd = Frame::Array(
            [
                Frame::BulkString("SET".as_bytes().to_vec()),
                Frame::BulkString(key.as_bytes().to_vec()),
                Frame::BulkString(value.as_bytes().to_vec()),
            ]
            .to_vec(),
        );
        self.write_frame(cmd).await?;
        let result = self.read_frame().await?;
        match result {
            Frame::SimpleString(data) => {
                if data == "OK" {
                    Ok(())
                } else {
                    Err("Invalid response".into())
                }
            }
            _ => Err("Invalid response".into()),
        }
    }
}

pub struct AsyncClient {
    conn: AsyncConnection,
}

impl AsyncClient {
    fn new(conn: AsyncConnection) -> Self {
        Self { conn }
    }

    pub async fn open(addr: &str) -> Result<Self, Box<dyn Error>> {
        let connection = AsyncConnection::new(TcpStream::connect(addr).await?);

        Ok(Self::new(connection))
    }

    pub fn get_connection(&mut self) -> &mut AsyncConnection {
        &mut self.conn
    }
}
