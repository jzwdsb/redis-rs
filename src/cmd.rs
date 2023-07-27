//! Redis command Definition
//! All Redis Commands are definied in this document: https://redis.io/commands
//! We can start with a simple command: GET, SET, DEL, EXPIRE
//!
//! This file defines the command types and the command parsing logic.
//! and how the command manipulates the database.

use crate::{db::Database, frame::Frame};
use std::{fmt::Display, time::Duration};

#[derive(Debug, PartialEq)]

pub(crate) struct Get {
    key: String,
}

impl Get {
    pub fn new(key: String) -> Self {
        Self { key }
    }

    #[allow(dead_code)]
    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn apply(self, db: &mut Database) -> Frame {
        match db.get(&self.key.into_bytes()) {
            Ok(value) => Frame::BulkStrings(value),
            Err(e) => match e {
                crate::db::ExecuteError::KeyNotFound => Frame::Nil,
                crate::db::ExecuteError::WrongType => Frame::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                ),
                _ => unreachable!("unexpect get error: {:?}", e),
            },
        }
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct Set {
    key: Vec<u8>,
    value: Vec<u8>,
    expire: Option<Duration>,
}

impl Set {
    pub fn new(key: Vec<u8>, value: Vec<u8>, expire: Option<Duration>) -> Self {
        Self { key, value, expire }
    }

    #[allow(dead_code)]
    pub fn key(&self) -> &Vec<u8> {
        &self.key
    }

    #[allow(dead_code)]
    pub fn value(&self) -> &Vec<u8> {
        &self.value
    }

    #[allow(dead_code)]
    pub fn expire(&self) -> &Option<Duration> {
        &self.expire
    }

    pub fn apply(self, db: &mut Database) -> Frame {
        let expire_at = match self.expire {
            Some(duration) => Some(std::time::Instant::now() + duration),
            None => None,
        };
        match db.set(self.key, self.value, expire_at) {
            Ok(_) => Frame::SimpleString("OK".to_string()),
            Err(e) => match e {
                crate::db::ExecuteError::WrongType => Frame::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                ),
                crate::db::ExecuteError::OutOfMemory => Frame::Error("Out of memory".to_string()),
                _ => unreachable!("unexpect set error: {:?}", e),
            },
        }
    }
}

#[derive(Debug, PartialEq)]

pub(crate) struct Del {
    key: Vec<u8>,
}

impl Del {
    pub fn new(key: Vec<u8>) -> Self {
        Self { key }
    }

    pub fn key(&self) -> &Vec<u8> {
        &self.key
    }

    pub fn apply(self, db: &mut Database) -> Frame {
        todo!("Apply command to database")
    }
}

#[derive(Debug, PartialEq)]
pub enum CommandErr {
    InvalidProtocol,
    SyntaxError,
    WrongNumberOfArguments,
    InvalidArgument,
    UnknownCommand,
}

impl Display for CommandErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CommandErr::InvalidProtocol => write!(f, "Invalid Protocol"),
            CommandErr::SyntaxError => write!(f, "Syntax Error"),
            CommandErr::WrongNumberOfArguments => write!(f, "Wrong Number Of Arguments"),
            CommandErr::InvalidArgument => write!(f, "Invalid Argument"),
            CommandErr::UnknownCommand => write!(f, "Unknown Command"),
        }
    }
}

impl From<std::string::FromUtf8Error> for CommandErr {
    fn from(_: std::string::FromUtf8Error) -> Self {
        CommandErr::InvalidArgument
    }
}

#[derive(Debug, PartialEq)]
pub(crate) enum Command {
    Get(Get),
    Set(Set),
    Del(Del),
}
impl Command {
    pub fn from_frame(frame: Frame) -> Result<Self, CommandErr> {
        // FIXME: in RESP protocol, the client request is actually a RESP Array
        if let Frame::Array(cmd) = frame {
            Self::from_frames(cmd)
        } else {
            Err(CommandErr::InvalidProtocol)
        }
    }

    // pub fn from_bytes(cmd: Vec<Frame>) -> Result<Self, CommandErr> {

    //     let cmd = String::from_utf8(parts.remove(0)).unwrap();
    //     match cmd.to_lowercase().as_str() {
    //         "get" => {
    //             if parts.len() != 1 {
    //                 return Err(CommandErr::WrongNumberOfArguments);
    //             }
    //             let key = String::from_utf8(parts.remove(0))?;
    //             Ok(Command::Get(Get::new(key)))
    //         }
    //         "set" => {
    //             if parts.len() != 2 {
    //                 return Err(CommandErr::WrongNumberOfArguments);
    //             }
    //             let key = parts.remove(0);
    //             let value = parts.remove(0);
    //             if parts.len() > 0 {
    //                 let expire = parts.remove(0);
    //                 let expire = String::from_utf8(expire).unwrap();
    //                 let expire = expire.parse::<u64>().unwrap();
    //                 return Ok(Command::Set(Set::new(
    //                     key,
    //                     value,
    //                     Some(Duration::from_secs(expire)),
    //                 )));
    //             }
    //             Ok(Command::Set(Set::new(key, value, None)))
    //         }
    //         "del" => {
    //             if parts.len() != 1 {
    //                 return Err(CommandErr::WrongNumberOfArguments);
    //             }
    //             let key = parts.remove(0);
    //             Ok(Command::Del(Del::new(key)))
    //         }
    //         "expire" => {
    //             todo!("Expire command")
    //         }
    //         _ => Err(CommandErr::UnknownCommand),
    //     }
    // }

    pub fn from_frames(frame: Vec<Frame>) -> Result<Self, CommandErr> {
        todo!("Convert frames into command")
    }

    pub fn apply(db: &mut crate::db::Database, cmd: Self) -> Frame {
        todo!("Apply command to database")
    }
}
