// Redis Commands
use crate::{frame::Frame, db::Database};
use std::time::Duration;

type Bytes = Vec<u8>;

#[derive(Debug, PartialEq)]

pub struct Get{
    key: String,
}

impl Get{
    pub fn new(key: String) -> Self{
        Self{
            key,
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }
}

#[derive(Debug, PartialEq)]

pub struct Del {
    key: Vec<u8>,
}

impl Del {
    pub fn new(key: Vec<u8>) -> Self {
        Self {
            key,
        }
    }

    pub fn apply(self, db: &mut Database) -> Frame {
        todo!("Apply command to database")
    }
}

#[derive(Debug, PartialEq)]
pub struct Set{
    key: Vec<u8>,
    value: Vec<u8>,
    expire: Option<Duration>,
}

impl Set {
    pub fn new(key: Vec<u8>, value: Vec<u8>, expire: Option<Duration>) -> Self {
        Self {
            key,
            value,
            expire,
        }
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

impl From<std::string::FromUtf8Error> for CommandErr {
    fn from(_: std::string::FromUtf8Error) -> Self {
        CommandErr::SyntaxError
    }
}



#[derive(Debug, PartialEq)]
pub enum Command {
    Get(Get),
    Set(Set),
    Del(Del),
}
impl Command {
    pub fn from_frame(frame: Frame) -> Result<Self, CommandErr> {
        if let Frame::BulkStrings(cmd) = frame {
            Self::from_bytes(cmd)
        } else {
            Err(CommandErr::InvalidProtocol)
        }
    }

    pub fn from_bytes(cmd: Vec<u8>) -> Result<Self, CommandErr> {
        let mut parts: Vec<Vec<u8>> = cmd
            .split(|&b| b == b' ')
            .map(|chunk| chunk.to_vec())
            .collect();

        if parts.len() == 0 {
            return Err(CommandErr::WrongNumberOfArguments);
        }

        let cmd = String::from_utf8(parts.remove(0)).unwrap();
        match cmd.to_lowercase().as_str() {
            "get" => {
                if parts.len() != 1 {
                    return Err(CommandErr::WrongNumberOfArguments);
                }
                let key = String::from_utf8(parts.remove(0))?;
                Ok(Command::Get(Get::new(key)))
            }
            "set" => {
                if parts.len() != 2 {
                    return Err(CommandErr::WrongNumberOfArguments);
                }
                let key = parts.remove(0);
                let value = parts.remove(0);
                if parts.len() > 0 {
                    let expire = parts.remove(0);
                    let expire = String::from_utf8(expire).unwrap();
                    let expire = expire.parse::<u64>().unwrap();
                    return Ok(Command::Set(Set::new(key, value, Some(Duration::from_secs(expire)))));
                }
                Ok(Command::Set(Set::new(key, value, None)))
            }
            "del" => {
                if parts.len() != 1 {
                    return Err(CommandErr::WrongNumberOfArguments);
                }
                let key = parts.remove(0);
                Ok(Command::Del(Del::new(key)))
            }
            "expire" => {
                todo!("Expire command")
               
            }
            _ => Err(CommandErr::UnknownCommand),
        }
    }

    pub fn apply(db : &mut crate::db::Database, cmd: Self) -> Frame {
       todo!("Apply command to database")
    }
}
