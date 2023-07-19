// Redis Commands
use crate::{err::Err, protocol::Protocol, data::Value};
use std::time::SystemTime;

type Bytes = Vec<u8>;

pub struct Request {
    pub cmd: Command,
    pub expire_at: Option<SystemTime>,
}

pub struct Response {
    pub data: Value,
    pub err: Option<Err>,
}

pub trait RequestParser {
    fn from_protocol(&self, protocol: Protocol) -> Result<Request, Err>;
}

pub trait ResponseSerializer {
    fn to_protocol(&self, resp: Response) -> Protocol;
}

#[derive(Debug, PartialEq)]
pub enum Command {
    Get(Bytes),
    Set(Bytes, Bytes, Option<SystemTime>),
    Del(Bytes),
    Expire(Bytes, SystemTime),
}

impl Command {
    pub fn from_protocol(protocol: Protocol) -> Result<Self, Err> {
        if let Protocol::BulkStrings(cmd) = protocol {
            Self::from_bytes(cmd)
        } else {
            Err(Err::SyntaxError)
        }
    }

    pub fn from_bytes(cmd: Vec<u8>) -> Result<Self, Err> {
        let mut parts: Vec<Vec<u8>> = cmd
            .split(|&b| b == b' ')
            .map(|chunk| chunk.to_vec())
            .collect();

        if parts.len() == 0 {
            return Err(Err::SyntaxError);
        }

        let cmd = String::from_utf8(parts.remove(0)).unwrap();
        match cmd.to_lowercase().as_str() {
            "get" => {
                if parts.len() != 1 {
                    return Err(Err::SyntaxError);
                }
                Ok(Command::Get(parts.remove(0)))
            }
            "set" => {
                if parts.len() != 2 {
                    return Err(Err::SyntaxError);
                }
                let key = parts.remove(0);
                let value = parts.remove(0);
                if parts.len() > 0 {
                    let expire_at = parts.remove(0);
                    let expire_at = String::from_utf8(expire_at).unwrap();
                    let expire_at = expire_at.parse::<u64>().unwrap();
                    let expire_at = SystemTime::UNIX_EPOCH
                        .checked_add(std::time::Duration::from_secs(expire_at))
                        .unwrap();
                    return Ok(Command::Set(key, value, Some(expire_at)));
                }
                Ok(Command::Set(key, value, None))
            }
            "del" => {
                if parts.len() != 1 {
                    return Err(Err::SyntaxError);
                }
                let key = parts.remove(0);
                Ok(Command::Del(key))
            }
            "expire" => {
                if parts.len() != 2 {
                    return Err(Err::SyntaxError);
                }
                let key = parts.remove(0);
                let seconds = parts.remove(0);
                let seconds = String::from_utf8(seconds)?;
                let seconds = seconds.parse::<u64>().unwrap();
                let expire_at = SystemTime::UNIX_EPOCH
                    .checked_add(std::time::Duration::from_secs(seconds))
                    .unwrap();
                Ok(Command::Expire(key, expire_at))
            }
            _ => Err(Err::SyntaxError),
        }
    }
}
