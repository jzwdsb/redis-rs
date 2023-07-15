// Redis Commands

use crate::{err::Err, protocol::Protocol};

type Bytes = Vec<u8>;

#[derive(Debug, PartialEq)]
pub enum Command {
    Get(Bytes),
    Set(Bytes, Bytes),
    Del(Bytes),
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
                Ok(Command::Set(key, value))
            }
            "del" => {
                if parts.len() != 1 {
                    return Err(Err::SyntaxError);
                }
                let key = parts.remove(0);
                Ok(Command::Del(key))
            }
            _ => Err(Err::SyntaxError),
        }
    }
}
