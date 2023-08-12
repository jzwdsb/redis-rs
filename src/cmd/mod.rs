//! Redis command Definition
//! All Redis Commands are definied in this document: https://redis.io/commands
//! We can start with a simple command: GET, SET, DEL, EXPIRE
//!
//! This file defines the command types and the command parsing logic.
//! and how the command manipulates the database.

mod kv;
pub use kv::*;
mod list;
pub use list::*;
mod hash;
pub use hash::*;
mod sort_set;
pub use sort_set::*;
mod meta;
pub use meta::*;
mod db;
pub use db::*;

use log::trace;

use crate::frame::Frame;
use std::fmt::Display;


#[derive(Debug)]
#[allow(dead_code)]
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

// marco generated code is not working, match the wrong arm
// FIXME: use macro to generate the command enum
#[allow(unused_macros)]
macro_rules! def_command_enum {
    ($($cmd:ident),*) => {
        #[derive(Debug)]
        pub enum Command {
            $($cmd($cmd),)*
        }

        impl Command {
            pub fn from_frame(frame:Frame) -> Result<Self, CommandErr> {
                trace!("from_frame: {:?}", frame);
                if let Frame::Array(cmd) = frame {
                    Self::from_frames(cmd)
                } else {
                    Err(CommandErr::InvalidProtocol)
                }
            }
            fn from_frames(frame: Vec<Frame>) -> Result<Self,CommandErr> {
                if frame.is_empty() {
                    return Err(CommandErr::InvalidProtocol);
                }

                let command = frame_to_string(&frame[0])?;
                trace!("from_frames: {:?}", command);
                match command.to_uppercase().as_str() {
                    $(stringify!($cmd) => {
                        trace!("cmd: {:?} got str: {:?}", stringify!($cmd), command);
                        Ok(Command::$cmd($cmd::from_frames(frame)?))},)*
                    #[allow(unreachable_patterns)]
                    _ => Err(CommandErr::UnknownCommand),
                }
            }

            pub fn apply(self, db: &mut crate::db::Database) -> Frame {
                trace!("apply command: {:?}", self);
                match self {
                    $(Command::$cmd(cmd) => cmd.apply(db),)*
                }
            }
        }
    }
}


// def_command_enum!(
//     Get, MGet, Set, MSet, LPush, LRange, HSet, HGet, ZAdd, ZCard, ZRem, Del, Expire, Type, Ping,
//     Flush
// );

#[derive(Debug)]
pub enum Command {
    // KV
    Get(Get),
    MGet(MGet),
    Set(Set),
    MSet(MSet),

    // LIST
    LPush(LPush),
    LRange(LRange),

    // Hash
    HSet(HSet),
    HGet(HGet),

    // SORT SET
    ZAdd(ZAdd),
    ZCard(ZCard),
    ZRem(ZRem),

    // META
    Del(Del),
    Expire(Expire),
    Type(Type),

    // DB
    Ping(Ping),
    Flush(Flush),
}

impl Command {
    pub fn from_frame(frame: Frame) -> Result<Self, CommandErr> {
        if let Frame::Array(cmd) = frame {
            Self::from_frames(cmd)
        } else {
            Err(CommandErr::InvalidProtocol)
        }
    }

    fn from_frames(frame: Vec<Frame>) -> Result<Self, CommandErr> {
        if frame.is_empty() {
            return Err(CommandErr::InvalidProtocol);
        }
        let cmd = frame_to_string(&frame[0])?;
        match cmd.to_uppercase().as_str() {
            // KV
            "GET" => Ok(Command::Get(Get::from_frames(frame)?)),
            "MGET" => Ok(Command::MGet(MGet::from_frames(frame)?)),
            "SET" => Ok(Command::Set(Set::from_frames(frame)?)),
            "MSET" => Ok(Command::MSet(MSet::from_frames(frame)?)),

            // LIST
            "LPUSH" => Ok(Command::LPush(LPush::from_frames(frame)?)),
            "LRANGE" => Ok(Command::LRange(LRange::from_frames(frame)?)),

            // HASH
            "HSET" => Ok(Command::HSet(HSet::from_frames(frame)?)),
            "HGET" => Ok(Command::HGet(HGet::from_frames(frame)?)),

            // Sort Set
            "ZADD" => Ok(Command::ZAdd(ZAdd::from_frames(frame)?)),
            "ZCARD" => Ok(Command::ZCard(ZCard::from_frames(frame)?)),
            "ZREM" => Ok(Command::ZRem(ZRem::from_frames(frame)?)),

            // META
            "DEL" => Ok(Command::Del(Del::from_frames(frame)?)),
            "EXPIRE" => Ok(Command::Expire(Expire::from_frames(frame)?)),
            "TYPE" => Ok(Command::Type(Type::from_frames(frame)?)),

            // Other
            "PING" => Ok(Command::Ping(Ping::from_frames(frame)?)),
            "FLUSH" => Ok(Command::Flush(Flush::from_frames(frame)?)),
            _ => Err(CommandErr::UnknownCommand),
        }
    }

    pub fn apply(self, db: &mut crate::db::Database) -> Frame {
        trace!("apply command: {:?}", self);
        match self {
            Command::Get(get) => get.apply(db),
            Command::MGet(mget) => mget.apply(db),
            Command::Set(set) => set.apply(db),
            Command::MSet(mset) => mset.apply(db),
            Command::Del(del) => del.apply(db),
            Command::Expire(expire) => expire.apply(db),
            Command::LPush(lpush) => lpush.apply(db),
            Command::LRange(lrange) => lrange.apply(db),
            Command::HSet(hset) => hset.apply(db),
            Command::HGet(hget) => hget.apply(db),
            Command::ZAdd(zadd) => zadd.apply(db),
            Command::ZCard(zcard) => zcard.apply(db),
            Command::ZRem(zrem) => zrem.apply(db),
            Command::Type(get_type) => get_type.apply(db),
            Command::Ping(ping) => ping.apply(db),
            Command::Flush(flush) => flush.apply(db),
        }
    }
}

fn frame_to_string(frame: &Frame) -> Result<String, CommandErr> {
    match frame {
        Frame::SimpleString(s) => Ok(s.clone()),
        Frame::BulkString(bytes) => Ok(String::from_utf8(bytes.clone())?),
        _ => Err(CommandErr::InvalidProtocol),
    }
}

#[inline]
fn next_string(frame: &mut std::vec::IntoIter<Frame>) -> Result<String, CommandErr> {
    match frame.next() {
        Some(Frame::SimpleString(s)) => Ok(s),
        Some(Frame::BulkString(bytes)) => Ok(String::from_utf8(bytes)?),
        None => Err(CommandErr::WrongNumberOfArguments),
        _ => Err(CommandErr::InvalidProtocol),
    }
}

#[inline]
fn next_bytes(frame: &mut std::vec::IntoIter<Frame>) -> Result<Vec<u8>, CommandErr> {
    match frame.next() {
        Some(Frame::SimpleString(s)) => Ok(s.into_bytes()),
        Some(Frame::BulkString(bytes)) => Ok(bytes),
        None => Err(CommandErr::WrongNumberOfArguments),
        _ => Err(CommandErr::InvalidProtocol),
    }
}

#[inline]
fn next_integer(frame: &mut std::vec::IntoIter<Frame>) -> Result<i64, CommandErr> {
    match frame.next() {
        Some(Frame::Integer(i)) => Ok(i),
        Some(Frame::SimpleString(s)) => Ok(s.parse::<i64>().unwrap()),
        None => Err(CommandErr::WrongNumberOfArguments),
        _ => Err(CommandErr::InvalidProtocol),
    }
}

#[inline]
fn next_float(frame: &mut std::vec::IntoIter<Frame>) -> Result<f64, CommandErr> {
    match frame.next() {
        Some(Frame::SimpleString(s)) => Ok(s.parse::<f64>().unwrap()),
        Some(Frame::BulkString(bytes)) => Ok(String::from_utf8(bytes)?.parse::<f64>().unwrap()),
        None => Err(CommandErr::WrongNumberOfArguments),
        _ => Err(CommandErr::InvalidProtocol),
    }
}

#[inline]
fn check_cmd(frame: &mut std::vec::IntoIter<Frame>, cmd: &[u8]) -> Result<(), CommandErr> {
    trace!("check_cmd: {:?}", String::from_utf8(cmd.to_vec()));
    match frame.next() {
        Some(Frame::SimpleString(ref s)) if s.to_ascii_uppercase().as_bytes() == cmd => Ok(()),
        Some(Frame::BulkString(ref s)) if s.to_ascii_uppercase() == cmd => Ok(()),
        None => Err(CommandErr::WrongNumberOfArguments),
        _ => Err(CommandErr::InvalidProtocol),
    }
}