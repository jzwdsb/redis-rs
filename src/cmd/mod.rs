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

mod connections;
pub use connections::*;

mod db;
pub use db::*;

use crate::frame::Frame;
use crate::RedisErr;
use marco::to_upper_case_str;

use log::trace;

macro_rules! def_command_enum {
        ($($cmd:ident),*) => {

            #[derive(Debug)]
            pub enum Command {
                $($cmd($cmd),)*
            }

            impl Command {
                pub fn from_frame(frame:Frame) -> Result<Self, RedisErr> {
                    trace!("from_frame: {:?}", frame);
                    if let Frame::Array(cmd) = frame {
                        Self::from_frames(cmd)
                    } else {
                        Err(RedisErr::InvalidProtocol)
                    }
                }
                fn from_frames(frame: Vec<Frame>) -> Result<Self,RedisErr> {
                    if frame.is_empty() {
                        return Err(RedisErr::InvalidProtocol);
                    }

                    let command = frame_to_string(&frame[0])?;
                    trace!("from_frames: {:?}", command);
                    match command.to_uppercase().as_str() {
                        $(to_upper_case_str!($cmd) => {
                            trace!("cmd: {:?} got str: {:?}", stringify!($cmd), command);
                            Ok(Command::$cmd($cmd::from_frames(frame)?))},)*
                        _ => Err(RedisErr::UnknownCommand),
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

def_command_enum! {
    Get, MGet, Set, MSet,
    LPush, LRange,
    HSet, HGet,
    ZAdd, ZCard, ZRem,
    Del, Expire, Type,
    Quit,
    Ping, Flush
}

fn frame_to_string(frame: &Frame) -> Result<String, RedisErr> {
    match frame {
        Frame::SimpleString(s) => Ok(s.clone()),
        Frame::BulkString(bytes) => Ok(String::from_utf8(bytes.clone())?),
        _ => Err(RedisErr::InvalidProtocol),
    }
}

#[inline]
fn next_string(frame: &mut std::vec::IntoIter<Frame>) -> Result<String, RedisErr> {
    match frame.next() {
        Some(Frame::SimpleString(s)) => Ok(s),
        Some(Frame::BulkString(bytes)) => Ok(String::from_utf8(bytes)?),
        None => Err(RedisErr::WrongNumberOfArguments),
        _ => Err(RedisErr::InvalidProtocol),
    }
}

#[inline]
fn next_bytes(frame: &mut std::vec::IntoIter<Frame>) -> Result<Vec<u8>, RedisErr> {
    match frame.next() {
        Some(Frame::SimpleString(s)) => Ok(s.into_bytes()),
        Some(Frame::BulkString(bytes)) => Ok(bytes),
        None => Err(RedisErr::WrongNumberOfArguments),
        _ => Err(RedisErr::InvalidProtocol),
    }
}

#[inline]
fn next_integer(frame: &mut std::vec::IntoIter<Frame>) -> Result<i64, RedisErr> {
    match frame.next() {
        Some(Frame::Integer(i)) => Ok(i),
        Some(Frame::SimpleString(s)) => Ok(s.parse::<i64>().unwrap()),
        None => Err(RedisErr::WrongNumberOfArguments),
        _ => Err(RedisErr::InvalidProtocol),
    }
}

#[inline]
fn next_float(frame: &mut std::vec::IntoIter<Frame>) -> Result<f64, RedisErr> {
    match frame.next() {
        Some(Frame::SimpleString(s)) => Ok(s.parse::<f64>().unwrap()),
        Some(Frame::BulkString(bytes)) => Ok(String::from_utf8(bytes)?.parse::<f64>().unwrap()),
        None => Err(RedisErr::WrongNumberOfArguments),
        _ => Err(RedisErr::InvalidProtocol),
    }
}

#[inline]
fn check_cmd(frame: &mut std::vec::IntoIter<Frame>, cmd: &[u8]) -> Result<(), RedisErr> {
    match frame.next() {
        Some(Frame::SimpleString(ref s)) if s.to_ascii_uppercase().as_bytes() == cmd => Ok(()),
        Some(Frame::BulkString(ref s)) if s.to_ascii_uppercase() == cmd => Ok(()),
        None => Err(RedisErr::WrongNumberOfArguments),
        _ => Err(RedisErr::InvalidProtocol),
    }
}
