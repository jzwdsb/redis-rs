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
mod bf;
pub use bf::*;
mod meta;
pub use meta::*;
mod pub_sub;
use pub_sub::*;

mod connections;
pub use connections::*;

mod db;
pub use db::*;

use crate::connection::AsyncConnection;
use crate::db::DB;
use crate::frame::Frame;
use crate::RedisErr;
use crate::Result;

use marco::to_upper_case_str;
use trie::Trie;

use std::sync::Arc;

use bytes::Bytes;
use log::trace;
use tokio::sync::Notify;

type CommandParseFn = Box<dyn Fn(Vec<Frame>) -> Result<Command> + Send + Sync>;

pub struct Parser {
    trie: Trie<CommandParseFn>,
}

impl core::fmt::Debug for Parser {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "Parser")
    }
}

pub trait CommandApplyer {
    fn apply(self: Box<Self>, db: DB) -> Frame;
}

macro_rules! add_tire {
    ($tire:ident, $($cmd:ident),*) => {
        $(
            $tire.insert(to_upper_case_str!($cmd), Box::new(|frames: Vec<Frame>| -> Result<Command> {
                Ok(Command::$cmd($cmd::from_frames(frames)?))
            }));
        )*
        $tire.insert("SUBSCRIBE", Box::new(|frames: Vec<Frame>| -> Result<Command> {
            Ok(Command::Subscribe(Subscribe::from_frames(frames)?))
        }));
    };
}

macro_rules! def_command_impl_parse {
    ($($cmd:ident),*) => {
        #[derive(Debug)]
        pub enum Command {
                $($cmd($cmd),)*

                Subscribe(Subscribe),
                // Unsubscribe(Unsubscribe),
            }

        impl Command {
            pub async fn apply(self, db: &mut DB, dst: &mut AsyncConnection, shutdown: Arc<Notify>) -> Frame {
                trace!("apply command: {:?}", self);
                match self {
                    $(Command::$cmd(cmd) => cmd.apply(db),)*
                    Command::Subscribe(cmd) =>  cmd.apply(db, dst, shutdown).await,//cmd.apply(db.db()),
                }
            }
        }
        impl Parser {
            pub fn new() -> Self {
                let mut trie: Trie<CommandParseFn> = Trie::new();
                add_tire!(trie, $($cmd),*);
                Self { trie }
            }

            pub fn parse(&self, frame: Frame) -> Result<Command> {
                trace!("parse: {:?}", frame);
                if let Frame::Array(frames) = frame {
                    self.trie
                        .get(&frame_to_string(&frames[0])?.to_uppercase())
                        .ok_or(RedisErr::UnknownCommand)?(frames)
                } else {
                    Err(RedisErr::InvalidProtocol)
                }
            }
        }
    }
}

def_command_impl_parse! {
    Get, MGet, Set, MSet,
    LPush, LRange,
    HSet, HGet,
    ZAdd, ZCard, ZRem,
    BFAdd, BFExists,
    Publish, Unsubscribe,
    Del, Expire, Type, Object,
    Quit,
    Ping, Flush
}

#[inline]
fn frame_to_string(frame: &Frame) -> Result<String> {
    match frame {
        Frame::SimpleString(s) => Ok(s.clone()),
        Frame::BulkString(bytes) => Ok(String::from_utf8(bytes.to_vec())?),
        _ => Err(RedisErr::InvalidProtocol),
    }
}

#[inline]
fn next_string(frame: &mut std::vec::IntoIter<Frame>) -> Result<String> {
    match frame.next() {
        Some(Frame::SimpleString(s)) => Ok(s),
        Some(Frame::BulkString(bytes)) => Ok(String::from_utf8(bytes.to_vec())?),
        None => Err(RedisErr::WrongNumberOfArguments),
        _ => Err(RedisErr::InvalidProtocol),
    }
}

#[inline]
fn next_bytes(frame: &mut std::vec::IntoIter<Frame>) -> Result<Bytes> {
    match frame.next() {
        Some(Frame::SimpleString(s)) => Ok(Bytes::from(s)),
        Some(Frame::BulkString(bytes)) => Ok(bytes),
        None => Err(RedisErr::WrongNumberOfArguments),
        _ => Err(RedisErr::InvalidProtocol),
    }
}

#[inline]
fn next_integer(frame: &mut std::vec::IntoIter<Frame>) -> Result<i64> {
    match frame.next() {
        Some(Frame::Integer(i)) => Ok(i),
        Some(Frame::SimpleString(s)) => Ok(s.parse::<i64>().unwrap()),
        None => Err(RedisErr::WrongNumberOfArguments),
        _ => Err(RedisErr::InvalidProtocol),
    }
}

#[inline]
fn next_float(frame: &mut std::vec::IntoIter<Frame>) -> Result<f64> {
    match frame.next() {
        Some(Frame::SimpleString(s)) => Ok(s.parse::<f64>().unwrap()),
        Some(Frame::BulkString(bytes)) => {
            Ok(String::from_utf8(bytes.to_vec())?.parse::<f64>().unwrap())
        }
        None => Err(RedisErr::WrongNumberOfArguments),
        _ => Err(RedisErr::InvalidProtocol),
    }
}

#[inline]
fn check_cmd(frame: &mut std::vec::IntoIter<Frame>, cmd: &[u8]) -> Result<()> {
    match frame.next() {
        Some(Frame::SimpleString(ref s)) if s.to_ascii_uppercase().as_bytes() == cmd => Ok(()),
        Some(Frame::BulkString(ref s)) if s.to_ascii_uppercase() == cmd => Ok(()),
        None => Err(RedisErr::WrongNumberOfArguments),
        _ => Err(RedisErr::InvalidProtocol),
    }
}
