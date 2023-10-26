//! Redis command Definition
//! All Redis Commands are definied in this document: https://redis.io/commands
//! We can start with a simple command: GET, SET, DEL, EXPIRE
//!
//! This file defines the command types and the command parsing logic.
//! and how the command manipulates the database.

mod kv;
use std::cell::RefCell;
use std::rc::Rc;

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
use crate::db::Database;
use crate::frame::Frame;
use crate::RedisErr;

use marco::to_upper_case_str;
use trie::Trie;

use log::trace;

type CommandParseFn =
    Box<dyn Fn(Vec<Frame>, Rc<RefCell<AsyncConnection>>) -> Result<Command, RedisErr>>;

pub struct Parser {
    trie: Trie<CommandParseFn>,
}

pub trait CommandApplyer {
    fn apply(self: Box<Self>, db: &mut Database) -> Frame;
}

macro_rules! add_tire {
    ($tire:ident, $($cmd:ident),*) => {
        $(
            $tire.insert(to_upper_case_str!($cmd), Box::new(|frames: Vec<Frame>, _: Rc<RefCell<AsyncConnection>>| -> Result<Command, RedisErr> {
                Ok(Command::$cmd($cmd::from_frames(frames)?))
            }));
        )*
        $tire.insert("SUBSCRIBE", Box::new(|frames: Vec<Frame>, conn: Rc<RefCell<AsyncConnection>>| -> Result<Command, RedisErr> {
            Ok(Command::Subscribe(Subscribe::from_frames(frames, conn)?))
        }));
        $tire.insert("UNSUBSCRIBE", Box::new(|frames: Vec<Frame>, conn: Rc<RefCell<AsyncConnection>>| -> Result<Command, RedisErr> {
            Ok(Command::Unsubscribe(Unsubscribe::from_frames(frames, conn.borrow().id())?))
        }));
    };
}

macro_rules! def_command_impl_parse {
    ($($cmd:ident),*) => {
        #[derive(Debug)]
        pub enum Command {
                $($cmd($cmd),)*

                // don't have a unified type for publish and subscribe
                Subscribe(Subscribe),
                Unsubscribe(Unsubscribe),
            }

        impl Command {
            pub fn apply(self, db: &mut crate::db::Database) -> Frame {
                trace!("apply command: {:?}", self);
                match self {
                    $(Command::$cmd(cmd) => cmd.apply(db),)*
                    Command::Subscribe(cmd) => cmd.apply(db),
                    Command::Unsubscribe(cmd) => cmd.apply(db),
                }
            }
        }
        impl Parser {
            pub fn new() -> Self {
                let mut trie: Trie<CommandParseFn> = Trie::new();
                add_tire!(trie, $($cmd),*);
                Self { trie }
            }

            pub fn parse(&self, frame: Frame, conn: Rc<RefCell<AsyncConnection>>) -> Result<Command, RedisErr> {
                trace!("parse: {:?}", frame);
                if let Frame::Array(frames) = frame {
                    self.trie
                        .get(&frame_to_string(&frames[0])?.to_uppercase())
                        .ok_or(RedisErr::UnknownCommand)?(frames,conn)
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
    Publish,
    Del, Expire, Type, Object,
    Quit,
    Ping, Flush
}

#[inline]
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
