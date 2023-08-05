//! Redis command Definition
//! All Redis Commands are definied in this document: https://redis.io/commands
//! We can start with a simple command: GET, SET, DEL, EXPIRE
//!
//! This file defines the command types and the command parsing logic.
//! and how the command manipulates the database.

use log::trace;

use crate::{db::Database, frame::Frame};
use std::{fmt::Display, time::Duration};

#[derive(Debug, PartialEq)]

pub(crate) struct Get {
    key: String,
}

impl Get {
    #[allow(dead_code)]
    fn new(key: String) -> Self {
        Self { key }
    }

    fn from_frames(frames: Vec<Frame>) -> Result<Self, CommandErr> {
        let mut iter = frames.into_iter();
        check_cmd(&mut iter, b"GET")?;
        let key = next_string(&mut iter)?;
        Ok(Self { key })
    }

    #[allow(dead_code)]
    fn key(&self) -> &str {
        &self.key
    }

    fn apply(self, db: &mut Database) -> Frame {
        match db.get(&self.key) {
            Ok(value) => Frame::BulkString(value),
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
    key: String,
    value: Vec<u8>,
    expire: Option<Duration>,
}

impl Set {
    #[allow(dead_code)]
    fn new(key: String, value: Vec<u8>, expire: Option<Duration>) -> Self {
        Self { key, value, expire }
    }

    fn from_frames(frames: Vec<Frame>) -> Result<Self, CommandErr> {
        let mut iter = frames.into_iter();
        check_cmd(&mut iter, b"SET")?;

        let key = next_string(&mut iter)?; // key
        let value = next_bytes(&mut iter)?; // value
        let expire: Option<Duration> = match iter.next() {
            Some(Frame::Integer(i)) if i > 0 => Some(Duration::from_secs(i as u64)),
            _ => None,
        };

        Ok(Self { key, value, expire })
    }

    #[allow(dead_code)]
    fn key(&self) -> &str {
        &self.key
    }

    #[allow(dead_code)]
    fn value(&self) -> &Vec<u8> {
        &self.value
    }

    #[allow(dead_code)]
    fn expire(&self) -> &Option<Duration> {
        &self.expire
    }

    fn apply(self, db: &mut Database) -> Frame {
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
    key: String,
}

impl Del {
    #[allow(dead_code)]
    fn new(key: String) -> Self {
        Self { key }
    }

    fn from_frames(frames: Vec<Frame>) -> Result<Self, CommandErr> {
        let mut iter = frames.into_iter();
        check_cmd(&mut iter, b"DEL")?;
        let key = next_string(&mut iter)?; // key
        Ok(Self { key })
    }

    #[allow(dead_code)]
    fn key(&self) -> &str {
        &self.key
    }

    fn apply(self, db: &mut Database) -> Frame {
        match db.del(&self.key) {
            Some(_) => Frame::Integer(1),
            None => Frame::Integer(0),
        }
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct Expire {
    key: String,
    expire: Duration,
}

impl Expire {
    fn new(key: String, expire: Duration) -> Self {
        Self { key, expire }
    }

    fn from_frames(frames: Vec<Frame>) -> Result<Self, CommandErr> {
        let mut iter = frames.into_iter();
        check_cmd(&mut iter, b"EXPIRE")?;
        let key = next_string(&mut iter)?; // key
        let expire = Duration::from_secs(next_integer(&mut iter)? as u64); // expire
        Ok(Self::new(key, expire))
    }

    #[allow(dead_code)]
    fn key(&self) -> &str {
        &self.key
    }

    #[allow(dead_code)]
    fn expire(&self) -> &Duration {
        &self.expire
    }

    fn apply(self, db: &mut Database) -> Frame {
        let expire_at = std::time::Instant::now() + self.expire;
        match db.expire(&self.key, expire_at) {
            Ok(()) => Frame::Integer(1),
            Err(e) => match e {
                crate::db::ExecuteError::KeyNotFound => Frame::Integer(0),
                _ => unreachable!("unexpect expire error: {:?}", e),
            },
        }
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct LPush {
    key: String,
    values: Vec<Vec<u8>>,
}

impl LPush {
    fn new(key: String, values: Vec<Vec<u8>>) -> Self {
        Self { key, values }
    }

    fn from_frames(frames: Vec<Frame>) -> Result<Self, CommandErr> {
        let mut iter = frames.into_iter();
        check_cmd(&mut iter, b"LPUSH")?;

        let key = next_string(&mut iter)?; // key
        let mut value = Vec::new();
        while iter.len() > 0 {
            value.push(next_bytes(&mut iter)?);
        }
        Ok(Self::new(key, value))
    }

    #[allow(dead_code)]
    fn key(&self) -> &str {
        &self.key
    }

    #[allow(dead_code)]
    fn value(&self) -> &Vec<Vec<u8>> {
        &self.values
    }

    fn apply(self, db: &mut Database) -> Frame {
        match db.lpush(&self.key, self.values) {
            Ok(len) => Frame::Integer(len as i64),
            Err(e) => match e {
                crate::db::ExecuteError::WrongType => Frame::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                ),
                crate::db::ExecuteError::OutOfMemory => Frame::Error("Out of memory".to_string()),
                _ => unreachable!("unexpect lpush error: {:?}", e),
            },
        }
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct LRange {
    key: String,
    start: i64,
    stop: i64,
}

impl LRange {
    #[allow(dead_code)]
    fn new(key: String, start: i64, stop: i64) -> Self {
        Self { key, start, stop }
    }

    fn from_frames(frames: Vec<Frame>) -> Result<Self, CommandErr> {
        let mut iter = frames.into_iter();
        check_cmd(&mut iter, b"LRANGE")?;
        let key = next_string(&mut iter)?; // key
        let start = next_integer(&mut iter)?; // start
        let stop = next_integer(&mut iter)?; // stop
        Ok(Self { key, start, stop })
    }

    #[allow(dead_code)]
    fn key(&self) -> &str {
        &self.key
    }

    #[allow(dead_code)]
    fn start(&self) -> i64 {
        self.start
    }

    #[allow(dead_code)]
    fn stop(&self) -> i64 {
        self.stop
    }

    fn apply(self, db: &mut Database) -> Frame {
        match db.lrange(&self.key, self.start, self.stop) {
            Ok(values) => Frame::Array(
                values
                    .into_iter()
                    .map(|v| Frame::BulkString(v))
                    .collect::<Vec<_>>(),
            ),
            Err(e) => match e {
                crate::db::ExecuteError::KeyNotFound => Frame::Array(vec![]),
                crate::db::ExecuteError::WrongType => Frame::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                ),
                _ => unreachable!("unexpect lrange error: {:?}", e),
            },
        }
    }
}

#[derive(Debug)]
pub(crate) struct ZAdd {
    key: String,
    nx: bool,
    xx: bool,
    lt: bool,
    gt: bool,
    ch: bool,
    incr: bool,
    zset: Vec<(f64, Vec<u8>)>,
}

impl ZAdd {
    fn new(
        key: String,
        nx: bool,
        xx: bool,
        lt: bool,
        gt: bool,
        ch: bool,
        incr: bool,
        zset: Vec<(f64, Vec<u8>)>,
    ) -> Self {
        Self {
            key,
            zset,
            nx,
            xx,
            lt,
            gt,
            ch,
            incr,
        }
    }

    // ZADD key [NX|XX] [CH] [INCR] score member [score member ...]
    fn from_frames(frames: Vec<Frame>) -> Result<Self, CommandErr> {
        if frames.len() < 3 {
            return Err(CommandErr::WrongNumberOfArguments);
        }

        let mut iter = frames.into_iter();
        check_cmd(&mut iter, b"ZADD")?;
        let key = next_string(&mut iter)?;

        // [NX|XX] [CH] [INCR]
        let mut nx = false;
        let mut xx = false;
        let mut lt = false;
        let mut gt = false;
        let mut ch = false;
        let mut incr = false;
        let score = loop {
            match next_string(&mut iter)?.to_uppercase().as_str() {
                "NX" => {
                    nx = true;
                    continue;
                }
                "XX" => {
                    xx = true;
                    continue;
                }
                "LT" => {
                    lt = true;
                    continue;
                }
                "GT" => {
                    gt = true;
                    continue;
                }
                "CH" => {
                    ch = true;
                    continue;
                }
                "INCR" => {
                    incr = true;
                    continue;
                }
                s => break s.parse::<f64>().map_err(|_| CommandErr::SyntaxError)?,
            };
        };

        // conflict options
        if nx && xx {
            return Err(CommandErr::SyntaxError);
        }
        if lt && gt {
            return Err(CommandErr::SyntaxError);
        }

        let member = next_bytes(&mut iter)?; // member

        let mut zset = vec![(score, member)];
        while iter.len() > 0 {
            let score = next_float(&mut iter)?; // score
            let member = next_bytes(&mut iter)?; // member
            zset.push((score, member));
        }
        Ok(Self::new(key, nx, xx, lt, gt, ch, incr, zset))
    }

    fn apply(self, db: &mut Database) -> Frame {
        match db.zadd(
            &self.key, self.nx, self.xx, self.lt, self.gt, self.ch, self.incr, self.zset,
        ) {
            Ok(len) => Frame::Integer(len as i64),
            Err(e) => match e {
                crate::db::ExecuteError::WrongType => Frame::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                ),
                crate::db::ExecuteError::OutOfMemory => Frame::Error("Out of memory".to_string()),
                _ => unreachable!("unexpect zadd error: {:?}", e),
            },
        }
    }
}

#[derive(Debug)]
pub(crate) struct ZCard {
    key: String,
}

impl ZCard {
    fn new(key: String) -> Self {
        Self { key }
    }

    fn from_frames(frames: Vec<Frame>) -> Result<Self, CommandErr> {
        if frames.len() != 2 {
            return Err(CommandErr::WrongNumberOfArguments);
        }
        let mut iter = frames.into_iter();
        check_cmd(&mut iter, b"ZCARD")?;
        let key = next_string(&mut iter)?; // key
        Ok(Self::new(key))
    }

    fn apply(self, db: &Database) -> Frame {
        match db.zcard(&self.key) {
            Ok(len) => Frame::Integer(len as i64),
            Err(e) => match e {
                crate::db::ExecuteError::KeyNotFound => Frame::Integer(0),
                crate::db::ExecuteError::WrongType => Frame::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                ),
                _ => unreachable!("unexpect zcard error: {:?}", e),
            },
        }
    }
}

#[derive(Debug)]
pub(crate) struct ZRem {
    key: String,
    members: Vec<Vec<u8>>,
}

impl ZRem {
    fn new(key: String, members: Vec<Vec<u8>>) -> Self {
        Self { key, members }
    }

    fn from_frames(frames: Vec<Frame>) -> Result<Self, CommandErr> {
        if frames.len() < 3 {
            return Err(CommandErr::WrongNumberOfArguments);
        }
        let mut iter = frames.into_iter();
        check_cmd(&mut iter, b"ZREM")?;
        let key = next_string(&mut iter)?; // key
        let mut members = vec![];
        while iter.len() > 0 {
            let member = next_bytes(&mut iter)?; // member
            members.push(member);
        }
        Ok(Self::new(key, members))
    }

    fn apply(self, db: &mut Database) -> Frame {
        match db.zrem(&self.key, self.members) {
            Ok(len) => Frame::Integer(len as i64),
            Err(e) => match e {
                crate::db::ExecuteError::KeyNotFound => Frame::Integer(0),
                crate::db::ExecuteError::WrongType => Frame::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                ),
                _ => unreachable!("unexpect zrem error: {:?}", e),
            },
        }
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct Flush {}

impl Flush {
    fn new() -> Self {
        Self {}
    }

    fn from_frames(frames: Vec<Frame>) -> Result<Self, CommandErr> {
        if frames.len() != 1 {
            return Err(CommandErr::WrongNumberOfArguments);
        }
        check_cmd(&mut frames.into_iter(), b"FLUSH")?;
        Ok(Self::new())
    }

    fn apply(self, db: &mut Database) -> Frame {
        db.flush();
        Frame::SimpleString("OK".to_string())
    }
}

#[derive(Debug, PartialEq)]
#[allow(dead_code)]
pub(crate) enum CommandErr {
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

#[derive(Debug)]
pub(crate) enum Command {
    // KV
    Get(Get),
    Set(Set),

    // LIST
    LPush(LPush),
    LRange(LRange),
    // SET
    ZAdd(ZAdd),
    ZCard(ZCard),
    ZRem(ZRem),

    // META
    Del(Del),
    Expire(Expire),

    // DB
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

    pub fn from_frames(frame: Vec<Frame>) -> Result<Self, CommandErr> {
        if frame.is_empty() {
            return Err(CommandErr::InvalidProtocol);
        }
        let cmd = frame_to_string(&frame[0])?;
        match cmd.to_uppercase().as_str() {
            // KV
            "GET" => Ok(Command::Get(Get::from_frames(frame)?)),
            "SET" => Ok(Command::Set(Set::from_frames(frame)?)),

            // LIST
            "LPUSH" => Ok(Command::LPush(LPush::from_frames(frame)?)),
            "LRANGE" => Ok(Command::LRange(LRange::from_frames(frame)?)),

            // Sort Set
            "ZADD" => Ok(Command::ZAdd(ZAdd::from_frames(frame)?)),
            "ZCARD" => Ok(Command::ZCard(ZCard::from_frames(frame)?)),
            "ZREM" => Ok(Command::ZRem(ZRem::from_frames(frame)?)),

            // META
            "DEL" => Ok(Command::Del(Del::from_frames(frame)?)),
            "EXPIRE" => Ok(Command::Expire(Expire::from_frames(frame)?)),

            // Other
            "FLUSH" => Ok(Command::Flush(Flush::from_frames(frame)?)),
            _ => Err(CommandErr::UnknownCommand),
        }
    }

    pub fn apply(db: &mut crate::db::Database, cmd: Self) -> Frame {
        trace!("apply command: {:?}", cmd);
        match cmd {
            Command::Get(get) => get.apply(db),
            Command::Set(set) => set.apply(db),
            Command::Del(del) => del.apply(db),
            Command::Expire(expire) => expire.apply(db),
            Command::LPush(lpush) => lpush.apply(db),
            Command::LRange(lrange) => lrange.apply(db),
            Command::ZAdd(zadd) => zadd.apply(db),
            Command::ZCard(zcard) => zcard.apply(db),
            Command::ZRem(zrem) => zrem.apply(db),
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
    match frame.next() {
        Some(Frame::SimpleString(ref s)) if s.to_ascii_uppercase().as_bytes() == cmd => Ok(()),
        Some(Frame::BulkString(ref s)) if s.to_ascii_uppercase() == cmd => Ok(()),
        None => Err(CommandErr::WrongNumberOfArguments),
        _ => Err(CommandErr::InvalidProtocol),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get() {
        let mut db = Database::new();
        let cmd = Command::from_frames(vec![
            Frame::BulkString(b"get".to_vec()),
            Frame::BulkString(b"key".to_vec()),
        ])
        .unwrap();
        let result = Command::apply(&mut db, cmd);
        assert_eq!(result, Frame::Nil);
    }

    #[test]
    fn test_set() {
        let mut db = Database::new();
        let cmd = Command::from_frames(vec![
            Frame::BulkString(b"set".to_vec()),
            Frame::BulkString(b"key".to_vec()),
            Frame::BulkString(b"value".to_vec()),
        ])
        .unwrap();
        let result = Command::apply(&mut db, cmd);
        assert_eq!(result, Frame::SimpleString("OK".to_string()));
    }

    #[test]
    fn test_del() {
        let mut db = Database::new();
        let cmd = Command::from_frames(vec![
            Frame::BulkString(b"del".to_vec()),
            Frame::BulkString(b"key".to_vec()),
        ])
        .unwrap();
        let result = Command::apply(&mut db, cmd);
        assert_eq!(result, Frame::Integer(0));
    }

    #[test]
    fn test_expire() {
        let mut db = Database::new();
        let cmd = Command::from_frames(vec![
            Frame::BulkString(b"expire".to_vec()),
            Frame::BulkString(b"key".to_vec()),
            Frame::Integer(10),
        ])
        .unwrap();
        let result = Command::apply(&mut db, cmd);
        assert_eq!(result, Frame::Integer(0));
    }

    #[test]
    fn test_lpush() {
        let mut db = Database::new();
        let cmd = Command::from_frames(vec![
            Frame::BulkString(b"lpush".to_vec()),
            Frame::BulkString(b"key".to_vec()),
            Frame::BulkString(b"1".to_vec()),
            Frame::BulkString(b"2".to_vec()),
            Frame::BulkString(b"3".to_vec()),
        ])
        .unwrap();
        let result = Command::apply(&mut db, cmd);
        assert_eq!(result, Frame::Integer(3));
    }

    #[test]
    fn test_lrange() {
        let mut db = Database::new();
        let cmd = Command::from_frames(vec![
            Frame::BulkString(b"lrange".to_vec()),
            Frame::BulkString(b"key".to_vec()),
            Frame::Integer(0),
            Frame::Integer(-1),
        ])
        .unwrap();
        let result = Command::apply(&mut db, cmd);
        assert_eq!(result, Frame::Array(vec![]));
    }

    #[test]
    fn test_zadd() {
        let mut db = Database::new();
        let cmd = Command::from_frames(vec![
            Frame::BulkString(b"zadd".to_vec()),
            Frame::BulkString(b"key".to_vec()),
            Frame::BulkString(b"1".to_vec()),
            Frame::BulkString(b"one".to_vec()),
        ])
        .unwrap();
        let result = Command::apply(&mut db, cmd);
        assert_eq!(result, Frame::Integer(1));
    }

    #[test]
    fn test_zcard() {
        let mut db = Database::new();
        let cmd = Command::from_frames(vec![
            Frame::BulkString(b"zcard".to_vec()),
            Frame::BulkString(b"key".to_vec()),
        ])
        .unwrap();
        let result = Command::apply(&mut db, cmd);
        assert_eq!(result, Frame::Integer(0));
    }

    #[test]
    fn test_zrem() {
        let mut db = Database::new();
        let cmd = Command::from_frames(vec![
            Frame::BulkString(b"zrem".to_vec()),
            Frame::BulkString(b"key".to_vec()),
            Frame::BulkString(b"one".to_vec()),
        ])
        .unwrap();
        let result = Command::apply(&mut db, cmd);
        assert_eq!(result, Frame::Integer(0));
    }

    #[test]
    fn test_flush() {
        let mut db = Database::new();
        let cmd = Command::from_frames(vec![Frame::BulkString(b"flush".to_vec())]);
        assert_eq!(cmd.is_ok(), true);
        let cmd = cmd.unwrap();

        match cmd {
            Command::Flush(_) => {}
            _ => panic!("expect flush"),
        }

        let result = Command::apply(&mut db, cmd);
        assert_eq!(result, Frame::SimpleString("OK".to_string()));
    }
}
