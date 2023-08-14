use std::fmt::Display;

mod cmd;

mod data;

mod db;

mod frame;

mod helper;

pub mod server;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum RedisErr {
    FrameIncomplete,
    FrameMalformed,

    InvalidProtocol,
    SyntaxError,
    WrongNumberOfArguments,
    InvalidArgument,
    UnknownCommand,

    NoAction,
    WrongType,
    KeyNotFound,
    OutOfMemory,

    WrongAddressFormat,
    IOError,
    PollError,
}

impl std::error::Error for RedisErr {}

impl Display for RedisErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(format!("{:?}", self).as_str())
    }
}

impl From<std::io::Error> for RedisErr {
    fn from(_: std::io::Error) -> Self {
        RedisErr::IOError
    }
}

impl From<std::net::AddrParseError> for RedisErr {
    fn from(_: std::net::AddrParseError) -> Self {
        RedisErr::WrongAddressFormat
    }
}

impl From<std::string::FromUtf8Error> for RedisErr {
    fn from(_: std::string::FromUtf8Error) -> Self {
        RedisErr::InvalidArgument
    }
}

impl From<std::num::ParseIntError> for RedisErr {
    fn from(_: std::num::ParseIntError) -> Self {
        RedisErr::InvalidArgument
    }
}

impl Into<String> for RedisErr {
    fn into(self) -> String {
        std::fmt::format(format_args!("{:?}", self))
    }
}
