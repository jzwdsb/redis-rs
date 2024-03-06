use std::fmt::Display;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum RedisErr {
    // Frame Error
    FrameIncomplete,
    FrameMalformed,

    // Command Error
    InvalidProtocol,
    SyntaxError,
    WrongNumberOfArguments,
    InvalidArgument,
    UnknownCommand,

    // DB Error
    NoAction,
    WrongType,
    KeyNotFound,
    OutOfMemory,

    // Server Error
    WrongAddressFormat,
    IOError,
    PollError,
    ConnectionAborted,
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

impl From<RedisErr> for String {
    fn from(err: RedisErr) -> String {
        std::fmt::format(format_args!("{:?}", err))
    }
}
