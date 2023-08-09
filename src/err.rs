use std::fmt::Display;

#[derive(Debug, PartialEq, Clone)]
#[allow(dead_code)]
pub enum ServerErr {
    WrongType,
    SyntaxError,
    IOError(String),
    PollError,
    NoKey,
    NoValue,
}

impl std::error::Error for ServerErr {}

impl Display for ServerErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(format!("{:?}", self).as_str())
    }
}

impl From<std::io::Error> for ServerErr {
    fn from(err: std::io::Error) -> Self {
        ServerErr::IOError(err.to_string())
    }
}

impl From<std::string::FromUtf8Error> for ServerErr {
    fn from(_: std::string::FromUtf8Error) -> Self {
        ServerErr::SyntaxError
    }
}

impl From<std::num::ParseIntError> for ServerErr {
    fn from(_: std::num::ParseIntError) -> Self {
        ServerErr::SyntaxError
    }
}

impl Into<String> for ServerErr {
    fn into(self) -> String {
        std::fmt::format(format_args!("{:?}", self))
    }
}
