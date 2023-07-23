use std::fmt::Display;

#[derive(Debug, PartialEq, Clone)]
#[allow(dead_code)]
pub enum Err {
    WrongType,
    SyntaxError,
    IOError(String),
    NoKey,
    NoValue,
}

impl std::error::Error for Err {}

impl Display for Err {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(format!("{:?}", self).as_str())
    }
}

impl From<std::io::Error> for Err {
    fn from(err: std::io::Error) -> Self {
        Err::IOError(err.to_string())
    }
}

impl From<std::string::FromUtf8Error> for Err {
    fn from(_: std::string::FromUtf8Error) -> Self {
        Err::SyntaxError
    }
}

impl From<std::num::ParseIntError> for Err {
    fn from(_: std::num::ParseIntError) -> Self {
        Err::SyntaxError
    }
}

impl Into<String> for Err {
    fn into(self) -> String {
        std::fmt::format(format_args!("{:?}", self))
    }
}
