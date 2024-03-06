mod cmd;
mod connection;
mod db;
mod err;
mod frame;
mod helper;
mod shutdown;
// mod rdb;
mod handler;
mod value;

pub mod client;

pub mod arg;
pub mod server;

pub use arg::Arg;
pub use err::RedisErr;

type Result<T> = std::result::Result<T, RedisErr>;
