use std::io::{Read, Write};

use crate::err::Err;


type Bytes = Vec<u8>;
pub trait Transport {
    fn read(&mut self, stream: impl Read) -> Result<Bytes, Err>;
    fn write(&mut self, stream: impl Write, resp: Bytes) -> Result<(), Err>;   
}




