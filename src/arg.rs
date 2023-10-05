use clap::Parser;
use marco::Getter;

#[derive(Parser,Debug,Getter)]
#[command(author, version, about, long_about)]
pub struct Arg {
    #[clap(long, default_value = "0.0.0.0")]
    host: String,
    #[clap(short, long, default_value = "6379")]
    port: u16,

    #[clap(long, default_value = "1024")]
    max_clients: usize,
}

impl Arg {
    pub fn parse() -> Self {
        Arg::parse_from(std::env::args())
    }
}

