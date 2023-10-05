use redis_rs::arg::Arg;
use redis_rs::server;

extern crate env_logger;

fn main() {
    env_logger::init();
    let arg = Arg::parse();
    let mut server =
        server::Server::new(arg.get_host_ref(), arg.get_port(), arg.get_max_clients()).unwrap();

    server.run().unwrap();
}
