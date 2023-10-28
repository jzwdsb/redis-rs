use redis_rs::arg::Arg;
use redis_rs::server;

extern crate env_logger;

fn main() {
    env_logger::init();
    let arg = Arg::parse();

    let server_builder = server::ServerBuilder::new();

    let mut server = server_builder
        .addr(arg.get_host_ref())
        .port(arg.get_port())
        .max_client(arg.get_max_clients())
        .build()
        .unwrap();

    server.run().unwrap();
}
