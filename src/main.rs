use redis_rs::server;

extern crate env_logger;

fn main() {
    env_logger::init();
    let mut server = server::Server::new("0.0.0.0", 6379, 1024).unwrap();

    server.run().unwrap();
}
