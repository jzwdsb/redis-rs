mod data;
mod db;
mod err;
mod helper;
mod frame;
mod server;
mod event;
mod cmd;

fn main() {
    let mut server = server::Server::new(
        "0.0.0.0",
        6379,
        1024,
    ).unwrap();

    server.run().unwrap();
}
