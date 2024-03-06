use redis_rs::arg::Arg;
use redis_rs::server::ServerBuilder;

extern crate env_logger;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let arg = Arg::parse();

    let server_builder = ServerBuilder::new_with_arg(arg);

    let server = server_builder.build().await?;

    server.run().await?;

    Ok(())
}
