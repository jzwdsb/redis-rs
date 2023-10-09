
#[allow(unused_imports)]
use redis_rs::client::Client;

mod test {
    #[allow(unused_imports)]
    use super::*;
    use std::process::Child;

    #[allow(dead_code)]
    fn setup() -> Child {
        // Setup code here
        

        // start redis server in the background
        std::process::Command::new("cargo")
            .arg("run")
            .spawn()
            .expect("failed to execute process")
    }

    #[test]
    fn test_redis_integration() {
        // Connect to Redis
        // let mut resource = setup();
        // // TODO: fix the integration test with sync connection
        // std::thread::sleep(std::time::Duration::from_secs(5));
        // let mut client = Client::open("127.0.0.1:6379").unwrap();
        
        // let con = client.get_connection();

        // // Set a key-value pair
        // let _: () = con.set("my_key", "my_value").unwrap();
    
        // // Get the value of the key
        // let value = con.get("my_key").unwrap();
        // assert_eq!(value, "my_value".as_bytes()); 

        // resource.kill().unwrap();
    }
}
