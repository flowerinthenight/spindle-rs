use spindle::*;
use std::env;
use std::error::Error;
use std::{thread, time::Duration};

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    println!("args[0]={:?}", args.get(1));

    let mut lock = LockBuilder::new()
        .db("projects/mobingi-main/instances/alphaus-prod/databases/main".to_string())
        .table("locktable".to_string())
        .name("spindle-rs".to_string())
        .id(":8080".to_string())
        .timeout(5000)
        .build();

    lock.inc();
    lock.inc();
    lock.run();
    lock.close();

    thread::sleep(Duration::from_secs(2));

    Ok(())
}
