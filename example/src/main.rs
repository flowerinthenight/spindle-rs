use log::*;
use spindle::*;
use std::env;
use std::error::Error;
use std::{thread, time::Duration};

fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();

    let args: Vec<String> = env::args().collect();
    info!("args[0]={:?}", args.get(1));

    let mut lock = LockBuilder::new()
        .db("projects/mobingi-main/instances/alphaus-prod/databases/main".to_string())
        .table("testlease".to_string())
        .name("mylock".to_string())
        // .id(":8080".to_string())
        .duration_ms(5000)
        .build();

    lock.run();

    thread::sleep(Duration::from_secs(20));

    lock.close();

    Ok(())
}
