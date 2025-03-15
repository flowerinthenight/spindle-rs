use ctrlc;
use log::info;
use spindle::*;
use std::error::Error;
use std::sync::mpsc;

fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();
    let (tx, rx) = mpsc::channel();
    ctrlc::set_handler(move || tx.send(()).unwrap()).unwrap();
    let mut lock = LockBuilder::new()
        .db("projects/mobingi-main/instances/alphaus-prod/databases/main".to_string())
        .table("testlease".to_string())
        .name("spindle-rs".to_string())
        // .id(":8080".to_string())
        .duration_ms(5000)
        .build();

    lock.run();
    rx.recv().unwrap();
    info!("exit");
    lock.close();
    Ok(())
}
