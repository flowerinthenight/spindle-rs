use ctrlc;
use log::info;
use spindle::*;
use std::error::Error;
use std::sync::mpsc::channel;
use std::thread;
use std::time::Duration;

fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();
    let (tx, rx) = channel();
    ctrlc::set_handler(move || tx.send(()).unwrap()).unwrap();
    let mut lock = LockBuilder::new()
        .db("projects/mobingi-main/instances/alphaus-prod/databases/main".to_string())
        .table("testlease".to_string())
        .name("spindle-rs".to_string())
        // .id(":8080".to_string())
        .duration_ms(5000)
        .build();

    lock.run();

    // Wait for a bit before calling has_lock().
    thread::sleep(Duration::from_secs(10));
    let (locked, node, token) = lock.has_lock();
    info!("has_lock: {locked}, {node}, {token}");

    // Wait for Ctrl-C.
    rx.recv().unwrap();
    lock.close();

    Ok(())
}
