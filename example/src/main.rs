use ctrlc;
use log::*;
use spindle_rs::*;
use std::env;
use std::error::Error;
use std::sync::mpsc::channel;
use std::thread;
use std::time::Duration;

fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();
    let args: Vec<String> = env::args().collect();

    if args.len() < 3 {
        error!("provide the db and table args");
        return Ok(());
    }

    let (tx, rx) = channel();
    ctrlc::set_handler(move || tx.send(()).unwrap()).unwrap();
    let mut lock = LockBuilder::new()
        .db(args[1].clone())
        .table(args[2].clone())
        .name("spindle-rs".to_string())
        .duration_ms(3000)
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
