use anyhow::Result;
use ctrlc;
use log::*;
use spindle_rs::*;
use std::env;
use std::sync::mpsc::channel;
use std::thread;
use std::time::Duration;

fn main() -> Result<()> {
    env_logger::init();
    let args: Vec<String> = env::args().collect();

    if args.len() < 3 {
        error!("provide the db and table args");
        return Ok(());
    }

    let (tx, rx) = channel();
    ctrlc::set_handler(move || tx.send(()).unwrap())?;
    let mut lock = LockBuilder::new()
        .db(args[1].clone())
        .table(args[2].clone())
        .name("spindle-rs".to_string())
        .duration_ms(3000)
        .callback(Some(|v| info!("callback: leader={v}")))
        .build();

    lock.run()?;

    // Wait for a bit before calling has_lock().
    thread::sleep(Duration::from_secs(10));
    let (locked, node, token) = lock.has_lock();
    info!("has_lock: {locked}, {node}, {token}");

    // Wait for Ctrl-C.
    rx.recv()?;
    lock.close();

    Ok(())
}
