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

    // Optional (but faster) way of knowing if we
    // are leader or not. 1 = leader, 0 = not.
    let (tx_ldr, rx_ldr) = channel();

    let (tx, rx) = channel(); // for ctrl-c
    ctrlc::set_handler(move || tx.send(()).unwrap())?;
    let mut lock = LockBuilder::new()
        .db(args[1].clone())
        .table(args[2].clone())
        .name("spindle-rs".to_string())
        .lease_ms(3000)
        .leader_tx(Some(tx_ldr)) // optional
        .build();

    lock.run()?;

    thread::spawn(move || {
        loop {
            match rx_ldr.recv() {
                Ok(v) => info!("leader: {v}"),
                Err(e) => error!("recv failed: {e}"),
            };
        }
    });

    // Wait for a bit before calling has_lock().
    thread::sleep(Duration::from_secs(10));

    // Traditional way of knowing whether we are
    // leader or not (other than channels).
    let (locked, node, token) = lock.has_lock();
    info!("has_lock: {locked}, {node}, {token}");

    // Wait for Ctrl-C.
    rx.recv()?;
    lock.close();

    Ok(())
}
