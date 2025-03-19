[![main](https://github.com/flowerinthenight/spindle-rs/actions/workflows/main.yml/badge.svg)](https://github.com/flowerinthenight/spindle-rs/actions/workflows/main.yml)
[![crates.io](https://img.shields.io/crates/v/spindle_rs)](https://crates.io/crates/spindle_rs)
[![docs-rs](https://img.shields.io/docsrs/spindle_rs)](https://docs.rs/spindle_rs/latest/spindle_rs/)

## Overview

This crate implements distributed locking using [Cloud Spanner](https://cloud.google.com/spanner/). It relies on Spanner's [TrueTime](https://cloud.google.com/spanner/docs/true-time-external-consistency) and [transactions](https://cloud.google.com/spanner/docs/transactions) support to achieve its locking mechanism. It's a port of the original [spindle](https://github.com/flowerinthenight/spindle), which is written in Go.

## Use cases
One use case for this library is [leader election](https://en.wikipedia.org/wiki/Leader_election). If you want one host/node/pod to be the leader within a cluster/group, you can achieve that with this crate. When the leader fails, it will fail over to another host/node/pod within a specific timeout.

## Usage
At the moment, the table needs to be created beforehand using the following DDL (`locktable` is just an example):
```sql
CREATE TABLE locktable (
    name STRING(MAX) NOT NULL,
    heartbeat TIMESTAMP OPTIONS (allow_commit_timestamp=true),
    token TIMESTAMP OPTIONS (allow_commit_timestamp=true),
    writer STRING(MAX),
) PRIMARY KEY (name)
```

After creating the lock object, you will call the `run()` function which will attempt to acquire a named lock at a regular interval (lease duration). A `has_lock()` function is provided which returns true (along with the lock token) if the lock is successfully acquired. Something like:

```rust
use spindle_rs::*;
...

fn main() -> Result<(), Box<dyn Error>> {
    let (tx, rx) = channel();
    ctrlc::set_handler(move || tx.send(()).unwrap())?;
    let mut lock = LockBuilder::new()
        .db("projects/p/instances/i/databases/db".to_string())
        .table("locktable".to_string()) // see CREATE TABLE
        .name("spindle-rs".to_string()) // lock name
        .duration_ms(3000)
        .build();

    lock.run()?;

    // Wait for a bit before calling has_lock().
    thread::sleep(Duration::from_secs(10));
    let (locked, node, token) = lock.has_lock();
    println!("has_lock: {locked}, {node}, {token}");

    // Wait for Ctrl-C.
    rx.recv()?;
    lock.close();

    Ok(())
}
```

## Example

A sample [code](./example/src/main.rs) is provided to demonstrate the mechanism through logs. You can try running multiple processes in multiple terminals.

```bash
$ git clone https://github.com/flowerinthenight/spindle-rs
$ cd spindle-rs/
$ cargo build
# Update args with your actual values:
$ RUST_LOG=info ./target/debug/example \
  projects/p/instances/i/databases/db \
  locktable
```

The leader process should output something like `leader active (me)`. You can then try to stop (Ctrl+C) that process and observe another one taking over as leader.

## License

This library is licensed under the [Apache 2.0 License](./LICENSE).
