[![main](https://github.com/flowerinthenight/spindle-rs/actions/workflows/main.yml/badge.svg)](https://github.com/flowerinthenight/spindle-rs/actions/workflows/main.yml)

## Overview

This crate implements distributed locking using [Cloud Spanner](https://cloud.google.com/spanner/). It relies on Spanner's [TrueTime](https://cloud.google.com/spanner/docs/true-time-external-consistency) and [transactions](https://cloud.google.com/spanner/docs/transactions) support to achieve its locking mechanism. It's a port of [spindle](https://github.com/flowerinthenight/spindle).

## Use cases
One use case for this library is [leader election](https://en.wikipedia.org/wiki/Leader_election). If you want one host/node/pod to be the leader within a cluster/group, you can achieve that with this library. When the leader fails, it will fail over to another host/node/pod within a specific timeout.

## Usage
At the moment, the table needs to be created beforehand using the following DDL (`locktable` is just an example):
```SQL
CREATE TABLE locktable (
    name STRING(MAX) NOT NULL,
    heartbeat TIMESTAMP OPTIONS (allow_commit_timestamp=true),
    token TIMESTAMP OPTIONS (allow_commit_timestamp=true),
    writer STRING(MAX),
) PRIMARY KEY (name)
```

After creating the lock object, you will call the `run(...)` function which will attempt to acquire a named lock at a regular interval (lease duration). A `has_lock()` function is provided which returns true (along with the lock token) if the lock is successfully acquired. Something like:

```rust
use spindle_rs::*;
...

fn main() -> Result<(), Box<dyn Error>> {
    let (tx, rx) = channel();
    ctrlc::set_handler(move || tx.send(()).unwrap()).unwrap();
    let mut lock = LockBuilder::new()
        .db("projects/p/instances/i/databases/db".to_string())
        .table("locktable".to_string()) // see CREATE TABLE
        .name("spindle-rs".to_string())
        // .id(":8080".to_string())
        .duration_ms(5000)
        .build();

    lock.run();

    // Wait for a bit before calling has_lock().
    thread::sleep(Duration::from_secs(10));
    let (locked, node, token) = lock.has_lock();
    println!("has_lock: {locked}, {node}, {token}");

    // Wait for Ctrl-C.
    rx.recv().unwrap();
    lock.close();

    Ok(())
}
```

## Example

A sample [code](./example/src/main.rs) is provided to demonstrate the mechanism through logs. You can try running multiple processes in multiple terminals.

```bash
# Update args with your values as needed:
$ cd example/
$ cargo build
$ RUST_LOG=info ./target/debug/example \
  projects/p/instances/i/databases/db \
  locktable
```

The leader process should output something like `leader active (me)`. You can then try to stop (Ctrl+C) that process and observe another one taking over as leader.

## License

This library is licensed under the [Apache 2.0 License](./LICENSE).
