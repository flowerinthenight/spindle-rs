//! A distributed locking implementation using [Cloud Spanner](https://cloud.google.com/spanner/).
//! It relies on Spanner's [TrueTime](https://cloud.google.com/spanner/docs/true-time-external-consistency)
//! and [transactions](https://cloud.google.com/spanner/docs/transactions) support to achieve its locking
//! mechanism. It's a port of the original [spindle](https://github.com/flowerinthenight/spindle),
//! which is written in Go.
//!
//! One use case for this library is [leader election](https://en.wikipedia.org/wiki/Leader_election).
//! If you want one host/node/pod to be the leader within a cluster/group, you can achieve that with this
//! library. When the leader fails, it will fail over to another host/node/pod within a specific timeout.

pub mod spindle;

pub use spindle::{Lock, LockBuilder};

#[macro_use(defer)]
extern crate scopeguard;
