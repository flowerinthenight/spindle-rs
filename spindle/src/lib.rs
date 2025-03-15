use exp_backoff::BackoffBuilder;
use google_cloud_spanner::client::Client;
use google_cloud_spanner::client::ClientConfig;
use google_cloud_spanner::statement::Statement;
use google_cloud_spanner::value::CommitTimestamp;
use log::*;
use std::fmt::Write as _;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::thread;
use std::time::{Duration, Instant};
use time::OffsetDateTime;
use tokio::runtime::Runtime;
use uuid::Uuid;

#[macro_use(defer)]
extern crate scopeguard;

#[derive(Debug)]
struct DiffToken {
    diff: i64,
    token: i128,
}

#[derive(Debug)]
struct Record {
    name: String,
    heartbeat: i128,
    token: i128,
    writer: String,
}

#[derive(Debug)]
enum ProtoCtrl {
    Exit,
    Dummy(Sender<bool>),
    InitialLock(Sender<i128>),
    NextLockInsert { name: String, tx: Sender<i128> },
    NextLockUpdate { token: i128, tx: Sender<i128> },
    CheckLock(Sender<DiffToken>),
    CurrentToken(Sender<Record>),
    Heartbeat(Sender<i128>),
}

pub fn get_spanner_client(rt: &Runtime, db: String) -> Client {
    let (tx, rx) = channel();
    rt.block_on(async {
        let config = ClientConfig::default().with_auth().await.unwrap();
        let client = Client::new(db, config).await.unwrap();
        tx.send(client).unwrap();
    });

    rx.recv().unwrap()
}

fn spanner_caller(db: String, table: String, name: String, id: String, rx_ctrl: Receiver<ProtoCtrl>) {
    let rt = Runtime::new().unwrap();
    let client = get_spanner_client(&rt, db);
    for code in rx_ctrl {
        match code {
            ProtoCtrl::Exit => {
                rt.block_on(async { client.close().await });
                return;
            }
            ProtoCtrl::Dummy(tx) => {
                info!("dummy received");
                tx.send(true).unwrap();
            }
            ProtoCtrl::InitialLock(tx) => {
                let start = Instant::now();
                let (tx_in, rx_in): (Sender<i128>, Receiver<i128>) = channel();
                rt.block_on(async {
                    let mut q = String::new();
                    write!(&mut q, "insert {} ", table).unwrap();
                    write!(&mut q, "(name, heartbeat, token, writer) ").unwrap();
                    write!(&mut q, "values (").unwrap();
                    write!(&mut q, "'{}',", name).unwrap();
                    write!(&mut q, "PENDING_COMMIT_TIMESTAMP(),").unwrap();
                    write!(&mut q, "PENDING_COMMIT_TIMESTAMP(),").unwrap();
                    write!(&mut q, "'{}')", id).unwrap();
                    let stmt = Statement::new(q);
                    let mut tx = client.begin_read_write_transaction().await.unwrap();
                    let res = tx.update(stmt).await;
                    let res = tx.end(res, None).await;
                    match res {
                        Ok(s) => {
                            let ts = s.0.unwrap();
                            let dt = OffsetDateTime::from_unix_timestamp(ts.seconds)
                                .unwrap()
                                .replace_nanosecond(ts.nanos as u32)
                                .unwrap();
                            info!("InitialLock commit timestamp: {dt}");
                            tx_in.send(dt.unix_timestamp_nanos()).unwrap();
                        }
                        Err(e) => {
                            error!("InitialLock DML failed: {e}");
                            tx_in.send(-1).unwrap();
                        }
                    };
                });

                if let Err(e) = tx.send(rx_in.recv().unwrap()) {
                    error!("InitialLock reply failed: {e}")
                }

                info!("InitialLock took {:?}", start.elapsed());
            }
            ProtoCtrl::NextLockInsert { name, tx } => {
                let start = Instant::now();
                let (tx_in, rx_in): (Sender<i128>, Receiver<i128>) = channel();
                rt.block_on(async {
                    let mut q = String::new();
                    write!(&mut q, "insert {} ", table).unwrap();
                    write!(&mut q, "(name) ").unwrap();
                    write!(&mut q, "values ('{}')", name).unwrap();
                    let stmt = Statement::new(q);
                    let mut tx = client.begin_read_write_transaction().await.unwrap();
                    let res = tx.update(stmt).await;
                    let res = tx.end(res, None).await;
                    match res {
                        Ok(s) => {
                            let ts = s.0.unwrap();
                            let dt = OffsetDateTime::from_unix_timestamp(ts.seconds)
                                .unwrap()
                                .replace_nanosecond(ts.nanos as u32)
                                .unwrap();
                            tx_in.send(dt.unix_timestamp_nanos()).unwrap();
                        }
                        Err(e) => {
                            error!("NextLockInsert DML failed: {e}");
                            tx_in.send(-1).unwrap();
                        }
                    };
                });

                if let Err(e) = tx.send(rx_in.recv().unwrap()) {
                    error!("NextLockInsert reply failed: {e}")
                }

                debug!("NextLockInsert took {:?}", start.elapsed());
            }
            ProtoCtrl::NextLockUpdate { token, tx } => {
                let start = Instant::now();
                let (tx_in, rx_in): (Sender<i128>, Receiver<i128>) = channel();
                rt.block_on(async {
                    let mut q = String::new();
                    write!(&mut q, "update {} set ", table).unwrap();
                    write!(&mut q, "heartbeat = PENDING_COMMIT_TIMESTAMP(), ").unwrap();
                    write!(&mut q, "token = @token, ").unwrap();
                    write!(&mut q, "writer = @writer ").unwrap();
                    write!(&mut q, "where name = @name").unwrap();
                    let mut stmt1 = Statement::new(q);
                    let odt = OffsetDateTime::from_unix_timestamp_nanos(token).unwrap();
                    stmt1.add_param("token", &odt);
                    stmt1.add_param("writer", &id);
                    stmt1.add_param("name", &name);
                    let mut tx = client.begin_read_write_transaction().await.unwrap();
                    let res = tx.update(stmt1).await;

                    // Best-effort cleanup.
                    let mut q = String::new();
                    write!(&mut q, "delete from {} ", table).unwrap();
                    write!(&mut q, "where starts_with(name, '{}_')", name).unwrap();
                    let stmt2 = Statement::new(q);
                    let _ = tx.update(stmt2).await;

                    let res = tx.end(res, None).await;
                    match res {
                        Ok(s) => {
                            let ts = s.0.unwrap();
                            let dt = OffsetDateTime::from_unix_timestamp(ts.seconds)
                                .unwrap()
                                .replace_nanosecond(ts.nanos as u32)
                                .unwrap();
                            tx_in.send(dt.unix_timestamp_nanos()).unwrap();
                        }
                        Err(e) => {
                            error!("NextLockUpdate DML failed: {e}");
                            tx_in.send(-1).unwrap();
                        }
                    };
                });

                if let Err(e) = tx.send(rx_in.recv().unwrap()) {
                    error!("NextLockUpdate reply failed: {e}")
                }

                debug!("NextLockUpdate took {:?}", start.elapsed());
            }
            ProtoCtrl::CheckLock(tx) => {
                let start = Instant::now();
                let (tx_in, rx_in): (Sender<DiffToken>, Receiver<DiffToken>) = channel();
                rt.block_on(async {
                    let mut q = String::new();
                    write!(&mut q, "select ",).unwrap();
                    write!(
                        &mut q,
                        "timestamp_diff(current_timestamp(), heartbeat, millisecond) as diff, ",
                    )
                    .unwrap();
                    write!(&mut q, "token from {} ", table).unwrap();
                    write!(&mut q, "where name = @name").unwrap();
                    let mut stmt = Statement::new(q);
                    stmt.add_param("name", &name);
                    let mut tx = client.single().await.unwrap();
                    let mut iter = tx.query(stmt).await.unwrap();
                    let mut empty = true;
                    while let Some(row) = iter.next().await.unwrap() {
                        let d = row.column_by_name::<i64>("diff").unwrap();
                        let t = row.column_by_name::<CommitTimestamp>("token").unwrap();
                        tx_in
                            .send(DiffToken {
                                diff: d,
                                token: t.unix_timestamp_nanos(),
                            })
                            .unwrap();

                        empty = false;
                        break; // ensure single line
                    }

                    if empty {
                        tx_in.send(DiffToken { diff: 0, token: -1 }).unwrap();
                    }
                });

                if let Err(e) = tx.send(rx_in.recv().unwrap()) {
                    error!("CheckLock reply failed: {e}")
                }

                debug!("CheckLock took {:?}", start.elapsed());
            }
            ProtoCtrl::CurrentToken(tx) => {
                let start = Instant::now();
                let (tx_in, rx_in): (Sender<Record>, Receiver<Record>) = channel();
                rt.block_on(async {
                    let mut q = String::new();
                    write!(&mut q, "select token, writer from {} ", table).unwrap();
                    write!(&mut q, "where name = @name").unwrap();
                    let mut stmt = Statement::new(q);
                    stmt.add_param("name", &name);
                    let mut tx = client.single().await.unwrap();
                    let mut iter = tx.query(stmt).await.unwrap();
                    let mut empty = true;
                    while let Some(row) = iter.next().await.unwrap() {
                        let t = row.column_by_name::<CommitTimestamp>("token").unwrap();
                        let w = row.column_by_name::<String>("writer").unwrap();
                        tx_in
                            .send(Record {
                                name: String::from(""),
                                heartbeat: 0,
                                token: t.unix_timestamp_nanos(),
                                writer: w,
                            })
                            .unwrap();

                        empty = false;
                        break; // ensure single line
                    }

                    if empty {
                        tx_in
                            .send(Record {
                                name: String::from(""),
                                heartbeat: -1,
                                token: -1,
                                writer: String::from(""),
                            })
                            .unwrap();
                    }
                });

                if let Err(e) = tx.send(rx_in.recv().unwrap()) {
                    error!("CurrentToken reply failed: {e}")
                }

                info!("CurrentToken took {:?}", start.elapsed());
            }
            ProtoCtrl::Heartbeat(tx) => {
                let start = Instant::now();
                let (tx_in, rx_in): (Sender<i128>, Receiver<i128>) = channel();
                rt.block_on(async {
                    let mut q = String::new();
                    write!(&mut q, "update {} ", table).unwrap();
                    write!(&mut q, "set heartbeat = PENDING_COMMIT_TIMESTAMP() ").unwrap();
                    write!(&mut q, "where name = @name").unwrap();
                    let mut stmt = Statement::new(q);
                    stmt.add_param("name", &name);
                    let mut tx = client.begin_read_write_transaction().await.unwrap();
                    let res = tx.update(stmt).await;
                    let res = tx.end(res, None).await;
                    match res {
                        Ok(s) => {
                            let ts = s.0.unwrap();
                            let dt = OffsetDateTime::from_unix_timestamp(ts.seconds)
                                .unwrap()
                                .replace_nanosecond(ts.nanos as u32)
                                .unwrap();
                            debug!("Heartbeat commit timestamp: {dt}");
                            tx_in.send(dt.unix_timestamp_nanos()).unwrap();
                        }
                        Err(_) => tx_in.send(-1).unwrap(),
                    };
                });

                if let Err(e) = tx.send(rx_in.recv().unwrap()) {
                    error!("Heartbeat reply failed: {e}")
                }

                debug!("Heartbeat took {:?}", start.elapsed());
            }
        }
    }
}

pub struct Lock {
    db: String,
    table: String,
    name: String,
    id: String,
    duration_ms: u64,
    active: Arc<AtomicUsize>,
    token: Arc<AtomicU64>,
    exit_tx: Vec<Sender<ProtoCtrl>>,
}

impl Lock {
    pub fn builder() -> LockBuilder {
        LockBuilder::default()
    }

    pub fn run(&mut self) {
        info!(
            "run: table={}, name={}, id={}, duration={:?}",
            self.table,
            self.name,
            self.id,
            Duration::from_millis(self.duration_ms)
        );

        let leader = Arc::new(AtomicUsize::new(0));

        // Setup Spanner query thread. Delegate to a separate thread to have
        // a better control over async calls and a tokio runtime.
        let (tx_ctrl, rx_ctrl): (Sender<ProtoCtrl>, Receiver<ProtoCtrl>) = channel();
        self.exit_tx.push(tx_ctrl.clone());
        let db = self.db.clone();
        let table = self.table.clone();
        let lock_name = self.name.clone();
        let node_id = self.id.clone();
        thread::spawn(move || spanner_caller(db, table, lock_name, node_id, rx_ctrl));

        // Setup the heartbeat thread (leader only). No proper exit here;
        // let the OS do the cleanup upon termination of the main thread.
        let ldr_hb = leader.clone();
        let min = (self.duration_ms / 10) * 5;
        let max = (self.duration_ms / 10) * 8;
        let tx_hb = tx_ctrl.clone();
        thread::spawn(move || {
            info!(
                "start heartbeat thread: min={:?}, max={:?}",
                Duration::from_millis(min),
                Duration::from_millis(max)
            );

            // We don't really care about ns precision here; only for random pause.
            let mut bo = BackoffBuilder::new().initial_ns(min).max_ns(max).build();
            loop {
                let ldr_val = ldr_hb.load(Ordering::Acquire);
                if ldr_val > 0 {
                    let start = Instant::now();
                    let mut pause = 0;
                    _ = pause; // warn
                    loop {
                        let tmp = bo.pause();
                        if tmp >= min {
                            pause = tmp;
                            break;
                        }
                    }

                    let (tx, rx): (Sender<i128>, Receiver<i128>) = channel();
                    if let Ok(_) = tx_hb.send(ProtoCtrl::Heartbeat(tx)) {
                        if let Err(_) = rx.recv() {} // ignore
                    }

                    let latency = start.elapsed().as_millis() as u64;
                    if latency < pause {
                        pause -= latency;
                    }

                    info!("heartbeat[{ldr_val}]: pause for {:?}", Duration::from_millis(pause));
                    thread::sleep(Duration::from_millis(pause));
                } else {
                    info!("heartbeat[_]: pause for 1s");
                    thread::sleep(Duration::from_secs(1));
                }
            }
        });

        // Setup the main thread that drives the lock forward. No proper exit here;
        // let the OS do the cleanup upon termination of the main thread.
        let tx_main = tx_ctrl.clone();
        let duration_ms = self.duration_ms;
        let token = self.token.clone();
        let lock_name = self.name.clone();
        thread::spawn(move || {
            let mut round: u64 = 0;
            let mut initial = true;
            loop {
                round += 1;
                let start = Instant::now();
                info!("");

                defer! {
                    info!("round {} took {:?}", round, start.elapsed());
                    thread::sleep(Duration::from_millis(duration_ms));
                }

                // First, see if already locked (could be us or somebody else).
                let (tx, rx): (Sender<DiffToken>, Receiver<DiffToken>) = channel();
                if let Err(_) = tx_main.send(ProtoCtrl::CheckLock(tx)) {
                    continue;
                }

                let mut locked = false;
                match rx.recv() {
                    Ok(v) => {
                        'single: loop {
                            // We are leader now.
                            if token.load(Ordering::Acquire) == v.token as u64 {
                                leader.fetch_add(1, Ordering::Relaxed);
                                if leader.load(Ordering::Acquire) == 1 {
                                    let (tx, rx): (Sender<i128>, Receiver<i128>) = channel();
                                    if let Ok(_) = tx_main.send(ProtoCtrl::Heartbeat(tx)) {
                                        if let Err(_) = rx.recv() {} // ignore
                                    }
                                }

                                info!("leader active (me)");
                                locked = true;
                                break 'single;
                            }

                            // We're not leader now.
                            if v.diff > 0 {
                                let mut alive: bool = false;
                                let diff = v.diff as u64;
                                if diff <= duration_ms {
                                    info!("diff <= duration, diff={diff}, duration={duration_ms}");
                                    alive = true;
                                } else if diff > duration_ms {
                                    // Sometimes, its going to go beyond duration+drift, even
                                    // in normal situations. In that case, we will allow a
                                    // new leader for now.
                                    let ovr = diff - duration_ms;
                                    alive = ovr <= 1000; // allow 1s drift
                                    info!("diff > duration, diff={diff}, duration={duration_ms}, ovr={ovr}");
                                }

                                if alive {
                                    info!("leader active (not me)");
                                    leader.store(0, Ordering::Relaxed); // reset heartbeat
                                    locked = true;
                                    break 'single;
                                }
                            }

                            break 'single;
                        }
                    }
                    Err(_) => continue,
                }

                if locked {
                    continue;
                }

                if initial {
                    // Attempt first ever lock. The return commit timestamp will be our fencing
                    // token. Only one node should be able to do this successfully.
                    initial = false;
                    let (tx, rx): (Sender<i128>, Receiver<i128>) = channel();
                    if let Ok(_) = tx_main.send(ProtoCtrl::InitialLock(tx)) {
                        if let Ok(t) = rx.recv() {
                            if t > -1 {
                                token.store(t as u64, Ordering::Relaxed);
                                info!("init: got the lock with token {}", t);
                            }
                        }
                    }
                } else {
                    // For the succeeding lock attempts.
                    let (tx, rx): (Sender<Record>, Receiver<Record>) = channel();
                    if let Ok(_) = tx_main.send(ProtoCtrl::CurrentToken(tx)) {
                        if let Ok(v) = rx.recv() {
                            if v.token < 0 {
                                continue;
                            }

                            // Attempt to grab the next lock. Multiple nodes could potentially
                            // do this successfully.
                            let mut update = false;
                            let mut token_up: i128 = 0;
                            let mut name = String::new();
                            write!(&mut name, "{}_{}", lock_name, v.token).unwrap();
                            let (tx, rx): (Sender<i128>, Receiver<i128>) = channel();
                            if let Ok(_) = tx_main.send(ProtoCtrl::NextLockInsert { name, tx }) {
                                if let Ok(t) = rx.recv() {
                                    if t > 0 {
                                        update = true;
                                        token_up = t;
                                    }
                                }
                            }

                            if update {
                                // We got the lock. Attempt to update the current token
                                // to this commit timestamp.
                                let (tx, rx): (Sender<i128>, Receiver<i128>) = channel();
                                if let Ok(_) = tx_main.send(ProtoCtrl::NextLockUpdate { token: token_up, tx }) {
                                    if let Ok(t) = rx.recv() {
                                        if t > 0 {
                                            // Doesn't mean we're leader, yet.
                                            token.store(token_up as u64, Ordering::Relaxed);
                                            info!("next: got the lock with token {}", token_up);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });

        // Finally, set the system active.
        let active = self.active.clone();
        active.store(1, Ordering::Relaxed);
    }

    pub fn has_lock(&self) -> (bool, String, u64) {
        let active = self.active.clone();
        if active.load(Ordering::Acquire) < 1 {
            return (false, String::from(""), 0);
        }

        let token = self.token.clone();
        let (tx, rx): (Sender<Record>, Receiver<Record>) = channel();
        if let Ok(_) = self.exit_tx[0].send(ProtoCtrl::CurrentToken(tx)) {
            if let Ok(t) = rx.recv() {
                let tv = t.token as u64;
                if tv == token.load(Ordering::Acquire) {
                    return (true, t.writer, tv);
                }
            }
        }

        return (false, String::from(""), 0);
    }

    pub fn close(&mut self) {
        if let Err(e) = self.exit_tx[0].send(ProtoCtrl::Exit) {
            error!("ProtoCtrl::Exit failed: {e}");
        };
    }
}

#[derive(Default)]
pub struct LockBuilder {
    db: String,
    table: String,
    name: String,
    id: String,
    duration_ms: u64,
}

impl LockBuilder {
    pub fn new() -> LockBuilder {
        LockBuilder::default()
    }

    pub fn db(mut self, db: String) -> LockBuilder {
        self.db = db;
        self
    }

    pub fn table(mut self, table: String) -> LockBuilder {
        self.table = table;
        self
    }

    pub fn name(mut self, name: String) -> LockBuilder {
        self.name = name;
        self
    }

    pub fn id(mut self, id: String) -> LockBuilder {
        self.id = id;
        self
    }

    pub fn duration_ms(mut self, ms: u64) -> LockBuilder {
        self.duration_ms = ms;
        self
    }

    pub fn build(self) -> Lock {
        Lock {
            db: self.db,
            table: self.table,
            name: self.name,
            id: if self.id != "" {
                self.id
            } else {
                let id = Uuid::new_v4();
                id.to_string()
            },
            duration_ms: self.duration_ms,
            active: Arc::new(AtomicUsize::new(0)),
            token: Arc::new(AtomicU64::new(0)),
            exit_tx: vec![],
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn label() {
        let cond = 1;
        let result = 'b: {
            if cond < 5 {
                break 'b 1;
            } else {
                break 'b 2;
            }
        };
        assert_eq!(result, 1);
    }
}
