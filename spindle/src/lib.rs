use exp_backoff::BackoffBuilder;
use google_cloud_spanner::client::Client;
use google_cloud_spanner::client::ClientConfig;
use google_cloud_spanner::statement::Statement;
use google_cloud_spanner::value::CommitTimestamp;
use log::*;
use std::fmt::Write as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use std::time::{Duration, Instant};
use std::{io, thread};
use time::OffsetDateTime;
use tokio::runtime::Runtime;

#[derive(Debug, Clone)]
struct HeartbeatMeta {
    signal_as_reply: bool,
    signal: Arc<AtomicUsize>,
}

#[derive(Debug)]
enum ProtoCtrl {
    Exit,
    Dummy(Arc<AtomicUsize>),
    CheckLock,
    CurrentToken,
    Heartbeat(HeartbeatMeta),
}

#[derive(Debug)]
struct DiffToken {
    diff: i64,
    token: i128,
}

#[derive(Debug)]
struct LockVal {
    name: String,
    heartbeat: i128,
    token: i128,
    writer: String,
}

#[derive(Debug)]
enum ProtoData {
    CheckLock(DiffToken),
    CurrentToken(LockVal),
    Heartbeat(i128),
}

pub fn get_spanner_client(rt: &Runtime, db: String) -> Client {
    let (tx, rx) = mpsc::channel();
    rt.block_on(async {
        let config = ClientConfig::default().with_auth().await.unwrap();
        let client = Client::new(db, config).await.unwrap();
        tx.send(client).unwrap();
    });

    rx.recv().unwrap()
}

fn spanner_caller(
    db: String,
    table: String,
    name: String,
    rx_code: Receiver<ProtoCtrl>,
    tx_data: Sender<ProtoData>,
) {
    let rt = Runtime::new().unwrap();
    let client = get_spanner_client(&rt, db);
    for code in rx_code {
        match code {
            ProtoCtrl::Exit => {
                rt.block_on(async { client.close().await });
                return;
            }
            ProtoCtrl::Dummy(v) => {
                info!("dummy received");
                let vc = v.clone();
                vc.fetch_add(1, Ordering::Relaxed);
            }
            ProtoCtrl::CheckLock => {
                let start = Instant::now();
                let (tx_in, rx_in): (Sender<DiffToken>, Receiver<DiffToken>) = mpsc::channel();
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
                    while let Some(row) = iter.next().await.unwrap() {
                        let d = row.column_by_name::<i64>("diff").unwrap();
                        let t = row.column_by_name::<CommitTimestamp>("token").unwrap();
                        tx_in
                            .send(DiffToken {
                                diff: d,
                                token: t.unix_timestamp_nanos(),
                            })
                            .unwrap();
                        break; // ensure single line
                    }
                });

                tx_data
                    .send(ProtoData::CheckLock(rx_in.recv().unwrap()))
                    .unwrap();

                info!("ProtoData::CheckLock took {:?}", start.elapsed());
            }
            ProtoCtrl::CurrentToken => {
                let start = Instant::now();
                let (tx_in, rx_in): (Sender<LockVal>, Receiver<LockVal>) = mpsc::channel();
                rt.block_on(async {
                    let mut q = String::new();
                    write!(&mut q, "select token, writer from {} ", table).unwrap();
                    write!(&mut q, "where name = @name").unwrap();
                    let mut stmt = Statement::new(q);
                    stmt.add_param("name", &name);
                    let mut tx = client.single().await.unwrap();
                    let mut iter = tx.query(stmt).await.unwrap();
                    while let Some(row) = iter.next().await.unwrap() {
                        let t = row.column_by_name::<CommitTimestamp>("token").unwrap();
                        let w = row.column_by_name::<String>("writer").unwrap();
                        tx_in
                            .send(LockVal {
                                name: String::from(""),
                                heartbeat: 0,
                                token: t.unix_timestamp_nanos(),
                                writer: w,
                            })
                            .unwrap();
                        break; // ensure single line
                    }
                });

                tx_data
                    .send(ProtoData::CurrentToken(rx_in.recv().unwrap()))
                    .unwrap();

                info!("ProtoData::CurrentToken took {:?}", start.elapsed());
            }
            ProtoCtrl::Heartbeat(hbmeta) => {
                let start = Instant::now();
                let (tx_in, rx_in): (Sender<i128>, Receiver<i128>) = mpsc::channel();
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
                            info!("heartbeat_timestamp: {dt}");
                            tx_in.send(dt.unix_timestamp_nanos()).unwrap();
                        }
                        Err(_) => tx_in.send(-1).unwrap(),
                    };
                });

                if !hbmeta.signal_as_reply {
                    tx_data
                        .send(ProtoData::Heartbeat(rx_in.recv().unwrap()))
                        .unwrap();
                } else {
                    let sc = hbmeta.signal.clone();
                    sc.fetch_add(1, Ordering::Relaxed);
                }

                info!("ProtoData::Heartbeat took {:?}", start.elapsed());
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
    exit_tx: Vec<Sender<ProtoCtrl>>,
}

impl Lock {
    pub fn builder() -> LockBuilder {
        LockBuilder::default()
    }

    pub fn run(&mut self) {
        info!(
            "table={}, name={}, id={}, duration={:?}",
            self.table,
            self.name,
            self.id,
            Duration::from_millis(self.duration_ms)
        );

        let leader = Arc::new(AtomicUsize::new(0));

        // Setup Spanner query thread. Delegate to a separate thread to have
        // a better control over async calls and a tokio runtime.
        let (tx_data, rx_data): (Sender<ProtoData>, Receiver<ProtoData>) = mpsc::channel();
        let (tx_ctrl, rx_ctrl): (Sender<ProtoCtrl>, Receiver<ProtoCtrl>) = mpsc::channel();
        self.exit_tx.push(tx_ctrl.clone());
        let db = self.db.clone();
        let table = self.table.clone();
        let name = self.name.clone();
        let _thr = thread::spawn(move || spanner_caller(db, table, name, rx_ctrl, tx_data));

        // Setup the heartbeat thread (leader only). No proper exit here;
        // let the OS do the cleanup upon termination of main thread.
        let ldr_hb = leader.clone();
        let min = (self.duration_ms / 10) * 5;
        let max = (self.duration_ms / 10) * 8;
        let tx_ctrl_hb = tx_ctrl.clone();
        let _hb = thread::spawn(move || {
            info!(
                "min={:?}, max={:?}",
                Duration::from_millis(min),
                Duration::from_millis(max)
            );

            // We don't really care about ns precision here; only for random pause.
            let mut bo = BackoffBuilder::new().initial_ns(min).max_ns(max).build();
            loop {
                match ldr_hb.load(Ordering::Acquire) {
                    1 => {
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

                        let hbmeta = HeartbeatMeta {
                            signal_as_reply: true,
                            signal: Arc::new(AtomicUsize::new(1)),
                        };

                        match tx_ctrl_hb.send(ProtoCtrl::Heartbeat(hbmeta.clone())) {
                            Err(e) => error!("ProtoCtrl::Heartbeat failed: {e}"),
                            Ok(_) => loop {
                                let nval = hbmeta.signal.load(Ordering::Acquire);
                                if nval > 1 {
                                    break;
                                } else {
                                    thread::sleep(Duration::from_micros(10));
                                }
                            },
                        }

                        let pause_t = pause + start.elapsed().as_millis() as u64;
                        info!("({pause}) pause for {:?}", Duration::from_millis(pause_t));
                        thread::sleep(Duration::from_millis(pause_t));
                    }
                    _ => thread::sleep(Duration::from_secs(1)),
                }
            }
        });

        leader.store(1, Ordering::Relaxed);

        // Test check lock and get reply.
        tx_ctrl.send(ProtoCtrl::CheckLock).unwrap();
        match rx_data.recv().unwrap() {
            ProtoData::CheckLock(v) => {
                info!("reply for [CheckLock]: v={:?}", v);
            }
            _ => {}
        }

        // Test query token and get reply.
        tx_ctrl.send(ProtoCtrl::CurrentToken).unwrap();
        match rx_data.recv().unwrap() {
            ProtoData::CurrentToken(v) => {
                info!("reply for [CurrentToken]: v={:?}", v);
            }
            _ => {}
        }

        // Test heartbeat update and get reply.
        tx_ctrl
            .send(ProtoCtrl::Heartbeat(HeartbeatMeta {
                signal_as_reply: false,
                signal: Arc::new(AtomicUsize::new(0)),
            }))
            .unwrap();

        match rx_data.recv().unwrap() {
            ProtoData::Heartbeat(v) => {
                info!("reply for [Heartbeat]: v={:?}", v);
            }
            _ => {}
        }
    }

    pub fn close(&mut self) {
        if let Err(e) = self.exit_tx[0].send(ProtoCtrl::Exit) {
            error!("ProtoCtrl::Exit failed: {e}");
        };
    }

    pub fn inc(&self) {
        let v = Arc::clone(&self.active);
        info!("atomic={}", v.fetch_add(1, Ordering::Relaxed));
        // info!("timeout={}", &self.timeout.unwrap_or(5000));
        // info!("duration={}", self.duration);
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
            id: self.id,
            duration_ms: self.duration_ms,
            active: Arc::new(AtomicUsize::new(0)),
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
