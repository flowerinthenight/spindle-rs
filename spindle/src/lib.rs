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
use tokio::runtime::Runtime;

#[derive(Debug)]
struct LockVal {
    pub name: String,
    pub heartbeat: String,
    pub token: String,
    pub writer: String,
}

#[derive(Debug)]
enum ProtoCtrl {
    Exit,
    CurrentToken,
}

#[derive(Debug)]
enum ProtoData {
    CurrentToken(LockVal),
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
            ProtoCtrl::CurrentToken => {
                let start = Instant::now();
                let (tx_in, rx_in): (Sender<LockVal>, Receiver<LockVal>) = mpsc::channel();
                rt.block_on(async {
                    let mut s = String::new();
                    write!(&mut s, "select token, writer from {} ", table).unwrap();
                    write!(&mut s, "where name = @name").unwrap();
                    let mut stmt = Statement::new(s);
                    stmt.add_param("name", &name);
                    let mut tx = client.single().await.unwrap();
                    let mut iter = tx.query(stmt).await.unwrap();
                    while let Some(row) = iter.next().await.unwrap() {
                        let t = row.column_by_name::<CommitTimestamp>("token").unwrap();
                        let w = row.column_by_name::<String>("writer").unwrap();
                        tx_in
                            .send(LockVal {
                                name: "".to_string(),
                                heartbeat: "".to_string(),
                                token: t.to_string(),
                                writer: w,
                            })
                            .unwrap();
                        break; // ensure single line
                    }
                });

                tx_data
                    .send(ProtoData::CurrentToken(rx_in.recv().unwrap()))
                    .unwrap();

                info!("'CurrentToken' took {:?}", start.elapsed());
            }
        }
    }
}

pub struct Lock {
    db: String,
    table: String,
    name: String,
    id: String,
    duration: u64,
    active: Arc<AtomicUsize>,
    exit_tx: Vec<Sender<ProtoCtrl>>,
}

impl Lock {
    pub fn builder() -> LockBuilder {
        LockBuilder::default()
    }

    pub fn run(&mut self) {
        info!("table={}, name={}, id={}", self.table, self.name, self.id);

        // Setup Spanner query thread. Delegate to a separate thread to have
        // a better control over async calls and a tokio runtime.
        let (tx_data, rx_data): (Sender<ProtoData>, Receiver<ProtoData>) = mpsc::channel();
        let (tx_ctrl, rx_ctrl): (Sender<ProtoCtrl>, Receiver<ProtoCtrl>) = mpsc::channel();
        self.exit_tx.push(tx_ctrl.clone());
        let db = self.db.clone();
        let table = self.table.clone();
        let name = self.name.clone();
        let _thr = thread::spawn(move || spanner_caller(db, table, name, rx_ctrl, tx_data));

        // Test Spanner query and get reply.
        tx_ctrl.send(ProtoCtrl::CurrentToken).unwrap();
        let reply = rx_data.recv().unwrap();
        match reply {
            ProtoData::CurrentToken(v) => {
                info!("reply for 'CurrentToken': v={:?}", v);
            }
        }

        let mut bo = BackoffBuilder::new().build();
        thread::sleep(Duration::from_nanos(bo.pause()));
    }

    pub fn close(&mut self) {
        self.exit_tx[0].send(ProtoCtrl::Exit).unwrap(); // exit
    }

    pub fn inc(&self) {
        let v = Arc::clone(&self.active);
        info!("atomic={}", v.fetch_add(1, Ordering::Relaxed));
        // info!("timeout={}", &self.timeout.unwrap_or(5000));
        info!("duration={}", self.duration);
    }
}

#[derive(Default)]
pub struct LockBuilder {
    db: String,
    table: String,
    name: String,
    id: String,
    duration: u64,
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

    pub fn duration(mut self, duration: u64) -> LockBuilder {
        self.duration = duration;
        self
    }

    pub fn build(self) -> Lock {
        Lock {
            db: self.db,
            table: self.table,
            name: self.name,
            id: self.id,
            duration: self.duration,
            active: Arc::new(AtomicUsize::new(0)),
            exit_tx: vec![],
        }
    }
}

pub struct MyStruct {}

impl MyStruct {
    pub fn hello(&self) -> Result<usize, io::Error> {
        Ok(100)
    }
}

pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }

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
