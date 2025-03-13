use google_cloud_spanner::client::Client;
use google_cloud_spanner::client::ClientConfig;
use google_cloud_spanner::statement::Statement;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use std::{io, thread};
use tokio::runtime::Runtime;

pub fn get_client(rt: &Runtime, db: String) -> Client {
    let (tx, rx) = mpsc::channel();
    rt.block_on(async {
        let config = ClientConfig::default().with_auth().await.unwrap();
        let client = Client::new(db, config).await.unwrap();
        tx.send(client).unwrap();
    });

    rx.recv().unwrap()
}

struct LockVal {
    pub name: String,
    pub heartbeat: String,
    pub token: String,
    pub writer: String,
}

pub struct Lock {
    db: String,
    table: String,
    name: String,
    id: String,
    timeout: u64,
    active: Arc<AtomicUsize>,
    exit_tx: Vec<Sender<i32>>,
}

impl Lock {
    pub fn builder() -> LockBuilder {
        LockBuilder::default()
    }

    pub fn run(&mut self) {
        let (tx, rx): (Sender<i32>, Receiver<i32>) = mpsc::channel();
        self.exit_tx.push(tx.clone());
        let db = self.db.clone();
        let _thr = thread::spawn(move || {
            let rt = Runtime::new().unwrap();
            let client = get_client(&rt, db);
            for i in rx {
                match i {
                    0 => {
                        println!("exit");
                        rt.block_on(async {
                            client.close().await;
                        });
                        return;
                    }
                    1 => {
                        rt.block_on(async {
                            let mut stmt =
                                Statement::new("select mask from mask_id where name = @name");
                            stmt.add_param("name", &"000000009586");
                            let mut tx = client.single().await.unwrap();
                            let mut iter = tx.query(stmt).await.unwrap();
                            while let Some(row) = iter.next().await.unwrap() {
                                let m = row.column_by_name::<String>("mask");
                                println!("mask={:?}", m)
                            }
                        });
                    }
                    _ => {
                        println!("unsupported code: {}", i);
                    }
                }
            }
        });

        self.exit_tx[0].send(3).unwrap();
        tx.send(1).unwrap();
    }

    pub fn close(&mut self) {
        self.exit_tx[0].send(0).unwrap(); // exit
    }

    pub fn inc(&self) {
        let v = Arc::clone(&self.active);
        println!("atomic={}", v.fetch_add(1, Ordering::SeqCst));
        // println!("timeout={}", &self.timeout.unwrap_or(5000));
        println!("timeout={}", self.timeout);
    }
}

#[derive(Default)]
pub struct LockBuilder {
    db: String,
    table: String,
    name: String,
    id: String,
    timeout: u64,
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

    pub fn timeout(mut self, timeout: u64) -> LockBuilder {
        self.timeout = timeout;
        self
    }

    pub fn build(self) -> Lock {
        Lock {
            db: self.db,
            table: self.table,
            name: self.name,
            id: self.id,
            timeout: self.timeout,
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
