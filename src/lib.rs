use google_cloud_rust_raw::spanner::v1::{
    mutation::{Mutation, Mutation_Write},
    spanner::{
        BeginTransactionRequest, CommitRequest, CreateSessionRequest, ExecuteSqlRequest, Session,
    },
    spanner_grpc::SpannerClient,
    transaction::{TransactionOptions, TransactionOptions_ReadWrite},
};
use grpcio::{CallOption, ChannelBuilder, ChannelCredentials, EnvBuilder, MetadataBuilder};
use protobuf::well_known_types::{ListValue, Value, Value_oneof_kind};
use protobuf::RepeatedField;
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::runtime::Runtime;
use google_cloud_spanner::client::Client;
use google_cloud_spanner::client::ClientConfig;

struct LockVal {
    pub name: String,
    pub heartbeat: String,
    pub token: String,
    pub writer: String,
}

pub struct Lock {
    table: String,
    name: String,
    id: String,
    client: SpannerClient,
    session: Session,
    active: Arc<AtomicUsize>,
    timeout: u64,
    // spclient: Client,
}

impl Lock {
    pub fn builder() -> LockBuilder {
        LockBuilder::default()
    }

    pub fn query(&self) {
        println!("table={}, name={}, id={}", self.table, self.name, self.id);
        let mut req = ExecuteSqlRequest::new();
        req.session = self.session.get_name().to_string();
        req.sql = "select * from alerts".to_string();
        let out = self.client.execute_sql(&req).unwrap();
        for i in out.get_rows() {
            let v0 = i.get_values().get(0).unwrap();
            if let Some(Value_oneof_kind::string_value(v)) = &v0.kind {
                println!("val={}", v)
            }
        }
    }

    pub fn dml(&self) {
        let mut request = BeginTransactionRequest::new();
        let mut read_write = TransactionOptions::new();
        read_write.set_read_write(TransactionOptions_ReadWrite::new());
        request.set_session(self.session.get_name().to_string());
        request.set_options(read_write);
        let transaction = self.client.begin_transaction(&request).unwrap();

        let columns = vec![
            "name".to_string(),
            "heartbeat".to_string(),
            "token".to_string(),
            "writer".to_string(),
        ];

        let vals = vec![LockVal {
            name: "spindle-rs".to_string(),
            heartbeat: "PENDING_COMMIT_TIMESTAMP()".to_string(),
            token: "PENDING_COMMIT_TIMESTAMP()".to_string(),
            writer: ":8080".to_string(),
        }];

        // collect all values
        let mut list_values = Vec::new();
        for val in vals {
            let mut name = Value::new();
            name.set_string_value(val.name.to_string());
            let mut heartbeat = Value::new();
            heartbeat.set_string_value(val.heartbeat.clone());
            let mut token = Value::new();
            token.set_string_value(val.token.clone());
            let mut writer = Value::new();
            writer.set_string_value(val.writer.clone());

            let mut list = ListValue::new();
            list.set_values(RepeatedField::from_vec(vec![
                name, heartbeat, token, writer,
            ]));
            list_values.push(list);
        }

        // create a suitable mutation with all values
        println!("Preparing write mutation to add singers");
        let mut mutation_write = Mutation_Write::new();
        mutation_write.set_table("testlease".to_string());
        mutation_write.set_columns(RepeatedField::from_vec(columns));
        mutation_write.set_values(RepeatedField::from_vec(list_values));
        println!("Mutation write object");
        dbg!(mutation_write.clone());

        // finally commit to database
        println!("Commit data to database");
        let mut commit = CommitRequest::new();
        commit.set_transaction_id(transaction.get_id().to_vec());
        commit.set_session(self.session.get_name().to_string());
        let mut mutation = Mutation::new();
        mutation.set_insert_or_update(mutation_write);
        commit.set_mutations(RepeatedField::from_vec(vec![mutation]));
        let response = self.client.commit(&commit).unwrap();
        dbg!(response);
    }

    pub fn inc(&self) {
        let v = Arc::clone(&self.active);
        println!("atomic={}", v.fetch_add(1, Ordering::SeqCst));
        // println!("timeout={}", &self.timeout.unwrap_or(5000));
        println!("timeout={}", self.timeout);
    }

    async fn async_fn(&self, input: String){
        println!("this is an async fn!, input={}", input);
    }

    pub fn call_async(&self) {
        println!("start: call async from sync");
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            self.async_fn("hello-world".to_string()).await;
        });

        println!("end: call async from sync");
    }
}

async fn spclient() -> Client {
    const DB: &str = "projects/mobingi-main/instances/alphaus-prod/databases/main";
    let config = ClientConfig::default().with_auth().await.unwrap();
    let client = Client::new(DB, config).await.unwrap();
    client
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
        let env = Arc::new(EnvBuilder::new().build());
        let creds = ChannelCredentials::google_default_credentials().unwrap();
        let chan = ChannelBuilder::new(env.clone())
            .max_send_message_len(100 << 20)
            .max_receive_message_len(100 << 20)
            .set_credentials(creds)
            .connect("spanner.googleapis.com:443");
        let client = SpannerClient::new(chan);
        let mut req = CreateSessionRequest::new();
        req.database = self.db.to_string();
        let mut meta = MetadataBuilder::new();
        meta.add_str("google-cloud-resource-prefix", self.db.as_str())
            .unwrap();
        meta.add_str("x-goog-api-client", "googleapis-rs").unwrap();
        let opt = CallOption::default().headers(meta.build());
        let session = client.create_session_opt(&req, opt).unwrap();

        Lock {
            table: self.table,
            name: self.name,
            id: self.id,
            client,
            session,
            active: Arc::new(AtomicUsize::new(0)),
            timeout: self.timeout,
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
