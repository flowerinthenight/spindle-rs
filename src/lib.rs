use google_cloud_rust_raw::spanner::v1::{
    spanner::{CreateSessionRequest, ExecuteSqlRequest, Session},
    spanner_grpc::SpannerClient,
};
use grpcio::{CallOption, ChannelBuilder, ChannelCredentials, EnvBuilder, MetadataBuilder};
use protobuf::well_known_types::Value_oneof_kind;
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

pub struct Lock {
    name: String,
    client: SpannerClient,
    session: Session,
    active: Arc<AtomicUsize>,
    timeout: Option<u64>,
}

impl Lock {
    pub fn new(name: String, db: String) -> Self {
        let env = Arc::new(EnvBuilder::new().build());
        let creds = ChannelCredentials::google_default_credentials().unwrap();
        let chan = ChannelBuilder::new(env.clone())
            .max_send_message_len(100 << 20)
            .max_receive_message_len(100 << 20)
            .set_credentials(creds)
            .connect("spanner.googleapis.com:443");
        let client = SpannerClient::new(chan);
        let mut req = CreateSessionRequest::new();
        req.database = db.to_string();
        let mut meta = MetadataBuilder::new();
        meta.add_str("google-cloud-resource-prefix", db.as_str())
            .unwrap();
        meta.add_str("x-goog-api-client", "googleapis-rs").unwrap();
        let opt = CallOption::default().headers(meta.build());
        let session = client.create_session_opt(&req, opt).unwrap();

        Self {
            name,
            client,
            session,
            active: Arc::new(AtomicUsize::new(0)),
            timeout: Some(2000),
        }
    }

    pub fn query(&self) {
        println!("name={}", self.name);
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

    pub fn inc(&self) {
        let v = Arc::clone(&self.active);
        println!("atomic={}", v.fetch_add(1, Ordering::SeqCst));
        println!("timeout={}", &self.timeout.unwrap_or(5000));
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
}
