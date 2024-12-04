use google_cloud_rust_raw::spanner::v1::{
    spanner::{CreateSessionRequest, ExecuteSqlRequest},
    spanner_grpc::SpannerClient,
};
use grpcio::{CallOption, ChannelBuilder, ChannelCredentials, EnvBuilder, MetadataBuilder};
use spindle_rs::*;
use std::env;
use std::error::Error;
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    dbg!(&args);

    let a0 = args.get(1);
    println!("0 = {:?}", a0);

    let lock = Lock::new("spindle".to_string());
    lock.hello();

    let result = add(1, 2);
    let ms = MyStruct {};
    println!("Hello, world! {}, {}", result, ms.hello().unwrap());
    println!("{}", if true { 6 } else { 7 });

    // let thr = thread::spawn(|| loop {
    //     println!("from the spawned thread!");
    //     thread::sleep(Duration::from_millis(2000));
    // });

    // thr.join().unwrap();

    let (tx, rx) = mpsc::channel();

    let thr = thread::spawn(move || {
        let vals = vec![
            String::from("hi"),
            String::from("from"),
            String::from("the"),
            String::from("child"),
            String::from("thread"),
        ];

        for val in vals {
            tx.send(val).unwrap();
            thread::sleep(Duration::from_secs(1));
        }
    });

    for received in rx {
        println!("Got: {received}");
    }

    thr.join().unwrap();

    let db = "projects/mobingi-main/instances/alphaus-prod/databases/main";
    let endpoint = "spanner.googleapis.com:443";
    let env = Arc::new(EnvBuilder::new().build());
    let creds = ChannelCredentials::google_default_credentials().unwrap();

    // Create a Spanner client.
    let chan = ChannelBuilder::new(env.clone())
        .max_send_message_len(100 << 20)
        .max_receive_message_len(100 << 20)
        .set_credentials(creds)
        .connect(&endpoint);
    let client = SpannerClient::new(chan);

    // Connect to the instance and create a Spanner session.
    let mut req = CreateSessionRequest::new();
    req.database = db.to_string();
    let mut meta = MetadataBuilder::new();
    meta.add_str("google-cloud-resource-prefix", db).unwrap();
    meta.add_str("x-goog-api-client", "googleapis-rs").unwrap();
    let opt = CallOption::default().headers(meta.build());
    let session = client.create_session_opt(&req, opt).unwrap();

    // Prepare a SQL command to execute.
    let mut req = ExecuteSqlRequest::new();
    req.session = session.get_name().to_string();
    req.sql = "select * from alerts".to_string();
    let out = client.execute_sql(&req).unwrap();
    dbg!(out);

    Ok(())
}
