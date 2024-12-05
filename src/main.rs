use spindle_rs::*;
use std::env;
use std::error::Error;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    println!("args[0]={:?}", args.get(1));

    let lock = Lock::new(
        "spindle".to_string(),
        "projects/mobingi-main/instances/alphaus-prod/databases/main".to_string(),
    );

    lock.query();
    lock.query();
    lock.inc();
    lock.inc();

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
            thread::sleep(Duration::from_millis(100));
        }
    });

    for received in rx {
        println!("Got: {received}");
    }

    thr.join().unwrap();

    Ok(())
}
