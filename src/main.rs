use spindle_rs::*;
use std::env;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

fn main() {
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
}
