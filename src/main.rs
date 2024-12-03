use spindle_rs::*;
use std::env;
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

    let handle = thread::spawn(|| loop {
        println!("from the spawned thread!");
        thread::sleep(Duration::from_millis(2000));
    });

    handle.join().unwrap();
}
