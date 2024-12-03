use spindle_rs::*;

fn main() {
    let result = add(1, 2);
    let ms = MyStruct {};
    println!("Hello, world! {}, {}", result, ms.hello().unwrap());
    println!("{}", if true { 6 } else { 7 });
}
