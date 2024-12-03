use std::io;

pub struct Lock {
    name: String,
}

impl Lock {
    pub fn new(n: String) -> Self {
        Self { name: n }
    }

    pub fn hello(&self) {
        println!("{}", &self.name);
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
