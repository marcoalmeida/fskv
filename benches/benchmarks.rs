#![feature(test)]

extern crate test;

#[cfg(test)]
mod benches {
    use fskv::Store;
    use test::Bencher;

    #[bench]
    fn bench_put(b: &mut Bencher) {
        match Store::new("fskv_test", true) {
            Ok(ds) => b.iter(|| {
                for i in 0..1000 {
                    match ds.put(&i.to_string(), "b") {
                        Ok(_) => (),
                        Err(e) => print!("ooopsie: {}", e),
                    };
                }
            }),
            Err(e) => print!("{:?}", e),
        }
    }
}
