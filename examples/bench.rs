//! Rough throughput check: N clients each send SETs one at a time and wait
//! for the reply. Start a server first, e.g. `cargo run --release`.
//!
//! usage: cargo run --release --example bench -- [ADDR] [CLIENTS] [OPS_PER_CLIENT]

use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::thread;
use std::time::Instant;

fn main() {
    let mut args = std::env::args().skip(1);
    let addr = args.next().unwrap_or_else(|| "127.0.0.1:6380".into());
    let clients: usize = args.next().map_or(8, |s| s.parse().unwrap());
    let ops: usize = args.next().map_or(5000, |s| s.parse().unwrap());

    let start = Instant::now();
    let workers: Vec<_> = (0..clients)
        .map(|c| {
            let addr = addr.clone();
            thread::spawn(move || {
                let mut stream = TcpStream::connect(&addr).unwrap();
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let mut reply = String::new();
                for i in 0..ops {
                    writeln!(stream, "SET bench:{c}:{i} value-{i}").unwrap();
                    reply.clear();
                    reader.read_line(&mut reply).unwrap();
                    assert_eq!(reply.trim_end(), "+OK");
                }
            })
        })
        .collect();
    for w in workers {
        w.join().unwrap();
    }
    let secs = start.elapsed().as_secs_f64();
    let total = clients * ops;
    println!(
        "{total} SETs from {clients} clients in {secs:.2}s = {:.0} ops/sec",
        total as f64 / secs
    );
}
