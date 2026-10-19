use std::net::TcpListener;
use std::process::ExitCode;
use std::sync::Arc;

use kvlite::server;
use kvlite::store::{Durability, Store};

const USAGE: &str = "usage: kvlite [--addr HOST:PORT] [--wal PATH] [--sync MODE]

  --addr  address to listen on  (default 127.0.0.1:6380)
  --wal   write-ahead log file   (default kvlite.wal)
  --sync  group   batch concurrent writes into shared fsyncs (default)
          each    fsync every write on its own (slow, for comparison)
          none    never fsync (fast; a power cut can lose recent writes)";

fn main() -> ExitCode {
    let mut addr = "127.0.0.1:6380".to_string();
    let mut wal = "kvlite.wal".to_string();
    let mut durability = Durability::Group;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--addr" => addr = args.next().unwrap_or_default(),
            "--wal" => wal = args.next().unwrap_or_default(),
            "--sync" => {
                durability = match args.next().as_deref() {
                    Some("group") => Durability::Group,
                    Some("each") => Durability::EachWrite,
                    Some("none") => Durability::None,
                    _ => {
                        eprintln!("--sync takes group, each or none\n\n{USAGE}");
                        return ExitCode::FAILURE;
                    }
                }
            }
            "-h" | "--help" => {
                println!("{USAGE}");
                return ExitCode::SUCCESS;
            }
            other => {
                eprintln!("unknown argument '{other}'\n\n{USAGE}");
                return ExitCode::FAILURE;
            }
        }
    }

    let store = match Store::open(&wal, durability) {
        Ok(s) => Arc::new(s),
        Err(e) => {
            eprintln!("could not open {wal}: {e}");
            return ExitCode::FAILURE;
        }
    };
    let listener = match TcpListener::bind(&addr) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("could not listen on {addr}: {e}");
            return ExitCode::FAILURE;
        }
    };

    // Tests read this line to find the port when started with --addr ...:0
    println!(
        "kvlite listening on {} ({} keys loaded from {wal})",
        listener.local_addr().map_or(addr, |a| a.to_string()),
        store.len()
    );

    match server::serve(listener, store) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("server error: {e}");
            ExitCode::FAILURE
        }
    }
}
