use std::net::TcpListener;
use std::process::ExitCode;
use std::sync::Arc;

use kvlite::server;
use kvlite::store::{Durability, Store};

const USAGE: &str = "usage: kvlite [--addr HOST:PORT] [--wal PATH] [--sync MODE]
       kvlite --cluster --id N --peers 1=H:P,2=H:P,3=H:P --clients 1=H:P,2=H:P,3=H:P [--data DIR]

  --addr  address to listen on  (default 127.0.0.1:6380)
  --wal   write-ahead log file   (default kvlite.wal)
  --sync  group   batch concurrent writes into shared fsyncs (default)
          each    fsync every write on its own (slow, for comparison)
          none    never fsync (fast; a power cut can lose recent writes)

cluster mode (Raft replication):
  --id       this node's id
  --peers    Raft address of every node, including this one
  --clients  client address of every node (used for -MOVED redirects)
  --data     directory for Raft state (default kvlite-data)";

fn main() -> ExitCode {
    let mut addr = "127.0.0.1:6380".to_string();
    let mut wal = "kvlite.wal".to_string();
    let mut durability = Durability::Group;
    let (mut cluster, mut id, mut peers, mut clients, mut data) = (
        false,
        0u64,
        String::new(),
        String::new(),
        "kvlite-data".to_string(),
    );

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
            "--cluster" => cluster = true,
            "--id" => id = args.next().and_then(|v| v.parse().ok()).unwrap_or(0),
            "--peers" => peers = args.next().unwrap_or_default(),
            "--clients" => clients = args.next().unwrap_or_default(),
            "--data" => data = args.next().unwrap_or_default(),
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

    if cluster {
        return run_cluster(id, &peers, &clients, &data);
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

fn run_cluster(id: u64, peers: &str, clients: &str, data: &str) -> ExitCode {
    let parsed = kvlite::cluster::parse_addrs(peers)
        .and_then(|p| Ok((p, kvlite::cluster::parse_addrs(clients)?)));
    let (peers, clients) = match parsed {
        Ok(v) => v,
        Err(e) => {
            eprintln!("{e}\n\n{USAGE}");
            return ExitCode::FAILURE;
        }
    };
    if !peers.contains_key(&id) || !clients.contains_key(&id) {
        eprintln!("--id {id} must appear in both --peers and --clients\n\n{USAGE}");
        return ExitCode::FAILURE;
    }
    let cfg = kvlite::cluster::ClusterConfig {
        id,
        peers,
        clients: clients.clone(),
        data_dir: data.into(),
        tick: std::time::Duration::from_millis(50),
    };
    let node = match kvlite::cluster::Node::start(cfg) {
        Ok(n) => n,
        Err(e) => {
            eprintln!("could not start node {id}: {e}");
            return ExitCode::FAILURE;
        }
    };
    // Tests read this line to know the node is up.
    println!("kvlite node {id} ready, clients on {}", clients[&id]);
    if let Err(e) = node.serve_clients() {
        eprintln!("client server error: {e}");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}
