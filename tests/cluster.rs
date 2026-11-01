//! Three real kvlite processes running Raft over TCP.

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

struct Cluster {
    raft: BTreeMap<u64, u16>,
    client: BTreeMap<u64, u16>,
    procs: BTreeMap<u64, Child>,
    data: PathBuf,
}

impl Cluster {
    fn new(name: &str) -> Self {
        let data =
            std::env::temp_dir().join(format!("kvlite-cluster-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&data);
        let mut c = Cluster {
            raft: BTreeMap::new(),
            client: BTreeMap::new(),
            procs: BTreeMap::new(),
            data,
        };
        for id in 1..=3 {
            c.raft.insert(id, free_port());
            c.client.insert(id, free_port());
        }
        for id in 1..=3 {
            c.start(id);
        }
        c
    }

    fn list(ports: &BTreeMap<u64, u16>) -> String {
        ports
            .iter()
            .map(|(id, p)| format!("{id}=127.0.0.1:{p}"))
            .collect::<Vec<_>>()
            .join(",")
    }

    fn start(&mut self, id: u64) {
        let mut child = Command::new(env!("CARGO_BIN_EXE_kvlite"))
            .args(["--cluster", "--id", &id.to_string()])
            .args([
                "--peers",
                &Self::list(&self.raft),
                "--clients",
                &Self::list(&self.client),
            ])
            .arg("--data")
            .arg(&self.data)
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let mut line = String::new();
        BufReader::new(child.stdout.take().unwrap())
            .read_line(&mut line)
            .unwrap();
        assert!(line.contains("ready"), "node {id} failed to start: {line}");
        self.procs.insert(id, child);
    }

    fn kill(&mut self, id: u64) {
        let mut p = self.procs.remove(&id).unwrap();
        p.kill().unwrap(); // SIGKILL: no clean shutdown
        p.wait().unwrap();
    }

    fn send(&self, id: u64, cmd: &str) -> String {
        let mut s = TcpStream::connect(("127.0.0.1", self.client[&id])).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(8))).unwrap();
        writeln!(s, "{cmd}").unwrap();
        let mut reply = String::new();
        BufReader::new(s).read_line(&mut reply).unwrap();
        reply.trim_end().to_string()
    }

    /// Send a command the way a client would: follow redirects, retry while there's no leader.
    fn request(&self, cmd: &str) -> String {
        let deadline = Instant::now() + Duration::from_secs(15);
        let mut target = *self.procs.keys().next().unwrap();
        loop {
            let reply = self.send(target, cmd);
            if let Some(addr) = reply.strip_prefix("-MOVED ") {
                let port: u16 = addr.rsplit(':').next().unwrap().parse().unwrap();
                target = *self.client.iter().find(|(_, &p)| p == port).unwrap().0;
                if !self.procs.contains_key(&target) {
                    // Redirected to a node we killed: its replacement isn't known yet.
                    target = *self.procs.keys().next().unwrap();
                    thread::sleep(Duration::from_millis(100));
                }
                continue;
            }
            if reply.starts_with("-ERR no leader") || reply.starts_with("-ERR lost leadership") {
                assert!(Instant::now() < deadline, "no leader for 15s: {reply}");
                let alive: Vec<u64> = self.procs.keys().copied().collect();
                target =
                    alive[(alive.iter().position(|&a| a == target).unwrap_or(0) + 1) % alive.len()];
                thread::sleep(Duration::from_millis(100));
                continue;
            }
            return reply;
        }
    }

    fn leader(&self) -> u64 {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            for &id in self.procs.keys() {
                if self.send(id, "STATUS").starts_with("+leader") {
                    return id;
                }
            }
            assert!(Instant::now() < deadline, "no leader elected");
            thread::sleep(Duration::from_millis(100));
        }
    }

    fn field(&self, id: u64, name: &str) -> u64 {
        let status = self.send(id, "STATUS");
        status
            .split_whitespace()
            .find_map(|kv| kv.strip_prefix(&format!("{name}=")))
            .and_then(|v| v.parse().ok())
            .unwrap_or(0)
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for (_, mut p) in std::mem::take(&mut self.procs) {
            let _ = p.kill();
            let _ = p.wait();
        }
        let _ = std::fs::remove_dir_all(&self.data);
    }
}

#[test]
fn followers_redirect_to_the_leader() {
    let c = Cluster::new("redirect");
    let leader = c.leader();
    let follower = (1..=3).find(|&id| id != leader).unwrap();
    assert_eq!(
        c.send(follower, "SET a 1"),
        format!("-MOVED 127.0.0.1:{}", c.client[&leader])
    );
    assert_eq!(c.send(leader, "SET a 1"), "+OK");
    assert_eq!(c.send(leader, "GET a"), "$1");
}

#[test]
fn killing_the_leader_loses_nothing_and_a_new_leader_takes_over() {
    let mut c = Cluster::new("failover");
    for i in 0..20 {
        assert_eq!(c.request(&format!("SET key{i} value {i}")), "+OK");
    }
    let old = c.leader();
    c.kill(old);

    let new = c.leader();
    assert_ne!(new, old);
    for i in 0..20 {
        assert_eq!(
            c.request(&format!("GET key{i}")),
            format!("$value {i}"),
            "key{i} after failover"
        );
    }
    assert_eq!(c.request("SET after failover"), "+OK");

    // The old leader comes back as a follower and catches up.
    c.start(old);
    let target = c.field(new, "commit");
    let deadline = Instant::now() + Duration::from_secs(10);
    while c.field(old, "applied") < target {
        assert!(Instant::now() < deadline, "restarted node never caught up");
        thread::sleep(Duration::from_millis(100));
    }
    assert!(c.send(old, "STATUS").starts_with("+follower"));
}

#[test]
fn the_whole_cluster_survives_a_full_restart() {
    let mut c = Cluster::new("restart");
    assert_eq!(c.request("SET motto do your worst"), "+OK");
    assert_eq!(c.request("DEL motto"), "+OK");
    assert_eq!(c.request("SET city Bengaluru"), "+OK");
    for id in 1..=3 {
        c.kill(id);
    }
    for id in 1..=3 {
        c.start(id);
    }
    assert_eq!(c.request("GET city"), "$Bengaluru");
    assert_eq!(c.request("GET motto"), "(nil)");
}
