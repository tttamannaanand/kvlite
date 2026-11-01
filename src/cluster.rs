//! Running Raft for real: a kvlite node in a cluster.
//!
//! Each node has two ports: one for Raft traffic between nodes, one for
//! clients. The pure `RaftNode` is wrapped with:
//!   - a ticker thread (elections and heartbeats in wall-clock time),
//!   - per-peer sender threads that reconnect when a peer comes back,
//!   - durable storage that is written *before* any message is sent,
//!   - the client protocol, where followers answer `-MOVED <leader>`.
//!
//! Writes and reads both go through the Raft log, so every reply is
//! linearizable: the leader only answers once the command is committed.

use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread;
use std::time::{Duration, Instant};

use crate::codec::{self, Reader};
use crate::protocol::{self, Command};
use crate::raft::{
    Config, Entry, Envelope, HardState, Index, NodeId, Op, ProposeError, RaftNode, Role, Term,
};

#[derive(Debug, Clone)]
pub struct ClusterConfig {
    pub id: NodeId,
    /// Raft (node-to-node) address of every node, including this one.
    pub peers: BTreeMap<NodeId, SocketAddr>,
    /// Client address of every node, used for `-MOVED` redirects.
    pub clients: BTreeMap<NodeId, SocketAddr>,
    pub data_dir: PathBuf,
    pub tick: Duration,
}

// ------------------------------------------------------------ durable state

/// Persists Raft's hard state: term and vote in a small file replaced
/// atomically, and the log as an append-only file (rewritten on truncation).
struct Disk {
    dir: PathBuf,
    term: Term,
    voted_for: Option<NodeId>,
    log_terms: Vec<Term>, // terms of entries already on disk, to detect truncation
    log: BufWriter<File>,
}

impl Disk {
    fn open(dir: PathBuf) -> io::Result<(Disk, HardState)> {
        fs::create_dir_all(&dir)?;
        let (term, voted_for) = match fs::read(dir.join("state")) {
            Ok(b) if b.len() == 16 => {
                let mut r = Reader::new(&b);
                let (t, v) = (r.u64()?, r.u64()?);
                (t, if v == 0 { None } else { Some(v) })
            }
            _ => (0, None),
        };
        let mut log = Vec::new();
        if let Ok(b) = fs::read(dir.join("raft.log")) {
            let mut r = Reader::new(&b);
            // A torn final entry (crash mid-write) is simply dropped.
            while !r.done() {
                match codec::get_entry(&mut r) {
                    Ok(e) => log.push(e),
                    Err(_) => break,
                }
            }
        }
        let mut disk = Disk {
            log_terms: log.iter().map(|e| e.term).collect(),
            log: BufWriter::new(File::create(dir.join("raft.log.tmp"))?),
            dir,
            term,
            voted_for,
        };
        disk.rewrite_log(&log)?; // also discards any torn tail
        Ok((
            disk,
            HardState {
                term,
                voted_for,
                log,
            },
        ))
    }

    fn rewrite_log(&mut self, log: &[Entry]) -> io::Result<()> {
        let tmp = self.dir.join("raft.log.tmp");
        let mut w = BufWriter::new(File::create(&tmp)?);
        let mut buf = Vec::new();
        for e in log {
            buf.clear();
            codec::put_entry(&mut buf, e);
            w.write_all(&buf)?;
        }
        w.flush()?;
        w.get_ref().sync_all()?;
        fs::rename(&tmp, self.dir.join("raft.log"))?;
        self.log = BufWriter::new(
            OpenOptions::new()
                .append(true)
                .open(self.dir.join("raft.log"))?,
        );
        self.log_terms = log.iter().map(|e| e.term).collect();
        Ok(())
    }

    /// Make the on-disk state match `hs`, syncing before returning.
    fn save(&mut self, hs: &HardState) -> io::Result<()> {
        if hs.term != self.term || hs.voted_for != self.voted_for {
            let mut b = Vec::new();
            codec::put_u64(&mut b, hs.term);
            codec::put_u64(&mut b, hs.voted_for.unwrap_or(0));
            let tmp = self.dir.join("state.tmp");
            let mut f = File::create(&tmp)?;
            f.write_all(&b)?;
            f.sync_all()?;
            fs::rename(&tmp, self.dir.join("state"))?;
            self.term = hs.term;
            self.voted_for = hs.voted_for;
        }

        let on_disk = self.log_terms.len();
        let prefix_ok = hs.log.len() >= on_disk
            && hs.log[..on_disk]
                .iter()
                .zip(&self.log_terms)
                .all(|(e, &t)| e.term == t);
        if !prefix_ok {
            // The log was truncated by a new leader: rewrite it.
            return self.rewrite_log(&hs.log);
        }
        if hs.log.len() > on_disk {
            let mut buf = Vec::new();
            for e in &hs.log[on_disk..] {
                buf.clear();
                codec::put_entry(&mut buf, e);
                self.log.write_all(&buf)?;
                self.log_terms.push(e.term);
            }
            self.log.flush()?;
            self.log.get_ref().sync_data()?;
        }
        Ok(())
    }
}

// ------------------------------------------------------------ the node

struct State {
    raft: RaftNode,
    disk: Disk,
    data: HashMap<String, String>,
    /// Results of applied entries that a client may be waiting for: index -> (term, read value).
    results: HashMap<Index, (Term, Option<String>)>,
    applied: Index,
}

pub struct Node {
    cfg: ClusterConfig,
    state: Mutex<State>,
    applied: Condvar,
    outboxes: BTreeMap<NodeId, Sender<Vec<u8>>>,
}

impl Node {
    pub fn start(cfg: ClusterConfig) -> io::Result<Arc<Node>> {
        let (disk, hs) = Disk::open(cfg.data_dir.join(format!("node{}", cfg.id)))?;
        let ids: Vec<NodeId> = cfg.peers.keys().copied().collect();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos() as u64);
        let seed = cfg.id ^ nanos ^ u64::from(std::process::id());
        let raft = RaftNode::restore(cfg.id, &ids, Config::default(), seed, hs);

        let mut outboxes = BTreeMap::new();
        for (&peer, &addr) in &cfg.peers {
            if peer == cfg.id {
                continue;
            }
            let (tx, rx) = mpsc::channel::<Vec<u8>>();
            outboxes.insert(peer, tx);
            thread::spawn(move || sender(addr, rx));
        }

        let node = Arc::new(Node {
            state: Mutex::new(State {
                raft,
                disk,
                data: HashMap::new(),
                results: HashMap::new(),
                applied: 0,
            }),
            applied: Condvar::new(),
            outboxes,
            cfg,
        });

        let listener = TcpListener::bind(node.cfg.peers[&node.cfg.id])?;
        let n = Arc::clone(&node);
        thread::spawn(move || {
            for conn in listener.incoming().flatten() {
                let n = Arc::clone(&n);
                thread::spawn(move || n.receive(conn));
            }
        });

        let n = Arc::clone(&node);
        thread::spawn(move || loop {
            thread::sleep(n.cfg.tick);
            let mut s = n.lock();
            let out = s.raft.tick();
            n.after_step(&mut s, out);
        });
        Ok(node)
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Persist, apply newly committed entries, then send. Persisting first is
    /// what makes votes and log promises survive a crash.
    fn after_step(&self, s: &mut State, out: Vec<Envelope>) {
        if let Err(e) = s.disk.save(&s.raft.hard_state()) {
            eprintln!("node {}: cannot persist raft state: {e}", self.cfg.id);
            std::process::exit(1); // a node that can't keep its promises must stop
        }
        let committed = s.raft.take_committed();
        if !committed.is_empty() {
            for (index, entry) in committed {
                let value = match entry.op {
                    Op::Set(k, v) => {
                        s.data.insert(k, v);
                        None
                    }
                    Op::Del(k) => {
                        s.data.remove(&k);
                        None
                    }
                    Op::Get(k) => s.data.get(&k).cloned(),
                    Op::Noop => None,
                };
                s.results.insert(index, (entry.term, value));
                s.applied = index;
            }
            // Keep only recent results; old ones have no waiters left.
            if s.results.len() > 10_000 {
                let cutoff = s.applied.saturating_sub(5_000);
                s.results.retain(|&i, _| i > cutoff);
            }
            self.applied.notify_all();
        }
        for env in out {
            if let Some(tx) = self.outboxes.get(&env.to) {
                let _ = tx.send(codec::encode(&env));
            }
        }
    }

    fn receive(&self, conn: TcpStream) {
        let mut r = BufReader::new(conn);
        while let Ok(frame) = codec::read_frame(&mut r) {
            let Ok(env) = codec::decode(&frame) else {
                return;
            };
            if env.to != self.cfg.id {
                continue;
            }
            let mut s = self.lock();
            let out = s.raft.step(env);
            self.after_step(&mut s, out);
        }
    }

    /// Propose a command and wait until it's committed and applied here.
    fn submit(&self, op: Op) -> Result<Option<String>, String> {
        let mut s = self.lock();
        let (index, term) = match s.raft.propose(op) {
            Ok(it) => it,
            Err(ProposeError::NotLeader(hint)) => {
                return Err(match hint.and_then(|l| self.cfg.clients.get(&l)) {
                    Some(addr) => format!("-MOVED {addr}"),
                    None => "-ERR no leader yet, retry".into(),
                });
            }
        };
        let out = s.raft.flush();
        self.after_step(&mut s, out);

        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some((t, value)) = s.results.get(&index) {
                return if *t == term {
                    Ok(value.clone())
                } else {
                    Err("-ERR lost leadership before commit, retry".into())
                };
            }
            let now = Instant::now();
            if now >= deadline {
                return Err("-ERR timed out waiting for commit (outcome unknown)".into());
            }
            s = self
                .applied
                .wait_timeout(s, deadline - now)
                .unwrap_or_else(|e| e.into_inner())
                .0;
        }
    }

    pub fn status(&self) -> String {
        let s = self.lock();
        let role = match s.raft.role() {
            Role::Leader => "leader",
            Role::Candidate => "candidate",
            Role::Follower => "follower",
        };
        format!(
            "+{role} term={} leader={} commit={} applied={} last={}",
            s.raft.term(),
            s.raft.leader().map_or("none".into(), |l| l.to_string()),
            s.raft.commit_index(),
            s.applied,
            s.raft.last_index()
        )
    }

    /// Serve the text protocol on this node's client address.
    pub fn serve_clients(self: &Arc<Self>) -> io::Result<()> {
        let listener = TcpListener::bind(self.cfg.clients[&self.cfg.id])?;
        for conn in listener.incoming().flatten() {
            let n = Arc::clone(self);
            thread::spawn(move || {
                let _ = n.client(conn);
            });
        }
        Ok(())
    }

    fn client(&self, stream: TcpStream) -> io::Result<()> {
        let mut reader = BufReader::new(stream.try_clone()?);
        let mut writer = BufWriter::new(stream);
        let mut line = String::new();
        loop {
            line.clear();
            if reader.read_line(&mut line)? == 0 {
                return Ok(());
            }
            if line.trim().eq_ignore_ascii_case("STATUS") {
                writeln!(writer, "{}", self.status())?;
                writer.flush()?;
                continue;
            }
            let reply = match protocol::parse(&line) {
                Ok(Command::Ping) => "+PONG".to_string(),
                Ok(Command::Quit) => {
                    writeln!(writer, "+BYE")?;
                    return writer.flush();
                }
                Ok(Command::Set(k, v)) => self
                    .submit(Op::Set(k, v))
                    .map_or_else(|e| e, |_| "+OK".into()),
                Ok(Command::Del(k)) => self.submit(Op::Del(k)).map_or_else(|e| e, |_| "+OK".into()),
                Ok(Command::Get(k)) => self
                    .submit(Op::Get(k))
                    .map_or_else(|e| e, |v| v.map_or("(nil)".into(), |v| format!("${v}"))),
                Ok(Command::Len) | Ok(Command::Compact) => {
                    "-ERR not supported in cluster mode".into()
                }
                Err(msg) => format!("-ERR {msg}"),
            };
            writeln!(writer, "{reply}")?;
            writer.flush()?;
        }
    }
}

/// Keeps one connection to a peer, reconnecting when it drops. Messages that
/// can't be delivered are discarded: Raft retries on its own.
fn sender(addr: SocketAddr, rx: mpsc::Receiver<Vec<u8>>) {
    let mut conn: Option<BufWriter<TcpStream>> = None;
    for frame in rx {
        if conn.is_none() {
            conn = TcpStream::connect_timeout(&addr, Duration::from_millis(200))
                .ok()
                .map(|s| {
                    let _ = s.set_nodelay(true);
                    BufWriter::new(s)
                });
        }
        let Some(w) = conn.as_mut() else { continue };
        if codec::write_frame(w, &frame)
            .and_then(|()| w.flush())
            .is_err()
        {
            conn = None;
        }
    }
}

/// Parse "1=127.0.0.1:7001,2=127.0.0.1:7002".
pub fn parse_addrs(s: &str) -> Result<BTreeMap<NodeId, SocketAddr>, String> {
    s.split(',')
        .map(|part| {
            let (id, addr) = part
                .split_once('=')
                .ok_or_else(|| format!("expected id=host:port, got '{part}'"))?;
            let id: NodeId = id
                .trim()
                .parse()
                .map_err(|_| format!("bad node id '{id}'"))?;
            let addr: SocketAddr = addr
                .trim()
                .parse()
                .map_err(|_| format!("bad address '{addr}'"))?;
            Ok((id, addr))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!("kvlite-disk-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&p);
        p
    }

    fn e(term: Term, k: &str) -> Entry {
        Entry {
            term,
            op: Op::Set(k.into(), "v".into()),
        }
    }

    #[test]
    fn hard_state_survives_reopen() {
        let dir = tmp("reopen");
        let (mut disk, hs) = Disk::open(dir.clone()).unwrap();
        assert_eq!(
            hs,
            HardState {
                term: 0,
                voted_for: None,
                log: vec![]
            }
        );
        let want = HardState {
            term: 4,
            voted_for: Some(2),
            log: vec![e(1, "a"), e(4, "b")],
        };
        disk.save(&want).unwrap();
        drop(disk);
        assert_eq!(Disk::open(dir).unwrap().1, want);
    }

    #[test]
    fn truncation_by_a_new_leader_is_persisted() {
        let dir = tmp("truncate");
        let (mut disk, _) = Disk::open(dir.clone()).unwrap();
        disk.save(&HardState {
            term: 2,
            voted_for: None,
            log: vec![e(1, "a"), e(2, "b"), e(2, "c")],
        })
        .unwrap();
        let after = HardState {
            term: 3,
            voted_for: None,
            log: vec![e(1, "a"), e(3, "x")],
        };
        disk.save(&after).unwrap();
        drop(disk);
        assert_eq!(Disk::open(dir).unwrap().1, after);
    }

    #[test]
    fn a_torn_final_entry_is_dropped() {
        let dir = tmp("torn");
        let (mut disk, _) = Disk::open(dir.clone()).unwrap();
        disk.save(&HardState {
            term: 1,
            voted_for: None,
            log: vec![e(1, "a"), e(1, "b")],
        })
        .unwrap();
        drop(disk);
        let path = dir.join("raft.log");
        let len = fs::metadata(&path).unwrap().len();
        OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(len - 2)
            .unwrap();
        assert_eq!(Disk::open(dir).unwrap().1.log, vec![e(1, "a")]);
    }

    #[test]
    fn parses_peer_lists() {
        let m = parse_addrs("1=127.0.0.1:7001, 2=127.0.0.1:7002").unwrap();
        assert_eq!(m.len(), 2);
        assert!(parse_addrs("1:127.0.0.1").is_err());
    }
}
