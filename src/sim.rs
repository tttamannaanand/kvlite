//! A deterministic simulator for the Raft cluster: a Jepsen-style fault
//! injector that runs entirely in one thread from a seed.
//!
//! The network delays, drops, duplicates and reorders messages; partitions
//! split the cluster; nodes crash and restart from their persisted state.
//! Clients send reads and writes, follow leader redirects and give up on
//! slow requests. Every run is reproducible from its seed.
//!
//! Checked continuously:
//!   - election safety: at most one leader per term
//!   - state machine safety: no two nodes apply different entries at an index
//!
//! Checked at the end:
//!   - linearizability of the clients' history
//!   - liveness: once faults heal, the cluster makes progress again

use std::collections::{BTreeMap, HashMap};

use crate::linear::{self, Kind, Operation, Verdict};
use crate::raft::{
    Bug, Config, Entry, Envelope, Index, NodeId, Op, ProposeError, RaftNode, Role, Term,
};

#[derive(Debug, Clone)]
pub struct SimConfig {
    pub nodes: u64,
    pub ticks: u64,
    pub clients: usize,
    pub keys: usize,
    pub drop: f64,
    pub duplicate: f64,
    pub max_delay: u64,
    /// Change the partition layout this often (None = never partition).
    pub partition_every: Option<u64>,
    /// Chance per tick that a given live node crashes.
    pub crash_rate: f64,
    /// Entries per replication message. 1 exposes races that batching hides.
    pub max_batch: usize,
    pub bug: Option<Bug>,
}

impl Default for SimConfig {
    fn default() -> Self {
        SimConfig {
            nodes: 3,
            ticks: 1500,
            clients: 4,
            keys: 3,
            drop: 0.05,
            duplicate: 0.03,
            max_delay: 4,
            partition_every: Some(120),
            crash_rate: 0.002,
            max_batch: 64,
            bug: None,
        }
    }
}

#[derive(Debug)]
pub struct Report {
    pub seed: u64,
    pub ops_ok: usize,
    pub ops_unknown: usize,
    pub leaders: usize,
    pub crashes: usize,
    pub partitions: usize,
    pub max_term: Term,
    /// The first safety or liveness violation, if any.
    pub violation: Option<String>,
    pub history: Vec<Operation>,
}

impl Report {
    pub fn ok(&self) -> bool {
        self.violation.is_none()
    }
}

// ------------------------------------------------------------ plumbing

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
    fn chance(&mut self, p: f64) -> bool {
        (self.next() % 1_000_000) as f64 / 1_000_000.0 < p
    }
}

struct Node {
    raft: Option<RaftNode>, // None while crashed
    /// What survives a crash: written synchronously after every step.
    disk: crate::raft::HardState,
    data: HashMap<String, String>,
    restart_at: u64,
}

struct InFlight {
    op: Op,
    call: u64,
    /// Set once a leader accepted the proposal.
    proposed: Option<(NodeId, Index, Term)>,
    target: NodeId,
    deadline: u64,
}

struct Client {
    leader_hint: Option<NodeId>,
    current: Option<InFlight>,
    next_at: u64,
    counter: u64,
}

struct World {
    cfg: SimConfig,
    raft_cfg: Config,
    rng: Rng,
    ids: Vec<NodeId>,
    nodes: BTreeMap<NodeId, Node>,
    net: Vec<(u64, Envelope)>,   // (deliver at tick, message)
    group: HashMap<NodeId, u32>, // partition group per node
    clients: Vec<Client>,
    clock: u64,  // ticks
    events: u64, // strictly increasing event time for the history
    leaders_by_term: HashMap<Term, NodeId>,
    applied: HashMap<Index, Entry>,
    history: Vec<Operation>,
    violation: Option<String>,
    crashes: usize,
    partitions: usize,
    completed_in_final_phase: usize,
    final_phase: bool,
}

fn key_of(op: &Op) -> String {
    match op {
        Op::Set(k, _) | Op::Del(k) | Op::Get(k) => k.clone(),
        Op::Noop => String::new(),
    }
}

impl World {
    fn now(&mut self) -> u64 {
        self.events += 1;
        self.events
    }

    fn connected(&self, a: NodeId, b: NodeId) -> bool {
        self.group.get(&a) == self.group.get(&b)
    }

    fn send(&mut self, out: Vec<Envelope>) {
        for env in out {
            if self.rng.chance(self.cfg.drop) {
                continue;
            }
            let at = self.clock + 1 + self.rng.below(self.cfg.max_delay);
            if self.rng.chance(self.cfg.duplicate) {
                let again = self.clock + 1 + self.rng.below(self.cfg.max_delay * 2);
                self.net.push((again, env.clone()));
            }
            self.net.push((at, env));
        }
    }

    /// After a node did anything: persist it, check safety, apply commits.
    fn after_step(&mut self, id: NodeId) {
        let node = self.nodes.get_mut(&id).unwrap();
        let Some(raft) = node.raft.as_mut() else {
            return;
        };
        node.disk = raft.hard_state();

        if raft.role() == Role::Leader {
            let term = raft.term();
            match self.leaders_by_term.get(&term) {
                Some(&other) if other != id && self.violation.is_none() => {
                    self.violation = Some(format!(
                        "election safety: nodes {other} and {id} both led term {term}"
                    ));
                }
                None => {
                    self.leaders_by_term.insert(term, id);
                }
                _ => {}
            }
        }

        let committed = raft.take_committed();
        for (index, entry) in committed {
            match self.applied.get(&index) {
                Some(prev) if *prev != entry && self.violation.is_none() => {
                    self.violation = Some(format!(
                        "state machine safety: index {index} applied as {:?} and as {:?} (node {id})",
                        prev, entry
                    ));
                }
                None => {
                    self.applied.insert(index, entry.clone());
                }
                _ => {}
            }
            let data = &mut self.nodes.get_mut(&id).unwrap().data;
            let result = match &entry.op {
                Op::Set(k, v) => {
                    data.insert(k.clone(), v.clone());
                    None
                }
                Op::Del(k) => {
                    data.remove(k);
                    None
                }
                Op::Get(k) => data.get(k).cloned(),
                Op::Noop => None,
            };
            self.complete_clients(id, index, entry.term, result);
        }
    }

    /// Tell the client whose request just applied at this node.
    fn complete_clients(&mut self, node: NodeId, index: Index, term: Term, result: Option<String>) {
        for c in 0..self.clients.len() {
            let Some(req) = &self.clients[c].current else {
                continue;
            };
            let Some((n, i, t)) = req.proposed else {
                continue;
            };
            if n != node || i != index {
                continue;
            }
            let req = self.clients[c].current.take().unwrap();
            if t != term {
                // Our entry was replaced before it committed: it never happened.
                self.clients[c].next_at = self.clock + 1;
                continue;
            }
            let ret = self.now();
            let kind = match &req.op {
                Op::Set(_, v) => Kind::Write(Some(v.clone())),
                Op::Del(_) => Kind::Write(None),
                Op::Get(_) => Kind::Read(result.clone()),
                Op::Noop => unreachable!(),
            };
            self.history.push(Operation {
                key: key_of(&req.op),
                kind,
                call: req.call,
                ret: Some(ret),
            });
            self.clients[c].next_at = self.clock + 1 + self.rng.below(5);
            if self.final_phase {
                self.completed_in_final_phase += 1;
            }
        }
    }

    fn step_clients(&mut self) {
        for c in 0..self.clients.len() {
            // Give up on slow requests: the outcome is unknown.
            if let Some(req) = &self.clients[c].current {
                if self.clock >= req.deadline {
                    let req = self.clients[c].current.take().unwrap();
                    if req.proposed.is_some() && !matches!(req.op, Op::Get(_)) {
                        let kind = match &req.op {
                            Op::Set(_, v) => Kind::Write(Some(v.clone())),
                            _ => Kind::Write(None),
                        };
                        self.history.push(Operation {
                            key: key_of(&req.op),
                            kind,
                            call: req.call,
                            ret: None,
                        });
                    }
                    self.clients[c].leader_hint = None;
                    self.clients[c].next_at = self.clock + 1;
                }
            }

            // Start a new request.
            if self.clients[c].current.is_none() && self.clock >= self.clients[c].next_at {
                let key = format!("k{}", self.rng.below(self.cfg.keys as u64));
                self.clients[c].counter += 1;
                let roll = self.rng.below(10);
                let op = if roll < 5 {
                    Op::Set(key, format!("c{c}-{}", self.clients[c].counter))
                } else if roll < 6 {
                    Op::Del(key)
                } else {
                    Op::Get(key)
                };
                let target = self.clients[c]
                    .leader_hint
                    .unwrap_or_else(|| self.ids[self.rng.below(self.ids.len() as u64) as usize]);
                let call = self.now();
                self.clients[c].current = Some(InFlight {
                    op,
                    call,
                    proposed: None,
                    target,
                    deadline: self.clock + 80,
                });
            }

            // Try to get an unproposed request accepted by a leader.
            let Some(req) = &self.clients[c].current else {
                continue;
            };
            if req.proposed.is_some() {
                continue;
            }
            let (target, op) = (req.target, req.op.clone());
            let Some(raft) = self.nodes.get_mut(&target).and_then(|n| n.raft.as_mut()) else {
                self.retarget(c, None);
                continue;
            };
            match raft.propose(op) {
                Ok((index, term)) => {
                    let out = raft.flush();
                    self.clients[c].current.as_mut().unwrap().proposed =
                        Some((target, index, term));
                    self.clients[c].leader_hint = Some(target);
                    self.after_step(target);
                    self.send(out);
                }
                Err(ProposeError::NotLeader(hint)) => self.retarget(c, hint),
            }
        }
    }

    fn retarget(&mut self, c: usize, hint: Option<NodeId>) {
        let next = hint.unwrap_or_else(|| self.ids[self.rng.below(self.ids.len() as u64) as usize]);
        self.clients[c].leader_hint = hint;
        if let Some(req) = self.clients[c].current.as_mut() {
            req.target = next;
        }
    }

    fn faults(&mut self) {
        if self.final_phase {
            return;
        }
        if let Some(every) = self.cfg.partition_every {
            if self.clock % every == every / 2 {
                self.partitions += 1;
                // Half the time isolate the current leader, otherwise a random split.
                let leader = self
                    .nodes
                    .iter()
                    .find(|(_, n)| n.raft.as_ref().is_some_and(|r| r.role() == Role::Leader))
                    .map(|(id, _)| *id);
                for &id in &self.ids {
                    let g = match leader {
                        Some(l) if self.rng.chance(0.5) => u32::from(id == l),
                        _ => self.rng.below(2) as u32,
                    };
                    self.group.insert(id, g);
                }
            } else if self.clock.is_multiple_of(every) {
                self.heal();
            }
        }
        for &id in &self.ids.clone() {
            let node = self.nodes.get_mut(&id).unwrap();
            if node.raft.is_some() && self.rng.chance(self.cfg.crash_rate) {
                node.raft = None; // everything volatile is lost
                node.data.clear();
                node.restart_at = self.clock + 20 + self.rng.below(60);
                self.crashes += 1;
            }
        }
    }

    fn heal(&mut self) {
        for &id in &self.ids {
            self.group.insert(id, 0);
        }
    }

    fn restart_due(&mut self, force: bool) {
        for &id in &self.ids.clone() {
            let ids = self.ids.clone();
            let seed = self.rng.next();
            let cfg = self.raft_cfg.clone();
            let clock = self.clock;
            let node = self.nodes.get_mut(&id).unwrap();
            if node.raft.is_none() && (force || clock >= node.restart_at) {
                node.raft = Some(RaftNode::restore(id, &ids, cfg, seed, node.disk.clone()));
            }
        }
    }

    fn tick(&mut self) {
        self.clock += 1;
        self.faults();
        self.restart_due(false);

        for &id in &self.ids.clone() {
            if let Some(raft) = self.nodes.get_mut(&id).unwrap().raft.as_mut() {
                let out = raft.tick();
                self.after_step(id);
                self.send(out);
            }
        }

        // Deliver everything due this tick, in a shuffled order.
        let mut due = Vec::new();
        let mut later = Vec::new();
        for (at, env) in std::mem::take(&mut self.net) {
            if at <= self.clock {
                due.push(env)
            } else {
                later.push((at, env))
            }
        }
        self.net = later;
        for i in (1..due.len()).rev() {
            let j = self.rng.below(i as u64 + 1) as usize;
            due.swap(i, j);
        }
        for env in due {
            if !self.connected(env.from, env.to) {
                continue;
            }
            let to = env.to;
            if let Some(raft) = self.nodes.get_mut(&to).unwrap().raft.as_mut() {
                let out = raft.step(env);
                self.after_step(to);
                self.send(out);
            }
        }

        self.step_clients();
    }
}

/// Run one simulation from a seed.
pub fn run(seed: u64, cfg: SimConfig) -> Report {
    let ids: Vec<NodeId> = (1..=cfg.nodes).collect();
    let raft_cfg = Config {
        bug: cfg.bug,
        max_batch: cfg.max_batch,
        ..Config::default()
    };
    let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
    let mut nodes = BTreeMap::new();
    for &id in &ids {
        nodes.insert(
            id,
            Node {
                raft: Some(RaftNode::new(id, &ids, raft_cfg.clone(), rng.next())),
                disk: crate::raft::HardState {
                    term: 0,
                    voted_for: None,
                    log: Vec::new(),
                },
                data: HashMap::new(),
                restart_at: 0,
            },
        );
    }
    let clients = (0..cfg.clients)
        .map(|_| Client {
            leader_hint: None,
            current: None,
            next_at: 30,
            counter: 0,
        })
        .collect();
    let mut w = World {
        raft_cfg,
        rng,
        group: ids.iter().map(|&id| (id, 0)).collect(),
        ids,
        nodes,
        net: Vec::new(),
        clients,
        clock: 0,
        events: 0,
        leaders_by_term: HashMap::new(),
        applied: HashMap::new(),
        history: Vec::new(),
        violation: None,
        crashes: 0,
        partitions: 0,
        completed_in_final_phase: 0,
        final_phase: false,
        cfg,
    };

    for _ in 0..w.cfg.ticks {
        w.tick();
        if w.violation.is_some() {
            break;
        }
    }

    // Final phase: heal everything, stop dropping messages, and require progress.
    if w.violation.is_none() {
        w.final_phase = true;
        w.heal();
        w.cfg.drop = 0.0;
        w.restart_due(true);
        for _ in 0..400 {
            w.tick();
        }
        if w.violation.is_none() && w.completed_in_final_phase == 0 {
            w.violation =
                Some("liveness: no client request completed after all faults healed".into());
        }
    }

    // Requests still open when the run ends have unknown outcomes.
    for c in 0..w.clients.len() {
        if let Some(req) = w.clients[c].current.take() {
            if req.proposed.is_some() {
                if let Op::Set(_, _) | Op::Del(_) = req.op {
                    let kind = match &req.op {
                        Op::Set(_, v) => Kind::Write(Some(v.clone())),
                        _ => Kind::Write(None),
                    };
                    w.history.push(Operation {
                        key: key_of(&req.op),
                        kind,
                        call: req.call,
                        ret: None,
                    });
                }
            }
        }
    }

    if w.violation.is_none() {
        match linear::check(&w.history) {
            Verdict::Ok => {}
            Verdict::Violation { key } => {
                w.violation = Some(format!("linearizability: no valid order for key {key}"))
            }
            Verdict::Unknown { key } => {
                w.violation = Some(format!(
                    "linearizability: search gave up on key {key} (inconclusive)"
                ))
            }
        }
    }

    Report {
        seed,
        ops_ok: w.history.iter().filter(|o| o.ret.is_some()).count(),
        ops_unknown: w.history.iter().filter(|o| o.ret.is_none()).count(),
        leaders: w.leaders_by_term.len(),
        crashes: w.crashes,
        partitions: w.partitions,
        max_term: w.leaders_by_term.keys().copied().max().unwrap_or(0),
        violation: w.violation,
        history: w.history,
    }
}
