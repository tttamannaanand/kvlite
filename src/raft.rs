//! Raft consensus, as a pure state machine.
//!
//! A node never touches the network, disk or clock. Callers feed it timer
//! ticks and incoming messages, and it returns the messages to send. That
//! makes it deterministic, so the simulator in `sim.rs` can replay any
//! failure exactly from a seed.
//!
//! Follows the Raft paper (Ongaro & Ousterhout, 2014): leader election,
//! log replication, and commitment of entries from the current term only.
//! Reads go through the log, so every operation is linearizable.

use std::collections::{HashMap, HashSet};

pub type NodeId = u64;
pub type Term = u64;
pub type Index = u64;

/// A command in the replicated log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Op {
    /// Appended by every new leader, so it can commit entries from earlier terms.
    Noop,
    Set(String, String),
    Del(String),
    /// Reads are logged too: applying one returns the value at that point in the log.
    Get(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub term: Term,
    pub op: Op,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    RequestVote {
        term: Term,
        last_index: Index,
        last_term: Term,
    },
    Vote {
        term: Term,
        granted: bool,
    },
    Append {
        term: Term,
        prev_index: Index,
        prev_term: Term,
        entries: Vec<Entry>,
        commit: Index,
    },
    AppendReply {
        term: Term,
        success: bool,
        /// On success: the follower's last matching index.
        /// On failure: where the leader should try next.
        hint: Index,
    },
}

impl Message {
    pub fn term(&self) -> Term {
        match self {
            Message::RequestVote { term, .. }
            | Message::Vote { term, .. }
            | Message::Append { term, .. }
            | Message::AppendReply { term, .. } => *term,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Envelope {
    pub from: NodeId,
    pub to: NodeId,
    pub msg: Message,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

/// Deliberate, classic Raft bugs. The simulator must catch every one;
/// if it can't, the test harness isn't strong enough.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Bug {
    /// Grant votes without checking the candidate's log is up to date.
    VoteIgnoresLog,
    /// Commit entries from earlier terms by counting replicas (Raft paper, figure 8).
    CommitOldTermByCount,
    /// Followers overwrite their log from any leader message, even stale ones.
    TruncateOnStaleAppend,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub election_ticks: (u64, u64), // randomised timeout range, in ticks
    pub heartbeat_ticks: u64,
    pub max_batch: usize,
    pub bug: Option<Bug>,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            election_ticks: (10, 20),
            heartbeat_ticks: 3,
            max_batch: 64,
            bug: None,
        }
    }
}

/// What must survive a crash: the current term, who we voted for, and the log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HardState {
    pub term: Term,
    pub voted_for: Option<NodeId>,
    pub log: Vec<Entry>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum ProposeError {
    /// Not the leader. Carries the leader we last heard from, if any.
    NotLeader(Option<NodeId>),
}

pub struct RaftNode {
    pub id: NodeId,
    peers: Vec<NodeId>, // everyone except us
    cfg: Config,

    // persistent
    term: Term,
    voted_for: Option<NodeId>,
    log: Vec<Entry>, // log[0] is a sentinel so real entries start at index 1

    // volatile
    role: Role,
    leader: Option<NodeId>,
    commit: Index,
    applied: Index,
    elapsed: u64,
    timeout: u64,
    votes: HashSet<NodeId>,
    next_index: HashMap<NodeId, Index>,
    match_index: HashMap<NodeId, Index>,
    rng: u64,
}

impl RaftNode {
    pub fn new(id: NodeId, cluster: &[NodeId], cfg: Config, seed: u64) -> Self {
        Self::restore(
            id,
            cluster,
            cfg,
            seed,
            HardState {
                term: 0,
                voted_for: None,
                log: Vec::new(),
            },
        )
    }

    /// Rebuild a node after a crash: only the hard state survives.
    pub fn restore(id: NodeId, cluster: &[NodeId], cfg: Config, seed: u64, hs: HardState) -> Self {
        let mut log = vec![Entry {
            term: 0,
            op: Op::Noop,
        }];
        log.extend(hs.log);
        let mut node = RaftNode {
            id,
            peers: cluster.iter().copied().filter(|&p| p != id).collect(),
            cfg,
            term: hs.term,
            voted_for: hs.voted_for,
            log,
            role: Role::Follower,
            leader: None,
            commit: 0,
            applied: 0,
            elapsed: 0,
            timeout: 0,
            votes: HashSet::new(),
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            rng: seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1,
        };
        node.reset_timeout();
        node
    }

    // ------------------------------------------------------------ accessors

    pub fn role(&self) -> Role {
        self.role
    }
    pub fn term(&self) -> Term {
        self.term
    }
    pub fn leader(&self) -> Option<NodeId> {
        self.leader
    }
    pub fn commit_index(&self) -> Index {
        self.commit
    }
    pub fn last_index(&self) -> Index {
        self.log.len() as Index - 1
    }
    pub fn entry(&self, i: Index) -> Option<&Entry> {
        if i == 0 {
            None
        } else {
            self.log.get(i as usize)
        }
    }

    pub fn hard_state(&self) -> HardState {
        HardState {
            term: self.term,
            voted_for: self.voted_for,
            log: self.log[1..].to_vec(),
        }
    }

    fn quorum(&self) -> usize {
        // A strict majority of the whole cluster (peers plus us).
        let size = self.peers.len() + 1;
        size / 2 + 1
    }

    fn last_term(&self) -> Term {
        self.log.last().map_or(0, |e| e.term)
    }

    fn term_at(&self, i: Index) -> Option<Term> {
        self.log.get(i as usize).map(|e| e.term)
    }

    fn reset_timeout(&mut self) {
        // xorshift64*: cheap, deterministic per node.
        self.rng ^= self.rng >> 12;
        self.rng ^= self.rng << 25;
        self.rng ^= self.rng >> 27;
        let r = self.rng.wrapping_mul(0x2545_F491_4F6C_DD1D);
        let (lo, hi) = self.cfg.election_ticks;
        self.timeout = lo + r % (hi - lo + 1);
        self.elapsed = 0;
    }

    // ------------------------------------------------------------ driving

    /// Advance the clock by one tick.
    pub fn tick(&mut self) -> Vec<Envelope> {
        self.elapsed += 1;
        match self.role {
            Role::Leader => {
                if self.elapsed >= self.cfg.heartbeat_ticks {
                    self.elapsed = 0;
                    return self.broadcast_append();
                }
                Vec::new()
            }
            _ => {
                if self.elapsed >= self.timeout {
                    return self.start_election();
                }
                Vec::new()
            }
        }
    }

    /// Ask the cluster to agree on `op`. Returns the log index it will occupy.
    pub fn propose(&mut self, op: Op) -> Result<(Index, Term), ProposeError> {
        if self.role != Role::Leader {
            return Err(ProposeError::NotLeader(self.leader));
        }
        self.log.push(Entry {
            term: self.term,
            op,
        });
        self.maybe_commit(); // a single-node cluster commits at once
        Ok((self.last_index(), self.term))
    }

    /// Send any pending log entries now instead of waiting for the next heartbeat.
    pub fn flush(&mut self) -> Vec<Envelope> {
        if self.role == Role::Leader {
            self.broadcast_append()
        } else {
            Vec::new()
        }
    }

    /// Committed entries not yet handed to the state machine.
    pub fn take_committed(&mut self) -> Vec<(Index, Entry)> {
        let mut out = Vec::new();
        while self.applied < self.commit {
            self.applied += 1;
            out.push((self.applied, self.log[self.applied as usize].clone()));
        }
        out
    }

    pub fn step(&mut self, env: Envelope) -> Vec<Envelope> {
        let from = env.from;
        let msg = env.msg;

        // Any message from a newer term makes us a follower in that term.
        if msg.term() > self.term {
            self.become_follower(msg.term(), None);
        }

        match msg {
            Message::RequestVote {
                term,
                last_index,
                last_term,
            } => self.on_request_vote(from, term, last_index, last_term),
            Message::Vote { term, granted } => self.on_vote(from, term, granted),
            Message::Append {
                term,
                prev_index,
                prev_term,
                entries,
                commit,
            } => self.on_append(from, term, prev_index, prev_term, entries, commit),
            Message::AppendReply {
                term,
                success,
                hint,
            } => self.on_append_reply(from, term, success, hint),
        }
    }

    // ------------------------------------------------------------ elections

    fn become_follower(&mut self, term: Term, leader: Option<NodeId>) {
        if term > self.term {
            self.term = term;
            self.voted_for = None;
        }
        self.role = Role::Follower;
        self.leader = leader;
        self.votes.clear();
        self.reset_timeout();
    }

    fn start_election(&mut self) -> Vec<Envelope> {
        self.role = Role::Candidate;
        self.term += 1;
        self.voted_for = Some(self.id);
        self.leader = None;
        self.votes = HashSet::from([self.id]);
        self.reset_timeout();
        if self.votes.len() >= self.quorum() {
            return self.become_leader();
        }
        let (last_index, last_term) = (self.last_index(), self.last_term());
        let term = self.term;
        self.peers
            .iter()
            .map(|&to| Envelope {
                from: self.id,
                to,
                msg: Message::RequestVote {
                    term,
                    last_index,
                    last_term,
                },
            })
            .collect()
    }

    fn on_request_vote(
        &mut self,
        from: NodeId,
        term: Term,
        last_index: Index,
        last_term: Term,
    ) -> Vec<Envelope> {
        let log_ok = self.cfg.bug == Some(Bug::VoteIgnoresLog)
            || last_term > self.last_term()
            || (last_term == self.last_term() && last_index >= self.last_index());
        let granted = term == self.term && self.voted_for.is_none_or(|v| v == from) && log_ok;
        if granted {
            self.voted_for = Some(from);
            self.reset_timeout();
        }
        vec![self.reply(
            from,
            Message::Vote {
                term: self.term,
                granted,
            },
        )]
    }

    fn on_vote(&mut self, from: NodeId, term: Term, granted: bool) -> Vec<Envelope> {
        if self.role != Role::Candidate || term != self.term || !granted {
            return Vec::new();
        }
        self.votes.insert(from);
        if self.votes.len() >= self.quorum() {
            return self.become_leader();
        }
        Vec::new()
    }

    fn become_leader(&mut self) -> Vec<Envelope> {
        self.role = Role::Leader;
        self.leader = Some(self.id);
        let next = self.last_index() + 1;
        for &p in &self.peers {
            self.next_index.insert(p, next);
            self.match_index.insert(p, 0);
        }
        // A no-op from this term lets the leader commit everything before it.
        self.log.push(Entry {
            term: self.term,
            op: Op::Noop,
        });
        self.maybe_commit();
        self.elapsed = 0;
        self.broadcast_append()
    }

    // ------------------------------------------------------------ replication

    fn broadcast_append(&mut self) -> Vec<Envelope> {
        let peers = self.peers.clone();
        peers.into_iter().map(|p| self.append_to(p)).collect()
    }

    fn append_to(&self, to: NodeId) -> Envelope {
        let next = self.next_index[&to].max(1);
        let prev_index = next - 1;
        let prev_term = self.term_at(prev_index).unwrap_or(0);
        let end = (self.log.len()).min(next as usize + self.cfg.max_batch);
        let entries = if (next as usize) < self.log.len() {
            self.log[next as usize..end].to_vec()
        } else {
            Vec::new()
        };
        Envelope {
            from: self.id,
            to,
            msg: Message::Append {
                term: self.term,
                prev_index,
                prev_term,
                entries,
                commit: self.commit,
            },
        }
    }

    fn on_append(
        &mut self,
        from: NodeId,
        term: Term,
        prev_index: Index,
        prev_term: Term,
        entries: Vec<Entry>,
        leader_commit: Index,
    ) -> Vec<Envelope> {
        if term < self.term {
            return vec![self.reply(
                from,
                Message::AppendReply {
                    term: self.term,
                    success: false,
                    hint: 0,
                },
            )];
        }
        // A valid leader for our term: follow it.
        self.become_follower(term, Some(from));

        // Log consistency check: we must have prev_index with prev_term.
        if prev_index > self.last_index() {
            let hint = self.last_index() + 1;
            return vec![self.reply(
                from,
                Message::AppendReply {
                    term: self.term,
                    success: false,
                    hint,
                },
            )];
        }
        if self.term_at(prev_index) != Some(prev_term) {
            // Skip back over the whole conflicting term at once.
            let bad = self.term_at(prev_index).unwrap_or(0);
            let mut hint = prev_index;
            while hint > 1 && self.term_at(hint - 1) == Some(bad) {
                hint -= 1;
            }
            return vec![self.reply(
                from,
                Message::AppendReply {
                    term: self.term,
                    success: false,
                    hint,
                },
            )];
        }

        // Append, truncating only at a real conflict, so a delayed, shorter
        // message can't erase entries a newer one already added.
        let count = entries.len() as Index;
        for (i, e) in entries.into_iter().enumerate() {
            let idx = prev_index + 1 + i as Index;
            match self.term_at(idx) {
                Some(t) if t == e.term && self.cfg.bug != Some(Bug::TruncateOnStaleAppend) => {}
                Some(_) => {
                    self.log.truncate(idx as usize);
                    self.log.push(e);
                }
                None => self.log.push(e),
            }
        }
        if self.cfg.bug == Some(Bug::TruncateOnStaleAppend) {
            // The bug: treat this message as the whole truth and drop anything after it.
            self.log.truncate((prev_index + count + 1) as usize);
        }

        let last_new = prev_index + count;
        if leader_commit > self.commit {
            self.commit = leader_commit.min(last_new).max(self.commit);
        }
        vec![self.reply(
            from,
            Message::AppendReply {
                term: self.term,
                success: true,
                hint: last_new,
            },
        )]
    }

    fn on_append_reply(
        &mut self,
        from: NodeId,
        term: Term,
        success: bool,
        hint: Index,
    ) -> Vec<Envelope> {
        if self.role != Role::Leader || term != self.term {
            return Vec::new();
        }
        if success {
            let m = self.match_index.entry(from).or_insert(0);
            *m = (*m).max(hint);
            let matched = *m;
            self.next_index.insert(from, matched + 1);
            self.maybe_commit();
            // Keep streaming if the follower is still behind.
            if matched < self.last_index() {
                return vec![self.append_to(from)];
            }
            Vec::new()
        } else {
            let next = self.next_index[&from];
            let target = if hint == 0 {
                next.saturating_sub(1)
            } else {
                hint.min(next.saturating_sub(1))
            };
            self.next_index.insert(from, target.max(1));
            vec![self.append_to(from)]
        }
    }

    /// Commit the highest index replicated on a majority, but only if that
    /// entry is from the current term (the subtle rule from figure 8).
    fn maybe_commit(&mut self) {
        if self.role != Role::Leader {
            return;
        }
        let mut n = self.last_index();
        while n > self.commit {
            let replicas = 1 + self.match_index.values().filter(|&&m| m >= n).count();
            let this_term = self.term_at(n) == Some(self.term);
            if replicas >= self.quorum()
                && (this_term || self.cfg.bug == Some(Bug::CommitOldTermByCount))
            {
                self.commit = n;
                return;
            }
            n -= 1;
        }
    }

    fn reply(&self, to: NodeId, msg: Message) -> Envelope {
        Envelope {
            from: self.id,
            to,
            msg,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cluster(n: u64) -> Vec<RaftNode> {
        let ids: Vec<NodeId> = (1..=n).collect();
        ids.iter()
            .map(|&id| RaftNode::new(id, &ids, Config::default(), id))
            .collect()
    }

    /// Deliver messages until the network is quiet.
    fn settle(nodes: &mut [RaftNode], mut inbox: Vec<Envelope>) {
        while let Some(env) = inbox.pop() {
            let to = env.to as usize - 1;
            inbox.extend(nodes[to].step(env));
        }
    }

    fn elect(nodes: &mut [RaftNode]) -> usize {
        for _ in 0..100 {
            let mut out = Vec::new();
            for n in nodes.iter_mut() {
                out.extend(n.tick());
            }
            settle(nodes, out);
            if let Some(i) = nodes.iter().position(|n| n.role() == Role::Leader) {
                return i;
            }
        }
        panic!("no leader elected");
    }

    #[test]
    fn a_single_node_leads_itself_and_commits_immediately() {
        let mut n = RaftNode::new(1, &[1], Config::default(), 1);
        for _ in 0..30 {
            n.tick();
        }
        assert_eq!(n.role(), Role::Leader);
        let (idx, _) = n.propose(Op::Set("a".into(), "1".into())).unwrap();
        assert_eq!(n.commit_index(), idx);
    }

    #[test]
    fn three_nodes_elect_one_leader_and_replicate() {
        let mut nodes = cluster(3);
        let l = elect(&mut nodes);
        assert_eq!(nodes.iter().filter(|n| n.role() == Role::Leader).count(), 1);

        let (idx, _) = nodes[l].propose(Op::Set("k".into(), "v".into())).unwrap();
        let out = nodes[l].flush();
        settle(&mut nodes, out);
        assert!(nodes[l].commit_index() >= idx);

        // Followers learn the commit on the next heartbeat.
        let hb = nodes[l].flush();
        settle(&mut nodes, hb);
        for n in &nodes {
            assert_eq!(n.entry(idx).unwrap().op, Op::Set("k".into(), "v".into()));
            assert!(n.commit_index() >= idx);
        }
    }

    #[test]
    fn followers_redirect_proposals_to_the_leader() {
        let mut nodes = cluster(3);
        let l = elect(&mut nodes);
        let follower = (l + 1) % 3;
        assert_eq!(
            nodes[follower].propose(Op::Get("x".into())),
            Err(ProposeError::NotLeader(Some(nodes[l].id)))
        );
    }

    #[test]
    fn a_stale_candidate_cannot_win_against_a_longer_log() {
        let mut voter = RaftNode::new(1, &[1, 2, 3], Config::default(), 1);
        voter.step(Envelope {
            from: 2,
            to: 1,
            msg: Message::Append {
                term: 1,
                prev_index: 0,
                prev_term: 0,
                entries: vec![Entry {
                    term: 1,
                    op: Op::Noop,
                }],
                commit: 0,
            },
        });
        let reply = voter.step(Envelope {
            from: 3,
            to: 1,
            msg: Message::RequestVote {
                term: 2,
                last_index: 0,
                last_term: 0,
            },
        });
        assert_eq!(
            reply[0].msg,
            Message::Vote {
                term: 2,
                granted: false
            }
        );
    }

    #[test]
    fn a_delayed_shorter_append_does_not_erase_newer_entries() {
        let mut f = RaftNode::new(2, &[1, 2, 3], Config::default(), 2);
        let e = |op: &str| Entry {
            term: 1,
            op: Op::Set(op.into(), "".into()),
        };
        let append = |entries: Vec<Entry>| Envelope {
            from: 1,
            to: 2,
            msg: Message::Append {
                term: 1,
                prev_index: 0,
                prev_term: 0,
                entries,
                commit: 0,
            },
        };
        f.step(append(vec![e("a"), e("b")]));
        f.step(append(vec![e("a")])); // arrives late
        assert_eq!(f.last_index(), 2);
    }

    /// A tiny scripted network: nodes can be crashed, and every delivery is filtered.
    struct Script {
        ids: Vec<NodeId>,
        cfg: Config,
        nodes: Vec<Option<RaftNode>>,
        disk: Vec<HardState>,
    }

    impl Script {
        fn new(n: u64, cfg: Config) -> Self {
            let ids: Vec<NodeId> = (1..=n).collect();
            let nodes = ids
                .iter()
                .map(|&id| Some(RaftNode::new(id, &ids, cfg.clone(), id)))
                .collect();
            let disk = ids
                .iter()
                .map(|_| HardState {
                    term: 0,
                    voted_for: None,
                    log: vec![],
                })
                .collect();
            Script {
                ids,
                cfg,
                nodes,
                disk,
            }
        }
        fn node(&mut self, id: NodeId) -> &mut RaftNode {
            self.nodes[id as usize - 1].as_mut().unwrap()
        }
        fn crash(&mut self, id: NodeId) {
            let i = id as usize - 1;
            self.disk[i] = self.nodes[i].take().unwrap().hard_state();
        }
        fn restart(&mut self, id: NodeId) {
            let i = id as usize - 1;
            self.nodes[i] = Some(RaftNode::restore(
                id,
                &self.ids,
                self.cfg.clone(),
                id + 100,
                self.disk[i].clone(),
            ));
        }
        /// Deliver until quiet, but only messages between nodes in `allowed` that pass `keep`.
        fn deliver(
            &mut self,
            mut msgs: Vec<Envelope>,
            allowed: &[NodeId],
            keep: &dyn Fn(&Envelope) -> bool,
        ) {
            while let Some(env) = msgs.pop() {
                if !allowed.contains(&env.from) || !allowed.contains(&env.to) || !keep(&env) {
                    continue;
                }
                if let Some(n) = self.nodes[env.to as usize - 1].as_mut() {
                    msgs.extend(n.step(env));
                }
            }
        }
        /// Make `id` time out and win an election with only `voters` reachable.
        fn elect(&mut self, id: NodeId, voters: &[NodeId]) {
            let mut allowed = voters.to_vec();
            allowed.push(id);
            for _ in 0..40 {
                let out = self.node(id).tick();
                // Only votes travel during the election; replication comes later.
                self.deliver(out, &allowed, &|e| {
                    matches!(e.msg, Message::RequestVote { .. } | Message::Vote { .. })
                });
                if self.node(id).role() == Role::Leader {
                    return;
                }
            }
            panic!("node {id} failed to win an election");
        }
    }

    /// Figure 8 of the Raft paper, step by step. Returns what S1 committed at
    /// index 2 and what S5 later committed there.
    fn figure_8(bug: Option<Bug>) -> (Index, Option<Entry>, Option<Entry>) {
        let cfg = Config {
            bug,
            max_batch: 1,
            ..Config::default()
        };
        let mut s = Script::new(5, cfg);
        let all = [1, 2, 3, 4, 5];

        // S1 leads term 1 and everyone gets its no-op at index 1.
        s.elect(1, &[2, 3, 4, 5]);
        let out = s.node(1).flush();
        s.deliver(out, &all, &|_| true);
        // (a) S1 writes A at index 2, but only S2 receives it. Then S1 crashes.
        s.node(1).propose(Op::Set("x".into(), "A".into())).unwrap();
        let out = s.node(1).flush();
        s.deliver(out, &[1, 2], &|_| true);
        s.crash(1);

        // (b) S5 wins term 2 with S3 and S4, writes its own entries locally, and crashes.
        s.elect(5, &[3, 4]);
        s.node(5).propose(Op::Set("x".into(), "B".into())).unwrap();
        s.crash(5);

        // (c) S1 restarts and wins term 3 with S2 and S3. It pushes old entry A
        //     (term 1) to S3 and S4, one entry per message, but its own no-op never arrives.
        s.restart(1);
        s.elect(1, &[2, 3]);
        // The first message to each follower carries the no-op; S3 and S4 reject it
        // (they lack index 2), which makes S1 back up and send A on its own. After
        // that, every message carrying the no-op is lost.
        let term = s.node(1).term();
        let first_try = std::cell::RefCell::new(std::collections::HashSet::new());
        let out = s.node(1).flush();
        s.deliver(out, &[1, 3, 4], &move |e| match &e.msg {
            Message::Append { entries, .. } if entries.iter().any(|en| en.term == term) => {
                first_try.borrow_mut().insert(e.to)
            }
            _ => true,
        });
        let s1_commit = s.node(1).commit_index();
        let s1_index2 = (s1_commit >= 2).then(|| s.node(1).entry(2).unwrap().clone());

        // (d) S1 crashes. S5 returns and wins term 4 with the others: its log
        //     ends in term 2, which beats their term-1 entries. It replicates.
        s.crash(1);
        s.restart(5);
        s.elect(5, &[2, 3, 4]);
        for _ in 0..10 {
            let out = s.node(5).flush();
            s.deliver(out, &[2, 3, 4, 5], &|_| true);
        }
        let s5_index2 =
            (s.node(5).commit_index() >= 2).then(|| s.node(5).entry(2).unwrap().clone());
        (s1_commit, s1_index2, s5_index2)
    }

    #[test]
    fn figure_8_correct_raft_never_commits_the_old_entry() {
        let (s1_commit, s1_index2, s5_index2) = figure_8(None);
        assert!(
            s1_commit < 2,
            "S1 must not commit a term-1 entry by counting replicas"
        );
        assert_eq!(s1_index2, None);
        // S5 later commits its own entry at index 2, and nobody ever committed A there.
        assert!(s5_index2.is_some());
    }

    #[test]
    fn figure_8_counting_old_entries_commits_something_that_is_then_overwritten() {
        let (_, s1_index2, s5_index2) = figure_8(Some(Bug::CommitOldTermByCount));
        let a = s1_index2.expect("with the bug, S1 commits A at index 2");
        assert_eq!(a.op, Op::Set("x".into(), "A".into()));
        let overwritten = s5_index2.expect("S5 commits index 2 too");
        assert_ne!(
            a, overwritten,
            "two different entries committed at the same index: data loss"
        );
    }

    #[test]
    fn restore_keeps_term_vote_and_log_but_forgets_commit() {
        let mut nodes = cluster(3);
        let l = elect(&mut nodes);
        nodes[l].propose(Op::Set("a".into(), "1".into())).unwrap();
        let out = nodes[l].flush();
        settle(&mut nodes, out);
        let hs = nodes[l].hard_state();
        let back = RaftNode::restore(nodes[l].id, &[1, 2, 3], Config::default(), 9, hs.clone());
        assert_eq!(back.hard_state(), hs);
        assert_eq!(back.commit_index(), 0);
        assert_eq!(back.role(), Role::Follower);
    }
}
