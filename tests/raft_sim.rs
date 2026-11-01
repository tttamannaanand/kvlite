//! Fault-injection runs of the Raft cluster. Each seed is a different,
//! reproducible storm of partitions, crashes and network chaos.

use kvlite::raft::Bug;
use kvlite::sim::{run, SimConfig};

const SEEDS: u64 = 100;

#[test]
fn three_nodes_stay_safe_and_linearizable_under_faults() {
    let mut ops = 0;
    for seed in 1..=SEEDS {
        let r = run(seed, SimConfig::default());
        assert!(r.ok(), "seed {seed}: {}", r.violation.unwrap());
        ops += r.ops_ok;
    }
    assert!(
        ops > 1000,
        "too little traffic to be meaningful ({ops} ops)"
    );
}

#[test]
fn five_nodes_stay_safe_and_linearizable_under_faults() {
    for seed in 1..=SEEDS / 2 {
        let r = run(
            seed,
            SimConfig {
                nodes: 5,
                ..SimConfig::default()
            },
        );
        assert!(r.ok(), "seed {seed}: {}", r.violation.unwrap());
    }
}

#[test]
fn a_calm_network_makes_steady_progress() {
    let r = run(
        7,
        SimConfig {
            drop: 0.0,
            duplicate: 0.0,
            partition_every: None,
            crash_rate: 0.0,
            ..SimConfig::default()
        },
    );
    assert!(r.ok(), "{:?}", r.violation);
    assert_eq!(r.ops_unknown, 0, "nothing should time out with no faults");
    assert!(r.ops_ok > 300, "only {} ops completed", r.ops_ok);
}

#[test]
fn unbatched_replication_is_also_safe() {
    for seed in 1..=SEEDS / 2 {
        let r = run(
            seed,
            SimConfig {
                nodes: 5,
                max_batch: 1,
                crash_rate: 0.01,
                ..SimConfig::default()
            },
        );
        assert!(r.ok(), "seed {seed}: {}", r.violation.unwrap());
    }
}

/// The harness has teeth: every planted bug must be caught by some seed.
fn caught(bug: Bug) -> Option<(u64, String)> {
    caught_with(bug, SimConfig::default())
}

fn caught_with(bug: Bug, cfg: SimConfig) -> Option<(u64, String)> {
    (1..=400).find_map(|seed| {
        let r = run(
            seed,
            SimConfig {
                bug: Some(bug),
                ..cfg.clone()
            },
        );
        r.violation.map(|v| (seed, v))
    })
}

#[test]
fn catches_votes_that_ignore_the_log() {
    let (seed, why) = caught(Bug::VoteIgnoresLog).expect("VoteIgnoresLog went undetected");
    println!("VoteIgnoresLog caught on seed {seed}: {why}");
}

#[test]
fn catches_committing_old_term_entries_by_count() {
    // The Raft paper's figure 8. A new leader's no-op, sent in the same batch as
    // old entries, masks this bug; it needs one entry per message, five nodes
    // and frequent crashes to surface.
    // A search over seeds 1..=400 first found it on seed 176 (about 11 minutes);
    // this replays that neighbourhood so the test stays fast. The scripted,
    // step-by-step reproduction lives in raft.rs (figure_8_* tests).
    let figure8 = SimConfig {
        nodes: 5,
        max_batch: 1,
        crash_rate: 0.01,
        ..SimConfig::default()
    };
    let (seed, why) = (170..=180)
        .find_map(|seed| {
            run(
                seed,
                SimConfig {
                    bug: Some(Bug::CommitOldTermByCount),
                    ..figure8.clone()
                },
            )
            .violation
            .map(|v| (seed, v))
        })
        .expect("CommitOldTermByCount went undetected");
    println!("CommitOldTermByCount caught on seed {seed}: {why}");
}

#[test]
fn catches_truncating_on_stale_appends() {
    let (seed, why) =
        caught(Bug::TruncateOnStaleAppend).expect("TruncateOnStaleAppend went undetected");
    println!("TruncateOnStaleAppend caught on seed {seed}: {why}");
}
