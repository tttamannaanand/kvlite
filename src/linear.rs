//! Linearizability checking for a key-value store.
//!
//! Given a history of client operations with their real-time start and end,
//! decide whether there is a single sequential order that respects both the
//! real-time order (an op that finished before another started must come
//! first) and register semantics (each read sees the latest write).
//!
//! This is the Wing & Gong / Lowe search used by checkers such as Knossos and
//! Porcupine, with memoisation on (ops linearised so far, register value).
//! Keys are independent, so each key's history is checked separately.

use std::collections::{BTreeMap, HashSet};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Kind {
    /// Set to a value, or delete (None).
    Write(Option<String>),
    /// What the read returned.
    Read(Option<String>),
}

#[derive(Debug, Clone)]
pub struct Operation {
    pub key: String,
    pub kind: Kind,
    pub call: u64,
    /// None if the client never heard back: the op may or may not have happened.
    pub ret: Option<u64>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Verdict {
    Ok,
    /// No valid order exists for this key.
    Violation {
        key: String,
    },
    /// The search gave up (history too large); treated as inconclusive.
    Unknown {
        key: String,
    },
}

/// Check a whole history. Returns the first key that fails, if any.
pub fn check(history: &[Operation]) -> Verdict {
    let mut by_key: BTreeMap<&str, Vec<&Operation>> = BTreeMap::new();
    for op in history {
        // A read that never returned tells us nothing.
        if op.ret.is_none() && matches!(op.kind, Kind::Read(_)) {
            continue;
        }
        by_key.entry(&op.key).or_default().push(op);
    }
    for (key, ops) in by_key {
        match check_key(&ops) {
            Some(true) => {}
            Some(false) => {
                return Verdict::Violation {
                    key: key.to_string(),
                }
            }
            None => {
                return Verdict::Unknown {
                    key: key.to_string(),
                }
            }
        }
    }
    Verdict::Ok
}

const BUDGET: usize = 2_000_000;

/// Some(true) if linearizable, Some(false) if not, None if the search ran out of budget.
fn check_key(ops: &[&Operation]) -> Option<bool> {
    let n = ops.len();
    let words = n.div_ceil(64);
    let mut seen: HashSet<(Vec<u64>, Option<String>)> = HashSet::new();
    let mut budget = BUDGET;
    let done = vec![0u64; words];
    dfs(ops, &done, None, &mut seen, &mut budget)
}

fn is_done(done: &[u64], i: usize) -> bool {
    done[i / 64] >> (i % 64) & 1 == 1
}

fn dfs(
    ops: &[&Operation],
    done: &[u64],
    value: Option<String>,
    seen: &mut HashSet<(Vec<u64>, Option<String>)>,
    budget: &mut usize,
) -> Option<bool> {
    // Finished when every op that completed has been placed.
    let all_completed_placed = ops
        .iter()
        .enumerate()
        .all(|(i, op)| op.ret.is_none() || is_done(done, i));
    if all_completed_placed {
        return Some(true);
    }
    if !seen.insert((done.to_vec(), value.clone())) {
        return Some(false); // already explored this state
    }
    if *budget == 0 {
        return None;
    }
    *budget -= 1;

    // An op can go next only if it started before every unplaced op finished.
    let earliest_return = ops
        .iter()
        .enumerate()
        .filter(|(i, op)| !is_done(done, *i) && op.ret.is_some())
        .map(|(_, op)| op.ret.unwrap())
        .min()
        .unwrap_or(u64::MAX);

    let mut gave_up = false;
    for (i, op) in ops.iter().enumerate() {
        if is_done(done, i) || op.call > earliest_return {
            continue;
        }
        let next_value = match &op.kind {
            Kind::Write(v) => v.clone(),
            Kind::Read(seen_value) => {
                if *seen_value != value {
                    continue; // this read can't happen here
                }
                value.clone()
            }
        };
        let mut next = done.to_vec();
        next[i / 64] |= 1 << (i % 64);
        match dfs(ops, &next, next_value, seen, budget) {
            Some(true) => return Some(true),
            Some(false) => {}
            None => gave_up = true,
        }
    }
    if gave_up {
        None
    } else {
        Some(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn w(v: &str, call: u64, ret: u64) -> Operation {
        Operation {
            key: "x".into(),
            kind: Kind::Write(Some(v.into())),
            call,
            ret: Some(ret),
        }
    }
    fn r(v: Option<&str>, call: u64, ret: u64) -> Operation {
        Operation {
            key: "x".into(),
            kind: Kind::Read(v.map(String::from)),
            call,
            ret: Some(ret),
        }
    }

    #[test]
    fn sequential_history_is_fine() {
        assert_eq!(
            check(&[
                w("1", 0, 1),
                r(Some("1"), 2, 3),
                w("2", 4, 5),
                r(Some("2"), 6, 7)
            ]),
            Verdict::Ok
        );
    }

    #[test]
    fn reading_a_stale_value_after_a_write_finished_is_a_violation() {
        // The write of 2 finished at t=5, then a read starting at t=6 saw 1.
        let h = [w("1", 0, 1), w("2", 4, 5), r(Some("1"), 6, 7)];
        assert_eq!(check(&h), Verdict::Violation { key: "x".into() });
    }

    #[test]
    fn concurrent_ops_may_go_in_either_order() {
        // The read overlaps the write, so seeing either value is fine.
        assert_eq!(
            check(&[w("1", 0, 1), w("2", 2, 6), r(Some("1"), 3, 4)]),
            Verdict::Ok
        );
        assert_eq!(
            check(&[w("1", 0, 1), w("2", 2, 6), r(Some("2"), 3, 4)]),
            Verdict::Ok
        );
    }

    #[test]
    fn a_write_with_no_reply_may_or_may_not_have_happened() {
        let lost = Operation {
            key: "x".into(),
            kind: Kind::Write(Some("9".into())),
            call: 2,
            ret: None,
        };
        assert_eq!(
            check(&[w("1", 0, 1), lost.clone(), r(Some("9"), 5, 6)]),
            Verdict::Ok
        );
        assert_eq!(
            check(&[w("1", 0, 1), lost, r(Some("1"), 5, 6)]),
            Verdict::Ok
        );
    }

    #[test]
    fn a_value_that_was_never_written_is_a_violation() {
        assert_eq!(
            check(&[w("1", 0, 1), r(Some("7"), 2, 3)]),
            Verdict::Violation { key: "x".into() }
        );
    }

    #[test]
    fn values_cannot_flip_back_and_forth() {
        // Once a read saw 2 (after 2 was written), a later read can't see 1 again.
        let h = [
            w("1", 0, 1),
            w("2", 2, 10),
            r(Some("2"), 3, 4),
            r(Some("1"), 5, 6),
        ];
        assert_eq!(check(&h), Verdict::Violation { key: "x".into() });
    }

    #[test]
    fn keys_are_checked_independently() {
        let mut y = r(Some("1"), 2, 3);
        y.key = "y".into(); // nobody wrote y
        assert_eq!(
            check(&[w("1", 0, 1), y]),
            Verdict::Violation { key: "y".into() }
        );
    }
}
