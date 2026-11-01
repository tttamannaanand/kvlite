# kvlite

[![CI](https://github.com/tttamannaanand/kvlite/actions/workflows/ci.yml/badge.svg)](https://github.com/tttamannaanand/kvlite/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-000)](LICENSE)
![Rust](https://img.shields.io/badge/rust-std%20only-000?logo=rust&logoColor=white)

A tiny key-value store over TCP: crash-safe on one machine with a write-ahead log, and **replicated across a cluster with Raft consensus**, checked by a deterministic fault-injection simulator. Written in Rust with **zero dependencies**: just the standard library.

```
$ cargo run --release
kvlite listening on 127.0.0.1:6380 (0 keys loaded from kvlite.wal)

$ nc 127.0.0.1 6380
SET motto do your worst
+OK
GET motto
$do your worst
```

Kill it with `kill -9`, start it again, and your data is still there. That's tested, not promised: see [`tests/crash_recovery.rs`](tests/crash_recovery.rs).

## Protocol

One command per line. Command names are case-insensitive.

| Command | Reply |
|---|---|
| `PING` | `+PONG` |
| `SET key value` | `+OK` (the value is the rest of the line, spaces allowed) |
| `GET key` | `$value` or `(nil)` |
| `DEL key` | `:1` if it existed, `:0` if not |
| `LEN` | `:count` |
| `COMPACT` | `+OK` after rewriting the log |
| `QUIT` | `+BYE`, then the connection closes |

Errors come back as `-ERR message` and the connection stays open.

## How it works

```
client ──TCP──► thread per connection ──► Store (one Mutex)
                                            ├─ 1. append record to the WAL
                                            ├─ 2. update the HashMap
                                            └─ 3. wait for group commit to fsync it
startup: replay the WAL into the HashMap
```

- **Log first, then memory.** A write is appended to the log before it touches the map, so if the disk write fails, nothing changes. One mutex covers both, so log order always matches apply order.
- **Binary records:** `op | key_len | key | val_len | val`, little-endian and length-prefixed, so keys and values can contain anything.
- **Torn writes are expected.** If the process dies halfway through a record, replay stops cleanly at the last complete one instead of refusing to start.
- **Compaction** rewrites the log with one `SET` per live key into a temp file, fsyncs it, then atomically renames it over the old log. A crash at any point leaves either the old log or the new one, never a mix.

## Group commit

fsync is slow: on this Mac it's a full flush to the physical drive, about 3 ms. Doing one per write while holding the lock caps the store at roughly 350 writes a second, however many clients you add.

With group commit (the default), a write appends its record under the lock, takes a sequence number, **releases the lock**, then waits until the log is synced up to that number. The first waiter becomes the *leader*: it runs one fsync covering every record written so far, then wakes everyone it made durable. Writers that arrive mid-sync queue up for the next one, so under load **one fsync acknowledges a whole batch**.

```
writer A ─append─┐                ┌─ ack A
writer B ─append─┼─► leader: one fsync ─┼─ ack B
writer C ─append─┘                └─ ack C
```

A write is still only acknowledged once it's on disk. Two details the tests pin down:
- The leader reads "how far the log is written" and "which file to sync" **under the same lock**. Otherwise a compaction swapping files in between could make it fsync the old file while claiming writes in the new one were safe.
- If an fsync fails, every writer waiting on it gets the error. None of them is told "OK".

## Performance

Each client sends a `SET` and waits for the reply before sending the next (`examples/bench.rs`, M-series MacBook):

| `--sync` | 8 clients | 32 clients | Survives power loss? |
|---|---|---|---|
| `each` (fsync per write) | 379 /s | 354 /s | yes |
| **`group`** (default) | **1,148 /s** (3×) | **2,098 /s** (6×) | **yes** |
| `none` | 69,110 /s | 75,047 /s | no (process crashes only) |

`each` stays flat as clients are added, because every write queues for the disk alone. `group` scales with concurrency, because more clients means bigger batches per fsync. `none` shows the remaining gap, which is pure fsync latency.

## Raft cluster

Run three nodes and the data is replicated: any one can die (even with `kill -9`) and nothing acknowledged is lost.

```bash
P=1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003
C=1=127.0.0.1:6381,2=127.0.0.1:6382,3=127.0.0.1:6383
kvlite --cluster --id 1 --peers $P --clients $C &
kvlite --cluster --id 2 --peers $P --clients $C &
kvlite --cluster --id 3 --peers $P --clients $C &
```

```
$ nc 127.0.0.1 6382        # a follower
SET city Bengaluru
-MOVED 127.0.0.1:6381      # go to the leader
STATUS
+follower term=1 leader=1 commit=4 applied=4 last=4
```

- **[`raft.rs`](src/raft.rs): the algorithm as a pure state machine.** Ticks and messages go in, messages come out; no network, disk or clock inside. Leader election, log replication with fast backtracking, and the rule that leaders only commit entries from their own term.
- **[`cluster.rs`](src/cluster.rs): the real runtime.** Peer connections that reconnect, a 50 ms ticker, and durable term, vote and log that are **fsynced before any message is sent**, so a vote or log promise survives a crash.
- **Reads go through the log too**, so every reply is linearizable: a client never sees a value older than one it already saw.

### How it's tested: a Jepsen-style simulator

[`sim.rs`](src/sim.rs) runs a whole cluster in one thread from a seed, so any failure replays exactly. Each run throws a storm at it:

| Fault | |
|---|---|
| Network | messages dropped (5%), duplicated (3%), delayed and reordered |
| Partitions | random splits, and isolating the current leader |
| Crashes | nodes lose all memory and restart from their persisted state |
| Clients | 4 clients reading, writing and deleting, following redirects, timing out |

Checked **after every step**: at most one leader per term, and no two nodes ever apply different entries at the same index. Checked **at the end**: the clients' history is **linearizable** ([`linear.rs`](src/linear.rs), the Wing & Gong / Lowe search used by Knossos and Porcupine), and the cluster makes progress once faults heal.

**The harness has teeth.** Three classic Raft bugs are planted behind a switch, and the tests require the simulator to catch each one:

| Planted bug | Caught |
|---|---|
| Voting without checking the candidate's log is up to date | seed 1: two different entries applied at index 107 |
| Truncating the log on a stale, delayed message | seed 1: two different entries applied at index 147 |
| Committing old-term entries by counting replicas (**the Raft paper's figure 8**) | seed 176: index 12 applied as `Set(k2, c2-4)` and as a no-op |

The figure-8 bug taught me something: a new leader's no-op, sent in the same batch as older entries, *masks* it, because followers acknowledge both together. The simulator only found it once replication was limited to one entry per message (`max_batch: 1`) with five nodes and more crashes. There's also a scripted, step-by-step reproduction of the paper's figure in [`raft.rs`](src/raft.rs) (`figure_8_*` tests): with the bug, an entry is committed and later overwritten; without it, Raft refuses to commit it.

## Run it

```bash
cargo run --release -- --addr 127.0.0.1:6380 --wal kvlite.wal   # --sync group|each|none
cargo test --release                                             # everything, including ~210 fault-injection runs
cargo run --release --example bench -- 127.0.0.1:6380 8 2000     # throughput check
```

## Tests

- **Unit:** WAL replay, torn-tail recovery, compaction, reopen in every sync mode, protocol parsing, 16 threads under group commit, compaction racing group commit
- **End-to-end** against the real binary over TCP:
  - writes survive `kill -9`
  - compaction survives a crash
  - 8 concurrent clients × 100 writes lose nothing, before or after a crash
  - bad commands return errors without dropping the connection
- **Raft:** 25 unit tests, figure-8 reproduction, codec round trips and torn-write recovery
- **Simulation:** 3-node, 5-node and unbatched clusters over hundreds of seeds, plus the three planted bugs
- **Real cluster:** three processes; redirects, `kill -9` of the leader with no data loss, a restarted node catching up, and a full-cluster restart

## Roadmap

- ~~Group commit~~ ✓ done in v0.2
- ~~Raft replication~~ ✓ done in v0.3
- Snapshots and log compaction for the Raft log
- Membership changes (adding and removing nodes)
- Leader leases for faster reads
- Automatic compaction once the log grows past a size threshold
- A checksum per record, to catch corruption as well as truncation
- TTLs (`SET key value EX 60`)
- An async version on Tokio, to compare against thread-per-connection

## License

[MIT](LICENSE)
