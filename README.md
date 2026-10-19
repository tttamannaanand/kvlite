# kvlite

[![CI](https://github.com/tttamannaanand/kvlite/actions/workflows/ci.yml/badge.svg)](https://github.com/tttamannaanand/kvlite/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-000)](LICENSE)
![Rust](https://img.shields.io/badge/rust-std%20only-000?logo=rust&logoColor=white)

A tiny in-memory key-value store over TCP, made crash-safe by a write-ahead log. Written in Rust with **zero dependencies**: just the standard library.

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

## Run it

```bash
cargo run --release -- --addr 127.0.0.1:6380 --wal kvlite.wal   # --sync group|each|none
cargo test                                                       # 16 tests, including kill -9 recovery
cargo run --release --example bench -- 127.0.0.1:6380 8 2000     # throughput check
```

## Tests

- **Unit:** WAL replay, torn-tail recovery, compaction, reopen in every sync mode, protocol parsing, 16 threads under group commit, compaction racing group commit
- **End-to-end** against the real binary over TCP:
  - writes survive `kill -9`
  - compaction survives a crash
  - 8 concurrent clients × 100 writes lose nothing, before or after a crash
  - bad commands return errors without dropping the connection

## Roadmap

- ~~Group commit~~ ✓ done in v0.2
- Automatic compaction once the log grows past a size threshold
- A checksum per record, to catch corruption as well as truncation
- TTLs (`SET key value EX 60`)
- An async version on Tokio, to compare against thread-per-connection

## License

[MIT](LICENSE)
