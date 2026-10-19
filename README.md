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
                                            ├─ 1. append record to the WAL (+ fsync)
                                            └─ 2. then update the HashMap
startup: replay the WAL into the HashMap
```

- **Log first, then memory.** A write is appended to the log before it touches the map, so if the disk write fails, nothing changes. One mutex covers both, so log order always matches apply order.
- **Binary records:** `op | key_len | key | val_len | val`, little-endian and length-prefixed, so keys and values can contain anything.
- **Torn writes are expected.** If the process dies halfway through a record, replay stops cleanly at the last complete one instead of refusing to start.
- **Compaction** rewrites the log with one `SET` per live key into a temp file, fsyncs it, then atomically renames it over the old log. A crash at any point leaves either the old log or the new one, never a mix.

## Performance

8 clients, each sending `SET`s and waiting for every reply, on an M-series MacBook (`examples/bench.rs`):

| Mode | Throughput | Durability |
|---|---|---|
| default (fsync every write) | **~350 writes/s** | survives power loss |
| `--no-sync` | **~68,000 writes/s** | survives process crashes; a power cut can lose the last few writes |

The ~200× gap is the cost of waiting for the disk on every write, one writer at a time. **Group commit** (one fsync for a whole batch of concurrent writes) is the standard fix and the next thing on the roadmap.

## Run it

```bash
cargo run --release -- --addr 127.0.0.1:6380 --wal kvlite.wal   # add --no-sync for speed
cargo test                                                       # 14 tests, including kill -9 recovery
cargo run --release --example bench -- 127.0.0.1:6380 8 2000     # throughput check
```

## Tests

- **Unit:** WAL replay, torn-tail recovery, compaction, store reopen, protocol parsing
- **End-to-end** against the real binary over TCP:
  - writes survive `kill -9`
  - compaction survives a crash
  - 8 concurrent clients × 100 writes lose nothing, before or after a crash
  - bad commands return errors without dropping the connection

## Roadmap

- Group commit, to close the fsync throughput gap
- Automatic compaction once the log grows past a size threshold
- A checksum per record, to catch corruption as well as truncation
- TTLs (`SET key value EX 60`)
- An async version on Tokio, to compare against thread-per-connection

## License

[MIT](LICENSE)
