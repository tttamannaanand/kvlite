//! End-to-end tests against the real binary over TCP.

use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;

struct Server {
    child: Child,
    addr: String,
}

impl Server {
    fn start(wal: &Path) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_kvlite"))
            .args(["--addr", "127.0.0.1:0", "--wal"])
            .arg(wal)
            .stdout(Stdio::piped())
            .spawn()
            .expect("start kvlite");
        let mut banner = String::new();
        BufReader::new(child.stdout.take().unwrap())
            .read_line(&mut banner)
            .unwrap();
        // "kvlite listening on 127.0.0.1:PORT (...)"
        let addr = banner.split_whitespace().nth(3).unwrap().to_string();
        Self { child, addr }
    }

    fn client(&self) -> Client {
        let stream = TcpStream::connect(&self.addr).unwrap();
        Client {
            reader: BufReader::new(stream.try_clone().unwrap()),
            stream,
        }
    }

    /// SIGKILL: no destructors, no flushing, no goodbye.
    fn crash(mut self) {
        self.child.kill().unwrap();
        self.child.wait().unwrap();
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

struct Client {
    stream: TcpStream,
    reader: BufReader<TcpStream>,
}

impl Client {
    fn send(&mut self, cmd: &str) -> String {
        writeln!(self.stream, "{cmd}").unwrap();
        let mut reply = String::new();
        self.reader.read_line(&mut reply).unwrap();
        reply.trim_end().to_string()
    }
}

fn tmp_wal(name: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!("kvlite-e2e-{name}-{}.wal", std::process::id()));
    let _ = std::fs::remove_file(&p);
    p
}

#[test]
fn data_survives_kill_9() {
    let wal = tmp_wal("kill9");

    let server = Server::start(&wal);
    let mut c = server.client();
    assert_eq!(c.send("PING"), "+PONG");
    assert_eq!(c.send("SET city Bengaluru"), "+OK");
    assert_eq!(c.send("SET motto do your worst"), "+OK");
    assert_eq!(c.send("SET temp gone soon"), "+OK");
    assert_eq!(c.send("DEL temp"), ":1");
    server.crash();

    let server = Server::start(&wal);
    let mut c = server.client();
    assert_eq!(c.send("GET city"), "$Bengaluru");
    assert_eq!(c.send("GET motto"), "$do your worst");
    assert_eq!(c.send("GET temp"), "(nil)");
    assert_eq!(c.send("LEN"), ":2");
}

#[test]
fn compaction_survives_a_crash() {
    let wal = tmp_wal("compact");

    let server = Server::start(&wal);
    let mut c = server.client();
    for i in 0..200 {
        assert_eq!(c.send(&format!("SET counter {i}")), "+OK");
    }
    let before = std::fs::metadata(&wal).unwrap().len();
    assert_eq!(c.send("COMPACT"), "+OK");
    assert!(std::fs::metadata(&wal).unwrap().len() < before / 50);
    assert_eq!(c.send("SET after compact"), "+OK");
    server.crash();

    let server = Server::start(&wal);
    let mut c = server.client();
    assert_eq!(c.send("GET counter"), "$199");
    assert_eq!(c.send("GET after"), "$compact");
}

#[test]
fn concurrent_clients_lose_no_writes() {
    let wal = tmp_wal("concurrent");
    let server = Server::start(&wal);

    let workers: Vec<_> = (0..8)
        .map(|w| {
            let mut c = server.client();
            thread::spawn(move || {
                for i in 0..100 {
                    assert_eq!(c.send(&format!("SET w{w}:{i} {i}")), "+OK");
                }
            })
        })
        .collect();
    for w in workers {
        w.join().unwrap();
    }
    assert_eq!(server.client().send("LEN"), ":800");
    server.crash();

    let server = Server::start(&wal);
    let mut c = server.client();
    assert_eq!(c.send("LEN"), ":800");
    assert_eq!(c.send("GET w7:99"), "$99");
}

#[test]
fn bad_commands_get_errors_not_disconnects() {
    let server = Server::start(&tmp_wal("errors"));
    let mut c = server.client();
    assert_eq!(c.send("FLY away"), "-ERR unknown command 'FLY'");
    assert_eq!(c.send("SET lonely"), "-ERR usage: SET key value");
    assert_eq!(c.send("PING"), "+PONG");
    assert_eq!(c.send("QUIT"), "+BYE");
}
