//! TCP server: one thread per client, all sharing one Store.

use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;

use crate::protocol::{self, Command};
use crate::store::Store;

/// Accepts connections forever, handling each on its own thread.
pub fn serve(listener: TcpListener, store: Arc<Store>) -> io::Result<()> {
    for conn in listener.incoming() {
        let stream = match conn {
            Ok(s) => s,
            Err(e) => {
                eprintln!("accept failed: {e}");
                continue;
            }
        };
        let store = Arc::clone(&store);
        thread::spawn(move || {
            if let Err(e) = handle(stream, &store) {
                eprintln!("client error: {e}");
            }
        });
    }
    Ok(())
}

fn handle(stream: TcpStream, store: &Store) -> io::Result<()> {
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut writer = BufWriter::new(stream);
    let mut line = String::new();

    loop {
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            return Ok(()); // client hung up
        }
        let reply = match protocol::parse(&line) {
            Ok(Command::Quit) => {
                writeln!(writer, "+BYE")?;
                return writer.flush();
            }
            Ok(cmd) => execute(cmd, store),
            Err(msg) => format!("-ERR {msg}"),
        };
        writeln!(writer, "{reply}")?;
        writer.flush()?;
    }
}

fn execute(cmd: Command, store: &Store) -> String {
    let result = match cmd {
        Command::Ping => Ok("+PONG".to_string()),
        Command::Get(k) => Ok(store.get(&k).map_or("(nil)".into(), |v| format!("${v}"))),
        Command::Set(k, v) => store.set(k, v).map(|()| "+OK".into()),
        Command::Del(k) => store.del(&k).map(|hit| format!(":{}", u8::from(hit))),
        Command::Len => Ok(format!(":{}", store.len())),
        Command::Compact => store.compact().map(|()| "+OK".into()),
        Command::Quit => unreachable!("handled by the connection loop"),
    };
    result.unwrap_or_else(|e| format!("-ERR storage: {e}"))
}
