//! A line-based text protocol, one command per line:
//!
//! ```text
//! PING            -> +PONG
//! SET key value   -> +OK          (value is the rest of the line, spaces allowed)
//! GET key         -> $value | (nil)
//! DEL key         -> :1 | :0
//! LEN             -> :count
//! COMPACT         -> +OK
//! QUIT            -> +BYE, then the server closes the connection
//! ```
//! Errors come back as `-ERR message`. Command names are case-insensitive.

#[derive(Debug, PartialEq, Eq)]
pub enum Command {
    Ping,
    Get(String),
    Set(String, String),
    Del(String),
    Len,
    Compact,
    Quit,
}

pub fn parse(line: &str) -> Result<Command, String> {
    let line = line.trim_end_matches(['\r', '\n']);
    let (name, rest) = match line.split_once(' ') {
        Some((n, r)) => (n, r),
        None => (line, ""),
    };
    let mut args = rest.splitn(2, ' ');
    let key = args.next().filter(|k| !k.is_empty());

    match name.to_ascii_uppercase().as_str() {
        "PING" => Ok(Command::Ping),
        "LEN" => Ok(Command::Len),
        "COMPACT" => Ok(Command::Compact),
        "QUIT" => Ok(Command::Quit),
        "GET" => key
            .map(|k| Command::Get(k.into()))
            .ok_or_else(|| "usage: GET key".into()),
        "DEL" => key
            .map(|k| Command::Del(k.into()))
            .ok_or_else(|| "usage: DEL key".into()),
        "SET" => match (key, args.next()) {
            (Some(k), Some(v)) => Ok(Command::Set(k.into(), v.into())),
            _ => Err("usage: SET key value".into()),
        },
        "" => Err("empty command".into()),
        other => Err(format!("unknown command '{other}'")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_every_command() {
        assert_eq!(parse("PING"), Ok(Command::Ping));
        assert_eq!(parse("get user:1\r\n"), Ok(Command::Get("user:1".into())));
        assert_eq!(parse("DEL k"), Ok(Command::Del("k".into())));
        assert_eq!(parse("len"), Ok(Command::Len));
        assert_eq!(parse("COMPACT"), Ok(Command::Compact));
        assert_eq!(parse("QUIT"), Ok(Command::Quit));
    }

    #[test]
    fn set_value_keeps_spaces() {
        assert_eq!(
            parse("SET greeting hello there, world"),
            Ok(Command::Set("greeting".into(), "hello there, world".into()))
        );
    }

    #[test]
    fn set_allows_an_empty_value() {
        assert_eq!(parse("SET k "), Ok(Command::Set("k".into(), "".into())));
    }

    #[test]
    fn rejects_bad_input() {
        assert!(parse("").is_err());
        assert!(parse("GET").is_err());
        assert!(parse("SET onlykey").is_err());
        assert_eq!(parse("FLY away"), Err("unknown command 'FLY'".into()));
    }
}
