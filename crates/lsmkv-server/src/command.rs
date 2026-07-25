//! Turning commands into replies.
//!
//! Every command runs against the store on a blocking thread, so nothing here
//! is async and all of it is testable without a socket.
//!
//! The set is deliberately small: `PING`, `GET`, `SET` and `DEL` are the store,
//! and the handful that follow are what `redis-cli` and `redis-benchmark` probe
//! on connection. Anything else gets the same error Redis would give, rather
//! than a plausible answer this store cannot honour.

use lsmkv::Engine;

use crate::resp::{Command, Reply};

const SERVER_NAME: &str = "lsmkv";
const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Runs one command against the store.
pub fn execute(engine: &Engine, command: &Command) -> Reply {
    match command.name.as_str() {
        "PING" => ping(command),
        "GET" => get(engine, command),
        "SET" => set(engine, command),
        "DEL" => del(engine, command),
        "COMMAND" => Reply::Array(Vec::new()),
        "CONFIG" => config(command),
        "HELLO" => hello(command),
        "INFO" => Reply::Bulk(info(engine).into_bytes()),
        "SELECT" => select(command),
        "QUIT" => ok(),
        _ => unknown(command),
    }
}

fn ping(command: &Command) -> Reply {
    match command.args.len() {
        0 => Reply::Simple("PONG".to_owned()),
        1 => Reply::Bulk(command.args[0].clone()),
        _ => arity("ping"),
    }
}

fn get(engine: &Engine, command: &Command) -> Reply {
    let Some(key) = command.arg(0).filter(|_| command.args.len() == 1) else {
        return arity("get");
    };
    match engine.get(key) {
        Ok(Some(value)) => Reply::Bulk(value),
        Ok(None) => Reply::Nil,
        Err(err) => failed(&err),
    }
}

fn set(engine: &Engine, command: &Command) -> Reply {
    if command.args.len() > 2 {
        // EX, NX, KEEPTTL and the rest are Redis semantics this store does not
        // implement, and answering OK without honouring them would be a lie.
        return Reply::Error("ERR syntax error".to_owned());
    }
    let (Some(key), Some(value)) = (command.arg(0), command.arg(1)) else {
        return arity("set");
    };
    match engine.set(key, value) {
        Ok(()) => ok(),
        Err(err) => failed(&err),
    }
}

fn del(engine: &Engine, command: &Command) -> Reply {
    if command.args.is_empty() {
        return arity("del");
    }

    let mut removed = 0;
    for key in &command.args {
        // Redis counts the keys that were actually there, so each one is looked
        // up first. The count can be off by a concurrent write to the same key;
        // the delete itself is not.
        match engine.get(key) {
            Ok(Some(_)) => removed += 1,
            Ok(None) => continue,
            Err(err) => return failed(&err),
        }
        if let Err(err) = engine.delete(key) {
            return failed(&err);
        }
    }
    Reply::Integer(removed)
}

fn config(command: &Command) -> Reply {
    match command.arg(0).map(<[u8]>::to_ascii_uppercase).as_deref() {
        // No parameter is settable, so every lookup comes back empty, which is
        // what Redis answers for a name it does not know.
        Some(b"GET") => Reply::Array(Vec::new()),
        _ => Reply::Error("ERR only CONFIG GET is supported".to_owned()),
    }
}

fn hello(command: &Command) -> Reply {
    if command.arg(0).is_some_and(|version| version != b"2") {
        return Reply::Error(
            "NOPROTO unsupported protocol version, this server speaks RESP2".to_owned(),
        );
    }

    Reply::Array(vec![
        Reply::Bulk(b"server".to_vec()),
        Reply::Bulk(SERVER_NAME.as_bytes().to_vec()),
        Reply::Bulk(b"version".to_vec()),
        Reply::Bulk(SERVER_VERSION.as_bytes().to_vec()),
        Reply::Bulk(b"proto".to_vec()),
        Reply::Integer(2),
        Reply::Bulk(b"mode".to_vec()),
        Reply::Bulk(b"standalone".to_vec()),
        Reply::Bulk(b"role".to_vec()),
        Reply::Bulk(b"master".to_vec()),
        Reply::Bulk(b"modules".to_vec()),
        Reply::Array(Vec::new()),
    ])
}

/// What the store is doing, in the shape Redis uses for `INFO`.
fn info(engine: &Engine) -> String {
    let levels: Vec<String> = engine
        .level_sizes()
        .iter()
        .map(ToString::to_string)
        .collect();

    format!(
        "# Server\r\n\
         server:{SERVER_NAME}\r\n\
         version:{SERVER_VERSION}\r\n\
         proto:2\r\n\
         mode:standalone\r\n\
         role:master\r\n\
         \r\n\
         # Engine\r\n\
         files:{}\r\n\
         files_per_level:{}\r\n\
         last_sequence:{}\r\n\
         block_reads:{}\r\n\
         bytes_written:{}\r\n",
        engine.table_count(),
        levels.join(","),
        engine.last_sequence(),
        engine.block_reads(),
        engine.bytes_written(),
    )
}

fn select(command: &Command) -> Reply {
    match command.arg(0) {
        Some(b"0") => ok(),
        Some(_) => Reply::Error("ERR DB index is out of range".to_owned()),
        None => arity("select"),
    }
}

fn unknown(command: &Command) -> Reply {
    Reply::Error(format!(
        "ERR unknown command '{}', with args beginning with:",
        command.name
    ))
}

fn arity(name: &str) -> Reply {
    Reply::Error(format!(
        "ERR wrong number of arguments for '{name}' command"
    ))
}

fn failed(err: &lsmkv::Error) -> Reply {
    Reply::Error(format!("ERR the store failed: {err}"))
}

fn ok() -> Reply {
    Reply::Simple("OK".to_owned())
}

#[cfg(test)]
mod tests {
    use lsmkv::Config;

    use super::*;

    /// A store in a directory of its own, removed when the test ends.
    struct Store {
        dir: std::path::PathBuf,
        engine: Engine,
    }

    impl Store {
        fn new() -> Self {
            static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let dir = std::env::temp_dir().join(format!(
                "lsmkv-command-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            ));
            let engine = Engine::open(&dir, Config::default()).expect("open the store");
            Self { dir, engine }
        }

        fn run(&self, name: &str, args: &[&[u8]]) -> Reply {
            execute(
                &self.engine,
                &Command {
                    name: name.to_owned(),
                    args: args.iter().map(|arg| arg.to_vec()).collect(),
                },
            )
        }
    }

    impl Drop for Store {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    fn is_error(reply: &Reply, needle: &str) -> bool {
        matches!(reply, Reply::Error(text) if text.contains(needle))
    }

    #[test]
    fn a_value_is_set_and_read_back() {
        let store = Store::new();

        assert_eq!(store.run("SET", &[b"k", b"v"]), Reply::Simple("OK".into()));
        assert_eq!(store.run("GET", &[b"k"]), Reply::Bulk(b"v".to_vec()));
        assert_eq!(store.run("GET", &[b"missing"]), Reply::Nil);
    }

    #[test]
    fn del_reports_how_many_keys_were_there() {
        let store = Store::new();
        store.run("SET", &[b"a", b"1"]);
        store.run("SET", &[b"b", b"2"]);

        assert_eq!(store.run("DEL", &[b"a", b"b", b"c"]), Reply::Integer(2));
        assert_eq!(store.run("GET", &[b"a"]), Reply::Nil);
        assert_eq!(store.run("DEL", &[b"a"]), Reply::Integer(0));
    }

    #[test]
    fn ping_answers_with_or_without_a_message() {
        let store = Store::new();

        assert_eq!(store.run("PING", &[]), Reply::Simple("PONG".into()));
        assert_eq!(
            store.run("PING", &[b"hello"]),
            Reply::Bulk(b"hello".to_vec())
        );
    }

    #[test]
    fn the_wrong_number_of_arguments_is_refused() {
        let store = Store::new();

        assert!(is_error(&store.run("GET", &[]), "wrong number"));
        assert!(is_error(&store.run("GET", &[b"a", b"b"]), "wrong number"));
        assert!(is_error(&store.run("SET", &[b"a"]), "wrong number"));
        assert!(is_error(&store.run("DEL", &[]), "wrong number"));
    }

    #[test]
    fn a_set_option_this_store_cannot_honour_is_refused() {
        let store = Store::new();

        assert!(is_error(
            &store.run("SET", &[b"k", b"v", b"EX", b"10"]),
            "syntax error"
        ));
        assert_eq!(store.run("GET", &[b"k"]), Reply::Nil, "nothing was written");
    }

    #[test]
    fn keys_and_values_may_be_any_bytes() {
        let store = Store::new();
        let key = [0u8, 0xFF, b'\r', b'\n'];

        store.run("SET", &[&key, &[0, 0]]);

        assert_eq!(store.run("GET", &[&key]), Reply::Bulk(vec![0, 0]));
    }

    #[test]
    fn an_unknown_command_is_named_in_the_error() {
        let store = Store::new();

        assert!(is_error(&store.run("SUBSCRIBE", &[]), "unknown command"));
        assert!(is_error(&store.run("SUBSCRIBE", &[]), "SUBSCRIBE"));
    }

    #[test]
    fn the_handshake_commands_answer() {
        let store = Store::new();

        assert_eq!(store.run("COMMAND", &[b"DOCS"]), Reply::Array(Vec::new()));
        assert_eq!(
            store.run("CONFIG", &[b"GET", b"maxmemory"]),
            Reply::Array(Vec::new())
        );
        assert_eq!(store.run("SELECT", &[b"0"]), Reply::Simple("OK".into()));
        assert!(is_error(&store.run("SELECT", &[b"3"]), "out of range"));
    }

    #[test]
    fn hello_speaks_resp2_and_refuses_resp3() {
        let store = Store::new();

        let Reply::Array(items) = store.run("HELLO", &[]) else {
            panic!("HELLO must answer with an array");
        };
        assert_eq!(items[0], Reply::Bulk(b"server".to_vec()));
        assert!(items.contains(&Reply::Integer(2)), "the protocol is RESP2");

        assert!(is_error(&store.run("HELLO", &[b"3"]), "NOPROTO"));
    }

    #[test]
    fn info_reports_what_the_store_is_doing() {
        let store = Store::new();
        store.run("SET", &[b"k", b"v"]);

        let Reply::Bulk(bytes) = store.run("INFO", &[]) else {
            panic!("INFO must answer with a bulk string");
        };
        let text = String::from_utf8(bytes).expect("utf8");

        assert!(text.contains("server:lsmkv"), "{text}");
        assert!(text.contains("last_sequence:1"), "{text}");
        assert!(text.contains("files:0"), "{text}");
    }
}
