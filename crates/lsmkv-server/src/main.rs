//! Serves the LSM-tree store over the Redis wire protocol.
//!
//! ```text
//! lsmkv-server [--dir PATH] [--port N] [--sync always|group|<milliseconds>]
//!              [--memtable-bytes N] [--memtable btree|skiplist]
//! ```

use std::net::{Ipv4Addr, SocketAddr};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use lsmkv::{Config, Engine, MemtableKind, SyncPolicy};
use lsmkv_server::server;

const USAGE: &str = "\
lsmkv-server [options]

  --dir PATH              data directory (default: data)
  --port N                port to listen on (default: 6379)
  --sync POLICY           always, group, or a number of milliseconds for the
                          interval policy (default: group)
  --memtable-bytes N      bytes held in memory before a flush (default: 4194304)
  --memtable KIND         btree or skiplist (default: btree)
  --help                  print this
";

/// What the server was asked to do.
struct Options {
    dir: String,
    port: u16,
    config: Config,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            dir: "data".to_owned(),
            port: 6379,
            config: Config::default(),
        }
    }
}

fn main() -> ExitCode {
    let options = match parse_args(std::env::args().skip(1)) {
        Ok(Some(options)) => options,
        Ok(None) => {
            print!("{USAGE}");
            return ExitCode::SUCCESS;
        }
        Err(message) => {
            eprintln!("lsmkv-server: {message}\n\n{USAGE}");
            return ExitCode::FAILURE;
        }
    };

    match serve(&options) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("lsmkv-server: {err}");
            ExitCode::FAILURE
        }
    }
}

fn serve(options: &Options) -> Result<(), Box<dyn std::error::Error>> {
    let engine = Arc::new(Engine::open(&options.dir, options.config)?);
    let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, options.port));

    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(server::run(Arc::clone(&engine), addr))?;

    // Connections hold the store too, so the runtime goes first; the last drop
    // is what flushes the log and joins the background thread.
    drop(runtime);
    drop(engine);
    Ok(())
}

/// Reads the options, or `None` when help was asked for.
fn parse_args(args: impl Iterator<Item = String>) -> Result<Option<Options>, String> {
    let mut options = Options::default();
    let mut args = args.peekable();

    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--help" | "-h" => return Ok(None),
            "--dir" => options.dir = value(&mut args, &flag)?,
            "--port" => {
                options.port = value(&mut args, &flag)?
                    .parse()
                    .map_err(|_| "--port takes a port number".to_owned())?;
            }
            "--memtable-bytes" => {
                options.config.memtable_bytes = value(&mut args, &flag)?
                    .parse()
                    .map_err(|_| "--memtable-bytes takes a number of bytes".to_owned())?;
            }
            "--sync" => options.config.sync = sync_policy(&value(&mut args, &flag)?)?,
            "--memtable" => options.config.memtable = memtable_kind(&value(&mut args, &flag)?)?,
            other => return Err(format!("unknown option {other}")),
        }
    }
    Ok(Some(options))
}

fn value(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<String, String> {
    args.next().ok_or_else(|| format!("{flag} takes a value"))
}

fn memtable_kind(text: &str) -> Result<MemtableKind, String> {
    match text {
        "btree" => Ok(MemtableKind::BTree),
        "skiplist" => Ok(MemtableKind::Skiplist),
        _ => Err("--memtable takes btree or skiplist".to_owned()),
    }
}

fn sync_policy(text: &str) -> Result<SyncPolicy, String> {
    match text {
        "always" => Ok(SyncPolicy::Always),
        "group" => Ok(SyncPolicy::Group),
        other => other
            .parse()
            .map(|ms| SyncPolicy::Interval(Duration::from_millis(ms)))
            .map_err(|_| "--sync takes always, group, or a number of milliseconds".to_owned()),
    }
}
