//! The TCP server.
//!
//! One task per connection, and one hop to a blocking thread per batch of
//! commands rather than per command: a read that carries a pipeline of sixteen
//! pays the hop once. The store itself is synchronous, so it never runs on a
//! reactor thread, where a 3.8 ms device flush would stall every other
//! connection that thread carries.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use lsmkv::Engine;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task;

use crate::command;
use crate::resp::{self, Command, Reply, Request};

/// Bytes taken off a connection in one read.
const READ_CHUNK: usize = 16 * 1024;

/// Serves `engine` on `addr` until ctrl-c.
///
/// # Errors
///
/// Returns the error that binding or accepting failed with. A failure on one
/// connection ends that connection and is reported on stderr.
pub async fn run(engine: Arc<Engine>, addr: SocketAddr) -> io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    println!("lsmkv listening on {}", listener.local_addr()?);

    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, peer) = accepted?;
                let engine = Arc::clone(&engine);
                task::spawn(async move {
                    if let Err(err) = serve(engine, stream).await {
                        eprintln!("connection {peer} ended: {err}");
                    }
                });
            }
            result = tokio::signal::ctrl_c() => {
                result?;
                println!("stopping");
                return Ok(());
            }
        }
    }
}

/// Reads commands off one connection until it closes.
async fn serve(engine: Arc<Engine>, mut stream: TcpStream) -> io::Result<()> {
    // Replies are small and immediate, so waiting to coalesce them only adds
    // latency.
    stream.set_nodelay(true)?;

    let mut chunk = vec![0u8; READ_CHUNK];
    let mut input = Vec::new();
    let mut output = Vec::new();

    loop {
        let read = stream.read(&mut chunk).await?;
        if read == 0 {
            return Ok(());
        }
        input.extend_from_slice(&chunk[..read]);

        let batch = take_batch(&mut input);
        let closing = batch.commands.iter().any(|command| command.name == "QUIT");

        if !batch.commands.is_empty() {
            for reply in run_batch(&engine, batch.commands).await? {
                reply.encode(&mut output);
            }
        }
        if let Some(message) = &batch.protocol_error {
            Reply::Error(message.clone()).encode(&mut output);
        }

        if !output.is_empty() {
            stream.write_all(&output).await?;
            output.clear();
        }
        // A stream that broke the protocol cannot be read from any further: the
        // byte after the mistake is not a request boundary.
        if closing || batch.protocol_error.is_some() {
            return Ok(());
        }
    }
}

/// Everything that could be parsed out of the buffer in one pass.
struct Batch {
    commands: Vec<Command>,
    protocol_error: Option<String>,
}

/// Takes every complete request off the front of `input`.
fn take_batch(input: &mut Vec<u8>) -> Batch {
    let mut commands = Vec::new();
    let mut protocol_error = None;
    let mut consumed = 0;

    loop {
        match resp::parse(&input[consumed..]) {
            Ok(Some((Request::Command(command), used))) => {
                commands.push(command);
                consumed += used;
            }
            Ok(Some((Request::Empty, used))) => consumed += used,
            Ok(None) => break,
            Err(err) => {
                protocol_error = Some(err.message().to_owned());
                break;
            }
        }
    }

    input.drain(..consumed);
    Batch {
        commands,
        protocol_error,
    }
}

/// Runs a batch of commands on a blocking thread.
async fn run_batch(engine: &Arc<Engine>, commands: Vec<Command>) -> io::Result<Vec<Reply>> {
    let engine = Arc::clone(engine);
    task::spawn_blocking(move || {
        commands
            .iter()
            .map(|command| command::execute(&engine, command))
            .collect()
    })
    .await
    .map_err(|err| io::Error::other(format!("running commands failed: {err}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn batch(bytes: &[u8]) -> (Batch, Vec<u8>) {
        let mut input = bytes.to_vec();
        let batch = take_batch(&mut input);
        (batch, input)
    }

    #[test]
    fn a_batch_takes_every_whole_command() {
        let (batch, left) = batch(b"*1\r\n$4\r\nPING\r\n*2\r\n$3\r\nGET\r\n$1\r\na\r\n");

        assert_eq!(batch.commands.len(), 2);
        assert_eq!(batch.commands[0].name, "PING");
        assert_eq!(batch.commands[1].name, "GET");
        assert!(left.is_empty());
        assert!(batch.protocol_error.is_none());
    }

    #[test]
    fn a_partial_command_is_left_in_the_buffer() {
        let (batch, left) = batch(b"*1\r\n$4\r\nPING\r\n*2\r\n$3\r\nGE");

        assert_eq!(batch.commands.len(), 1);
        assert_eq!(left, b"*2\r\n$3\r\nGE", "the tail waits for the next read");
    }

    #[test]
    fn an_empty_request_is_consumed_without_a_command() {
        let (batch, left) = batch(b"*0\r\n*1\r\n$4\r\nPING\r\n");

        assert_eq!(batch.commands.len(), 1);
        assert!(left.is_empty());
    }

    #[test]
    fn a_protocol_error_stops_the_batch_where_it_happened() {
        let (batch, _) = batch(b"*1\r\n$4\r\nPING\r\n*1\r\n+OK\r\n");

        assert_eq!(batch.commands.len(), 1, "what came before still runs");
        assert!(
            batch
                .protocol_error
                .as_ref()
                .is_some_and(|message| message.contains("Protocol error")),
            "{:?}",
            batch.protocol_error
        );
    }

    #[test]
    fn an_empty_buffer_yields_nothing() {
        let (batch, _) = batch(b"");

        assert!(batch.commands.is_empty());
        assert!(batch.protocol_error.is_none());
    }
}
