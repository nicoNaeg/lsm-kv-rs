//! The Redis wire protocol, RESP2, written by hand.
//!
//! A request is an array of bulk strings:
//!
//! ```text
//! *3\r\n$3\r\nSET\r\n$5\r\nuser1\r\n$7\r\nnicolas\r\n
//! ```
//!
//! Clients may also send an inline request, a bare line of space-separated
//! words, which is what `redis-benchmark` uses for its `PING_INLINE` test.
//!
//! Parsing is incremental: [`parse`] is handed whatever bytes have arrived and
//! reports either one request and its length, or that more bytes are needed.
//! That is what lets one read carry a whole pipeline of commands.

use std::fmt;

/// Largest number of arguments a request may carry, as in Redis.
const MAX_ARGS: usize = 1024 * 1024;
/// Largest bulk string, as in Redis.
const MAX_BULK: usize = 512 * 1024 * 1024;
/// Largest inline request, as in Redis.
const MAX_INLINE: usize = 64 * 1024;

/// A request that does not follow the protocol.
///
/// The connection is answered with this and then closed, which is what Redis
/// does: once the byte stream is out of step there is no way back into it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtocolError(String);

impl ProtocolError {
    fn new(detail: &str) -> Self {
        Self(format!("Protocol error: {detail}"))
    }

    /// The message, as it goes out on the wire.
    pub fn message(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ProtocolError {}

/// A command and its arguments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Command {
    /// Command name, upper-cased, since Redis command names are case
    /// insensitive.
    pub name: String,
    /// Arguments, exactly as they arrived. Keys and values are binary.
    pub args: Vec<Vec<u8>>,
}

impl Command {
    /// The argument at `index`, if there is one.
    pub fn arg(&self, index: usize) -> Option<&[u8]> {
        self.args.get(index).map(Vec::as_slice)
    }
}

/// One request read off a connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Request {
    /// A command to run.
    Command(Command),
    /// An empty array, which Redis consumes and answers with nothing.
    Empty,
}

/// What one reply can be.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reply {
    /// A status line, `+OK`.
    Simple(String),
    /// An error line, `-ERR ...`.
    Error(String),
    /// A number, `:42`.
    Integer(i64),
    /// A byte string, `$5\r\nhello`.
    Bulk(Vec<u8>),
    /// The absence of a value, `$-1`.
    Nil,
    /// An array of replies.
    Array(Vec<Reply>),
}

impl Reply {
    /// Appends this reply to `out` in wire form.
    pub fn encode(&self, out: &mut Vec<u8>) {
        match self {
            Self::Simple(text) => {
                out.push(b'+');
                out.extend_from_slice(text.as_bytes());
                out.extend_from_slice(b"\r\n");
            }
            Self::Error(text) => {
                out.push(b'-');
                out.extend_from_slice(text.as_bytes());
                out.extend_from_slice(b"\r\n");
            }
            Self::Integer(value) => {
                out.push(b':');
                out.extend_from_slice(value.to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
            }
            Self::Bulk(bytes) => {
                out.push(b'$');
                out.extend_from_slice(bytes.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                out.extend_from_slice(bytes);
                out.extend_from_slice(b"\r\n");
            }
            Self::Nil => out.extend_from_slice(b"$-1\r\n"),
            Self::Array(items) => {
                out.push(b'*');
                out.extend_from_slice(items.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                for item in items {
                    item.encode(out);
                }
            }
        }
    }
}

/// Reads one request off the front of `input`.
///
/// Returns the request and the bytes it took, or `None` when `input` does not
/// hold a whole one yet.
///
/// # Errors
///
/// Returns [`ProtocolError`] when the bytes cannot be a request at all, which
/// no amount of waiting would fix.
pub fn parse(input: &[u8]) -> Result<Option<(Request, usize)>, ProtocolError> {
    let Some(&first) = input.first() else {
        return Ok(None);
    };
    if first == b'*' {
        parse_array(input)
    } else {
        parse_inline(input)
    }
}

fn parse_array(input: &[u8]) -> Result<Option<(Request, usize)>, ProtocolError> {
    let Some((head, mut at)) = line(input, 1) else {
        return Ok(None);
    };
    let count = integer(head).ok_or_else(|| ProtocolError::new("invalid multibulk length"))?;
    if count <= 0 {
        return Ok(Some((Request::Empty, at)));
    }
    let count = usize::try_from(count)
        .ok()
        .filter(|count| *count <= MAX_ARGS)
        .ok_or_else(|| ProtocolError::new("invalid multibulk length"))?;

    let mut args = Vec::with_capacity(count);
    for _ in 0..count {
        match bulk(input, at)? {
            Some((bytes, next)) => {
                args.push(bytes.to_vec());
                at = next;
            }
            None => return Ok(None),
        }
    }

    let name = args.remove(0);
    Ok(Some((
        Request::Command(Command {
            name: String::from_utf8_lossy(&name).to_uppercase(),
            args,
        }),
        at,
    )))
}

/// Reads one `$len\r\nbytes\r\n` at `from`.
fn bulk(input: &[u8], from: usize) -> Result<Option<(&[u8], usize)>, ProtocolError> {
    match input.get(from) {
        None => return Ok(None),
        Some(b'$') => {}
        Some(_) => return Err(ProtocolError::new("expected '$', got something else")),
    }

    let Some((head, at)) = line(input, from + 1) else {
        return Ok(None);
    };
    let len = integer(head).ok_or_else(|| ProtocolError::new("invalid bulk length"))?;
    let len = usize::try_from(len)
        .ok()
        .filter(|len| *len <= MAX_BULK)
        .ok_or_else(|| ProtocolError::new("invalid bulk length"))?;

    // The payload and the terminator that follows it both have to be here.
    if input.len() < at + len + 2 {
        return Ok(None);
    }
    if &input[at + len..at + len + 2] != b"\r\n" {
        return Err(ProtocolError::new("unbalanced bulk string"));
    }
    Ok(Some((&input[at..at + len], at + len + 2)))
}

/// Reads a bare line of space-separated words.
fn parse_inline(input: &[u8]) -> Result<Option<(Request, usize)>, ProtocolError> {
    let Some(end) = input.iter().position(|byte| *byte == b'\n') else {
        if input.len() > MAX_INLINE {
            return Err(ProtocolError::new("too big inline request"));
        }
        return Ok(None);
    };

    let mut words = input[..end]
        .strip_suffix(b"\r")
        .unwrap_or(&input[..end])
        .split(u8::is_ascii_whitespace)
        .filter(|word| !word.is_empty());

    let Some(name) = words.next() else {
        return Ok(Some((Request::Empty, end + 1)));
    };
    Ok(Some((
        Request::Command(Command {
            name: String::from_utf8_lossy(name).to_uppercase(),
            args: words.map(<[u8]>::to_vec).collect(),
        }),
        end + 1,
    )))
}

/// The line starting at `from`, without its terminator, and the offset past it.
fn line(input: &[u8], from: usize) -> Option<(&[u8], usize)> {
    let rest = input.get(from..)?;
    let at = rest.windows(2).position(|pair| pair == b"\r\n")?;
    Some((&rest[..at], from + at + 2))
}

fn integer(bytes: &[u8]) -> Option<i64> {
    std::str::from_utf8(bytes).ok()?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn command(input: &[u8]) -> (Command, usize) {
        match parse(input).expect("parse") {
            Some((Request::Command(command), used)) => (command, used),
            other => panic!("expected a command, got {other:?}"),
        }
    }

    fn encoded(reply: &Reply) -> Vec<u8> {
        let mut out = Vec::new();
        reply.encode(&mut out);
        out
    }

    #[test]
    fn a_command_and_its_arguments_are_read_back() {
        let input = b"*3\r\n$3\r\nSET\r\n$5\r\nuser1\r\n$7\r\nnicolas\r\n";

        let (command, used) = command(input);

        assert_eq!(command.name, "SET");
        assert_eq!(command.args, vec![b"user1".to_vec(), b"nicolas".to_vec()]);
        assert_eq!(used, input.len());
    }

    #[test]
    fn a_command_name_is_upper_cased() {
        let (command, _) = command(b"*1\r\n$4\r\nping\r\n");

        assert_eq!(command.name, "PING");
    }

    #[test]
    fn every_prefix_of_a_command_is_incomplete() {
        let input = b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n";

        for len in 1..input.len() {
            assert_eq!(
                parse(&input[..len]),
                Ok(None),
                "{len} bytes should not parse yet"
            );
        }
        assert!(parse(input).expect("parse").is_some());
    }

    #[test]
    fn a_pipeline_is_read_one_command_at_a_time() {
        let input = b"*1\r\n$4\r\nPING\r\n*2\r\n$3\r\nGET\r\n$1\r\na\r\n";

        let (first, used) = command(input);
        assert_eq!(first.name, "PING");

        let (second, rest) = command(&input[used..]);
        assert_eq!(second.name, "GET");
        assert_eq!(used + rest, input.len());
    }

    #[test]
    fn an_inline_command_is_accepted() {
        let (command, used) = command(b"PING\r\n");

        assert_eq!(command.name, "PING");
        assert!(command.args.is_empty());
        assert_eq!(used, 6);
    }

    #[test]
    fn an_inline_command_carries_its_words() {
        let (command, _) = command(b"set  foo bar\r\n");

        assert_eq!(command.name, "SET");
        assert_eq!(command.args, vec![b"foo".to_vec(), b"bar".to_vec()]);
    }

    #[test]
    fn an_inline_command_without_a_terminator_waits() {
        assert_eq!(parse(b"PING"), Ok(None));
    }

    #[test]
    fn an_empty_line_is_a_request_with_nothing_in_it() {
        assert_eq!(parse(b"\r\n"), Ok(Some((Request::Empty, 2))));
        assert_eq!(parse(b"*0\r\n"), Ok(Some((Request::Empty, 4))));
        assert_eq!(parse(b"*-1\r\n"), Ok(Some((Request::Empty, 5))));
    }

    #[test]
    fn binary_values_survive_the_protocol() {
        let mut input = b"*3\r\n$3\r\nSET\r\n$3\r\n".to_vec();
        input.extend_from_slice(&[0, b'\r', 255]);
        input.extend_from_slice(b"\r\n$1\r\n\n\r\n");

        let (command, used) = command(&input);

        assert_eq!(command.arg(0), Some([0, b'\r', 255].as_slice()));
        assert_eq!(command.arg(1), Some(b"\n".as_slice()));
        assert_eq!(used, input.len());
    }

    #[test]
    fn a_length_that_is_not_a_number_is_a_protocol_error() {
        let err = parse(b"*x\r\n").expect_err("must reject");
        assert!(err.message().contains("multibulk"), "{err}");

        let err = parse(b"*1\r\n$x\r\n").expect_err("must reject");
        assert!(err.message().contains("bulk length"), "{err}");
    }

    #[test]
    fn a_bulk_length_past_the_limit_is_refused_without_allocating() {
        let err = parse(b"*1\r\n$999999999999\r\n").expect_err("must reject");
        assert!(err.message().contains("bulk length"), "{err}");
    }

    #[test]
    fn an_argument_count_past_the_limit_is_refused() {
        let err = parse(b"*99999999\r\n").expect_err("must reject");
        assert!(err.message().contains("multibulk"), "{err}");
    }

    #[test]
    fn an_argument_that_is_not_a_bulk_string_is_a_protocol_error() {
        let err = parse(b"*1\r\n+OK\r\n").expect_err("must reject");
        assert!(err.message().contains("expected '$'"), "{err}");
    }

    #[test]
    fn a_bulk_string_that_is_not_terminated_is_a_protocol_error() {
        let err = parse(b"*1\r\n$1\r\nabc\r\n").expect_err("must reject");
        assert!(err.message().contains("unbalanced"), "{err}");
    }

    #[test]
    fn every_reply_shape_encodes_to_its_wire_form() {
        assert_eq!(encoded(&Reply::Simple("OK".into())), b"+OK\r\n");
        assert_eq!(
            encoded(&Reply::Error("ERR nope".into())),
            b"-ERR nope\r\n".as_slice()
        );
        assert_eq!(encoded(&Reply::Integer(-42)), b":-42\r\n");
        assert_eq!(encoded(&Reply::Bulk(b"hello".to_vec())), b"$5\r\nhello\r\n");
        assert_eq!(encoded(&Reply::Bulk(Vec::new())), b"$0\r\n\r\n");
        assert_eq!(encoded(&Reply::Nil), b"$-1\r\n");
        assert_eq!(encoded(&Reply::Array(Vec::new())), b"*0\r\n");
        assert_eq!(
            encoded(&Reply::Array(vec![
                Reply::Integer(1),
                Reply::Bulk(b"a".to_vec())
            ])),
            b"*2\r\n:1\r\n$1\r\na\r\n".as_slice()
        );
    }

    #[test]
    fn a_value_with_a_terminator_in_it_encodes_by_length() {
        // A bulk reply is length prefixed, so nothing in it needs escaping.
        assert_eq!(
            encoded(&Reply::Bulk(b"a\r\nb".to_vec())),
            b"$4\r\na\r\nb\r\n".as_slice()
        );
    }
}
