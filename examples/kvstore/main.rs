//! A replicated key-value store on top of vsr-rs.
//!
//! Every node runs one replica. Nodes exchange protocol messages over TCP
//! using a one-line-per-message text encoding, and every node also accepts
//! client connections speaking a Redis-like inline protocol, so `nc` works:
//!
//! ```text
//! SET foo bar
//! +OK
//! GET foo
//! $3
//! bar
//! ```
//!
//! A SET goes through the replicated log. A GET is a query: the primary
//! answers it from its state once a quorum has confirmed its view, so it
//! is linearizable without a log entry or an fsync.
//!
//! Each node keeps two things on disk. The replica's log and counters go to
//! a write-ahead log through the `writeahead` crate, whose writer thread
//! writes what the replica changed and fsyncs once, while the event loop
//! steps on and sends only what need not wait for the write. The store
//! keeps what the operations write, the clients' replies among it, in
//! memory. Once a second a flush on a thread of its own writes all of it
//! to a `fjall` database in one atomic batch that also records the op
//! number it reaches, and fsyncs. When the flush lands, the replica
//! compacts its log up to there, less a retention window. On restart the
//! node replays the write-ahead log, opens the store, persists whatever the
//! store recovered, and applies the committed entries the store had not
//! made durable. See README.md next to this file.
//!
//! The node itself, with its store and event loop, is in `node.rs`, which
//! the benchmark shares; this file puts it on the network.

use log::{debug, info, warn};
use std::cell::Cell;
use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::mpsc::{channel, sync_channel, Receiver, Sender, SyncSender, TrySendError};
use std::thread;
use std::time::{Duration, Instant};
use vsr_rs::{LogBase, LogSegment, Message, RecoveryState, ReplicaID, Reply};

mod journal;
mod node;

use journal::push_number;
use node::*;

#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

// ---------------------------------------------------------------------------
// Wire encoding between nodes: one message per line, whitespace separated.

fn encode_chunk(chunk: &StoreChunk) -> String {
    let mut out = chunk.clients.len().to_string();
    for record in &chunk.clients {
        out.push_str(&format!(" {} {}", record.client_id, encode_client(record)));
    }
    out.push_str(&format!(" {}", chunk.pairs.len()));
    for (key, value) in &chunk.pairs {
        out.push_str(&format!(" {key} {value}"));
    }
    out
}

fn encode_segment(segment: &KvSegment) -> String {
    let base = match &segment.base {
        LogBase::Op(op_number) => format!("- {op_number}"),
        LogBase::Checkpoint(op_number) => format!("+ {op_number}"),
    };
    format!("{base} {}", encode_entries(&segment.entries))
}

fn encode(frame: &Frame) -> String {
    match frame {
        Frame::Message(message) => match message {
            Message::Register { client_id } => format!("REGISTER {client_id}"),
            Message::Request {
                client_id,
                session,
                request_number,
                answered,
                op,
            } => {
                let mut out = "REQUEST".to_string();
                for number in [client_id, session, request_number, answered] {
                    out.push(' ');
                    push_number(&mut out, *number);
                }
                out.push(' ');
                write_op(&mut out, op);
                out
            }
            Message::Query {
                client_id,
                query_number,
                query,
            } => format!("QUERY {client_id} {query_number} {query}"),
            Message::ConfirmView { view_number, round } => {
                format!("CONFIRMVIEW {view_number} {round}")
            }
            Message::ConfirmViewOk {
                view_number,
                round,
                replica_id,
            } => format!("CONFIRMVIEWOK {view_number} {round} {replica_id}"),
            Message::Prepare {
                view_number,
                op_number,
                entry,
                commit_number,
            } => {
                let mut out = "PREPARE".to_string();
                for number in [view_number, op_number, commit_number] {
                    out.push(' ');
                    push_number(&mut out, *number);
                }
                out.push(' ');
                write_entry(&mut out, entry);
                out
            }
            Message::PrepareOk {
                view_number,
                op_number,
                replica_id,
            } => format!("PREPAREOK {view_number} {op_number} {replica_id}"),
            Message::Commit {
                view_number,
                commit_number,
            } => format!("COMMIT {view_number} {commit_number}"),
            Message::GetState {
                replica_id,
                view_number,
                op_number,
            } => format!("GETSTATE {replica_id} {view_number} {op_number}"),
            Message::NewState {
                replica_id,
                view_number,
                segment,
                commit_number,
            } => format!(
                "NEWSTATE {replica_id} {view_number} {commit_number} {}",
                encode_segment(segment)
            ),
            Message::GetChunk {
                replica_id,
                op_number,
                index,
            } => format!("GETCHUNK {replica_id} {op_number} {index}"),
            Message::NewChunk {
                replica_id,
                op_number,
                index,
                chunk,
                last,
            } => format!(
                "NEWCHUNK {replica_id} {op_number} {index} {} {}",
                u8::from(*last),
                encode_chunk(chunk)
            ),
            Message::StartViewChange {
                view_number,
                replica_id,
            } => format!("STARTVIEWCHANGE {view_number} {replica_id}"),
            Message::DoViewChange {
                view_number,
                replica_id,
                last_normal_view,
                segment,
                commit_number,
            } => format!(
                "DOVIEWCHANGE {view_number} {replica_id} {last_normal_view} {commit_number} {}",
                encode_segment(segment)
            ),
            Message::StartView {
                view_number,
                segment,
                commit_number,
            } => format!(
                "STARTVIEW {view_number} {commit_number} {}",
                encode_segment(segment)
            ),
            Message::Recovery {
                replica_id,
                nonce,
                view_number,
            } => format!("RECOVERY {replica_id} {nonce} {view_number}"),
            Message::RecoveryResponse {
                view_number,
                nonce,
                replica_id,
                state,
            } => match state {
                Some(state) => format!(
                    "RECOVERYRESPONSE {view_number} {nonce} {replica_id} + {} {}",
                    state.commit_number,
                    encode_segment(&state.segment)
                ),
                None => format!("RECOVERYRESPONSE {view_number} {nonce} {replica_id} -"),
            },
        },
        Frame::Reply(reply) => match reply {
            Reply::Registered {
                view_number,
                client_id,
                session,
            } => format!("REGISTERED {view_number} {client_id} {session}"),
            Reply::Executed {
                view_number,
                client_id,
                session,
                request_number,
                result,
            } => format!(
                "EXECUTED {view_number} {client_id} {session} {request_number} {}",
                encode_result(result)
            ),
            Reply::Evicted {
                view_number,
                client_id,
                session,
            } => format!("EVICTED {view_number} {client_id} {session}"),
            Reply::Queried {
                view_number,
                client_id,
                query_number,
                result,
            } => format!(
                "QUERIED {view_number} {client_id} {query_number} {}",
                encode_result(result)
            ),
        },
    }
}

impl<'a> Tokens<'a> {
    fn chunk(&mut self) -> Result<StoreChunk, String> {
        let count = self.num()?;
        let mut clients = Vec::with_capacity(count.min(RESERVED_MAX));
        for _ in 0..count {
            let client_id = self.num()?;
            clients.push(self.client(client_id)?);
        }
        let count = self.num()?;
        let mut pairs = Vec::with_capacity(count.min(RESERVED_MAX));
        for _ in 0..count {
            pairs.push((self.word()?.to_string(), self.word()?.to_string()));
        }
        Ok(StoreChunk { pairs, clients })
    }

    fn flag(&mut self) -> Result<bool, String> {
        match self.word()? {
            "0" => Ok(false),
            "1" => Ok(true),
            word => Err(format!("bad flag {word:?}")),
        }
    }

    fn segment(&mut self) -> Result<KvSegment, String> {
        let base = match self.word()? {
            "-" => LogBase::Op(self.num()?),
            "+" => LogBase::Checkpoint(self.num()?),
            word => return Err(format!("bad segment base {word:?}")),
        };
        let entries = self.entries()?;
        Ok(LogSegment { base, entries })
    }
}

fn decode(line: &str) -> Result<Frame, String> {
    let mut t = Tokens::new(line);
    let message = match t.word()? {
        "REGISTER" => Message::Register {
            client_id: t.num()?,
        },
        "REQUEST" => Message::Request {
            client_id: t.num()?,
            session: t.num()?,
            request_number: t.num()?,
            answered: t.num()?,
            op: t.op()?,
        },
        "QUERY" => Message::Query {
            client_id: t.num()?,
            query_number: t.num()?,
            query: t.word()?.to_string(),
        },
        "CONFIRMVIEW" => Message::ConfirmView {
            view_number: t.num()?,
            round: t.word()?.parse().map_err(|_| "bad round".to_string())?,
        },
        "CONFIRMVIEWOK" => Message::ConfirmViewOk {
            view_number: t.num()?,
            round: t.word()?.parse().map_err(|_| "bad round".to_string())?,
            replica_id: t.num()?,
        },
        "PREPARE" => Message::Prepare {
            view_number: t.num()?,
            op_number: t.num()?,
            commit_number: t.num()?,
            entry: t.entry()?,
        },
        "PREPAREOK" => Message::PrepareOk {
            view_number: t.num()?,
            op_number: t.num()?,
            replica_id: t.num()?,
        },
        "COMMIT" => Message::Commit {
            view_number: t.num()?,
            commit_number: t.num()?,
        },
        "GETSTATE" => Message::GetState {
            replica_id: t.num()?,
            view_number: t.num()?,
            op_number: t.num()?,
        },
        "NEWSTATE" => Message::NewState {
            replica_id: t.num()?,
            view_number: t.num()?,
            commit_number: t.num()?,
            segment: t.segment()?,
        },
        "GETCHUNK" => Message::GetChunk {
            replica_id: t.num()?,
            op_number: t.num()?,
            index: t.num()?,
        },
        "NEWCHUNK" => Message::NewChunk {
            replica_id: t.num()?,
            op_number: t.num()?,
            index: t.num()?,
            last: t.flag()?,
            chunk: t.chunk()?,
        },
        "STARTVIEWCHANGE" => Message::StartViewChange {
            view_number: t.num()?,
            replica_id: t.num()?,
        },
        "DOVIEWCHANGE" => Message::DoViewChange {
            view_number: t.num()?,
            replica_id: t.num()?,
            last_normal_view: t.num()?,
            commit_number: t.num()?,
            segment: t.segment()?,
        },
        "STARTVIEW" => Message::StartView {
            view_number: t.num()?,
            commit_number: t.num()?,
            segment: t.segment()?,
        },
        "RECOVERY" => Message::Recovery {
            replica_id: t.num()?,
            nonce: t.word()?.parse().map_err(|_| "bad nonce".to_string())?,
            view_number: t.num()?,
        },
        "RECOVERYRESPONSE" => {
            let view_number = t.num()?;
            let nonce = t.word()?.parse().map_err(|_| "bad nonce".to_string())?;
            let replica_id = t.num()?;
            let state = match t.word()? {
                "+" => Some(RecoveryState {
                    commit_number: t.num()?,
                    segment: t.segment()?,
                }),
                _ => None,
            };
            Message::RecoveryResponse {
                view_number,
                nonce,
                replica_id,
                state,
            }
        }
        "REGISTERED" => {
            return Ok(Frame::Reply(Reply::Registered {
                view_number: t.num()?,
                client_id: t.num()?,
                session: t.num()?,
            }))
        }
        "EXECUTED" => {
            return Ok(Frame::Reply(Reply::Executed {
                view_number: t.num()?,
                client_id: t.num()?,
                session: t.num()?,
                request_number: t.num()?,
                result: t.result()?,
            }))
        }
        "EVICTED" => {
            return Ok(Frame::Reply(Reply::Evicted {
                view_number: t.num()?,
                client_id: t.num()?,
                session: t.num()?,
            }))
        }
        "QUERIED" => {
            return Ok(Frame::Reply(Reply::Queried {
                view_number: t.num()?,
                client_id: t.num()?,
                query_number: t.num()?,
                result: t.result()?,
            }))
        }
        kind => return Err(format!("unknown message {kind:?}")),
    };
    Ok(Frame::Message(message))
}

// ---------------------------------------------------------------------------
// Networking between nodes

/// Frames queued for one node before further ones are dropped, which a
/// node that keeps reading drains.
const OUTBOX_FRAMES: usize = 4096;
/// The most frames a sender takes into one write, so that a steady stream
/// of them still goes out.
const MAX_FRAMES_PER_FLUSH: usize = 1024;
/// The most memory a sender keeps for its next write between batches; a
/// larger batch, of a checkpoint say, gives back the rest.
const MAX_KEPT_BATCH: usize = 1024 * 1024;

/// One queue and sender thread per other node, so that a node that stops
/// reading holds up only its own frames.
struct Outboxes {
    events: Sender<Event>,
    /// None for this node, whose frames go to its own event loop.
    peers: Vec<Option<Peer>>,
}

struct Peer {
    queue: SyncSender<Frame>,
    full: Cell<bool>,
}

impl Outboxes {
    fn new(self_id: ReplicaID, addresses: &[SocketAddr], events: Sender<Event>) -> Outboxes {
        let peers = addresses
            .iter()
            .enumerate()
            .map(|(dst, &address)| {
                (dst != self_id).then(|| {
                    let (queue, frames) = sync_channel(OUTBOX_FRAMES);
                    thread::spawn(move || run_sender(dst, address, frames));
                    Peer {
                        queue,
                        full: Cell::new(false),
                    }
                })
            })
            .collect();
        Outboxes { events, peers }
    }
}

impl Outbox for Outboxes {
    /// A full queue drops the frame; the protocol re-sends what matters.
    fn send_to(&self, dst: ReplicaID, frame: Frame) {
        let Some(peer) = &self.peers[dst] else {
            let _ = self.events.send(frame.into());
            return;
        };
        match peer.queue.try_send(frame) {
            Ok(()) => {
                if peer.full.replace(false) {
                    info!("node {dst} is taking frames again");
                }
            }
            Err(TrySendError::Full(_)) => {
                if !peer.full.replace(true) {
                    warn!("node {dst} is not keeping up, dropping its frames");
                }
            }
            Err(TrySendError::Disconnected(_)) => {}
        }
    }
}

/// Sends the frames queued for node `dst`, those queued at once in one
/// write, connecting on demand. A node that cannot be reached loses them.
fn run_sender(dst: ReplicaID, address: SocketAddr, frames: Receiver<Frame>) {
    let mut stream: Option<TcpStream> = None;
    let mut last_failure: Option<Instant> = None;
    let mut batch = Vec::new();
    while let Ok(first) = frames.recv() {
        let queued = std::iter::once(first)
            .chain(frames.try_iter())
            .take(MAX_FRAMES_PER_FLUSH);
        let backing_off = last_failure.is_some_and(|at| at.elapsed() < Duration::from_millis(500));
        if stream.is_none() && !backing_off {
            match connect(address) {
                Ok(connected) => {
                    info!("connected to node {dst} at {address}");
                    stream = Some(connected);
                }
                Err(err) => {
                    debug!("node {dst} unreachable: {err}");
                    last_failure = Some(Instant::now());
                }
            }
        }
        let Some(connected) = &mut stream else {
            queued.for_each(drop);
            continue;
        };
        for frame in queued {
            batch.extend_from_slice(encode(&frame).as_bytes());
            batch.push(b'\n');
        }
        if let Err(err) = connected.write_all(&batch) {
            // The node was reachable a moment ago: reconnect at once.
            warn!("lost connection to node {dst}: {err}");
            stream = None;
        }
        batch.clear();
        batch.shrink_to(MAX_KEPT_BATCH);
    }
}

fn connect(address: SocketAddr) -> std::io::Result<TcpStream> {
    let stream = TcpStream::connect_timeout(&address, Duration::from_millis(200))?;
    no_delay(&stream);
    Ok(stream)
}

/// Turns Nagle's algorithm off on `stream`: a write would otherwise wait
/// for the peer to acknowledge the one before, a round trip. A stream that
/// keeps it still works, only slower.
fn no_delay(stream: &TcpStream) {
    if let Err(err) = stream.set_nodelay(true) {
        warn!("cannot turn off Nagle's algorithm: {err}");
    }
}

/// Accepts connections from other nodes and feeds their frames to the event
/// loop.
fn run_peer_acceptor(listener: TcpListener, events: Sender<Event>) {
    for stream in listener.incoming().flatten() {
        let events = events.clone();
        thread::spawn(move || {
            for line in read_lines(BufReader::new(stream)).map_while(Result::ok) {
                match decode(&line) {
                    Ok(frame) => {
                        let _ = events.send(frame.into());
                    }
                    Err(err) => warn!("bad message from peer: {err}"),
                }
            }
        });
    }
}

/// The lines of `reader` that end in a newline, without it. A line the
/// stream cut off is dropped: a cut-off `PUT` or `SET` still parses, with
/// a shorter value.
fn read_lines(mut reader: impl BufRead) -> impl Iterator<Item = std::io::Result<String>> {
    std::iter::from_fn(move || {
        let mut line = String::new();
        match reader.read_line(&mut line) {
            Ok(_) if line.ends_with('\n') => {
                line.pop();
                if line.ends_with('\r') {
                    line.pop();
                }
                Some(Ok(line))
            }
            Ok(_) => None,
            Err(err) => Some(Err(err)),
        }
    })
}

// ---------------------------------------------------------------------------
// Client connections

fn parse_command(line: &str) -> Result<Option<Command>, String> {
    let words: Vec<&str> = line.split_whitespace().collect();
    let Some(name) = words.first() else {
        return Ok(None);
    };
    match (name.to_ascii_uppercase().as_str(), words.len()) {
        ("PING", 1) => {
            Err("+PONG".to_string()) // not an error, just a canned response
        }
        ("SET", 3) => Ok(Some(Command::Set(
            words[1].to_string(),
            words[2].to_string(),
        ))),
        ("SET", _) => Err("-ERR usage: SET key value".to_string()),
        ("GET", 2) => Ok(Some(Command::Get(words[1].to_string()))),
        ("GET", _) => Err("-ERR usage: GET key".to_string()),
        _ => Err(format!("-ERR unknown command {name:?}")),
    }
}

/// Commands a connection has read and not yet answered before it stops
/// reading more.
const CONNECTION_PIPELINE: usize = 1024;

/// Serves one client connection. Commands are read as they arrive and
/// answered in order, each once the replicated store has executed it; the
/// event loop answers a connection's commands in the order they came. When
/// the client stops sending, what it sent is answered first. However the
/// connection ends, the event loop is told it is gone.
fn run_client_connection(
    stream: TcpStream,
    connection: u64,
    events: Sender<Event>,
) -> std::io::Result<()> {
    no_delay(&stream);
    let writer = stream.try_clone()?;
    let (respond, answers) = channel::<String>();
    // For each line, in order: the event loop's next answer, or one
    // answered here.
    let (script, steps) = sync_channel::<Option<String>>(CONNECTION_PIPELINE);
    let writing = thread::spawn(move || write_responses(writer, steps, answers));
    let reader = BufReader::new(stream);
    // A read error breaks out of the loop rather than returning, or it
    // would carry us past the Event::Disconnect below and the event loop
    // would keep this connection's client for the life of the process.
    let mut outcome = Ok(());
    for line in read_lines(reader) {
        let line = match line {
            Ok(line) => line,
            Err(err) => {
                outcome = Err(err);
                break;
            }
        };
        let step = match parse_command(&line) {
            Ok(None) => continue,
            Ok(Some(command)) => {
                let command = Event::Command {
                    connection,
                    command,
                    respond: respond.clone(),
                };
                if script.send(None).is_err() {
                    break;
                }
                let _ = events.send(command);
                continue;
            }
            Err(canned) => Some(format!("{canned}\r\n")),
        };
        if script.send(step).is_err() {
            break;
        }
    }
    drop((script, respond));
    if outcome.is_ok() {
        let _ = writing.join();
    }
    let _ = events.send(Event::Disconnect(connection));
    outcome
}

/// Writes the responses in the order of `steps`. Ends when the connection
/// stops reading or writing, or the event loop drops a command it never
/// answers.
fn write_responses(
    mut writer: TcpStream,
    steps: Receiver<Option<String>>,
    answers: Receiver<String>,
) {
    for step in steps {
        let response = match step {
            Some(response) => response,
            None => match answers.recv() {
                Ok(answer) => answer,
                Err(_) => return,
            },
        };
        if writer.write_all(response.as_bytes()).is_err() {
            return;
        }
    }
}

fn run_client_acceptor(
    listener: TcpListener,
    node_id: ReplicaID,
    incarnation: u64,
    events: Sender<Event>,
) {
    for (next, stream) in listener.incoming().flatten().enumerate() {
        let connection = client_id(node_id, incarnation, next as u64);
        let events = events.clone();
        thread::spawn(move || {
            if let Err(err) = run_client_connection(stream, connection, events) {
                debug!("client connection {connection} closed: {err}");
            }
        });
    }
}

struct Args {
    id: ReplicaID,
    replicas: Vec<SocketAddr>,
    listen: SocketAddr,
    data_dir: PathBuf,
    start: Start,
}

fn parse_args() -> Result<Args, String> {
    let mut id = None;
    let mut replicas = None;
    let mut listen = None;
    let mut data_dir = None;
    let mut init = false;
    let mut recover = false;
    let mut view = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        let mut value = || args.next().ok_or_else(|| format!("{arg} needs a value"));
        match arg.as_str() {
            "--id" => id = Some(value()?.parse().map_err(|_| "bad --id")?),
            "--replicas" => {
                replicas = Some(
                    value()?
                        .split(',')
                        .map(|a| a.parse().map_err(|_| format!("bad address {a:?}")))
                        .collect::<Result<Vec<SocketAddr>, _>>()?,
                )
            }
            "--listen" => listen = Some(value()?.parse().map_err(|_| "bad --listen")?),
            "--data" => data_dir = Some(PathBuf::from(value()?)),
            "--init" => init = true,
            "--recover" => recover = true,
            "--view" => view = Some(value()?.parse().map_err(|_| "bad --view")?),
            _ => return Err(format!("unknown argument {arg}")),
        }
    }
    let usage = "usage: kvstore --id N --replicas ADDR,ADDR,... --listen ADDR [--data DIR] [--init | --recover --view N]";
    let id: ReplicaID = id.ok_or(usage)?;
    let start = match (init, recover, view) {
        (false, false, None) => Start::Restart,
        (true, false, None) => Start::Init,
        (false, true, Some(view)) => Start::Recover { view },
        _ => return Err(usage.to_string()),
    };
    let args = Args {
        id,
        replicas: replicas.ok_or(usage)?,
        listen: listen.ok_or(usage)?,
        data_dir: data_dir.unwrap_or_else(|| PathBuf::from(format!("kvstore-node-{id}"))),
        start,
    };
    if args.replicas.len() < 3 {
        return Err("--replicas needs at least three addresses".to_string());
    }
    if args.id >= args.replicas.len() {
        return Err("--id must index into --replicas".to_string());
    }
    Ok(args)
}

fn main() {
    env_logger::init();
    let args = match parse_args() {
        Ok(args) => args,
        Err(err) => {
            eprintln!("{err}");
            std::process::exit(2);
        }
    };

    let config = config(args.replicas.len());

    let mut node =
        Node::open(args.id, config, &args.data_dir, args.start).unwrap_or_else(|err| fatal(&err));

    let (events_tx, events_rx) = channel::<Event>();
    let outboxes = Outboxes::new(args.id, &args.replicas, events_tx.clone());
    {
        let listener = TcpListener::bind(args.replicas[args.id]).unwrap_or_else(|err| {
            eprintln!("cannot listen on {}: {err}", args.replicas[args.id]);
            std::process::exit(1);
        });
        let events = events_tx.clone();
        thread::spawn(move || run_peer_acceptor(listener, events));
    }
    {
        let listener = TcpListener::bind(args.listen).unwrap_or_else(|err| {
            eprintln!("cannot listen on {}: {err}", args.listen);
            std::process::exit(1);
        });
        let (node_id, incarnation, events) = (args.id, node.incarnation, events_tx.clone());
        thread::spawn(move || run_client_acceptor(listener, node_id, incarnation, events));
    }
    {
        let events = events_tx.clone();
        thread::spawn(move || run_timer(events, || false));
    }

    println!(
        "node {} of {}: replicas on {}, clients on {}, data in {}, primary is node {}",
        args.id,
        args.replicas.len(),
        args.replicas[args.id],
        args.listen,
        args.data_dir.display(),
        node.replica.primary_id()
    );
    node.run(events_rx, events_tx, outboxes);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::net::Shutdown;
    use std::sync::mpsc::RecvTimeoutError;
    use vsr_rs::{
        Client, ClientRecord, Config, LogEntry, OpNumber, QueryNumber, StateMachine, Status,
    };

    /// A fresh directory for one node of one test.
    fn temp_dir(test: &str, id: ReplicaID) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("vsr-kvstore-{}-{test}-{id}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    fn config(replica_count: usize) -> Config {
        let mut config = Config::new();
        for _ in 0..replica_count {
            config.add_replica();
        }
        config
    }

    /// Opens new nodes in `dirs`.
    fn open_nodes(dirs: &[PathBuf]) -> Vec<Node> {
        let config = config(dirs.len());
        dirs.iter()
            .enumerate()
            .map(|(id, dir)| Node::open(id, config.clone(), dir, Start::Init).unwrap())
            .collect()
    }

    /// A client of the primary's own node sends to the replica in the batch
    /// that took its command: the step appends the client's registration,
    /// writes it, and sends its `Prepare`s, and sends the node itself
    /// nothing.
    #[test]
    fn own_client_request_joins_the_batch() {
        let dir = temp_dir("own-client", 0);
        let mut node = Node::open(0, config(3), &dir, Start::Init).unwrap();
        let (respond, _responses) = channel();
        node.handle(Event::Command {
            connection: client_id(0, node.incarnation, 0),
            command: Command::Set("a".into(), "1".into()),
            respond,
        });
        let mut sent = Vec::new();
        let written = node
            .step(|dst, message| sent.push((dst, message)), &mut Vec::new())
            .unwrap();
        assert!(written);
        assert_eq!(1, node.replica.op_number());
        let prepared: Vec<ReplicaID> = sent
            .iter()
            .filter(|(_, message)| matches!(message, Message::Prepare { op_number: 1, .. }))
            .map(|(dst, _)| *dst)
            .collect();
        assert_eq!(vec![1, 2], prepared);
        assert!(sent.iter().all(|(dst, _)| *dst != 0), "{sent:?}");
        drop(node);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Waits for the node's journal to wake it, through the channel whose
    /// sender the test gave the node as its event loop's, and hands the
    /// node the event. Returns what the node's batch returns.
    fn land(
        node: &mut Node,
        woken: &Receiver<Event>,
        frames: &Sender<(ReplicaID, Frame)>,
        wake: &Sender<Event>,
    ) -> bool {
        let event = woken
            .recv_timeout(Duration::from_secs(10))
            .expect("the write lands");
        assert!(matches!(event, Event::Written));
        node.batch([event], frames, wake)
    }

    /// The op numbers the node's write that is out covers.
    fn out(node: &Node) -> Option<(OpNumber, OpNumber)> {
        let writing = node.writing.as_ref()?;
        Some((writing.write.entries_from, writing.write.op_number()))
    }

    /// A reply another node routes to one of this node's connections goes
    /// out in the batch that brings it, even one that stops the node.
    #[test]
    fn routed_reply_goes_out_in_its_batch() {
        let dir = temp_dir("routed-reply", 1);
        let mut node = Node::open(1, config(3), &dir, Start::Init).unwrap();
        let (wake, _woken) = channel();
        let (frames, _frames_rx) = channel();
        let (respond, responses) = channel();
        let connection = client_id(1, node.incarnation, 0);
        let command = Event::Command {
            connection,
            command: Command::Set("a".into(), "1".into()),
            respond,
        };
        assert!(node.batch([command], &frames, &wake));
        let client_id = connection as vsr_rs::ClientID;
        let registered = Event::Reply(Reply::Registered {
            view_number: 0,
            client_id,
            session: 1,
        });
        assert!(node.batch([registered], &frames, &wake));
        let executed = Event::Reply(Reply::Executed {
            view_number: 0,
            client_id,
            session: 1,
            request_number: 1,
            result: None,
        });
        node.batch([executed, Event::Stop], &frames, &wake);
        assert_eq!("+OK\r\n", responses.try_recv().unwrap());
        drop(node);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A connection's answers leave in the order its commands came, even
    /// when a later command's reply arrives first.
    #[test]
    fn answers_leave_in_the_order_commands_came() {
        let dir = temp_dir("answer-order", 1);
        let mut node = Node::open(1, config(3), &dir, Start::Init).unwrap();
        let (wake, _woken) = channel();
        let (frames, _frames_rx) = channel();
        let (respond, responses) = channel();
        let connection = client_id(1, node.incarnation, 0);
        let command = |command| Event::Command {
            connection,
            command,
            respond: respond.clone(),
        };
        let commands = [
            command(Command::Get("a".into())),
            command(Command::Get("b".into())),
        ];
        assert!(node.batch(commands, &frames, &wake));
        let client_id = connection as vsr_rs::ClientID;
        let queried = |query_number, result: &str| {
            Event::Reply(Reply::Queried {
                view_number: 0,
                client_id,
                query_number,
                result: Some(result.into()),
            })
        };
        assert!(node.batch([queried(2, "y")], &frames, &wake));
        assert!(responses.try_recv().is_err());
        assert!(node.batch([queried(1, "x")], &frames, &wake));
        let answers: Vec<String> = responses.try_iter().collect();
        assert_eq!(vec!["$1\r\nx\r\n", "$1\r\ny\r\n"], answers);
        drop(node);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// An eviction fails the commands of a connection that have no answer
    /// yet, and keeps the answers held for later ones.
    #[test]
    fn eviction_keeps_answers_already_held() {
        let dir = temp_dir("evicted-held", 1);
        let mut node = Node::open(1, config(3), &dir, Start::Init).unwrap();
        let (wake, _woken) = channel();
        let (frames, _frames_rx) = channel();
        let (respond, responses) = channel();
        let connection = client_id(1, node.incarnation, 0);
        let client_id = connection as vsr_rs::ClientID;
        let command = |command| Event::Command {
            connection,
            command,
            respond: respond.clone(),
        };
        let reply = |reply| Event::Reply(reply);
        let set = command(Command::Set("a".into(), "x".into()));
        assert!(node.batch([set], &frames, &wake));
        let registered = Reply::Registered {
            view_number: 0,
            client_id,
            session: 1,
        };
        let executed = Reply::Executed {
            view_number: 0,
            client_id,
            session: 1,
            request_number: 1,
            result: None,
        };
        assert!(node.batch([reply(registered), reply(executed)], &frames, &wake));
        assert_eq!(Ok("+OK\r\n".to_string()), responses.try_recv());
        let gets = [
            command(Command::Get("a".into())),
            command(Command::Get("b".into())),
        ];
        assert!(node.batch(gets, &frames, &wake));
        let queried = Reply::Queried {
            view_number: 0,
            client_id,
            query_number: 2,
            result: Some("y".into()),
        };
        let evicted = Reply::Evicted {
            view_number: 0,
            client_id,
            session: 1,
        };
        assert!(node.batch([reply(queried), reply(evicted)], &frames, &wake));
        let answers: Vec<String> = responses.try_iter().collect();
        assert_eq!(
            vec![EVICTED.to_string(), "$1\r\ny\r\n".to_string()],
            answers
        );
        drop(node);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The event loop's step sends a write that needs a sync to the journal
    /// and steps on while it is out: the next entry's `Prepare` leaves at
    /// once, and the replies and the next write wait until the event loop
    /// hears that the write landed. A client's registration answered in a
    /// step sends its request in that step.
    #[test]
    fn deliver_steps_on_while_a_write_is_out() {
        let dir = temp_dir("pipelined", 0);
        let mut node = Node::open(0, config(3), &dir, Start::Init).unwrap();
        let (wake, woken) = channel();
        let (frames, frames_rx) = channel();
        let request = |node: &mut Node, connection: u64, key: &str| {
            let (respond, responses) = channel();
            let command = Event::Command {
                connection: client_id(0, node.incarnation, connection),
                command: Command::Set(key.into(), "1".into()),
                respond,
            };
            assert!(node.batch([command], &frames, &wake));
            responses
        };
        let prepared = |frames_rx: &Receiver<Addressed>| -> Vec<OpNumber> {
            frames_rx
                .try_iter()
                .filter_map(|(_, frame)| match frame {
                    Frame::Message(Message::Prepare { op_number, .. }) => Some(op_number),
                    _ => None,
                })
                .collect()
        };
        let ack = |op_number| {
            Event::Message(Message::PrepareOk {
                view_number: 0,
                op_number,
                replica_id: 1,
            })
        };
        // The two clients' registrations are ops 1 and 2.
        let first_responses = request(&mut node, 0, "a");
        assert_eq!(Some((1, 1)), out(&node));
        let second_responses = request(&mut node, 1, "b");
        assert_eq!(Some((1, 1)), out(&node));
        assert_eq!(vec![1, 1, 2, 2], prepared(&frames_rx));
        // A backup's acknowledgement of both ops is no quorum while the
        // primary's own write is out.
        assert!(node.batch([ack(2)], &frames, &wake));
        assert_eq!(0, node.replica.commit_number());
        // The first registration commits, and its client's request goes
        // out as op 3 in the same step, behind the write of op 2 it takes.
        assert!(land(&mut node, &woken, &frames, &wake));
        assert_eq!(1, node.replica.commit_number());
        assert_eq!(vec![3, 3], prepared(&frames_rx));
        assert_eq!(Some((2, 2)), out(&node));
        assert!(node.batch([ack(3)], &frames, &wake));
        assert!(land(&mut node, &woken, &frames, &wake));
        assert_eq!(2, node.replica.commit_number());
        assert_eq!(Some((3, 3)), out(&node));
        assert!(first_responses.try_recv().is_err());
        assert!(land(&mut node, &woken, &frames, &wake));
        assert_eq!(3, node.replica.commit_number());
        assert_eq!(Ok("+OK\r\n".to_string()), first_responses.try_recv());
        assert!(second_responses.try_recv().is_err());
        assert_eq!(Some((4, 4)), out(&node));
        drop(node);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The event loop stops only once the write that is out has landed.
    #[test]
    fn stop_waits_for_the_write_that_is_out() {
        let dir = temp_dir("stop", 0);
        let mut node = Node::open(0, config(3), &dir, Start::Init).unwrap();
        let (wake, woken) = channel();
        let (frames, _frames_rx) = channel();
        let (respond, _responses) = channel();
        let command = Event::Command {
            connection: client_id(0, node.incarnation, 0),
            command: Command::Set("a".into(), "1".into()),
            respond,
        };
        assert!(node.batch([command], &frames, &wake));
        assert!(node.writing.is_some());
        assert!(node.batch([Event::Stop], &frames, &wake));
        assert!(!land(&mut node, &woken, &frames, &wake));
        assert!(node.writing.is_none());
        // One wake per write, and one that finds no write out changes
        // nothing.
        assert!(woken.try_recv().is_err());
        assert!(!node.batch([Event::Written], &frames, &wake));
        drop(node);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A write out for `STALLED_WRITE_TICKS` ticks silences the node, so
    /// that the backups stop hearing from a primary whose disk stalled and
    /// elect another. It speaks again once the write lands.
    #[test]
    fn stalled_write_silences_the_node() {
        let dir = temp_dir("stalled", 0);
        let mut node = Node::open(0, config(3), &dir, Start::Init).unwrap();
        let (wake, woken) = channel();
        let (frames, frames_rx) = channel();
        let (respond, _responses) = channel();
        let command = Event::Command {
            connection: client_id(0, node.incarnation, 0),
            command: Command::Set("a".into(), "1".into()),
            respond,
        };
        assert!(node.batch([command], &frames, &wake));
        assert!(node.writing.is_some());
        assert!(frames_rx.try_iter().count() > 0);
        for _ in 1..STALLED_WRITE_TICKS {
            assert!(node.batch([Event::Tick], &frames, &wake));
            assert!(frames_rx.try_iter().count() > 0);
        }
        for _ in 0..PRIMARY_TIMEOUT {
            assert!(node.batch([Event::Tick], &frames, &wake));
            assert_eq!(0, frames_rx.try_iter().count());
        }
        assert!(land(&mut node, &woken, &frames, &wake));
        assert!(frames_rx.try_iter().count() > 0);
        drop(node);
        let _ = std::fs::remove_dir_all(&dir);
    }

    type Addressed = (ReplicaID, Frame);

    /// Three nodes stepped in batches as their event loops step them. A
    /// node hears that its write landed when the test hands it the event
    /// that says so, and its write stays out until then.
    struct Pipelined {
        dirs: Vec<PathBuf>,
        nodes: Vec<Node>,
        frames: Vec<(Sender<Addressed>, Receiver<Addressed>)>,
        wakes: Vec<(Sender<Event>, Receiver<Event>)>,
        connections: u64,
    }

    impl Pipelined {
        fn new(test: &str) -> Pipelined {
            Pipelined::with_config(test, config(3))
        }

        fn with_config(test: &str, config: Config) -> Pipelined {
            let dirs: Vec<PathBuf> = (0..3).map(|id| temp_dir(test, id)).collect();
            let mut nodes: Vec<Node> = dirs
                .iter()
                .enumerate()
                .map(|(id, dir)| Node::open(id, config.clone(), dir, Start::Init).unwrap())
                .collect();
            for node in &mut nodes {
                node.announce_views = false;
            }
            Pipelined {
                dirs,
                nodes,
                frames: (0..3).map(|_| channel()).collect(),
                wakes: (0..3).map(|_| channel()).collect(),
                connections: 0,
            }
        }

        fn batch(&mut self, id: ReplicaID, events: impl IntoIterator<Item = Event>) {
            let (frames, wake) = (&self.frames[id].0, &self.wakes[id].0);
            assert!(self.nodes[id].batch(events, frames, wake));
        }

        /// A new client of node 0 sets `key`. Returns where its response
        /// comes.
        fn set(&mut self, key: &str) -> Receiver<String> {
            let connection = self.connections;
            self.connections += 1;
            self.set_on(connection, key)
        }

        /// The client of node 0's connection `connection` sets `key`.
        fn set_on(&mut self, connection: u64, key: &str) -> Receiver<String> {
            let (respond, responses) = channel();
            let connection = client_id(0, self.nodes[0].incarnation, connection);
            let command = Command::Set(key.into(), "1".into());
            self.batch(
                0,
                [Event::Command {
                    connection,
                    command,
                    respond,
                }],
            );
            responses
        }

        /// Lets the nodes in `landing` hear that their writes landed, and
        /// delivers what every node sends, until nothing moves.
        fn settle(&mut self, landing: &[ReplicaID]) {
            loop {
                let mut moved = false;
                for &id in landing {
                    if self.nodes[id].writing.is_some() {
                        let (wake, woken) = &self.wakes[id];
                        let node = &mut self.nodes[id];
                        assert!(land(node, woken, &self.frames[id].0, wake));
                        moved = true;
                    }
                }
                for id in 0..self.nodes.len() {
                    let sent: Vec<_> = self.frames[id].1.try_iter().collect();
                    for (dst, frame) in sent {
                        self.batch(dst, [Event::from(frame)]);
                        moved = true;
                    }
                }
                if !moved {
                    return;
                }
            }
        }

        fn tick(&mut self) {
            for id in 0..self.nodes.len() {
                self.batch(id, [Event::Tick]);
            }
        }

        /// Node `id` loses power: its store goes back to its last persist,
        /// and it restarts from its disk.
        fn lose_power(&mut self, id: ReplicaID) {
            drop(self.nodes.remove(id));
            power::lose_power(&self.dirs[id]);
            let mut node = Node::open(id, config(3), &self.dirs[id], Start::Restart).unwrap();
            node.announce_views = false;
            self.wakes[id] = channel();
            self.nodes.insert(id, node);
            self.batch(id, []);
        }
    }

    /// Node 2's write lands while nodes 0 and 1 commit an op, but node 2
    /// loses power before it hears so: it restarts with the op it never
    /// acknowledged, and catches up.
    #[test]
    fn pipelined_power_loss_with_a_write_unacknowledged() {
        let mut cluster = Pipelined::new("pipelined-power-loss");
        let a = cluster.set("a");
        cluster.settle(&[0, 1, 2]);
        assert_eq!(Ok("+OK\r\n".to_string()), a.try_recv());
        let b = cluster.set("b");
        cluster.settle(&[0, 1]);
        assert_eq!(Ok("+OK\r\n".to_string()), b.try_recv());
        // Its registration, op 3, is the write node 2 has out.
        assert_eq!(Some((3, 3)), out(&cluster.nodes[2]));
        let landed = cluster.wakes[2].1.recv_timeout(Duration::from_secs(10));
        assert!(matches!(landed, Ok(Event::Written)));
        cluster.lose_power(2);
        assert_eq!(3, cluster.nodes[2].replica.op_number());
        let c = cluster.set("c");
        for _ in 0..3 {
            cluster.tick();
            cluster.settle(&[0, 1, 2]);
        }
        assert_eq!(Ok("+OK\r\n".to_string()), c.try_recv());
        let node = &cluster.nodes[2];
        assert_eq!(6, node.replica.commit_number());
        assert_eq!(Some("1".to_string()), node.replica.state_machine().get("b"));
        remove_dirs(cluster.nodes, &cluster.dirs);
    }

    /// With room for one session, a second connection's registration
    /// evicts the first. The first connection's next command fails, as it
    /// may or may not have run, and the one after it runs in a new session.
    #[test]
    fn evicted_connection_fails_its_command_and_registers_again() {
        let mut config = config(3);
        config.set_clients_max(1);
        let mut cluster = Pipelined::with_config("evicted", config);
        let all = [0, 1, 2];
        let a = cluster.set_on(0, "a");
        cluster.settle(&all);
        assert_eq!(Ok("+OK\r\n".to_string()), a.try_recv());
        let b = cluster.set_on(1, "b");
        cluster.settle(&all);
        assert_eq!(Ok("+OK\r\n".to_string()), b.try_recv());
        let c = cluster.set_on(0, "c");
        cluster.settle(&all);
        assert_eq!(Ok(EVICTED.to_string()), c.try_recv());
        let d = cluster.set_on(0, "d");
        cluster.settle(&all);
        assert_eq!(Ok("+OK\r\n".to_string()), d.try_recv());
        let store = cluster.nodes[0].replica.state_machine();
        assert_eq!(None, store.get("c"));
        assert_eq!(Some("1".to_string()), store.get("d"));
        remove_dirs(cluster.nodes, &cluster.dirs);
    }

    /// The event loops of a cluster answer a client and stop on
    /// `Event::Stop`; a restart finds the op in the journal.
    #[test]
    fn run_answers_and_stops() {
        let dirs: Vec<PathBuf> = (0..3).map(|id| temp_dir("run", id)).collect();
        let nodes = open_nodes(&dirs);
        let incarnation = nodes[0].incarnation;
        let (events, receivers): (Vec<Sender<Event>>, Vec<Receiver<Event>>) =
            (0..nodes.len()).map(|_| channel()).unzip();
        let running: Vec<_> = nodes
            .into_iter()
            .zip(receivers)
            .map(|(mut node, events_rx)| {
                node.announce_views = false;
                let (frames, frames_rx) = channel::<(ReplicaID, Frame)>();
                let peers = events.clone();
                thread::spawn(move || {
                    for (dst, frame) in frames_rx {
                        let _ = peers[dst].send(frame.into());
                    }
                });
                let wake = events[node.id].clone();
                thread::spawn(move || {
                    node.run(events_rx, wake, frames);
                    node
                })
            })
            .collect();
        let (respond, responses) = channel();
        events[0]
            .send(Event::Command {
                connection: client_id(0, incarnation, 0),
                command: Command::Set("a".into(), "1".into()),
                respond,
            })
            .unwrap();
        let response = responses.recv_timeout(Duration::from_secs(10));
        assert_eq!(Ok("+OK\r\n".to_string()), response);
        for events in &events {
            events.send(Event::Stop).unwrap();
        }
        let nodes: Vec<Node> = running
            .into_iter()
            .map(|node| node.join().unwrap())
            .collect();
        drop(nodes);
        let node = Node::open(0, config(3), &dirs[0], Start::Restart).unwrap();
        assert_eq!(2, node.replica.op_number());
        remove_dirs(vec![node], &dirs);
    }

    /// Persists every node, then moves what the nodes and the client want
    /// sent through the wire encoding to their destination, until nothing
    /// is left. Messages to `down` nodes are dropped. Returns the replies.
    fn deliver(
        nodes: &mut [Node],
        client: &mut Client<Op, String>,
        down: &[ReplicaID],
    ) -> Vec<KvReply> {
        let mut replies = Vec::new();
        loop {
            let mut frames: Vec<(ReplicaID, String)> = Vec::new();
            for node in nodes.iter_mut() {
                let mut node_replies = Vec::new();
                let send = |dst, message| frames.push((dst, encode(&Frame::Message(message))));
                node.step(send, &mut node_replies).unwrap();
                for reply in node_replies {
                    client.on_reply(reply.clone());
                    replies.push(reply);
                }
            }
            for (dst, message) in client.drain() {
                frames.push((dst, encode(&Frame::Message(message))));
            }
            if frames.is_empty() {
                return replies;
            }
            for (dst, line) in frames {
                if down.contains(&dst) {
                    continue;
                }
                match decode(&line).unwrap() {
                    Frame::Message(message) => nodes[dst].replica.on_message(message),
                    Frame::Reply(_) => unreachable!(),
                }
            }
        }
    }

    fn idle(nodes: &mut [Node], down: &[ReplicaID]) {
        for (id, node) in nodes.iter_mut().enumerate() {
            if !down.contains(&id) {
                node.replica.on_idle();
            }
        }
    }

    /// Closes the nodes and removes their directories.
    fn remove_dirs(nodes: Vec<Node>, dirs: &[PathBuf]) {
        drop(nodes);
        for dir in dirs {
            let _ = std::fs::remove_dir_all(dir);
        }
    }

    /// Runs `op` through the cluster and returns its result.
    fn run(
        nodes: &mut [Node],
        client: &mut Client<Op, String>,
        op: Op,
        down: &[ReplicaID],
    ) -> Option<String> {
        let request_number = client.on_request(op);
        let replies = deliver(nodes, client, down);
        replies
            .into_iter()
            .find_map(|reply| match reply {
                Reply::Executed {
                    request_number: n,
                    result,
                    ..
                } if n == request_number => Some(result),
                _ => None,
            })
            .expect("a reply")
    }

    /// Reads `key` through the cluster and returns its value.
    fn read(
        nodes: &mut [Node],
        client: &mut Client<Op, String>,
        key: &str,
        down: &[ReplicaID],
    ) -> Option<String> {
        let query_number = client.on_query(key.to_string());
        let replies = deliver(nodes, client, down);
        queried(replies, query_number).expect("a reply")
    }

    /// The result of the query numbered `query_number` among `replies`.
    fn queried(replies: Vec<KvReply>, query_number: QueryNumber) -> Option<Option<String>> {
        replies.into_iter().find_map(|reply| match reply {
            Reply::Queried {
                query_number: n,
                result,
                ..
            } if n == query_number => Some(result),
            _ => None,
        })
    }

    /// Three nodes execute a few commands. One of them shuts down and comes
    /// back from its store and journal with the same state, and takes part
    /// again.
    #[test]
    fn restart_from_disk() {
        let dirs: Vec<PathBuf> = (0..3).map(|id| temp_dir("restart", id)).collect();
        let mut nodes = open_nodes(&dirs);
        let mut client = Client::new(1, config(3));
        assert_eq!(
            None,
            run(
                &mut nodes,
                &mut client,
                Op::Put("a".into(), "1".into()),
                &[]
            )
        );
        assert_eq!(
            None,
            run(
                &mut nodes,
                &mut client,
                Op::Put("b".into(), "2".into()),
                &[]
            )
        );
        assert_eq!(Some("1".into()), read(&mut nodes, &mut client, "a", &[]));
        idle(&mut nodes, &[]);
        deliver(&mut nodes, &mut client, &[]);
        for node in &nodes {
            assert_eq!(3, node.replica.commit_number());
        }
        // Node 1 persists its store, which has executed every op.
        nodes[1].flush_store().unwrap();
        assert_eq!(3, nodes[1].replica.state_machine().applied);
        let before = nodes[1].replica.persistent_state();
        let client_table = nodes[1].replica.client_table();

        let node = nodes.remove(1);
        drop(node);
        let restarted = Node::open(1, config(3), &dirs[1], Start::Restart).unwrap();
        nodes.insert(1, restarted);
        // The last step only moved the commit number, so the journal was
        // behind the store, and the restart takes the store's count as a
        // checkpoint: the log now starts there.
        let after = nodes[1].replica.persistent_state();
        assert_eq!(before.commit_number, after.commit_number);
        assert_eq!(before.commit_number, after.log_start);
        assert_eq!(before.log.len(), after.log_start + after.log.len());
        assert_eq!(client_table, nodes[1].replica.client_table());
        assert_eq!(
            Some("1".to_string()),
            nodes[1].replica.state_machine().get("a")
        );
        assert_eq!(
            Some("2".to_string()),
            nodes[1].replica.state_machine().get("b")
        );
        assert_eq!(3, nodes[1].replica.state_machine().applied);

        // Only node 1 acknowledges the next op, so its acknowledgement is
        // what commits it.
        assert_eq!(
            None,
            run(
                &mut nodes,
                &mut client,
                Op::Put("c".into(), "3".into()),
                &[2]
            )
        );
        assert_eq!(4, nodes[0].replica.commit_number());
        idle(&mut nodes, &[2]);
        deliver(&mut nodes, &mut client, &[2]);
        assert_eq!(4, nodes[1].replica.commit_number());
        assert_eq!(
            Some("3".to_string()),
            nodes[1].replica.state_machine().get("c")
        );
        remove_dirs(nodes, &dirs);
    }

    /// The primary persists its store and compacts its log with no
    /// retention while a node misses every op. That node then catches up
    /// from the primary's checkpoint, which restores its store, and the op
    /// after it. A power loss takes its store back to the checkpoint, and
    /// it executes that op again from its journal.
    #[test]
    fn checkpoint_restores_store() {
        let dirs: Vec<PathBuf> = (0..3).map(|id| temp_dir("checkpoint", id)).collect();
        let mut nodes = open_nodes(&dirs);
        for node in &mut nodes {
            node.log_retention = 0;
        }
        let mut client = Client::new(1, config(3));
        for i in 0..5 {
            run(
                &mut nodes,
                &mut client,
                Op::Put(format!("k{i}"), format!("v{i}")),
                &[2],
            );
        }
        nodes[0].flush_store().unwrap();
        assert_eq!(6, nodes[0].replica.log_start());
        assert_eq!(0, nodes[2].replica.op_number());

        // With node 1 cut off, node 2 hears about op 7, finds the gap, and
        // gets a checkpoint: the primary's state as executed, and op 7
        // after it, which node 2's acknowledgement then commits.
        run(
            &mut nodes,
            &mut client,
            Op::Put("k5".into(), "v5".into()),
            &[1],
        );
        idle(&mut nodes, &[]);
        deliver(&mut nodes, &mut client, &[]);
        assert_eq!(6, nodes[2].replica.log_start());
        assert_eq!(7, nodes[2].replica.commit_number());
        assert_eq!(7, nodes[2].replica.state_machine().applied);
        for i in 0..6 {
            assert_eq!(
                Some(format!("v{i}")),
                nodes[2].replica.state_machine().get(&format!("k{i}"))
            );
        }
        assert_eq!(
            nodes[0].replica.client_table(),
            nodes[2].replica.state_machine().client_table()
        );

        // Node 2 loses power. Its store goes back to the checkpoint, which
        // it made durable as it restored it, and its journal holds op 7,
        // which it executes again once it hears that op 7 committed.
        reopen(&mut nodes, &dirs, 2, true);
        assert_eq!(6, nodes[2].replica.state_machine().applied);
        assert_eq!(
            Some("v3".to_string()),
            nodes[2].replica.state_machine().get("k3")
        );
        run(
            &mut nodes,
            &mut client,
            Op::Put("k6".into(), "v6".into()),
            &[],
        );
        idle(&mut nodes, &[]);
        deliver(&mut nodes, &mut client, &[]);
        assert_eq!(8, nodes[2].replica.commit_number());
        assert_eq!(8, nodes[2].replica.state_machine().applied);
        assert_eq!(
            Some("v5".to_string()),
            nodes[2].replica.state_machine().get("k5")
        );
        remove_dirs(nodes, &dirs);
    }

    /// A node far behind rejoins from a checkpoint of ten chunks, which it
    /// fetches over the wire a chunk at a time; then another node loses its
    /// disk and recovers from the same checkpoint.
    #[test]
    fn rejoin_from_a_checkpoint_of_several_chunks() {
        let dirs: Vec<PathBuf> = (0..3).map(|id| temp_dir("chunked", id)).collect();
        let mut nodes = open_nodes(&dirs);
        for node in &mut nodes {
            node.log_retention = 0;
        }
        let mut client = Client::new(1, config(3));
        let value = "v".repeat(CHUNK_BYTES / 4);
        let keys = 40;
        for i in 0..keys {
            run(
                &mut nodes,
                &mut client,
                Op::Put(format!("k{i}"), value.clone()),
                &[2],
            );
        }
        nodes[0].flush_store().unwrap();
        assert_eq!(keys + 1, nodes[0].replica.log_start());

        run(
            &mut nodes,
            &mut client,
            Op::Put("last".into(), "1".into()),
            &[1],
        );
        idle(&mut nodes, &[]);
        deliver(&mut nodes, &mut client, &[]);
        assert_eq!(keys + 1, nodes[2].replica.log_start());
        assert_eq!(keys + 2, nodes[2].replica.commit_number());
        holds_every_key(&nodes[2], keys, &value);
        assert_eq!(
            nodes[0].replica.client_table(),
            nodes[2].replica.state_machine().client_table()
        );

        drop(nodes.remove(1));
        std::fs::remove_dir_all(&dirs[1]).unwrap();
        let node = Node::open(1, config(3), &dirs[1], Start::Recover { view: 0 }).unwrap();
        nodes.insert(1, node);
        for _ in 0..3 {
            idle(&mut nodes, &[]);
            deliver(&mut nodes, &mut client, &[]);
        }
        assert!(!nodes[1].replica.is_recovering());
        assert_eq!(keys + 1, nodes[1].replica.log_start());
        holds_every_key(&nodes[1], keys, &value);
        remove_dirs(nodes, &dirs);
    }

    /// Whether `node`'s store holds `value` under each of the first `keys`
    /// keys.
    fn holds_every_key(node: &Node, keys: usize, value: &String) {
        for i in 0..keys {
            let held = node.replica.state_machine().get(&format!("k{i}"));
            assert_eq!(Some(value), held.as_ref(), "k{i}");
        }
    }

    /// Reads `key` through the cluster, with the client re-sending to every
    /// node until a reply comes, and returns the value: after a view change
    /// the client's first try goes to the old primary.
    fn read_resending(
        nodes: &mut [Node],
        client: &mut Client<Op, String>,
        key: &str,
        down: &[ReplicaID],
    ) -> Option<String> {
        let query_number = client.on_query(key.to_string());
        for _ in 0..20 {
            let replies = deliver(nodes, client, down);
            if let Some(result) = queried(replies, query_number) {
                return result;
            }
            idle(nodes, down);
            client.on_idle();
        }
        panic!("no reply to query {query_number}");
    }

    /// Takes node `id` down and brings it back from its data directory,
    /// after a power loss if `power_loss` is set.
    fn reopen(nodes: &mut Vec<Node>, dirs: &[PathBuf], id: ReplicaID, power_loss: bool) {
        drop(nodes.remove(id));
        if power_loss {
            power::lose_power(&dirs[id]);
        }
        let node = Node::open(id, config(dirs.len()), &dirs[id], Start::Restart).unwrap();
        nodes.insert(id, node);
    }

    /// A node restores a checkpoint, which the store makes durable at
    /// once, and loses power before the journal records the step. The
    /// restart takes the log start from the store, which puts a
    /// compaction in the journal beyond its last op number.
    #[test]
    fn power_loss_after_a_checkpoint_restore() {
        let dirs: Vec<PathBuf> = (0..3).map(|id| temp_dir("restore-power", id)).collect();
        let mut nodes = open_nodes(&dirs);
        nodes[0].log_retention = 0;
        let mut client = Client::new(1, config(3));
        for i in 0..5 {
            run(
                &mut nodes,
                &mut client,
                Op::Put(format!("k{i}"), format!("v{i}")),
                &[2],
            );
        }
        nodes[0].flush_store().unwrap();
        assert_eq!(6, nodes[0].replica.log_start());

        // Node 2 learns it is behind and fetches the primary's checkpoint,
        // with none of its steps journaled.
        nodes[2].replica.on_message(Message::Commit {
            view_number: 0,
            commit_number: 6,
        });
        exchange_early_messages(&mut nodes, 2, 0);
        assert_eq!(6, nodes[2].replica.state_machine().applied);
        reopen(&mut nodes, &dirs, 2, true);
        assert_eq!(6, nodes[2].replica.log_start());
        assert_eq!(6, nodes[2].replica.commit_number());
        assert_eq!(6, nodes[2].replica.op_number());
        reopen(&mut nodes, &dirs, 2, true);
        assert_eq!(6, nodes[2].replica.log_start());
        for i in 0..5 {
            assert_eq!(
                Some(format!("v{i}")),
                nodes[2].replica.state_machine().get(&format!("k{i}"))
            );
        }
        run(
            &mut nodes,
            &mut client,
            Op::Put("k5".into(), "v5".into()),
            &[1],
        );
        idle(&mut nodes, &[1]);
        deliver(&mut nodes, &mut client, &[1]);
        assert_eq!(7, nodes[2].replica.commit_number());
        assert_eq!(7, nodes[2].replica.state_machine().applied);
        remove_dirs(nodes, &dirs);
    }

    /// Delivers what nodes `a` and `b` send each other before their steps
    /// are journaled, until neither sends more.
    fn exchange_early_messages(nodes: &mut [Node], a: ReplicaID, b: ReplicaID) {
        loop {
            let mut quiet = true;
            for (from, to) in [(a, b), (b, a)] {
                let early: Vec<_> = nodes[from]
                    .replica
                    .drain_messages_before_persist()
                    .collect();
                for (dst, message) in early {
                    assert_eq!(to, dst);
                    nodes[to].replica.on_message(message);
                    quiet = false;
                }
            }
            if quiet {
                return;
            }
        }
    }

    /// Restarts from the journal in each status it records: the primary
    /// starts the next view, a node in a view change enters it again, and
    /// a node still recovering recovers again. The cluster serves every
    /// value after each.
    #[test]
    fn restart_as_primary_in_a_view_change_and_recovering() {
        let dirs: Vec<PathBuf> = (0..3).map(|id| temp_dir("restart-status", id)).collect();
        let mut nodes = open_nodes(&dirs);
        let mut client = Client::new(1, config(3));
        run(
            &mut nodes,
            &mut client,
            Op::Put("a".into(), "1".into()),
            &[],
        );

        // The primary of view 0 restarts into a view change to view 1.
        reopen(&mut nodes, &dirs, 0, false);
        assert_eq!(Status::ViewChange, nodes[0].replica.status());
        assert_eq!(1, nodes[0].replica.view_number());
        assert_eq!(
            Some("1".to_string()),
            read_resending(&mut nodes, &mut client, "a", &[])
        );
        assert_eq!(1, nodes[1].replica.view_number());
        assert!(nodes[1].replica.is_primary());

        // Node 2 times out on its own, starts a view change, and goes
        // down before hearing from anyone.
        for _ in 0..4 {
            nodes[2].replica.on_idle();
        }
        assert_eq!(Status::ViewChange, nodes[2].replica.status());
        assert_eq!(2, nodes[2].replica.view_number());
        nodes[2].write().unwrap();
        reopen(&mut nodes, &dirs, 2, true);
        assert_eq!(Status::ViewChange, nodes[2].replica.status());
        assert_eq!(2, nodes[2].replica.view_number());
        assert_eq!(
            Some("1".to_string()),
            read_resending(&mut nodes, &mut client, "a", &[])
        );
        assert!(nodes[2].replica.is_primary());

        // Node 0 loses its data and recovers, and goes down before any
        // answer arrives.
        drop(nodes.remove(0));
        std::fs::remove_dir_all(&dirs[0]).unwrap();
        let node = Node::open(0, config(3), &dirs[0], Start::Recover { view: 2 }).unwrap();
        nodes.insert(0, node);
        reopen(&mut nodes, &dirs, 0, true);
        assert_eq!(Status::Recovering, nodes[0].replica.status());
        assert_eq!(2, nodes[0].replica.view_number());
        assert_eq!(
            Some("1".to_string()),
            read_resending(&mut nodes, &mut client, "a", &[])
        );
        idle(&mut nodes, &[]);
        deliver(&mut nodes, &mut client, &[]);
        assert_eq!(Status::Normal, nodes[0].replica.status());
        assert_eq!(nodes[2].replica.op_number(), nodes[0].replica.op_number());
        remove_dirs(nodes, &dirs);
    }

    /// Every node loses power at once, with its store behind its journal
    /// by a different amount: one compacted its log with no retention, one
    /// persisted its store and kept its log, one never persisted it. Every
    /// committed value comes back.
    #[test]
    fn cluster_power_loss_with_stores_behind() {
        let dirs: Vec<PathBuf> = (0..3).map(|id| temp_dir("cluster-power", id)).collect();
        let mut nodes = open_nodes(&dirs);
        nodes[0].log_retention = 0;
        let mut client = Client::new(1, config(3));
        let mut flushed = [0; 3];
        for i in 0..15 {
            run(
                &mut nodes,
                &mut client,
                Op::Put(format!("k{i}"), format!("v{i}")),
                &[],
            );
            let id = match i {
                4 => 0,
                9 => 1,
                _ => continue,
            };
            nodes[id].flush_store().unwrap();
            flushed[id] = nodes[id].replica.applied();
        }
        assert_eq!(flushed[0], nodes[0].replica.log_start());
        drop(nodes);
        for (id, dir) in dirs.iter().enumerate() {
            power::lose_power(dir);
            let store = Store::open(&dir.join("store")).unwrap();
            assert_eq!(flushed[id], store.applied, "node {id}");
        }
        let mut nodes: Vec<Node> = (0..3)
            .map(|id| Node::open(id, config(3), &dirs[id], Start::Restart).unwrap())
            .collect();
        for i in 0..15 {
            assert_eq!(
                Some(format!("v{i}")),
                read_resending(&mut nodes, &mut client, &format!("k{i}"), &[])
            );
        }
        remove_dirs(nodes, &dirs);
    }

    /// The wire encoding round-trips the messages that carry entries or
    /// checkpoints, and the replies.
    #[test]
    fn codec_round_trip() {
        let chunk = StoreChunk {
            pairs: vec![("a".into(), "1".into()), ("b".into(), "2".into())],
            clients: vec![
                ClientRecord {
                    client_id: 3,
                    session: 1,
                    request_number: 4,
                    replies: VecDeque::from([None, Some("1".into())]),
                    op_number: 6,
                },
                ClientRecord {
                    client_id: 5,
                    session: 2,
                    request_number: 0,
                    replies: VecDeque::new(),
                    op_number: 2,
                },
            ],
        };
        let entries = vec![
            LogEntry::Register { client_id: 3 },
            LogEntry::Request {
                client_id: 3,
                session: 1,
                request_number: 5,
                answered: 3,
                op: Op::Put("a".into(), "1".into()),
            },
            LogEntry::Request {
                client_id: usize::MAX,
                session: 10,
                request_number: 1,
                answered: 0,
                op: Op::Put("b".into(), "2".into()),
            },
        ];
        let messages: Vec<KvMessage> = vec![
            Message::Register { client_id: 3 },
            Message::Request {
                client_id: 3,
                session: 1,
                request_number: 5,
                answered: 4,
                op: Op::Put("a".into(), "2".into()),
            },
            Message::Prepare {
                view_number: 2,
                op_number: 8,
                entry: entries[1].clone(),
                commit_number: 7,
            },
            Message::Query {
                client_id: 3,
                query_number: 2,
                query: "a".into(),
            },
            Message::ConfirmView {
                view_number: 2,
                round: 9,
            },
            Message::ConfirmViewOk {
                view_number: 2,
                round: 9,
                replica_id: 1,
            },
            Message::NewState {
                replica_id: 0,
                view_number: 2,
                segment: LogSegment {
                    base: LogBase::Checkpoint(7),
                    entries: entries.clone(),
                },
                commit_number: 7,
            },
            Message::GetChunk {
                replica_id: 2,
                op_number: 7,
                index: 1,
            },
            Message::NewChunk {
                replica_id: 0,
                op_number: 7,
                index: 1,
                chunk,
                last: true,
            },
            Message::NewChunk {
                replica_id: 0,
                op_number: 7,
                index: 0,
                chunk: StoreChunk::default(),
                last: false,
            },
            Message::NewState {
                replica_id: 0,
                view_number: 2,
                segment: LogSegment {
                    base: LogBase::Op(3),
                    entries: entries.clone(),
                },
                commit_number: 3,
            },
            Message::RecoveryResponse {
                view_number: 2,
                nonce: 99,
                replica_id: 1,
                state: Some(RecoveryState {
                    segment: LogSegment {
                        base: LogBase::Checkpoint(7),
                        entries: entries.clone(),
                    },
                    commit_number: 7,
                }),
            },
            Message::DoViewChange {
                view_number: 3,
                replica_id: 2,
                last_normal_view: 2,
                segment: LogSegment {
                    base: LogBase::Op(6),
                    entries: entries.clone(),
                },
                commit_number: 6,
            },
            Message::StartView {
                view_number: 3,
                segment: LogSegment {
                    base: LogBase::Op(6),
                    entries,
                },
                commit_number: 7,
            },
        ];
        for message in messages {
            let line = encode(&Frame::Message(message.clone()));
            match decode(&line).unwrap() {
                Frame::Message(decoded) => assert_eq!(message, decoded, "{line}"),
                Frame::Reply(_) => panic!("{line}"),
            }
        }
        let replies: Vec<KvReply> = vec![
            Reply::Registered {
                view_number: 2,
                client_id: 3,
                session: 1,
            },
            Reply::Executed {
                view_number: 2,
                client_id: 3,
                session: 1,
                request_number: 5,
                result: Some("1".into()),
            },
            Reply::Evicted {
                view_number: 2,
                client_id: 3,
                session: 1,
            },
            Reply::Queried {
                view_number: 2,
                client_id: 3,
                query_number: 2,
                result: None,
            },
        ];
        for reply in replies {
            let line = encode(&Frame::Reply(reply.clone()));
            match decode(&line).unwrap() {
                Frame::Reply(decoded) => assert_eq!(reply, decoded, "{line}"),
                Frame::Message(_) => panic!("{line}"),
            }
        }
    }

    /// A sender connects with Nagle's algorithm off and writes more frames
    /// than one flush takes whole and in order; a frame for the node itself
    /// goes to its event loop.
    #[test]
    fn sender_writes_queued_frames_in_order() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        assert!(connect(address).unwrap().nodelay().unwrap());
        let _ = listener.accept().unwrap();
        let commit = |commit_number| {
            Frame::Message(Message::Commit {
                view_number: 1,
                commit_number,
            })
        };
        let (frames, frames_rx) = channel();
        let count = 3 * MAX_FRAMES_PER_FLUSH + 1;
        for commit_number in 0..count {
            frames.send(commit(commit_number)).unwrap();
        }
        drop(frames);
        let sender = thread::spawn(move || run_sender(1, address, frames_rx));
        let (stream, _) = listener.accept().unwrap();
        let received: Vec<usize> = BufReader::new(stream)
            .lines()
            .map(|line| match decode(&line.unwrap()).unwrap() {
                Frame::Message(Message::Commit { commit_number, .. }) => commit_number,
                _ => panic!("not a commit"),
            })
            .collect();
        sender.join().unwrap();
        assert_eq!(received, (0..count).collect::<Vec<_>>());

        let (events, events_rx) = channel();
        Outboxes::new(0, &[address; 3], events).send_to(0, commit(0));
        assert!(matches!(
            events_rx.try_recv(),
            Ok(Event::Message(Message::Commit { .. }))
        ));
    }

    /// Regression test case for https://github.com/penberg/vsr-rs/issues/12
    #[test]
    fn peer_eof_does_not_complete_an_unterminated_prepare() {
        let (address, received) = peer_acceptor();
        let frame = encode(&prepare_put("ABCDEFGHIJ"));
        let prefix = frame.strip_suffix("DEFGHIJ").unwrap();

        let mut peer = TcpStream::connect(address).unwrap();
        peer.write_all(prefix.as_bytes()).unwrap();
        peer.shutdown(Shutdown::Write).unwrap();

        match received.recv_timeout(Duration::from_secs(1)) {
            Ok(Event::Message(Message::Prepare { entry, .. })) => {
                panic!("incomplete frame was dispatched as {entry:?}")
            }
            Ok(_) => panic!("incomplete frame dispatched an unexpected event"),
            Err(RecvTimeoutError::Timeout) => {}
            Err(err) => panic!("event channel failed: {err}"),
        }
    }

    fn prepare_put(value: &str) -> Frame {
        Frame::Message(Message::Prepare {
            view_number: 0,
            op_number: 2,
            commit_number: 0,
            entry: LogEntry::Request {
                client_id: 7,
                session: 1,
                request_number: 1,
                answered: 0,
                op: Op::Put("key".into(), value.into()),
            },
        })
    }

    fn peer_acceptor() -> (std::net::SocketAddr, Receiver<Event>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (events, received) = channel();
        thread::spawn(move || run_peer_acceptor(listener, events));
        (address, received)
    }

    #[test]
    fn peer_eof_keeps_the_terminated_frames_before_it() {
        let (address, received) = peer_acceptor();
        let complete = encode(&prepare_put("ABCDEFGHIJ"));
        let frame = encode(&prepare_put("KLMNOPQRST"));
        let prefix = frame.strip_suffix("NOPQRST").unwrap();

        let mut peer = TcpStream::connect(address).unwrap();
        peer.write_all(format!("{complete}\n{prefix}").as_bytes())
            .unwrap();
        peer.shutdown(Shutdown::Write).unwrap();

        match received.recv_timeout(Duration::from_secs(1)) {
            Ok(Event::Message(Message::Prepare { entry, .. })) => {
                let LogEntry::Request { op, .. } = entry else {
                    panic!("not a request: {entry:?}");
                };
                assert_eq!(op, Op::Put("key".into(), "ABCDEFGHIJ".into()))
            }
            Ok(_) => panic!("expected the complete PREPARE, got another event"),
            Err(err) => panic!("expected the complete PREPARE, got {err}"),
        }
        match received.recv_timeout(Duration::from_millis(500)) {
            Err(RecvTimeoutError::Timeout) => {}
            Ok(Event::Message(Message::Prepare { entry, .. })) => {
                panic!("incomplete frame was dispatched as {entry:?}")
            }
            Ok(_) => panic!("unexpected second event"),
            Err(err) => panic!("event channel failed: {err}"),
        }
    }

    /// A command the client's end of the connection cuts off is dropped,
    /// as a peer's frame is.
    #[test]
    fn client_eof_does_not_complete_an_unterminated_command() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (events, received) = channel();
        thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let _ = run_client_connection(stream, 1, events);
        });

        let mut client = TcpStream::connect(address).unwrap();
        client.write_all(b"SET key ABC").unwrap();
        client.shutdown(Shutdown::Write).unwrap();

        match received.recv_timeout(Duration::from_secs(1)) {
            Ok(Event::Disconnect(1)) => {}
            Ok(Event::Command { .. }) => panic!("incomplete command was dispatched"),
            Ok(_) => panic!("unexpected event"),
            Err(err) => panic!("expected a disconnect, got {err}"),
        }
    }

    /// A connection reads commands as they come, answers them in the order
    /// they came, those it answers itself among the event loop's, and
    /// answers what it read before the client stopped sending.
    #[test]
    fn connection_answers_pipelined_commands_in_order() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (events, received) = channel();
        thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let _ = run_client_connection(stream, 1, events);
        });

        let mut client = TcpStream::connect(address).unwrap();
        client.write_all(b"SET a 1\r\nPING\r\nGET a\r\n").unwrap();
        client.shutdown(Shutdown::Write).unwrap();
        let mut commands = Vec::new();
        for _ in 0..2 {
            match received.recv_timeout(Duration::from_secs(1)) {
                Ok(Event::Command { respond, .. }) => commands.push(respond),
                Ok(_) => panic!("unexpected event"),
                Err(err) => panic!("expected a command, got {err}"),
            }
        }
        commands[0].send("+OK\r\n".into()).unwrap();
        commands[1].send("$1\r\n1\r\n".into()).unwrap();
        let mut response = String::new();
        std::io::Read::read_to_string(&mut client, &mut response).unwrap();
        assert_eq!("+OK\r\n+PONG\r\n$1\r\n1\r\n", response);
        assert!(matches!(
            received.recv_timeout(Duration::from_secs(1)),
            Ok(Event::Disconnect(1))
        ));
    }

    /// Regression test case for https://github.com/penberg/vsr-rs/issues/15
    #[test]
    fn stalled_peer_does_not_block_other_peers() {
        let stalled = TcpListener::bind("127.0.0.1:0").unwrap();
        let healthy = TcpListener::bind("127.0.0.1:0").unwrap();
        let addresses = vec![
            stalled.local_addr().unwrap(),
            healthy.local_addr().unwrap(),
            "127.0.0.1:1".parse().unwrap(),
        ];
        let (events, _events_rx) = channel();
        let outboxes = Outboxes::new(2, &addresses, events);
        // Accept the stalled peer's connection but never read from it.
        let _stalled_stream = thread::spawn(move || stalled.accept().unwrap().0);

        for _ in 0..16 {
            outboxes.send_to(0, prepare_put(&"v".repeat(4 << 20)));
        }
        outboxes.send_to(
            1,
            Frame::Message(Message::Commit {
                view_number: 0,
                commit_number: 0,
            }),
        );

        let (line_tx, line_rx) = channel();
        thread::spawn(move || {
            let (stream, _) = healthy.accept().unwrap();
            let mut line = String::new();
            BufReader::new(stream).read_line(&mut line).unwrap();
            line_tx.send(line).unwrap();
        });
        let line = line_rx
            .recv_timeout(Duration::from_secs(3))
            .expect("healthy peer got nothing");
        assert_eq!(line, "COMMIT 0 0\n");
    }
}

#[cfg(test)]
mod disk_tests {
    use super::*;
    use fjall::PersistMode;
    use std::collections::HashSet;
    use std::collections::VecDeque;
    use std::path::Path;
    use std::process::Command;
    use std::sync::atomic::Ordering;
    use vsr_rs::{ClientRecord, Config, LogEntry, OpNumber, StateMachine};

    fn temp_dir(test: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("vsr-kvstore-{}-{test}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    fn config() -> Config {
        let mut config = Config::new();
        for _ in 0..3 {
            config.add_replica();
        }
        config
    }

    /// A restart writes and syncs the journal even when nothing it changed
    /// needs a sync, so that records a process crash left in the page
    /// cache, which the replay took for durable, reach the disk.
    #[test]
    fn restart_syncs_the_journal() {
        let dir = temp_dir("restart-syncs");
        drop(Node::open(1, config(), &dir, Start::Init).unwrap());
        let node = Node::open(1, config(), &dir, Start::Restart).unwrap();
        let writes = node.stats.journal_writes.load(Ordering::Relaxed);
        assert_eq!(1, writes);
        drop(node);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Feeds a backup node the primary's `Prepare` for `op_number`, then
    /// writes the journal and drains the replica in one step.
    fn prepare(node: &mut Node, op_number: OpNumber) {
        prepare_committed(node, op_number, op_number - 1);
    }

    /// `prepare`, with the primary's commit number at `commit_number`.
    fn prepare_committed(node: &mut Node, op_number: OpNumber, commit_number: usize) {
        node.replica.on_message(Message::Prepare {
            view_number: 0,
            op_number,
            entry: entry(op_number),
            commit_number,
        });
        step(node);
    }

    /// The primary's entry at `op_number`: client 7's registration, then
    /// its requests, each of which puts `k{op_number}`.
    fn entry(op_number: OpNumber) -> LogEntry<Op> {
        match op_number {
            1 => LogEntry::Register { client_id: 7 },
            _ => LogEntry::Request {
                client_id: 7,
                session: 1,
                request_number: op_number - 1,
                answered: op_number - 2,
                op: Op::Put(format!("k{op_number}"), format!("v{op_number}")),
            },
        }
    }

    /// Tells a backup node that the primary committed up to `commit_number`.
    fn commit(node: &mut Node, commit_number: usize) {
        node.replica.on_message(Message::Commit {
            view_number: 0,
            commit_number,
        });
        step(node);
    }

    /// Ends the node's step, dropping what it sends. Returns whether the
    /// journal was written.
    fn step(node: &mut Node) -> bool {
        node.step(|_, _| {}, &mut Vec::new()).unwrap()
    }

    /// The event loop's store flush runs on a thread of its own, and the
    /// node compacts the log only up to what that flush made durable, not
    /// up to what it applied while the flush was out.
    #[test]
    fn compaction_waits_for_the_store_flush() {
        let dir = temp_dir("flush-compacts");
        let mut node = Node::open(1, config(), &dir, Start::Init).unwrap();
        node.log_retention = 0;
        for op_number in 1..=3 {
            prepare_committed(&mut node, op_number, op_number - 1);
        }
        commit(&mut node, 3);
        let (wake, events) = channel();
        node.start_flush(&wake);
        prepare_committed(&mut node, 4, 3);
        commit(&mut node, 4);
        assert_eq!(4, node.replica.applied());
        assert!(matches!(events.recv().unwrap(), Event::Flushed));
        node.handle(Event::Flushed);
        assert_eq!(3, node.replica.log_start());
        node.flush_store().unwrap();
        assert_eq!(4, node.replica.log_start());
        drop(node);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Journal files small enough to rotate every few entries.
    const SMALL_WAL_FILE: u64 = 14 * 1024;

    fn journal_files(dir: &Path) -> Vec<u64> {
        let mut ids: Vec<u64> = std::fs::read_dir(dir.join("journal"))
            .unwrap()
            .filter_map(Result::ok)
            .filter_map(|entry| {
                let name = entry.file_name().into_string().ok()?;
                name.strip_suffix(".log")?.parse().ok()
            })
            .collect();
        ids.sort_unstable();
        ids
    }

    /// With journal files a few entries long, compaction after every store
    /// persist deletes the files that hold nothing a replay needs, and a
    /// restart replays what is left.
    #[test]
    fn journal_rotates_and_trims() {
        let dir = temp_dir("rotate");
        let mut node = Node::open_with(1, config(), &dir, Start::Init, SMALL_WAL_FILE).unwrap();
        node.log_retention = 3;
        for op_number in 1..=120 {
            prepare(&mut node, op_number);
            if op_number % 20 == 0 {
                node.flush_store().unwrap();
                node.write().unwrap();
            }
        }
        assert_eq!(120, node.replica.op_number());
        assert_eq!(119, node.replica.commit_number());
        assert_eq!(116, node.replica.log_start());
        let before = node.replica.persistent_state();
        // Files rotated every few entries, and once the journal has closed,
        // which waits for its deletions, every file behind the retained
        // entries is gone.
        drop(node);
        let files = journal_files(&dir);
        assert!(files[0] > 0, "the first file was never deleted: {files:?}");
        assert!(files.len() <= 2, "files {files:?}");

        let mut node = Node::open_with(1, config(), &dir, Start::Restart, SMALL_WAL_FILE).unwrap();
        assert_eq!(before, node.replica.persistent_state());
        assert_eq!(
            Some("v119".to_string()),
            node.replica.state_machine().get("k119")
        );
        assert_eq!(None, node.replica.state_machine().get("k120"));
        commit(&mut node, 120);
        assert_eq!(
            Some("v120".to_string()),
            node.replica.state_machine().get("k120")
        );
        drop(node);

        // A used directory refuses --recover, and a store that fell behind
        // the journal's compaction point, as after restoring the wrong
        // backup, is refused rather than replayed from a gap.
        let err = Node::open_with(
            1,
            config(),
            &dir,
            Start::Recover { view: 0 },
            SMALL_WAL_FILE,
        )
        .err()
        .expect("--recover refused");
        assert!(err.contains("--recover"), "{err}");
        std::fs::remove_dir_all(dir.join("store")).unwrap();
        let err = Node::open_with(1, config(), &dir, Start::Restart, SMALL_WAL_FILE)
            .err()
            .expect("store behind the journal refused");
        assert!(err.contains("compacted"), "{err}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// writeahead grows the journal's files with zeros ahead of the
    /// records, so that each write lands in blocks already allocated.
    #[cfg(unix)]
    #[test]
    fn journal_grows_its_files_with_zeros() {
        use std::os::unix::fs::MetadataExt;
        let dir = temp_dir("zeros");
        let mut node = Node::open(1, config(), &dir, Start::Init).unwrap();
        // A filesystem that compresses, such as ZFS or btrfs, can store
        // written zeros as holes, and then the file's blocks tell nothing.
        let probe = dir.join("probe");
        std::fs::write(&probe, [0; 64 * 1024]).unwrap();
        std::fs::File::open(&probe).unwrap().sync_all().unwrap();
        if std::fs::metadata(&probe).unwrap().blocks() * 512 < 64 * 1024 {
            eprintln!("skipped: the temp directory stores written zeros as holes");
            drop(node);
            let _ = std::fs::remove_dir_all(&dir);
            return;
        }
        prepare(&mut node, 1);
        let last = *journal_files(&dir).last().unwrap();
        let file = std::fs::metadata(dir.join("journal").join(format!("{last:010}.log"))).unwrap();
        assert!(
            file.len() >= crate::journal::PREALLOCATION,
            "{} bytes",
            file.len()
        );
        assert!(
            file.blocks() * 512 >= file.len(),
            "{} of {} bytes allocated",
            file.blocks() * 512,
            file.len()
        );
        drop(node);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A write lands as one record whatever its size: here a backup
    /// installs a new view's log of about 6.5 MB, past writeahead's default
    /// limit on a batch, and a restart replays it.
    #[test]
    fn journal_takes_a_write_of_a_long_log() {
        let dir = temp_dir("long-log");
        let mut node = Node::open(1, config(), &dir, Start::Init).unwrap();
        let value = "v".repeat(100);
        let entries: Vec<_> = (1..=50_000)
            .map(|i| LogEntry::Request {
                client_id: 7,
                session: 1,
                request_number: i,
                answered: i - 1,
                op: Op::Put(format!("k{i}"), value.clone()),
            })
            .collect();
        node.replica.on_message(Message::StartView {
            view_number: 2,
            segment: LogSegment {
                base: LogBase::Op(0),
                entries,
            },
            commit_number: 0,
        });
        assert!(step(&mut node));
        assert_eq!(50_000, node.replica.op_number());
        let before = node.replica.persistent_state();
        drop(node);

        let node = Node::open(1, config(), &dir, Start::Restart).unwrap();
        assert_eq!(before, node.replica.persistent_state());
        drop(node);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A step that only moved the commit number is not written. Once the
    /// store persists what it applied, the journal's commit number is
    /// behind the store's applied count, and after a restart the replica
    /// takes the store's.
    #[test]
    fn journal_skips_commit_only_steps() {
        let dir = temp_dir("commit-only");
        let mut node = Node::open(1, config(), &dir, Start::Init).unwrap();
        for op_number in 1..=3 {
            prepare(&mut node, op_number);
        }
        node.replica.on_message(Message::Commit {
            view_number: 0,
            commit_number: 3,
        });
        assert!(!step(&mut node));
        assert_eq!(3, node.replica.applied());
        node.replica.state_machine().persist().unwrap();
        drop(node);

        let node = Node::open(1, config(), &dir, Start::Restart).unwrap();
        assert_eq!(3, node.replica.commit_number());
        assert_eq!(3, node.replica.log_start());
        assert_eq!(3, node.replica.state_machine().applied);
        assert_eq!(
            Some("v3".to_string()),
            node.replica.state_machine().get("k3")
        );
        drop(node);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Writes `batch` to the journal in `dir` as one record, the way the
    /// journal writes a batch, then zeroes its last bytes, as a power loss
    /// in the middle of the write leaves it.
    fn write_torn_batch(dir: &Path, batch: &str) {
        use std::io::{Seek, SeekFrom, Write};
        let mut wal = writeahead::WriteAhead::<writeahead::SimpleFile>::with_options(
            writeahead::WriteAheadOptions {
                log_dir: dir.join("journal"),
                max_file_size: WAL_FILE_SIZE,
                ..Default::default()
            },
        );
        wal.start().unwrap();
        let ids =
            futures::executor::block_on(wal.write_batch(vec![batch.as_bytes().to_vec()])).unwrap();
        drop(wal);
        let path = dir
            .join("journal")
            .join(format!("{:010}.log", ids[0].file_id));
        let bytes = std::fs::read(&path).unwrap();
        let start = ids[0].file_offset as usize;
        let at = start
            + bytes[start..]
                .windows(batch.len())
                .position(|window| window == batch.as_bytes())
                .expect("the batch in its file");
        let mut file = std::fs::OpenOptions::new().write(true).open(path).unwrap();
        file.seek(SeekFrom::Start((at + batch.len() - 4) as u64))
            .unwrap();
        file.write_all(&[0; 4]).unwrap();
    }

    /// A power loss in the middle of a journal write can leave part of a
    /// batch on disk. A batch is one checksummed record, so a replay drops
    /// all of it, and the next batch lands on the state before it: here a
    /// batch that replaced op 4, or ops 4 and 5, with entries the replica
    /// never acknowledged.
    #[test]
    fn journal_drops_a_torn_batch() {
        for replaced in [4..=4, 4..=5] {
            let dir = temp_dir("torn-batch");
            let mut node = Node::open(1, config(), &dir, Start::Init).unwrap();
            for op_number in 1..=5 {
                prepare(&mut node, op_number);
            }
            let before = node.replica.persistent_state();
            drop(node);

            let mut batch = String::new();
            for op_number in replaced.clone() {
                batch.push_str(&format!(
                    "E {op_number} REQ 8 1 {op_number} 0 PUT other value\n"
                ));
            }
            batch.push_str(&format!("H 0 0 3 0 4 {} 0", replaced.end()));
            write_torn_batch(&dir, &batch);

            let mut node = Node::open(1, config(), &dir, Start::Restart).unwrap();
            assert_eq!(before, node.replica.persistent_state(), "{replaced:?}");
            prepare(&mut node, 6);
            let before = node.replica.persistent_state();
            drop(node);
            let node = Node::open(1, config(), &dir, Start::Restart).unwrap();
            assert_eq!(before, node.replica.persistent_state(), "{replaced:?}");
            assert_eq!(6, node.replica.op_number());
            assert!(
                node.replica
                    .log()
                    .iter()
                    .all(|entry| entry.client_id() == 7),
                "{replaced:?}: {:?}",
                node.replica.log()
            );
            drop(node);
            let _ = std::fs::remove_dir_all(&dir);
        }
    }

    /// A directory with no journal is a node that never started, or one
    /// that lost its data: it starts only with `--init` or `--recover`,
    /// and those two need a directory that holds nothing.
    #[test]
    fn start_modes_check_the_directory() {
        let dir = temp_dir("start-modes");
        let err = Node::open(1, config(), &dir, Start::Restart)
            .err()
            .expect("restart of an empty directory refused");
        assert!(err.contains("--init"), "{err}");
        let node = Node::open(1, config(), &dir, Start::Init).unwrap();
        drop(node);
        for start in [Start::Init, Start::Recover { view: 3 }] {
            let err = Node::open(1, config(), &dir, start)
                .err()
                .expect("a used directory refused");
            assert!(err.contains("empty data directory"), "{err}");
        }
        let node = Node::open(1, config(), &dir, Start::Restart).unwrap();
        drop(node);
        let _ = std::fs::remove_dir_all(&dir);

        let dir = temp_dir("start-recover");
        let node = Node::open(1, config(), &dir, Start::Recover { view: 3 }).unwrap();
        assert!(node.replica.is_recovering());
        assert_eq!(3, node.replica.view_number());
        drop(node);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Every start of a node takes a later incarnation, durably, so its
    /// client ids differ from those of every earlier run, however quickly
    /// it restarts, and whatever power loss comes between. A node that
    /// lost its data starts at the time in seconds.
    #[test]
    fn client_ids_differ_across_restarts() {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let dir = temp_dir("incarnation");
        let mut incarnations = Vec::new();
        for start in [Start::Init, Start::Restart, Start::Restart] {
            let node = Node::open(1, config(), &dir, start).unwrap();
            incarnations.push(node.incarnation);
            drop(node);
            power::lose_power(&dir);
        }
        assert!(incarnations[0] >= now, "{incarnations:?}");
        assert!(
            incarnations.windows(2).all(|pair| pair[0] < pair[1]),
            "{incarnations:?}"
        );
        let ids: HashSet<u64> = incarnations
            .iter()
            .map(|incarnation| client_id(1, *incarnation, 0))
            .collect();
        assert_eq!(3, ids.len());
        std::fs::remove_dir_all(&dir).unwrap();
        let node = Node::open(1, config(), &dir, Start::Recover { view: 0 }).unwrap();
        assert!(node.incarnation >= now);
        drop(node);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A store with data and no format of its own, or with the one
    /// keyspace of the formats before 3, was written by another version of
    /// the kvstore, and the node refuses it.
    #[test]
    fn store_of_another_format_is_refused() {
        let dir = temp_dir("format");
        let mut store = new_store(&dir);
        store.apply(1, &Op::Put("a".into(), "1".into()));
        store.persist().unwrap();
        store.meta.remove(FORMAT_KEY).unwrap();
        drop(store);
        let err = Store::open(&dir).err().expect("the store refused");
        assert!(err.contains("another version"), "{err}");
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(dir.with_extension("durable"));

        let db = fjall::Database::builder(&dir).open().unwrap();
        db.keyspace("kv", fjall::KeyspaceCreateOptions::default)
            .unwrap();
        drop(db);
        let err = Store::open(&dir).err().expect("the store refused");
        assert!(err.contains("another version"), "{err}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A store of a node that has not started yet, which records its
    /// format as `Node::open` has it do.
    fn new_store(dir: &Path) -> Store {
        let store = Store::open(dir).unwrap();
        store.stamp_format().unwrap();
        store
    }

    /// A checkpoint replaces keys the store already holds, keeps none the
    /// checkpoint lacks, and leaves the node's own records alone.
    #[test]
    fn restore_over_existing_keys() {
        let dir = temp_dir("restore");
        let mut store = new_store(&dir);
        let incarnation = store.next_incarnation().unwrap();
        for (op_number, (key, value)) in [("a", "1"), ("b", "2")].into_iter().enumerate() {
            store.apply(op_number + 1, &Op::Put(key.into(), value.into()));
        }
        let record = |client_id, request_number| ClientRecord {
            client_id,
            session: 1,
            request_number,
            replies: VecDeque::from([Some("x".to_string())]),
            op_number: 5,
        };
        store.record_client(2, 3, Some(&record(3, 1)));
        let client_table = vec![record(4, 9)];
        let chunks = [
            StoreChunk {
                pairs: Vec::new(),
                clients: client_table.clone(),
            },
            StoreChunk {
                pairs: vec![("a".into(), "x".into()), ("c".into(), "y".into())],
                clients: Vec::new(),
            },
        ];
        for (index, chunk) in chunks.into_iter().enumerate() {
            store.stage_chunk(7, index, chunk);
        }
        store.restore(7);
        drop(store);
        let mut store = Store::open(&dir).unwrap();
        assert_eq!(7, store.applied);
        assert_eq!(Some("x".to_string()), store.get("a"));
        assert_eq!(None, store.get("b"));
        assert_eq!(Some("y".to_string()), store.get("c"));
        assert_eq!(client_table, store.client_table());
        assert!(store.next_incarnation().unwrap() > incarnation);
        drop(store);
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(dir.with_extension("durable"));
    }

    /// A checkpoint of more than `CHUNK_BYTES` comes in several chunks,
    /// all as of the flush it was kept at while the store moves on. Asked
    /// for out of order, as by replicas fetching at once, and again, the
    /// chunks split the state the same way, with nothing past the last one.
    /// Another store restores them whole and durable.
    #[test]
    fn checkpoint_comes_in_chunks() {
        let dir = temp_dir("chunks");
        let mut store = new_store(&dir);
        let value = "v".repeat(1000);
        let count = 3 * CHUNK_BYTES / 1000;
        for op_number in 1..=count {
            store.apply(
                op_number,
                &Op::Put(format!("k{op_number:05}"), value.clone()),
            );
        }
        let record = ClientRecord {
            client_id: 3,
            session: 1,
            request_number: 2,
            replies: VecDeque::from([None]),
            op_number: count,
        };
        store.record_client(count, 3, Some(&record));
        store.persist().unwrap();
        let pairs = store.pairs();
        assert_eq!(count, store.checkpoint());
        store.apply(count + 1, &Op::Put("k00001".into(), "later".into()));
        store.persist().unwrap();

        let mut chunks = std::collections::BTreeMap::new();
        for index in [2, 0, 1] {
            let (chunk, last) = store.checkpoint_chunk(index);
            assert!(!last);
            chunks.insert(index, chunk);
        }
        for index in 3.. {
            let (chunk, last) = store.checkpoint_chunk(index);
            chunks.insert(index, chunk);
            if last {
                break;
            }
        }
        let split: Vec<_> = chunks
            .values()
            .flat_map(|chunk| chunk.pairs.clone())
            .collect();
        assert_eq!(pairs, split);
        assert_eq!(chunks[&1], store.checkpoint_chunk(1).0);
        let past = store.checkpoint_chunk(chunks.len());
        assert_eq!((StoreChunk::default(), true), past);
        store.release_checkpoint();

        let other_dir = temp_dir("chunks-restored");
        let mut other = new_store(&other_dir);
        other.apply(1, &Op::Put("gone".into(), "1".into()));
        for (index, chunk) in chunks {
            other.stage_chunk(count, index, chunk);
        }
        other.restore(count);
        assert_eq!(pairs, other.pairs());
        assert_eq!(vec![record.clone()], other.client_table());
        drop(other);
        let durable = Store::open(&other_dir.with_extension("durable")).unwrap();
        assert_eq!(count, durable.applied);
        assert_eq!(pairs, durable.pairs());
        assert_eq!(vec![record], durable.client_table());
        drop((store, durable));
        for dir in [dir, other_dir] {
            let _ = std::fs::remove_dir_all(dir.with_extension("durable"));
            let _ = std::fs::remove_dir_all(dir);
        }
    }

    /// Chunks staged and synced with a later persist leave the state as it
    /// was: after a power loss before the restore the store holds the old
    /// state, and opening it clears them.
    #[test]
    fn staged_chunks_stay_out_of_the_state() {
        let dir = temp_dir("staged");
        let mut store = new_store(&dir.join("store"));
        store.apply(1, &Op::Put("a".into(), "1".into()));
        let chunk = |key: &str| StoreChunk {
            pairs: vec![(key.into(), "x".into())],
            clients: Vec::new(),
        };
        store.stage_chunk(9, 0, chunk("b"));
        store.apply(2, &Op::Put("c".into(), "3".into()));
        store.persist().unwrap();
        store.stage_chunk(9, 1, chunk("e"));
        drop(store);
        power::lose_power(&dir);
        let mut store = Store::open(&dir.join("store")).unwrap();
        assert_eq!(2, store.applied);
        let pairs = vec![("a".into(), "1".into()), ("c".into(), "3".into())];
        assert_eq!(pairs, store.pairs());
        assert!(store.staging_is_empty());
        store.stage_chunk(10, 0, chunk("d"));
        store.restore(10);
        assert_eq!(vec![("d".into(), "x".into())], store.pairs());
        drop(store);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The store reads its writes before a flush makes them durable and
    /// while one is out; a flush makes durable exactly the ops applied when
    /// it began; a checkpoint restored while a flush is out replaces what
    /// that flush wrote.
    #[test]
    fn store_writes_back() {
        let dir = temp_dir("writes-back");
        let durable_dir = dir.with_extension("durable");
        let mut store = new_store(&dir);
        let put = |key: &str, value: &str| Op::Put(key.into(), value.into());
        store.apply(1, &put("a", "1"));
        assert_eq!(Some("1".into()), store.get("a"));
        let (landed, flushed) = channel();
        assert!(store.flush(move || landed.send(()).unwrap()));
        store.apply(2, &put("a", "2"));
        let record = ClientRecord {
            client_id: 3,
            session: 1,
            request_number: 2,
            replies: VecDeque::from([None, None]),
            op_number: 3,
        };
        store.apply(3, &put("b", "3"));
        store.record_client(3, 3, Some(&record));
        assert!(!store.flush(|| {}), "one flush is out at a time");
        let pairs = vec![("a".into(), "2".into()), ("b".into(), "3".into())];
        assert_eq!(pairs, store.pairs());
        flushed.recv().unwrap();
        assert_eq!(Some(1), store.landed_flush().unwrap());
        let durable = Store::open(&durable_dir).unwrap();
        assert_eq!(1, durable.applied);
        assert_eq!(Some("1".into()), durable.get("a"));
        assert_eq!(None, durable.get("b"));
        drop(durable);
        assert_eq!(pairs, store.pairs());
        store.persist().unwrap();
        let durable = Store::open(&durable_dir).unwrap();
        assert_eq!(3, durable.applied);
        assert_eq!(pairs, durable.pairs());
        assert_eq!(vec![record], durable.client_table());
        drop(durable);
        // A flush this large is still out when the reads below run, so they
        // see its writes through it rather than through fjall.
        let many = 20_000;
        for op_number in 4..4 + many {
            store.apply(op_number, &put(&format!("k{op_number}"), "v"));
        }
        assert!(store.flush(|| {}));
        assert_eq!(Some("v".into()), store.get("k4"));
        assert_eq!(2 + many, store.pairs().len());
        store.apply(4 + many, &put("z", "6"));
        let checkpoint_pairs = vec![("c".into(), "5".into())];
        let chunk = StoreChunk {
            pairs: checkpoint_pairs.clone(),
            clients: Vec::new(),
        };
        store.stage_chunk(9, 0, chunk);
        store.restore(9);
        assert_eq!(checkpoint_pairs, store.pairs());
        drop(store);
        let durable = Store::open(&durable_dir).unwrap();
        assert_eq!(9, durable.applied);
        assert_eq!(checkpoint_pairs, durable.pairs());
        assert_eq!(Vec::<KvClientRecord>::new(), durable.client_table());
        drop(durable);
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&durable_dir);
    }

    /// Runs the test `child` in a process of its own, with the data
    /// directory `dir` in the environment variable `variable`, and waits
    /// for it to exit, as a crash leaves its data.
    fn run_child(child: &str, variable: &str, dir: &Path) {
        let status = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", child, "--nocapture"])
            .env(variable, dir)
            .status()
            .unwrap();
        assert!(status.success(), "child failed: {status}");
    }

    const CHILD_DIR: &str = "KVSTORE_TORN_TAIL_CHILD";
    const CHILD_OPS: usize = 60;
    const CHILD_PERSIST_AT: usize = 25;

    /// The child of `store_behind_journal_after_crash`: a backup node that
    /// persists its store once, applies more ops with the store buffered
    /// only, and dies without flushing anything.
    #[test]
    fn torn_tail_child() {
        let Ok(dir) = std::env::var(CHILD_DIR) else {
            return;
        };
        let dir = PathBuf::from(dir);
        let mut node = Node::open(1, config(), &dir, Start::Init).unwrap();
        node.log_retention = 0;
        for op_number in 1..=CHILD_OPS {
            prepare(&mut node, op_number);
            if op_number == CHILD_PERSIST_AT {
                node.flush_store().unwrap();
            }
        }
        commit(&mut node, CHILD_OPS);
        assert_eq!(CHILD_OPS, node.replica.state_machine().applied);
        assert_eq!(CHILD_PERSIST_AT - 1, node.replica.log_start());
        std::process::exit(0);
    }

    /// A node dies without flushing, the way a power loss takes it, and
    /// the store's journal loses its tail on top. The store comes back at
    /// some prefix of what it applied, behind the replica's journal, and
    /// the restart applies the rest again.
    #[test]
    fn store_behind_journal_after_crash() {
        let dir = temp_dir("torn");
        run_child("disk_tests::torn_tail_child", CHILD_DIR, &dir);

        // Tear the store's journal: drop the last byte of its newest file.
        let store_dir = dir.join("store");
        let mut journals: Vec<PathBuf> = std::fs::read_dir(&store_dir)
            .unwrap()
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| path.extension().is_some_and(|ext| ext == "jnl"))
            .collect();
        journals.sort();
        let newest = journals.last().expect("a store journal");
        let len = std::fs::metadata(newest).unwrap().len();
        if len > 0 {
            std::fs::OpenOptions::new()
                .write(true)
                .open(newest)
                .unwrap()
                .set_len(len - 1)
                .unwrap();
        }

        // The store persisted while op CHILD_PERSIST_AT was appended but
        // not yet committed, so it comes back at or after the op before.
        let store = Store::open(&store_dir).unwrap();
        let applied = store.applied;
        assert!(
            (CHILD_PERSIST_AT - 1..CHILD_OPS).contains(&applied),
            "store applied {applied} ops"
        );
        for op_number in 2..=applied {
            assert_eq!(
                Some(format!("v{op_number}")),
                store.get(&format!("k{op_number}"))
            );
        }
        drop(store);

        // The journal's last write was the Prepare of the last op, which
        // committed the one before; the final commit-only step was never
        // written, and the primary's next Commit brings it back.
        let mut node = Node::open(1, config(), &dir, Start::Restart).unwrap();
        assert_eq!(CHILD_OPS - 1, node.replica.commit_number());
        assert_eq!(CHILD_OPS, node.replica.op_number());
        assert_eq!(CHILD_OPS - 1, node.replica.state_machine().applied);
        assert_eq!(CHILD_PERSIST_AT - 1, node.replica.log_start());
        commit(&mut node, CHILD_OPS);
        assert_eq!(CHILD_OPS, node.replica.state_machine().applied);
        for op_number in 2..=CHILD_OPS {
            assert_eq!(
                Some(format!("v{op_number}")),
                node.replica.state_machine().get(&format!("k{op_number}"))
            );
        }
        assert_eq!(
            vec![ClientRecord {
                client_id: 7,
                session: 1,
                request_number: CHILD_OPS - 1,
                replies: VecDeque::from([None]),
                op_number: CHILD_OPS,
            }],
            node.replica.client_table()
        );
        drop(node);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The child of `process_crash_then_power_loss`: a backup node that
    /// persists its store once, then holds ops that commit all at once in
    /// a step that only moves the commit number, which the journal skips,
    /// and dies in the store's next flush, before its fsync.
    #[test]
    fn process_crash_child() {
        let Ok(dir) = std::env::var(PROCESS_CRASH_CHILD_DIR) else {
            return;
        };
        let dir = PathBuf::from(dir);
        let mut node = Node::open(1, config(), &dir, Start::Init).unwrap();
        node.log_retention = 0;
        for op_number in 1..=CHILD_OPS {
            prepare_committed(&mut node, op_number, (op_number - 1).min(CHILD_PERSIST_AT));
            if op_number == CHILD_PERSIST_AT {
                node.flush_store().unwrap();
            }
        }
        commit(&mut node, CHILD_OPS);
        assert_eq!(CHILD_OPS, node.replica.state_machine().applied);
        // A flush writes the store's writes and they reach the OS, as they
        // do a moment later in a running node, and the process dies before
        // the flush's fsync.
        let store = node.replica.state_machine();
        store.write_unsynced().unwrap();
        store.db.persist(PersistMode::Buffer).unwrap();
        std::process::exit(0);
    }

    const PROCESS_CRASH_CHILD_DIR: &str = "KVSTORE_PROCESS_CRASH_CHILD";

    /// A node's process dies with operations the store applied only in the
    /// page cache, beyond the journal's commit number. The restart finds
    /// them, and compacts the log up to there, which it may do only once
    /// the store has made them durable: a power loss after the restart must
    /// not take the store back behind the journal's compaction point. The
    /// power model leaves out the fsync fjall does as it recovers, see
    /// `power`, so the node must make the store durable itself.
    #[test]
    fn process_crash_then_power_loss() {
        let dir = temp_dir("process-crash");
        run_child(
            "disk_tests::process_crash_child",
            PROCESS_CRASH_CHILD_DIR,
            &dir,
        );

        let node = Node::open(1, config(), &dir, Start::Restart).unwrap();
        let applied = node.replica.state_machine().applied;
        assert!(
            applied > CHILD_PERSIST_AT + 1,
            "store applied {applied} ops"
        );
        assert_eq!(applied, node.replica.log_start());
        drop(node);
        power::lose_power(&dir);

        let node = Node::open(1, config(), &dir, Start::Restart).unwrap();
        assert_eq!(applied, node.replica.state_machine().applied);
        for op_number in 2..=applied {
            assert_eq!(
                Some(format!("v{op_number}")),
                node.replica.state_machine().get(&format!("k{op_number}"))
            );
        }
        drop(node);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
