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
//! Reads and writes both go through the replicated log, so a GET is
//! linearizable.
//!
//! Each node keeps two things on disk. The replica's log and counters go
//! to a write-ahead log through the `writeahead` crate: after every batch
//! of events, before anything the batch produced is sent, the node writes
//! what changed and fsyncs once. The store itself is a `fjall` database
//! that never fsyncs on its own: every operation is one atomic batch that
//! also records the op number and the client's reply, and a timer persists
//! the database every second. What it had persisted is then durable, and
//! the replica compacts its log up to there, less a retention window. On
//! restart the node replays the write-ahead log, opens the store, persists
//! whatever the store recovered, and applies the committed entries the
//! store had not made durable. See README.md next to this file.

use fjall::{Database, Keyspace, KeyspaceCreateOptions, PersistMode, Readable};
use journal::{EntryCodec, Journal};
use log::{debug, info, warn};
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::thread;
use std::time::{Duration, Instant};
use vsr_rs::{
    Checkpoint, Client, ClientID, ClientRecord, Config, LogBase, LogEntry, LogSegment, Message,
    MessageFor, OpNumber, RecoveryState, Replica, ReplicaID, Reply, RequestNumber, StateMachine,
    ViewNumber,
};

mod journal;

/// How often the replica and the clients run their idle logic.
const TICK: Duration = Duration::from_millis(100);
/// Ticks between two re-sends of a request that got no reply.
const CLIENT_RESEND_TICKS: u64 = 5;
/// Idle periods without hearing from the primary before a view change.
const PRIMARY_TIMEOUT: usize = 5;
/// Ticks between two persists of the store, each of which lets the
/// replica compact its log.
const FLUSH_TICKS: u64 = 10;
/// Log entries kept behind what the store has persisted, so that a replica
/// a little behind catches up from the log rather than from a checkpoint.
const LOG_RETENTION: usize = 1_000;
/// Size of a write-ahead log file. Small, so that compaction reclaims
/// space in a short run.
const WAL_FILE_SIZE: u64 = 4 * 1024 * 1024;

// ---------------------------------------------------------------------------
// Operations

#[derive(Clone, Debug, PartialEq, Eq)]
enum Op {
    Put(String, String),
    Get(String),
}

fn encode_op(op: &Op) -> String {
    match op {
        Op::Put(key, value) => format!("PUT {key} {value}"),
        Op::Get(key) => format!("GET {key}"),
    }
}

fn encode_entry(entry: &LogEntry<Op>) -> String {
    format!(
        "{} {} {}",
        entry.client_id,
        entry.request_number,
        encode_op(&entry.op)
    )
}

fn decode_entry(text: &str) -> Result<LogEntry<Op>, String> {
    let mut t = Tokens::new(text);
    let entry = t.entry()?;
    t.done()?;
    Ok(entry)
}

const ENTRY_CODEC: EntryCodec<Op> = EntryCodec {
    encode: encode_entry,
    decode: decode_entry,
};

fn encode_entries(log: &[LogEntry<Op>]) -> String {
    let mut out = log.len().to_string();
    for entry in log {
        out.push(' ');
        out.push_str(&encode_entry(entry));
    }
    out
}

fn encode_result(result: &Option<String>) -> String {
    match result {
        Some(value) => format!("+{value}"),
        None => "-".to_string(),
    }
}

/// A cursor over the tokens of one encoded line.
struct Tokens<'a> {
    iter: std::str::SplitWhitespace<'a>,
}

impl<'a> Tokens<'a> {
    fn new(line: &'a str) -> Tokens<'a> {
        Tokens {
            iter: line.split_whitespace(),
        }
    }

    fn word(&mut self) -> Result<&'a str, String> {
        self.iter
            .next()
            .ok_or_else(|| "truncated message".to_string())
    }

    fn num(&mut self) -> Result<usize, String> {
        let word = self.word()?;
        word.parse().map_err(|_| format!("bad number {word:?}"))
    }

    fn op(&mut self) -> Result<Op, String> {
        match self.word()? {
            "PUT" => Ok(Op::Put(self.word()?.to_string(), self.word()?.to_string())),
            "GET" => Ok(Op::Get(self.word()?.to_string())),
            kind => Err(format!("bad op {kind:?}")),
        }
    }

    fn entry(&mut self) -> Result<LogEntry<Op>, String> {
        Ok(LogEntry {
            client_id: self.num()?,
            request_number: self.num()?,
            op: self.op()?,
        })
    }

    fn entries(&mut self) -> Result<Vec<LogEntry<Op>>, String> {
        let count = self.num()?;
        let mut log = Vec::with_capacity(count);
        for _ in 0..count {
            log.push(self.entry()?);
        }
        Ok(log)
    }

    fn result(&mut self) -> Result<Option<String>, String> {
        Ok(match self.word()? {
            "-" => None,
            value => Some(value[1..].to_string()),
        })
    }

    fn done(&mut self) -> Result<(), String> {
        match self.iter.next() {
            None => Ok(()),
            Some(word) => Err(format!("trailing {word:?}")),
        }
    }
}

// ---------------------------------------------------------------------------
// The store: a fjall database that never fsyncs on its own

/// Key prefixes in the store's one keyspace.
const KEY_PREFIX: &str = "k/";
const CLIENT_PREFIX: &str = "c/";
/// The number of operations applied to the store, written in the same
/// batch as each operation.
const APPLIED_KEY: &str = "m/applied";
/// The number of times the node has started, which keeps the client ids
/// of one run apart from those of the others.
const INCARNATION_KEY: &str = "m/incarnation";

/// The key-value store. The output of an op is the value read by a GET.
struct Store {
    path: PathBuf,
    db: Database,
    keyspace: Keyspace,
    /// The number of operations applied, as recorded in the store.
    applied: OpNumber,
}

/// Every key and value in the store, for a replica that fell behind a
/// compacted log.
#[derive(Clone, Debug, PartialEq, Eq)]
struct StoreSnapshot {
    pairs: Vec<(String, String)>,
}

fn utf8(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

impl Store {
    fn open(path: &Path) -> Result<Store, String> {
        let db = Database::builder(path)
            .manual_journal_persist(true)
            .open()
            .map_err(|err| format!("cannot open store at {}: {err}", path.display()))?;
        let keyspace = db
            .keyspace("kv", KeyspaceCreateOptions::default)
            .map_err(|err| format!("cannot open keyspace: {err}"))?;
        let mut store = Store {
            path: path.to_path_buf(),
            db,
            keyspace,
            applied: 0,
        };
        store.applied = store.read_counter(APPLIED_KEY)?;
        Ok(store)
    }

    /// The number the store keeps under `key`, or 0 if it keeps none.
    fn read_counter<T: std::str::FromStr + Default>(&self, key: &str) -> Result<T, String> {
        match self.keyspace.get(key).map_err(|err| err.to_string())? {
            Some(value) => utf8(&value)
                .parse()
                .map_err(|_| format!("bad {key} in store")),
            None => Ok(T::default()),
        }
    }

    /// Counts one more start of the node, and returns its incarnation: the
    /// time in seconds, or one more than the last incarnation if that is
    /// later. It increases across quick restarts, and a node that lost its
    /// data, its count with it, starts past its earlier runs unless they
    /// followed each other faster than once a second right before. The
    /// count is durable once the store is persisted.
    fn next_incarnation(&mut self) -> Result<u64, String> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|since| since.as_secs())
            .unwrap_or(0);
        let incarnation = (self.read_counter::<u64>(INCARNATION_KEY)? + 1).max(now);
        self.keyspace
            .insert(INCARNATION_KEY, incarnation.to_string())
            .map_err(|err| format!("cannot write store: {err}"))?;
        Ok(incarnation)
    }

    /// The client table as the store recorded it, for `Replica::restart`.
    fn client_table(&self) -> Result<Vec<ClientRecord<Option<String>>>, String> {
        let mut table = Vec::new();
        for guard in self.keyspace.prefix(CLIENT_PREFIX) {
            let (key, value) = guard.into_inner().map_err(|err| err.to_string())?;
            let client_id = utf8(&key[CLIENT_PREFIX.len()..])
                .parse()
                .map_err(|_| "bad client id in store".to_string())?;
            let value = utf8(&value);
            let mut t = Tokens::new(&value);
            table.push(ClientRecord {
                client_id,
                request_number: t.num()?,
                reply: t.result()?,
            });
        }
        Ok(table)
    }

    /// Makes everything applied so far durable.
    fn persist(&self) -> Result<(), String> {
        self.db
            .persist(PersistMode::SyncData)
            .map_err(|err| format!("cannot persist store at {}: {err}", self.path.display()))?;
        #[cfg(test)]
        power::record_durable(&self.path);
        Ok(())
    }

    fn get(&self, key: &str) -> Option<String> {
        self.keyspace
            .get(format!("{KEY_PREFIX}{key}"))
            .unwrap_or_else(|err| fatal(&format!("cannot read store: {err}")))
            .map(|value| utf8(&value))
    }
}

fn fatal(message: &str) -> ! {
    eprintln!("{message}");
    std::process::exit(1);
}

impl StateMachine for Store {
    type Input = Op;
    type Output = Option<String>;
    type Snapshot = StoreSnapshot;

    fn apply(&mut self, op_number: OpNumber, entry: &LogEntry<Op>) -> Option<String> {
        let mut batch = self.db.batch();
        let result = match &entry.op {
            Op::Put(key, value) => {
                batch.insert(&self.keyspace, format!("{KEY_PREFIX}{key}"), value.as_str());
                None
            }
            Op::Get(key) => self.get(key),
        };
        batch.insert(
            &self.keyspace,
            format!("{CLIENT_PREFIX}{}", entry.client_id),
            format!("{} {}", entry.request_number, encode_result(&result)),
        );
        batch.insert(&self.keyspace, APPLIED_KEY, op_number.to_string());
        batch
            .commit()
            .unwrap_or_else(|err| fatal(&format!("cannot write store: {err}")));
        self.applied = op_number;
        result
    }

    fn snapshot(&self) -> StoreSnapshot {
        let snapshot = self.db.snapshot();
        let mut pairs = Vec::new();
        for guard in snapshot.prefix(&self.keyspace, KEY_PREFIX) {
            let (key, value) = guard
                .into_inner()
                .unwrap_or_else(|err| fatal(&format!("cannot read store: {err}")));
            pairs.push((utf8(&key[KEY_PREFIX.len()..]), utf8(&value)));
        }
        StoreSnapshot { pairs }
    }

    /// Replaces the keys, values, and client table with the checkpoint's,
    /// in one batch that removes only the keys the checkpoint lacks, and
    /// makes it durable before returning, as the library requires.
    fn restore(&mut self, checkpoint: Checkpoint<Option<String>, StoreSnapshot>) {
        let mut pairs = checkpoint.state.pairs;
        pairs.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        let mut table = checkpoint.client_table;
        table.sort_unstable_by_key(|record| record.client_id);
        let mut batch = self.db.batch();
        for guard in self.keyspace.prefix(KEY_PREFIX) {
            let key = guard
                .key()
                .unwrap_or_else(|err| fatal(&format!("cannot read store: {err}")));
            let name = &key[KEY_PREFIX.len()..];
            if pairs
                .binary_search_by(|(kept, _)| kept.as_bytes().cmp(name))
                .is_err()
            {
                batch.remove(&self.keyspace, key);
            }
        }
        for guard in self.keyspace.prefix(CLIENT_PREFIX) {
            let key = guard
                .key()
                .unwrap_or_else(|err| fatal(&format!("cannot read store: {err}")));
            let kept = utf8(&key[CLIENT_PREFIX.len()..]).parse().is_ok_and(|id| {
                table
                    .binary_search_by_key(&id, |record| record.client_id)
                    .is_ok()
            });
            if !kept {
                batch.remove(&self.keyspace, key);
            }
        }
        for (key, value) in pairs {
            batch.insert(&self.keyspace, format!("{KEY_PREFIX}{key}"), value);
        }
        for record in table {
            batch.insert(
                &self.keyspace,
                format!("{CLIENT_PREFIX}{}", record.client_id),
                format!("{} {}", record.request_number, encode_result(&record.reply)),
            );
        }
        batch.insert(
            &self.keyspace,
            APPLIED_KEY,
            checkpoint.op_number.to_string(),
        );
        batch
            .commit()
            .unwrap_or_else(|err| fatal(&format!("cannot write store: {err}")));
        self.persist().unwrap_or_else(|err| fatal(&err));
        self.applied = checkpoint.op_number;
    }
}

// ---------------------------------------------------------------------------
// Wire encoding between nodes: one message per line, whitespace separated.

type KvReply = Reply<Option<String>>;
type KvMessage = MessageFor<Store>;
type KvCheckpoint = Checkpoint<Option<String>, StoreSnapshot>;
type KvSegment = LogSegment<Op, Option<String>, StoreSnapshot>;

/// What travels between nodes: protocol messages, and replies routed back
/// to the node that owns the client connection.
enum Frame {
    Message(KvMessage),
    Reply(KvReply),
}

fn encode_checkpoint(checkpoint: &KvCheckpoint) -> String {
    let mut out = format!("{} {}", checkpoint.op_number, checkpoint.client_table.len());
    for record in &checkpoint.client_table {
        out.push_str(&format!(
            " {} {} {}",
            record.client_id,
            record.request_number,
            encode_result(&record.reply)
        ));
    }
    out.push_str(&format!(" {}", checkpoint.state.pairs.len()));
    for (key, value) in &checkpoint.state.pairs {
        out.push_str(&format!(" {key} {value}"));
    }
    out
}

fn encode_segment(segment: &KvSegment) -> String {
    let base = match &segment.base {
        LogBase::Op(op_number) => format!("- {op_number}"),
        LogBase::Checkpoint(checkpoint) => format!("+ {}", encode_checkpoint(checkpoint)),
    };
    format!("{base} {}", encode_entries(&segment.entries))
}

fn encode(frame: &Frame) -> String {
    match frame {
        Frame::Message(message) => match message {
            Message::Request {
                client_id,
                request_number,
                op,
            } => format!("REQUEST {client_id} {request_number} {}", encode_op(op)),
            Message::Prepare {
                view_number,
                op_number,
                client_id,
                request_number,
                op,
                commit_number,
            } => format!(
                "PREPARE {view_number} {op_number} {commit_number} {client_id} {request_number} {}",
                encode_op(op)
            ),
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
                view_number,
                segment,
                commit_number,
            } => format!(
                "NEWSTATE {view_number} {commit_number} {}",
                encode_segment(segment)
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
        Frame::Reply(reply) => format!(
            "REPLY {} {} {} {}",
            reply.view_number,
            reply.client_id,
            reply.request_number,
            encode_result(&reply.result)
        ),
    }
}

impl<'a> Tokens<'a> {
    fn checkpoint(&mut self) -> Result<KvCheckpoint, String> {
        let op_number = self.num()?;
        let count = self.num()?;
        let mut client_table = Vec::with_capacity(count);
        for _ in 0..count {
            client_table.push(ClientRecord {
                client_id: self.num()?,
                request_number: self.num()?,
                reply: self.result()?,
            });
        }
        let count = self.num()?;
        let mut pairs = Vec::with_capacity(count);
        for _ in 0..count {
            pairs.push((self.word()?.to_string(), self.word()?.to_string()));
        }
        Ok(Checkpoint {
            op_number,
            state: StoreSnapshot { pairs },
            client_table,
        })
    }

    fn segment(&mut self) -> Result<KvSegment, String> {
        let base = match self.word()? {
            "-" => LogBase::Op(self.num()?),
            "+" => LogBase::Checkpoint(self.checkpoint()?),
            word => return Err(format!("bad segment base {word:?}")),
        };
        let entries = self.entries()?;
        Ok(LogSegment { base, entries })
    }
}

fn decode(line: &str) -> Result<Frame, String> {
    let mut t = Tokens::new(line);
    let message = match t.word()? {
        "REQUEST" => Message::Request {
            client_id: t.num()?,
            request_number: t.num()?,
            op: t.op()?,
        },
        "PREPARE" => Message::Prepare {
            view_number: t.num()?,
            op_number: t.num()?,
            commit_number: t.num()?,
            client_id: t.num()?,
            request_number: t.num()?,
            op: t.op()?,
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
            view_number: t.num()?,
            commit_number: t.num()?,
            segment: t.segment()?,
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
        "REPLY" => {
            let view_number = t.num()?;
            let client_id = t.num()?;
            let request_number = t.num()?;
            let result = t.result()?;
            return Ok(Frame::Reply(Reply {
                view_number,
                client_id,
                request_number,
                result,
            }));
        }
        kind => return Err(format!("unknown message {kind:?}")),
    };
    Ok(Frame::Message(message))
}

// ---------------------------------------------------------------------------
// Networking between nodes

/// Sends frames to other nodes, connecting on demand. A node that cannot be
/// reached just loses the message; the protocol re-sends what matters.
fn run_sender(
    self_id: ReplicaID,
    addresses: Vec<SocketAddr>,
    frames: Receiver<(ReplicaID, Frame)>,
    events: Sender<Event>,
) {
    let mut streams: HashMap<ReplicaID, TcpStream> = HashMap::new();
    let mut last_failure: HashMap<ReplicaID, Instant> = HashMap::new();
    for (dst, frame) in frames {
        if dst == self_id {
            // Our own messages, for example a client request when we are
            // the primary, go straight to our event loop.
            let event = match frame {
                Frame::Message(message) => Event::Message(message),
                Frame::Reply(reply) => Event::Reply(reply),
            };
            let _ = events.send(event);
            continue;
        }
        let stream = match streams.entry(dst) {
            std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::hash_map::Entry::Vacant(entry) => {
                if last_failure
                    .get(&dst)
                    .is_some_and(|at| at.elapsed() < Duration::from_millis(500))
                {
                    continue;
                }
                match TcpStream::connect_timeout(&addresses[dst], Duration::from_millis(200)) {
                    Ok(stream) => {
                        info!("connected to node {dst} at {}", addresses[dst]);
                        entry.insert(stream)
                    }
                    Err(err) => {
                        debug!("node {dst} unreachable: {err}");
                        last_failure.insert(dst, Instant::now());
                        continue;
                    }
                }
            }
        };
        let line = encode(&frame);
        if let Err(err) = stream
            .write_all(line.as_bytes())
            .and_then(|_| stream.write_all(b"\n"))
        {
            // Drop the stream but do not start the backoff: the peer was
            // reachable a moment ago, so the next frame retries the connect
            // right away. If that connect fails, the backoff starts then.
            warn!("lost connection to node {dst}: {err}");
            streams.remove(&dst);
        }
    }
}

/// Accepts connections from other nodes and feeds their frames to the event
/// loop.
fn run_peer_acceptor(listener: TcpListener, events: Sender<Event>) {
    for stream in listener.incoming().flatten() {
        let events = events.clone();
        thread::spawn(move || {
            for line in BufReader::new(stream).lines().map_while(Result::ok) {
                match decode(&line) {
                    Ok(Frame::Message(message)) => {
                        let _ = events.send(Event::Message(message));
                    }
                    Ok(Frame::Reply(reply)) => {
                        let _ = events.send(Event::Reply(reply));
                    }
                    Err(err) => warn!("bad message from peer: {err}"),
                }
            }
        });
    }
}

// ---------------------------------------------------------------------------
// Client connections

enum Command {
    Set(String, String),
    Get(String),
}

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

/// Serves one client connection: one command at a time, each answered once
/// the replicated store has executed it. However the connection ends, the
/// event loop is told it is gone.
fn run_client_connection(
    stream: TcpStream,
    connection: u64,
    events: Sender<Event>,
) -> std::io::Result<()> {
    let (respond_tx, respond_rx) = channel::<String>();
    let mut writer = stream.try_clone()?;
    let reader = BufReader::new(stream);
    // A read or write error breaks out of the loop rather than returning, or
    // it would carry us past the Event::Disconnect below and the event loop
    // would keep this connection's client for the life of the process.
    let mut outcome = Ok(());
    for line in reader.lines() {
        let step = line.and_then(|line| {
            run_client_command(
                &line,
                &mut writer,
                connection,
                &events,
                &respond_tx,
                &respond_rx,
            )
        });
        match step {
            Ok(true) => continue,
            Ok(false) => break,
            Err(err) => {
                outcome = Err(err);
                break;
            }
        }
    }
    let _ = events.send(Event::Disconnect(connection));
    outcome
}

/// Runs one command from a client connection. Returns false once the
/// connection should be closed.
fn run_client_command(
    line: &str,
    writer: &mut TcpStream,
    connection: u64,
    events: &Sender<Event>,
    respond_tx: &Sender<String>,
    respond_rx: &Receiver<String>,
) -> std::io::Result<bool> {
    let command = match parse_command(line) {
        Ok(None) => return Ok(true),
        Ok(Some(command)) => command,
        Err(response) => {
            writer.write_all(format!("{response}\r\n").as_bytes())?;
            return Ok(true);
        }
    };
    let _ = events.send(Event::Command {
        connection,
        command,
        respond: respond_tx.clone(),
    });
    let Ok(response) = respond_rx.recv() else {
        return Ok(false);
    };
    writer.write_all(response.as_bytes())?;
    Ok(true)
}

/// The client id of a node's `next` connection in its `incarnation`.
///
/// Client ids must never repeat, or the primary's client table mistakes a
/// new connection's first request for a re-send of an old one and answers
/// it from the cache (section 4.5 of the paper). The node id in the top
/// byte tells the primary which node to route the reply to, the node's
/// incarnation in the next 32 bits separates its runs, and the low 24 bits
/// count its connections.
fn client_id(node_id: ReplicaID, incarnation: u64, next: u64) -> u64 {
    ((node_id as u64) << 56) | ((incarnation & 0xFFFF_FFFF) << 24) | (next & 0xFF_FFFF)
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

fn node_of(client_id: ClientID) -> ReplicaID {
    (client_id >> 56) as ReplicaID
}

// ---------------------------------------------------------------------------
// The node: one thread owning the replica and the proxied clients

enum Event {
    Message(KvMessage),
    Reply(KvReply),
    Command {
        connection: u64,
        command: Command,
        respond: Sender<String>,
    },
    Disconnect(u64),
    Tick,
}

/// A client connection's VSR client and the command it is waiting on.
struct Connection {
    client: Client<Op>,
    pending: Option<(RequestNumber, Command, Sender<String>)>,
}

/// Hands a reply to the connection waiting for it, if it is still there
/// and still waiting for that request.
fn deliver_reply(connections: &mut HashMap<u64, Connection>, reply: KvReply) {
    let Some(connection) = connections.get_mut(&(reply.client_id as u64)) else {
        return;
    };
    let answers_pending = connection
        .client
        .on_reply(reply.request_number, reply.view_number);
    if answers_pending {
        if let Some((_, command, respond)) = connection.pending.take() {
            let _ = respond.send(format_reply(&command, reply.result));
        }
    }
}

fn format_reply(command: &Command, result: Option<String>) -> String {
    match (command, result) {
        (Command::Set(..), _) => "+OK\r\n".to_string(),
        (Command::Get(_), Some(value)) => format!("${}\r\n{value}\r\n", value.len()),
        (Command::Get(_), None) => "$-1\r\n".to_string(),
    }
}

/// How a node starts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Start {
    /// As a new node of a new cluster, on an empty data directory.
    Init,
    /// From what its data directory holds, which it wrote before.
    Restart,
    /// As a node that lost its data, on an empty data directory: it
    /// recovers the state from the others. `view` must be at least every
    /// view the lost node could have taken part in; the highest view any
    /// other node reports bounds them all.
    Recover { view: ViewNumber },
}

/// The node's state: the replica, its journal, and the client connections.
struct Node {
    id: ReplicaID,
    config: Config,
    replica: Replica<Store>,
    journal: Journal<Op>,
    /// The number of times this node has started, which its client ids
    /// carry.
    incarnation: u64,
    connections: HashMap<u64, Connection>,
    ticks: u64,
    view: usize,
    /// Log entries kept behind what the store has persisted.
    log_retention: usize,
}

impl Node {
    /// Opens the store and the journal in `data_dir`, and builds the
    /// replica as `start` says. The journal holds the replica's counters
    /// once this returns, so a node that started can only restart.
    fn open(id: ReplicaID, config: Config, data_dir: &Path, start: Start) -> Result<Node, String> {
        Node::open_with(id, config, data_dir, start, WAL_FILE_SIZE)
    }

    /// `open`, with journal files of `wal_file_size` bytes.
    fn open_with(
        id: ReplicaID,
        config: Config,
        data_dir: &Path,
        start: Start,
        wal_file_size: u64,
    ) -> Result<Node, String> {
        let dir = data_dir.display();
        let journal_dir = data_dir.join("journal");
        if start == Start::Restart && !journal_dir.exists() {
            return Err(format!(
                "{dir} holds no journal: start a node of a new cluster with --init, or a node that lost its data with --recover --view N"
            ));
        }
        let mut store = Store::open(&data_dir.join("store"))?;
        let (journal, state) = Journal::open(&journal_dir, wal_file_size, ENTRY_CODEC)?;
        let inconsistent = |what: &str| {
            format!(
                "{what} in {dir}; restore the store and the journal from the same backup, or remove both and start with --recover"
            )
        };
        match (&state, start) {
            (Some(_), Start::Init | Start::Recover { .. }) => {
                return Err(format!(
                    "--init and --recover need an empty data directory, and {dir} has a journal"
                ));
            }
            (Some(state), Start::Restart) if store.applied < state.log_start => {
                return Err(inconsistent(&format!(
                    "the store has applied {} ops but the journal has compacted up to {}",
                    store.applied, state.log_start
                )));
            }
            (None, _) if store.applied > 0 => {
                return Err(inconsistent(&format!(
                    "the store has applied {} ops but the journal is empty",
                    store.applied
                )));
            }
            (None, Start::Restart) => {
                return Err(format!(
                    "{dir} holds no journal: start a node of a new cluster with --init, or a node that lost its data with --recover --view N"
                ));
            }
            _ => {}
        }
        let incarnation = store.next_incarnation()?;
        // What the store recovered can include operations it applied only
        // in the page cache before the process died. `Replica::restart`
        // needs a state machine that is durable as of what it applied, and
        // may compact the log up to there.
        store.persist()?;
        // Differs from the nonce of every earlier recovery of this node,
        // since the incarnation does.
        let nonce = incarnation;
        let replica = match (state, start) {
            (Some(state), Start::Restart) => {
                let client_table = store.client_table()?;
                let applied = store.applied;
                println!(
                    "restarting from view {} with {} ops, {} committed, {applied} applied by the store",
                    state.view_number,
                    state.log_start + state.log.len(),
                    state.commit_number
                );
                Replica::restart(
                    id,
                    config.clone(),
                    store,
                    applied,
                    client_table,
                    state,
                    nonce,
                )
            }
            (None, Start::Recover { view }) => {
                println!("recovering with an empty disk, in view {view} or later");
                Replica::recover(id, config.clone(), store, view, nonce)
            }
            (None, Start::Init) => Replica::new(id, config.clone(), store),
            _ => unreachable!("checked above"),
        };
        let mut node = Node {
            id,
            view: replica.view_number(),
            replica,
            journal,
            incarnation,
            config,
            connections: HashMap::new(),
            ticks: 0,
            log_retention: LOG_RETENTION,
        };
        node.write()?;
        Ok(node)
    }

    /// Writes what the last steps changed to the journal, if the write
    /// needs a sync, and hands it back to the replica, which releases what
    /// waited for it. Returns whether the journal was written.
    fn write(&mut self) -> Result<bool, String> {
        let mut synced = false;
        let journal = &mut self.journal;
        self.replica.persist(|write| {
            synced = write.sync;
            if write.sync {
                journal.append(write)?;
            }
            Ok::<(), String>(())
        })?;
        Ok(synced)
    }

    /// Ends a batch of steps the way the library requires: the messages
    /// that need not wait go to `send` first, so that the other nodes'
    /// writes overlap this one's, then the journal is written, then the
    /// rest goes to `send`, and the replies to `replies`. Returns whether
    /// the journal was written.
    fn step(
        &mut self,
        mut send: impl FnMut(ReplicaID, KvMessage),
        replies: &mut Vec<KvReply>,
    ) -> Result<bool, String> {
        for (dst, message) in self.replica.drain_messages_before_persist() {
            send(dst, message);
        }
        let written = self.write()?;
        for (dst, message) in self.replica.drain_messages() {
            send(dst, message);
        }
        replies.extend(self.replica.drain_replies());
        Ok(written)
    }

    /// Steps the node, and sends what it and its clients produced:
    /// protocol messages to the sender thread, and replies to the node
    /// that owns the client connection, which may be this one.
    fn deliver(&mut self, frames: &Sender<(ReplicaID, Frame)>) {
        let mut replies = Vec::new();
        let send = |dst, message| {
            let _ = frames.send((dst, Frame::Message(message)));
        };
        self.step(send, &mut replies)
            .unwrap_or_else(|err| fatal(&err));
        for reply in replies {
            let owner = node_of(reply.client_id);
            if owner == self.id {
                deliver_reply(&mut self.connections, reply);
            } else {
                let _ = frames.send((owner, Frame::Reply(reply)));
            }
        }
        for connection in self.connections.values_mut() {
            for (dst, message) in connection.client.drain() {
                let _ = frames.send((dst, Frame::Message(message)));
            }
        }
    }

    /// Steps the replica or a client with one event. Returns whether the
    /// store is due for a persist.
    fn handle(&mut self, event: Event) -> bool {
        match event {
            Event::Message(message) => self.replica.on_message(message),
            Event::Reply(reply) => deliver_reply(&mut self.connections, reply),
            Event::Command {
                connection: id,
                command,
                respond,
            } => {
                let config = &self.config;
                let connection = self.connections.entry(id).or_insert_with(|| Connection {
                    client: Client::new(id as ClientID, config.clone()),
                    pending: None,
                });
                let op = match &command {
                    Command::Set(key, value) => Op::Put(key.clone(), value.clone()),
                    Command::Get(key) => Op::Get(key.clone()),
                };
                let request_number = connection.client.on_request(op);
                connection.pending = Some((request_number, command, respond));
            }
            Event::Disconnect(id) => {
                self.connections.remove(&id);
            }
            Event::Tick => {
                self.ticks += 1;
                self.replica.on_idle();
                if self.ticks.is_multiple_of(CLIENT_RESEND_TICKS) {
                    for connection in self.connections.values_mut() {
                        connection.client.on_idle();
                    }
                }
                return self.ticks.is_multiple_of(FLUSH_TICKS);
            }
        }
        false
    }

    /// Persists the store, which makes everything it has applied durable,
    /// and compacts the log up to there, less the retention window. The
    /// compaction reaches the journal with the next batch.
    fn flush_store(&mut self) -> Result<(), String> {
        let applied = self.replica.applied();
        self.replica.state_machine().persist()?;
        self.replica
            .compact(applied.saturating_sub(self.log_retention));
        Ok(())
    }

    /// Runs the event loop: every batch of events already queued is
    /// stepped, and then delivered, see [`Node::step`].
    fn run(&mut self, events: Receiver<Event>, frames: Sender<(ReplicaID, Frame)>) {
        while let Ok(event) = events.recv() {
            let mut flush_store = self.handle(event);
            while let Ok(event) = events.try_recv() {
                flush_store |= self.handle(event);
            }
            self.deliver(&frames);
            if flush_store {
                self.flush_store().unwrap_or_else(|err| fatal(&err));
            }
            if self.replica.view_number() != self.view {
                self.view = self.replica.view_number();
                println!(
                    "view {}: primary is node {}{}",
                    self.view,
                    self.replica.primary_id(),
                    if self.replica.is_primary() {
                        " (this node)"
                    } else {
                        ""
                    }
                );
            }
        }
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

    let mut config = Config::new();
    for _ in &args.replicas {
        config.add_replica();
    }
    config.set_primary_timeout(PRIMARY_TIMEOUT);

    let mut node =
        Node::open(args.id, config, &args.data_dir, args.start).unwrap_or_else(|err| fatal(&err));

    let (events_tx, events_rx) = channel::<Event>();
    let (frames_tx, frames_rx) = channel::<(ReplicaID, Frame)>();

    {
        let (self_id, addresses, events) = (args.id, args.replicas.clone(), events_tx.clone());
        thread::spawn(move || run_sender(self_id, addresses, frames_rx, events));
    }
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
        thread::spawn(move || loop {
            thread::sleep(TICK);
            if events.send(Event::Tick).is_err() {
                break;
            }
        });
    }
    drop(events_tx);

    println!(
        "node {} of {}: replicas on {}, clients on {}, data in {}, primary is node {}",
        args.id,
        args.replicas.len(),
        args.replicas[args.id],
        args.listen,
        args.data_dir.display(),
        node.replica.primary_id()
    );
    // Whatever the replica produced on the way up goes out now.
    node.deliver(&frames_tx);
    node.run(events_rx, frames_tx);
}

#[cfg(test)]
mod tests {
    use super::*;
    use vsr_rs::Status;

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

    /// Persists every node, then moves what the nodes and the client want
    /// sent through the wire encoding to their destination, until nothing
    /// is left. Messages to `down` nodes are dropped. Returns the replies.
    fn deliver(nodes: &mut [Node], client: &mut Client<Op>, down: &[ReplicaID]) -> Vec<KvReply> {
        let mut replies = Vec::new();
        loop {
            let mut frames: Vec<(ReplicaID, String)> = Vec::new();
            for node in nodes.iter_mut() {
                let mut node_replies = Vec::new();
                let send = |dst, message| frames.push((dst, encode(&Frame::Message(message))));
                node.step(send, &mut node_replies).unwrap();
                for reply in node_replies {
                    client.on_reply(reply.request_number, reply.view_number);
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
        client: &mut Client<Op>,
        op: Op,
        down: &[ReplicaID],
    ) -> Option<String> {
        let request_number = client.on_request(op);
        let replies = deliver(nodes, client, down);
        let reply = replies
            .iter()
            .find(|reply| reply.request_number == request_number)
            .expect("a reply");
        reply.result.clone()
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
        assert_eq!(
            Some("1".into()),
            run(&mut nodes, &mut client, Op::Get("a".into()), &[])
        );
        idle(&mut nodes, &[]);
        deliver(&mut nodes, &mut client, &[]);
        for node in &nodes {
            assert_eq!(3, node.replica.commit_number());
        }
        // Node 1 persists its store after the first two ops, then executes
        // the third, so on restart the store is one op behind the journal.
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
    /// from the primary's checkpoint, which restores its store, and a
    /// restart brings it back with that state.
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
        assert_eq!(5, nodes[0].replica.log_start());
        assert_eq!(0, nodes[2].replica.op_number());

        // Node 2 hears about op 6, finds the gap, and gets a checkpoint:
        // the primary's state as executed, which trails its commit number
        // within a batch, and the entries after it.
        run(
            &mut nodes,
            &mut client,
            Op::Put("k5".into(), "v5".into()),
            &[],
        );
        idle(&mut nodes, &[]);
        deliver(&mut nodes, &mut client, &[]);
        assert_eq!(5, nodes[2].replica.log_start());
        assert_eq!(6, nodes[2].replica.commit_number());
        assert_eq!(6, nodes[2].replica.state_machine().applied);
        for i in 0..6 {
            assert_eq!(
                Some(format!("v{i}")),
                nodes[2].replica.state_machine().get(&format!("k{i}"))
            );
        }
        assert_eq!(
            nodes[0].replica.client_table(),
            nodes[2].replica.state_machine().client_table().unwrap()
        );

        let before = nodes[2].replica.persistent_state();
        let node = nodes.remove(2);
        drop(node);
        let restarted = Node::open(2, config(3), &dirs[2], Start::Restart).unwrap();
        nodes.insert(2, restarted);
        assert_eq!(before, nodes[2].replica.persistent_state());
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
        assert_eq!(7, nodes[2].replica.commit_number());
        remove_dirs(nodes, &dirs);
    }

    /// Runs `op` through the cluster, with the client re-sending to every
    /// node until a reply comes, and returns the result: after a view
    /// change the client's first try goes to the old primary.
    fn run_resending(
        nodes: &mut [Node],
        client: &mut Client<Op>,
        op: Op,
        down: &[ReplicaID],
    ) -> Option<String> {
        let request_number = client.on_request(op);
        for _ in 0..20 {
            let replies = deliver(nodes, client, down);
            if let Some(reply) = replies
                .iter()
                .find(|reply| reply.request_number == request_number)
            {
                return reply.result.clone();
            }
            idle(nodes, down);
            client.on_idle();
        }
        panic!("no reply to request {request_number}");
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
        assert_eq!(5, nodes[0].replica.log_start());

        // Node 2 learns it is behind and asks the primary, which answers
        // with its checkpoint.
        nodes[2].replica.on_message(Message::Commit {
            view_number: 0,
            commit_number: 5,
        });
        let get_state: Vec<_> = nodes[2].replica.drain_messages_before_persist().collect();
        for (dst, message) in get_state {
            assert_eq!(0, dst);
            nodes[0].replica.on_message(message);
        }
        let new_state: Vec<_> = nodes[0].replica.drain_messages_before_persist().collect();
        for (dst, message) in new_state {
            assert_eq!(2, dst);
            nodes[2].replica.on_message(message);
        }
        assert_eq!(5, nodes[2].replica.state_machine().applied);
        reopen(&mut nodes, &dirs, 2, true);
        assert_eq!(5, nodes[2].replica.log_start());
        assert_eq!(5, nodes[2].replica.commit_number());
        assert_eq!(5, nodes[2].replica.op_number());
        reopen(&mut nodes, &dirs, 2, true);
        assert_eq!(5, nodes[2].replica.log_start());
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
        assert_eq!(6, nodes[2].replica.commit_number());
        assert_eq!(6, nodes[2].replica.state_machine().applied);
        remove_dirs(nodes, &dirs);
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
            run_resending(&mut nodes, &mut client, Op::Get("a".into()), &[])
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
            run_resending(&mut nodes, &mut client, Op::Get("a".into()), &[])
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
            run_resending(&mut nodes, &mut client, Op::Get("a".into()), &[])
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
        let nodes_down: Vec<Node> = nodes.drain(..).collect();
        drop(nodes_down);
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
                run_resending(&mut nodes, &mut client, Op::Get(format!("k{i}")), &[])
            );
        }
        remove_dirs(nodes, &dirs);
    }

    /// The wire encoding round-trips the messages that carry checkpoints.
    #[test]
    fn codec_round_trip() {
        let checkpoint = Checkpoint {
            op_number: 7,
            state: StoreSnapshot {
                pairs: vec![("a".into(), "1".into()), ("b".into(), "2".into())],
            },
            client_table: vec![
                ClientRecord {
                    client_id: 3,
                    request_number: 4,
                    reply: Some("1".into()),
                },
                ClientRecord {
                    client_id: 5,
                    request_number: 0,
                    reply: None,
                },
            ],
        };
        let entries = vec![LogEntry {
            client_id: 3,
            request_number: 5,
            op: Op::Get("a".into()),
        }];
        let messages: Vec<KvMessage> = vec![
            Message::NewState {
                view_number: 2,
                segment: LogSegment {
                    base: LogBase::Checkpoint(checkpoint.clone()),
                    entries: entries.clone(),
                },
                commit_number: 7,
            },
            Message::NewState {
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
                        base: LogBase::Checkpoint(checkpoint),
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
    }
}

#[cfg(test)]
mod disk_tests {
    use super::*;
    use std::collections::HashSet;
    use std::process::Command;

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

    /// Feeds a backup node the primary's `Prepare` for `op_number`, then
    /// persists the journal and drains the replica, as the event loop does
    /// after every batch.
    fn prepare(node: &mut Node, op_number: OpNumber) {
        prepare_committed(node, op_number, op_number - 1);
    }

    /// `prepare`, with the primary's commit number at `commit_number`.
    fn prepare_committed(node: &mut Node, op_number: OpNumber, commit_number: usize) {
        node.replica.on_message(Message::Prepare {
            view_number: 0,
            op_number,
            client_id: 7,
            request_number: op_number - 1,
            op: Op::Put(format!("k{op_number}"), format!("v{op_number}")),
            commit_number,
        });
        step(node);
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
        // Files rotated every few entries, and every file behind the
        // retained entries has been deleted.
        let files = journal_files(&dir);
        assert!(files[0] > 0, "the first file was never deleted: {files:?}");
        assert!(files.len() <= 2, "files {files:?}");
        assert_eq!(120, node.replica.op_number());
        assert_eq!(119, node.replica.commit_number());
        assert_eq!(116, node.replica.log_start());
        let before = node.replica.persistent_state();
        drop(node);

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

    /// A step that only moved the commit number is not written. After a
    /// restart the journal's commit number is behind the store's applied
    /// count, and the replica takes the store's.
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
                batch.push_str(&format!("E {op_number} 8 {op_number} PUT other value\n"));
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
                node.replica.log().iter().all(|entry| entry.client_id == 7),
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

    /// A checkpoint replaces keys the store already holds, keeps none the
    /// checkpoint lacks, and leaves the node's own records alone.
    #[test]
    fn restore_over_existing_keys() {
        let dir = temp_dir("restore");
        let mut store = Store::open(&dir).unwrap();
        let incarnation = store.next_incarnation().unwrap();
        for (op_number, (key, value)) in [("a", "1"), ("b", "2")].into_iter().enumerate() {
            store.apply(
                op_number + 1,
                &LogEntry {
                    client_id: 3,
                    request_number: op_number,
                    op: Op::Put(key.into(), value.into()),
                },
            );
        }
        let client_table = vec![ClientRecord {
            client_id: 4,
            request_number: 9,
            reply: Some("x".into()),
        }];
        store.restore(Checkpoint {
            op_number: 7,
            state: StoreSnapshot {
                pairs: vec![("a".into(), "x".into()), ("c".into(), "y".into())],
            },
            client_table: client_table.clone(),
        });
        drop(store);
        let mut store = Store::open(&dir).unwrap();
        assert_eq!(7, store.applied);
        assert_eq!(Some("x".to_string()), store.get("a"));
        assert_eq!(None, store.get("b"));
        assert_eq!(Some("y".to_string()), store.get("c"));
        assert_eq!(client_table, store.client_table().unwrap());
        assert!(store.next_incarnation().unwrap() > incarnation);
        drop(store);
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(dir.with_extension("durable"));
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
        for op_number in 1..=applied {
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
        for op_number in 1..=CHILD_OPS {
            assert_eq!(
                Some(format!("v{op_number}")),
                node.replica.state_machine().get(&format!("k{op_number}"))
            );
        }
        assert_eq!(
            vec![ClientRecord {
                client_id: 7,
                request_number: CHILD_OPS - 1,
                reply: None,
            }],
            node.replica.client_table()
        );
        drop(node);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The child of `process_crash_then_power_loss`: a backup node that
    /// persists its store once, then holds ops that commit all at once in
    /// a step that only moves the commit number, which the journal skips,
    /// and dies without flushing anything.
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
        // The store's writes reach the OS, as they do a moment later in a
        // running node, and none of them is durable.
        node.replica
            .state_machine()
            .db
            .persist(PersistMode::Buffer)
            .unwrap();
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
        for op_number in 1..=applied {
            assert_eq!(
                Some(format!("v{op_number}")),
                node.replica.state_machine().get(&format!("k{op_number}"))
            );
        }
        drop(node);
        let _ = std::fs::remove_dir_all(&dir);
    }
}

/// Power losses for the tests. A node's journal keeps every write, since
/// it fsyncs each one; its store goes back to what it last persisted, a
/// copy of which every `Store::persist` keeps. fjall 3.1.10 also fsyncs
/// its journal when it recovers on open, which this model leaves out: it
/// holds the kvstore to its own contract with the library rather than to
/// what one version of fjall happens to do.
#[cfg(test)]
mod power {
    use std::path::{Path, PathBuf};

    fn durable(store: &Path) -> PathBuf {
        store.with_extension("durable")
    }

    /// Copies a directory the store may be changing: a file or directory
    /// it removes along the way is left out.
    fn copy_dir(from: &Path, to: &Path) {
        let _ = std::fs::remove_dir_all(to);
        std::fs::create_dir_all(to).unwrap();
        let gone = |err: std::io::Error| {
            assert_eq!(std::io::ErrorKind::NotFound, err.kind(), "{err}");
        };
        let entries = match std::fs::read_dir(from) {
            Ok(entries) => entries,
            Err(err) => return gone(err),
        };
        for entry in entries {
            let entry = match entry {
                Ok(entry) => entry,
                Err(err) => {
                    gone(err);
                    continue;
                }
            };
            let target = to.join(entry.file_name());
            match entry.file_type() {
                Ok(kind) if kind.is_dir() => copy_dir(&entry.path(), &target),
                Ok(_) => {
                    if let Err(err) = std::fs::copy(entry.path(), target) {
                        gone(err);
                    }
                }
                Err(err) => gone(err),
            }
        }
    }

    /// Keeps a copy of the store at `path` as it is right after a persist.
    pub fn record_durable(path: &Path) {
        copy_dir(path, &durable(path));
    }

    /// Cuts the power of the node whose data is in `data_dir`, which must
    /// be closed: its store goes back to its last persist.
    pub fn lose_power(data_dir: &Path) {
        let store = data_dir.join("store");
        let _ = std::fs::remove_dir_all(&store);
        if durable(&store).exists() {
            copy_dir(&durable(&store), &store);
        }
    }
}
