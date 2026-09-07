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
//! restart the node replays the write-ahead log, opens the store, and
//! applies the committed entries the store had not made durable. See
//! README.md next to this file.

use fjall::{Database, Keyspace, KeyspaceCreateOptions, PersistMode, Readable};
use futures::executor::block_on;
use futures::StreamExt;
use log::{debug, info, warn};
use std::collections::{BTreeMap, HashMap};
use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::thread;
use std::time::{Duration, Instant};
use vsr_rs::{
    Checkpoint, Client, ClientID, ClientRecord, Config, LogEntry, LogSegment, Message, MessageFor,
    OpNumber, PersistentState, RecoveryState, Replica, ReplicaID, Reply, RequestNumber,
    StateMachine,
};
use writeahead::{SimpleFile, WriteAhead, WriteAheadOptions, WriteHandle};

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

/// The key-value store. The output of an op is the value read by a GET.
struct Store {
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
        let applied = match keyspace.get(APPLIED_KEY).map_err(|err| err.to_string())? {
            Some(value) => utf8(&value)
                .parse()
                .map_err(|_| "bad applied op number in store".to_string())?,
            None => 0,
        };
        Ok(Store {
            db,
            keyspace,
            applied,
        })
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
            .map_err(|err| format!("cannot persist store: {err}"))
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

    /// Replaces the store with the checkpoint, in one batch, and makes it
    /// durable before returning, as the library requires.
    fn restore(&mut self, checkpoint: Checkpoint<Option<String>, StoreSnapshot>) {
        let mut batch = self.db.batch();
        for guard in self.keyspace.iter() {
            let key = guard
                .key()
                .unwrap_or_else(|err| fatal(&format!("cannot read store: {err}")));
            batch.remove(&self.keyspace, key);
        }
        for (key, value) in checkpoint.state.pairs {
            batch.insert(&self.keyspace, format!("{KEY_PREFIX}{key}"), value);
        }
        for record in checkpoint.client_table {
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
        self.db
            .persist(PersistMode::SyncAll)
            .unwrap_or_else(|err| fatal(&format!("cannot persist store: {err}")));
        self.applied = checkpoint.op_number;
    }
}

type KvState = PersistentState<Op, Option<String>>;

// ---------------------------------------------------------------------------
// The journal: the replica's persistent state in a write-ahead log

/// The replica's counters, as last written to the journal.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Header {
    view_number: usize,
    last_normal_view: usize,
    commit_number: usize,
    log_start: OpNumber,
    op_number: OpNumber,
    recovering: bool,
}

/// The replica's persistent state, kept in an append-only log of records:
/// an entry (`E`), a truncation of the entries from an op number on (`T`),
/// a compaction of the entries up to an op number (`C`), and the counters
/// (`H`), which close every batch. Replaying the records in order rebuilds
/// the state. Files that hold nothing a replay needs are deleted.
struct Journal {
    dir: PathBuf,
    writer: WriteHandle,
    header: Header,
    /// The file holding the latest copy of each retained entry.
    files: BTreeMap<OpNumber, u64>,
    trimmed_before: u64,
}

impl Journal {
    /// Opens the journal in `dir` and replays it. Returns the replica's
    /// state if the node has run before, without the client table, which
    /// the store keeps.
    fn open(dir: &Path) -> Result<(Journal, Option<KvState>), String> {
        let mut wal = WriteAhead::<SimpleFile>::with_options(WriteAheadOptions {
            log_dir: dir.to_path_buf(),
            max_file_size: WAL_FILE_SIZE,
            ..Default::default()
        });
        wal.start()
            .map_err(|err| format!("cannot open journal at {}: {err}", dir.display()))?;
        let writer = wal
            .writer()
            .map_err(|err| format!("cannot write journal: {err}"))?;
        let mut entries: BTreeMap<OpNumber, (LogEntry<Op>, u64)> = BTreeMap::new();
        let mut header: Option<Header> = None;
        let mut stream = wal
            .create_stream()
            .map_err(|err| format!("cannot read journal: {err}"))?;
        block_on(async {
            while let Some(item) = stream.next().await {
                let (id, bytes) = item.map_err(|err| format!("cannot read journal: {err}"))?;
                let line = utf8(&bytes);
                let mut t = Tokens::new(&line);
                match t.word()? {
                    "E" => {
                        let op_number = t.num()?;
                        entries.insert(op_number, (t.entry()?, id.file_id));
                    }
                    "T" => {
                        entries.split_off(&t.num()?);
                    }
                    "C" => {
                        entries = entries.split_off(&(t.num()? + 1));
                    }
                    "H" => {
                        header = Some(Header {
                            view_number: t.num()?,
                            last_normal_view: t.num()?,
                            commit_number: t.num()?,
                            log_start: t.num()?,
                            op_number: t.num()?,
                            recovering: t.num()? != 0,
                        });
                    }
                    kind => return Err(format!("bad journal record {kind:?}")),
                }
                t.done()?;
            }
            Ok::<(), String>(())
        })?;
        drop(stream);
        let mut journal = Journal {
            dir: dir.to_path_buf(),
            writer,
            header: Header::default(),
            files: BTreeMap::new(),
            trimmed_before: 0,
        };
        let Some(header) = header else {
            return Ok((journal, None));
        };
        let mut log = Vec::with_capacity(header.op_number - header.log_start);
        for op_number in header.log_start + 1..=header.op_number {
            let (entry, file) = entries
                .remove(&op_number)
                .ok_or_else(|| format!("journal lacks op {op_number}"))?;
            log.push(entry);
            journal.files.insert(op_number, file);
        }
        journal.header = header;
        let state = KvState {
            view_number: header.view_number,
            last_normal_view: header.last_normal_view,
            commit_number: header.commit_number,
            log_start: header.log_start,
            log,
            client_table: Vec::new(),
            recovering: header.recovering,
        };
        Ok((journal, Some(state)))
    }

    /// Writes what the last steps changed, in one batch with one fsync,
    /// then deletes the files that hold nothing a replay needs any more.
    fn persist(&mut self, replica: &mut Replica<Store>) -> Result<(), String> {
        let mut records: Vec<Vec<u8>> = Vec::new();
        let mut written: Vec<OpNumber> = Vec::new();
        let log_start = replica.log_start();
        if log_start > self.header.log_start {
            records.push(format!("C {log_start}").into_bytes());
        }
        if let Some(from) = replica.take_log_changes() {
            if from <= self.header.op_number {
                records.push(format!("T {from}").into_bytes());
            }
            let first = from.max(log_start + 1);
            for (i, entry) in replica.log_from(first).iter().enumerate() {
                let op_number = first + i;
                records.push(format!("E {op_number} {}", encode_entry(entry)).into_bytes());
                written.push(op_number);
            }
        }
        let header = Header {
            view_number: replica.view_number(),
            last_normal_view: replica.last_normal_view(),
            commit_number: replica.commit_number(),
            log_start,
            op_number: replica.op_number(),
            recovering: replica.is_recovering(),
        };
        if records.is_empty() && header == self.header {
            return Ok(());
        }
        records.push(
            format!(
                "H {} {} {} {} {} {}",
                header.view_number,
                header.last_normal_view,
                header.commit_number,
                header.log_start,
                header.op_number,
                u8::from(header.recovering)
            )
            .into_bytes(),
        );
        let ids = block_on(self.writer.write_batch(records))
            .map_err(|err| format!("cannot write journal: {err}"))?;
        let file = ids.last().map(|id| id.file_id).unwrap_or(0);
        for op_number in written {
            self.files.insert(op_number, file);
        }
        // Entries compacted or truncated away no longer pin their files.
        self.files = self.files.split_off(&(log_start + 1));
        self.files.split_off(&(header.op_number + 1));
        self.header = header;
        // A replay needs the files from the oldest retained entry's on, and
        // the one just written, which holds the counters.
        let oldest = self.files.values().min().copied().unwrap_or(file).min(file);
        if oldest > self.trimmed_before {
            let stats = block_on(self.writer.trim_before(oldest))
                .map_err(|err| format!("cannot trim journal: {err}"))?;
            if stats.files_deleted > 0 {
                debug!(
                    "journal {}: deleted {} files, {} bytes",
                    self.dir.display(),
                    stats.files_deleted,
                    stats.bytes_reclaimed
                );
            }
            self.trimmed_before = oldest;
        }
        Ok(())
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
    let checkpoint = match &segment.checkpoint {
        Some(checkpoint) => format!("+ {}", encode_checkpoint(checkpoint)),
        None => "-".to_string(),
    };
    format!(
        "{} {} {checkpoint}",
        segment.start,
        encode_entries(&segment.entries)
    )
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
                log_start,
                log,
                op_number,
                commit_number,
            } => format!(
                "DOVIEWCHANGE {view_number} {replica_id} {last_normal_view} {log_start} {op_number} {commit_number} {}",
                encode_entries(log)
            ),
            Message::StartView {
                view_number,
                log_start,
                log,
                op_number,
                commit_number,
            } => format!(
                "STARTVIEW {view_number} {log_start} {op_number} {commit_number} {}",
                encode_entries(log)
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
        let start = self.num()?;
        let entries = self.entries()?;
        let checkpoint = match self.word()? {
            "+" => Some(self.checkpoint()?),
            "-" => None,
            word => return Err(format!("bad checkpoint marker {word:?}")),
        };
        Ok(LogSegment {
            checkpoint,
            start,
            entries,
        })
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
            log_start: t.num()?,
            op_number: t.num()?,
            commit_number: t.num()?,
            log: t.entries()?,
        },
        "STARTVIEW" => Message::StartView {
            view_number: t.num()?,
            log_start: t.num()?,
            op_number: t.num()?,
            commit_number: t.num()?,
            log: t.entries()?,
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

fn run_client_acceptor(listener: TcpListener, node_id: ReplicaID, events: Sender<Event>) {
    // Client ids must never repeat, or the primary's client table mistakes
    // a new connection's first request for a re-send of an old one and
    // answers it from the cache (section 4.5 of the paper). The node id in
    // the top byte tells the primary which node to route the reply to, the
    // start time below it separates restarts of the node, and the low bits
    // count connections.
    let started = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| since.as_secs())
        .unwrap_or(0)
        & 0xFF_FFFF;
    for (next, stream) in listener.incoming().flatten().enumerate() {
        let connection = ((node_id as u64) << 56) | (started << 32) | next as u64;
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

/// Sends out everything the replica and the clients produced: protocol
/// messages to the sender thread, and replies to the node that owns the
/// client connection, which may be this one.
fn flush(
    node_id: ReplicaID,
    replica: &mut Replica<Store>,
    connections: &mut HashMap<u64, Connection>,
    frames: &Sender<(ReplicaID, Frame)>,
) {
    for (dst, message) in replica.drain_messages() {
        let _ = frames.send((dst, Frame::Message(message)));
    }
    for reply in replica.drain_replies() {
        let owner = node_of(reply.client_id);
        if owner == node_id {
            deliver_reply(connections, reply);
        } else {
            let _ = frames.send((owner, Frame::Reply(reply)));
        }
    }
    for connection in connections.values_mut() {
        for (dst, message) in connection.client.drain() {
            let _ = frames.send((dst, Frame::Message(message)));
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

/// The node's state: the replica, its journal, and the client connections.
struct Node {
    id: ReplicaID,
    config: Config,
    replica: Replica<Store>,
    journal: Journal,
    connections: HashMap<u64, Connection>,
    ticks: u64,
    view: usize,
    /// Log entries kept behind what the store has persisted.
    log_retention: usize,
}

impl Node {
    /// Opens the store and the journal in `data_dir`, and builds the
    /// replica: a restart from what they hold if the node has run before,
    /// a new replica otherwise, or a recovering one if asked to.
    fn open(id: ReplicaID, config: Config, data_dir: &Path, recover: bool) -> Result<Node, String> {
        let store = Store::open(&data_dir.join("store"))?;
        let (journal, state) = Journal::open(&data_dir.join("journal"))?;
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|since| since.as_nanos() as u64)
            .unwrap_or(0);
        let replica = match state {
            Some(mut state) => {
                state.client_table = store.client_table()?;
                let applied = store.applied;
                println!(
                    "restarting from view {} with {} ops, {} committed, {applied} applied by the store",
                    state.view_number,
                    state.log_start + state.log.len(),
                    state.commit_number
                );
                Replica::restart(id, config.clone(), store, applied, state, nonce)
            }
            None if store.applied > 0 => {
                return Err(format!(
                    "the store in {} has applied {} ops but the journal is empty",
                    data_dir.display(),
                    store.applied
                ));
            }
            None if recover => {
                println!("recovering with an empty disk");
                Replica::recover(id, config.clone(), store, 0, nonce)
            }
            None => Replica::new(id, config.clone(), store),
        };
        let mut node = Node {
            id,
            view: replica.view_number(),
            replica,
            journal,
            config,
            connections: HashMap::new(),
            ticks: 0,
            log_retention: LOG_RETENTION,
        };
        node.journal.persist(&mut node.replica)?;
        Ok(node)
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
        let applied = self.replica.state_machine().applied;
        self.replica.state_machine().persist()?;
        self.replica
            .compact(applied.saturating_sub(self.log_retention));
        Ok(())
    }

    /// Runs the event loop: every batch of events already queued is
    /// stepped, the journal is written once, and only then is anything the
    /// batch produced sent.
    fn run(&mut self, events: Receiver<Event>, frames: Sender<(ReplicaID, Frame)>) {
        while let Ok(event) = events.recv() {
            let mut flush_store = self.handle(event);
            while let Ok(event) = events.try_recv() {
                flush_store |= self.handle(event);
            }
            self.journal
                .persist(&mut self.replica)
                .unwrap_or_else(|err| fatal(&err));
            flush(self.id, &mut self.replica, &mut self.connections, &frames);
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
    recover: bool,
}

fn parse_args() -> Result<Args, String> {
    let mut id = None;
    let mut replicas = None;
    let mut listen = None;
    let mut data_dir = None;
    let mut recover = false;
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
            "--recover" => recover = true,
            _ => return Err(format!("unknown argument {arg}")),
        }
    }
    let usage =
        "usage: kvstore --id N --replicas ADDR,ADDR,... --listen ADDR [--data DIR] [--recover]";
    let id: ReplicaID = id.ok_or(usage)?;
    let args = Args {
        id,
        replicas: replicas.ok_or(usage)?,
        listen: listen.ok_or(usage)?,
        data_dir: data_dir.unwrap_or_else(|| PathBuf::from(format!("kvstore-node-{id}"))),
        recover,
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
        Node::open(args.id, config, &args.data_dir, args.recover).unwrap_or_else(|err| fatal(&err));

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
        let (node_id, events) = (args.id, events_tx.clone());
        thread::spawn(move || run_client_acceptor(listener, node_id, events));
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
    flush(
        args.id,
        &mut node.replica,
        &mut node.connections,
        &frames_tx,
    );
    node.run(events_rx, frames_tx);
}

#[cfg(test)]
mod tests {
    use super::*;

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

    /// Opens the nodes of `test` from their directories.
    fn open_nodes(test: &str, dirs: &[PathBuf]) -> Vec<Node> {
        let config = config(dirs.len());
        dirs.iter()
            .enumerate()
            .map(|(id, dir)| Node::open(id, config.clone(), dir, false).unwrap())
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
                node.journal.persist(&mut node.replica).unwrap();
                for (dst, message) in node.replica.drain_messages() {
                    frames.push((dst, encode(&Frame::Message(message))));
                }
                for reply in node.replica.drain_replies() {
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
        let mut nodes = open_nodes("restart", &dirs);
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

        let node = nodes.remove(1);
        drop(node);
        let restarted = Node::open(1, config(3), &dirs[1], false).unwrap();
        nodes.insert(1, restarted);
        assert_eq!(before, nodes[1].replica.persistent_state());
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
    }

    /// The primary persists its store and compacts its log with no
    /// retention while a node misses every op. That node then catches up
    /// from the primary's checkpoint, which restores its store, and a
    /// restart brings it back with that state.
    #[test]
    fn checkpoint_restores_store() {
        let dirs: Vec<PathBuf> = (0..3).map(|id| temp_dir("checkpoint", id)).collect();
        let mut nodes = open_nodes("checkpoint", &dirs);
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

        // Node 2 hears about op 6, finds the gap, and gets a checkpoint,
        // taken once the primary has committed op 6.
        run(
            &mut nodes,
            &mut client,
            Op::Put("k5".into(), "v5".into()),
            &[],
        );
        idle(&mut nodes, &[]);
        deliver(&mut nodes, &mut client, &[]);
        assert_eq!(6, nodes[2].replica.log_start());
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
        let restarted = Node::open(2, config(3), &dirs[2], false).unwrap();
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
                    checkpoint: Some(checkpoint.clone()),
                    start: 7,
                    entries: entries.clone(),
                },
                commit_number: 7,
            },
            Message::NewState {
                view_number: 2,
                segment: LogSegment {
                    checkpoint: None,
                    start: 3,
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
                        checkpoint: Some(checkpoint),
                        start: 7,
                        entries: entries.clone(),
                    },
                    commit_number: 7,
                }),
            },
            Message::DoViewChange {
                view_number: 3,
                replica_id: 2,
                last_normal_view: 2,
                log_start: 6,
                log: entries.clone(),
                op_number: 7,
                commit_number: 6,
            },
            Message::StartView {
                view_number: 3,
                log_start: 6,
                log: entries,
                op_number: 7,
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
