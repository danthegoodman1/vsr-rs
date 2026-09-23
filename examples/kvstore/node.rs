//! A kvstore node: the replica, its store and journal, and the clients it
//! proxies, stepped by one event loop. `main.rs` puts it on the network;
//! the benchmark runs three of them in one process.

use crate::journal::{EntryCodec, Journal};
use fjall::{Database, Keyspace, KeyspaceCreateOptions, PersistMode, Readable};
use std::cell::Cell;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::Arc;
use std::time::Duration;
use vsr_rs::{
    Checkpoint, Client, ClientID, ClientRecord, Config, LogEntry, LogSegment, LogWrite, MessageFor,
    OpNumber, Replica, ReplicaID, Reply, RequestNumber, StateMachine, ViewNumber,
};

/// How often the replica and the clients run their idle logic.
pub(crate) const TICK: Duration = Duration::from_millis(100);
/// Ticks between two re-sends of a request that got no reply.
pub(crate) const CLIENT_RESEND_TICKS: u64 = 5;
/// Idle periods without hearing from the primary before a view change.
pub(crate) const PRIMARY_TIMEOUT: usize = 5;
/// Ticks between two persists of the store, each of which lets the
/// replica compact its log.
pub(crate) const FLUSH_TICKS: u64 = 10;
/// Ticks a journal write may stay out before the node takes its disk for
/// stalled, see [`Node::batch`].
pub(crate) const STALLED_WRITE_TICKS: u64 = 2;
/// The most events the event loop steps before it delivers, so that a
/// steady stream of them still lets the next write go out.
pub(crate) const MAX_BATCH_EVENTS: usize = 1024;
/// Log entries kept behind what the store has persisted, so that a replica
/// a little behind catches up from the log rather than from a checkpoint.
pub(crate) const LOG_RETENTION: usize = 1_000;
/// Size of a write-ahead log file. Small, so that compaction reclaims
/// space in a short run.
pub(crate) const WAL_FILE_SIZE: u64 = 4 * 1024 * 1024;

// ---------------------------------------------------------------------------
// Operations

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Op {
    Put(String, String),
    Get(String),
}

pub(crate) fn encode_op(op: &Op) -> String {
    match op {
        Op::Put(key, value) => format!("PUT {key} {value}"),
        Op::Get(key) => format!("GET {key}"),
    }
}

pub(crate) fn encode_entry(entry: &LogEntry<Op>) -> String {
    format!(
        "{} {} {}",
        entry.client_id,
        entry.request_number,
        encode_op(&entry.op)
    )
}

pub(crate) fn decode_entry(text: &str) -> Result<LogEntry<Op>, String> {
    let mut t = Tokens::new(text);
    let entry = t.entry()?;
    t.done()?;
    Ok(entry)
}

pub(crate) const ENTRY_CODEC: EntryCodec<Op> = EntryCodec {
    encode: encode_entry,
    decode: decode_entry,
};

pub(crate) fn encode_entries(log: &[LogEntry<Op>]) -> String {
    let mut out = log.len().to_string();
    for entry in log {
        out.push(' ');
        out.push_str(&encode_entry(entry));
    }
    out
}

pub(crate) fn encode_result(result: &Option<String>) -> String {
    match result {
        Some(value) => format!("+{value}"),
        None => "-".to_string(),
    }
}

/// A cursor over the tokens of one encoded line.
pub(crate) struct Tokens<'a> {
    iter: std::str::SplitWhitespace<'a>,
}

impl<'a> Tokens<'a> {
    pub(crate) fn new(line: &'a str) -> Tokens<'a> {
        Tokens {
            iter: line.split_whitespace(),
        }
    }

    pub(crate) fn word(&mut self) -> Result<&'a str, String> {
        self.iter
            .next()
            .ok_or_else(|| "truncated message".to_string())
    }

    pub(crate) fn num(&mut self) -> Result<usize, String> {
        let word = self.word()?;
        word.parse().map_err(|_| format!("bad number {word:?}"))
    }

    pub(crate) fn op(&mut self) -> Result<Op, String> {
        match self.word()? {
            "PUT" => Ok(Op::Put(self.word()?.to_string(), self.word()?.to_string())),
            "GET" => Ok(Op::Get(self.word()?.to_string())),
            kind => Err(format!("bad op {kind:?}")),
        }
    }

    pub(crate) fn entry(&mut self) -> Result<LogEntry<Op>, String> {
        Ok(LogEntry {
            client_id: self.num()?,
            request_number: self.num()?,
            op: self.op()?,
        })
    }

    pub(crate) fn entries(&mut self) -> Result<Vec<LogEntry<Op>>, String> {
        let count = self.num()?;
        let mut log = Vec::with_capacity(count);
        for _ in 0..count {
            log.push(self.entry()?);
        }
        Ok(log)
    }

    pub(crate) fn result(&mut self) -> Result<Option<String>, String> {
        Ok(match self.word()? {
            "-" => None,
            value => Some(value[1..].to_string()),
        })
    }

    pub(crate) fn done(&mut self) -> Result<(), String> {
        match self.iter.next() {
            None => Ok(()),
            Some(word) => Err(format!("trailing {word:?}")),
        }
    }
}

// ---------------------------------------------------------------------------
// The store: a fjall database that never fsyncs on its own

/// Key prefixes in the store's one keyspace.
pub(crate) const KEY_PREFIX: &str = "k/";
pub(crate) const CLIENT_PREFIX: &str = "c/";
/// The number of operations applied to the store, written in the same
/// batch as each operation.
pub(crate) const APPLIED_KEY: &str = "m/applied";
/// The number of times the node has started, which keeps the client ids
/// of one run apart from those of the others.
pub(crate) const INCARNATION_KEY: &str = "m/incarnation";

/// The key-value store. The output of an op is the value read by a GET.
pub(crate) struct Store {
    pub(crate) path: PathBuf,
    pub(crate) db: Database,
    pub(crate) keyspace: Keyspace,
    /// The number of operations applied, as recorded in the store.
    pub(crate) applied: OpNumber,
    /// The number of times the store was persisted, each with an fsync.
    pub(crate) persists: Cell<u64>,
}

/// Every key and value in the store, for a replica that fell behind a
/// compacted log.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StoreSnapshot {
    pub(crate) pairs: Vec<(String, String)>,
}

pub(crate) fn utf8(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

impl Store {
    pub(crate) fn open(path: &Path) -> Result<Store, String> {
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
            persists: Cell::new(0),
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
    pub(crate) fn next_incarnation(&mut self) -> Result<u64, String> {
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
    pub(crate) fn client_table(&self) -> Result<Vec<ClientRecord<Option<String>>>, String> {
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
    pub(crate) fn persist(&self) -> Result<(), String> {
        self.db
            .persist(PersistMode::SyncData)
            .map_err(|err| format!("cannot persist store at {}: {err}", self.path.display()))?;
        self.persists.set(self.persists.get() + 1);
        #[cfg(test)]
        power::record_durable(&self.path);
        Ok(())
    }

    pub(crate) fn get(&self, key: &str) -> Option<String> {
        self.keyspace
            .get(format!("{KEY_PREFIX}{key}"))
            .unwrap_or_else(|err| fatal(&format!("cannot read store: {err}")))
            .map(|value| utf8(&value))
    }
}

pub(crate) fn fatal(message: &str) -> ! {
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
// What nodes send each other

pub(crate) type KvReply = Reply<Option<String>>;
pub(crate) type KvMessage = MessageFor<Store>;
pub(crate) type KvCheckpoint = Checkpoint<Option<String>, StoreSnapshot>;
pub(crate) type KvSegment = LogSegment<Op, Option<String>, StoreSnapshot>;

/// What travels between nodes: protocol messages, and replies routed back
/// to the node that owns the client connection.
pub(crate) enum Frame {
    Message(KvMessage),
    Reply(KvReply),
}

impl From<Frame> for Event {
    /// A frame as the event loop of the node it is for takes it.
    fn from(frame: Frame) -> Event {
        match frame {
            Frame::Message(message) => Event::Message(message),
            Frame::Reply(reply) => Event::Reply(reply),
        }
    }
}

/// The configuration of a cluster of `replicas` nodes.
pub(crate) fn config(replicas: usize) -> Config {
    let mut config = Config::new();
    for _ in 0..replicas {
        config.add_replica();
    }
    config.set_primary_timeout(PRIMARY_TIMEOUT);
    config
}

/// Ticks a node every `TICK` on a fixed schedule, however busy it is,
/// until it is gone or `stopped` says so.
pub(crate) fn run_timer(events: Sender<Event>, stopped: impl Fn() -> bool) {
    while !stopped() {
        std::thread::sleep(TICK);
        if events.send(Event::Tick).is_err() {
            break;
        }
    }
}

// ---------------------------------------------------------------------------
// Clients

pub(crate) enum Command {
    Set(String, String),
    Get(String),
}

/// The client id of a node's `next` connection in its `incarnation`.
///
/// Client ids must never repeat, or the primary's client table mistakes a
/// new connection's first request for a re-send of an old one and answers
/// it from the cache (section 4.5 of the paper). The node id in the top
/// byte tells the primary which node to route the reply to, the node's
/// incarnation in the next 32 bits separates its runs, and the low 24 bits
/// count its connections.
pub(crate) fn client_id(node_id: ReplicaID, incarnation: u64, next: u64) -> u64 {
    ((node_id as u64) << 56) | ((incarnation & 0xFFFF_FFFF) << 24) | (next & 0xFF_FFFF)
}

pub(crate) fn node_of(client_id: ClientID) -> ReplicaID {
    (client_id >> 56) as ReplicaID
}

// ---------------------------------------------------------------------------
// The node: one thread owning the replica and the proxied clients

pub(crate) enum Event {
    Message(KvMessage),
    Reply(KvReply),
    Command {
        connection: u64,
        command: Command,
        respond: Sender<String>,
    },
    Disconnect(u64),
    Tick,
    /// The journal thread wrote the replica's write, see [`Node::run`].
    Written(LogWrite<Op>),
    /// Ends [`Node::run`] once no write is out. The kvstore runs until it
    /// is killed; the benchmark and the tests stop their nodes.
    #[cfg_attr(not(test), allow(dead_code))]
    Stop,
}

/// A client connection's VSR client and the command it is waiting on.
pub(crate) struct Connection {
    pub(crate) client: Client<Op>,
    pub(crate) pending: Option<(RequestNumber, Command, Sender<String>)>,
}

/// Hands a reply to the connection waiting for it, if it is still there
/// and still waiting for that request.
pub(crate) fn deliver_reply(connections: &mut HashMap<u64, Connection>, reply: KvReply) {
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

pub(crate) fn format_reply(command: &Command, result: Option<String>) -> String {
    match (command, result) {
        (Command::Set(..), _) => "+OK\r\n".to_string(),
        (Command::Get(_), Some(value)) => format!("${}\r\n{value}\r\n", value.len()),
        (Command::Get(_), None) => "$-1\r\n".to_string(),
    }
}

/// How a node starts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Start {
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
pub(crate) struct Node {
    pub(crate) id: ReplicaID,
    pub(crate) config: Config,
    pub(crate) replica: Replica<Store>,
    /// The journal, unless [`Node::run`] has handed it to its thread.
    pub(crate) journal: Option<Journal<Op>>,
    /// Whether the journal thread has a write of the replica's, and the
    /// ticks since it took it.
    pub(crate) writing: bool,
    pub(crate) write_ticks: u64,
    /// Whether the event loop is to end, once no write is out.
    pub(crate) stopping: bool,
    /// The connections whose clients may have requests to send.
    pub(crate) requesting: Vec<u64>,
    /// The number of times this node has started, which its client ids
    /// carry.
    pub(crate) incarnation: u64,
    pub(crate) connections: HashMap<u64, Connection>,
    pub(crate) ticks: u64,
    pub(crate) view: usize,
    /// Log entries kept behind what the store has persisted.
    pub(crate) log_retention: usize,
    /// Whether writes go to the journal. The benchmark turns it off to
    /// measure what the journal costs; a node without it forgets what it
    /// acknowledged.
    pub(crate) journaled: bool,
    /// What the node has done, for whoever watches it run.
    pub(crate) stats: Arc<Stats>,
    /// Whether the node prints each view it enters.
    pub(crate) announce_views: bool,
}

/// What a node has done so far, readable while it runs.
#[derive(Debug, Default)]
pub(crate) struct Stats {
    /// Batches of events stepped.
    pub(crate) batches: AtomicU64,
    /// Journal writes and store persists, each with its fsync. The syncs
    /// with which the journal starts a new file are left out.
    pub(crate) journal_writes: AtomicU64,
    pub(crate) store_persists: AtomicU64,
    /// The replica's view and commit number after the last batch.
    pub(crate) view_number: AtomicU64,
    pub(crate) commit_number: AtomicU64,
}

impl Node {
    /// Opens the store and the journal in `data_dir`, and builds the
    /// replica as `start` says. The journal holds the replica's counters
    /// once this returns, so a node that started can only restart.
    pub(crate) fn open(
        id: ReplicaID,
        config: Config,
        data_dir: &Path,
        start: Start,
    ) -> Result<Node, String> {
        Node::open_with(id, config, data_dir, start, WAL_FILE_SIZE)
    }

    /// `open`, with journal files of `wal_file_size` bytes.
    pub(crate) fn open_with(
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
            journal: Some(journal),
            writing: false,
            write_ticks: 0,
            stopping: false,
            requesting: Vec::new(),
            incarnation,
            config,
            connections: HashMap::new(),
            ticks: 0,
            log_retention: LOG_RETENTION,
            journaled: true,
            stats: Arc::default(),
            announce_views: true,
        };
        // A process crash can leave journal records that reached only the
        // page cache: the replay took them for durable, and the replica
        // restarts on them. The first write goes to the journal whether it
        // needs a sync or not, and its sync covers them.
        let journal = node
            .journal
            .as_mut()
            .expect("the journal is with the event loop's thread");
        node.replica.persist(|write| journal.append(write))?;
        node.stats.journal_writes.fetch_add(1, Ordering::Relaxed);
        Ok(node)
    }

    /// Writes what the last steps changed to the journal, if the write
    /// needs a sync, and hands it back to the replica, which releases what
    /// waited for it. Returns whether the journal was written.
    #[cfg(test)]
    pub(crate) fn write(&mut self) -> Result<bool, String> {
        let mut synced = false;
        let journaled = self.journaled;
        let journal = self
            .journal
            .as_mut()
            .expect("the journal is with the event loop's thread");
        self.replica.persist(|write| {
            synced = write.sync && journaled;
            if synced {
                journal.append(write)?;
            }
            Ok::<(), String>(())
        })?;
        if synced {
            self.stats.journal_writes.fetch_add(1, Ordering::Relaxed);
        }
        Ok(synced)
    }

    /// The part of ending a batch of steps that comes before the write.
    /// The requests of this node's clients go first: those for this node's
    /// replica go straight to it, so that they join the next write, and the
    /// rest to `send`. Then the messages that need not wait go to `send`,
    /// so that the other nodes' writes overlap this one's, and the replies
    /// ready so far to `replies`.
    fn before_write(
        &mut self,
        send: &mut impl FnMut(ReplicaID, KvMessage),
        replies: &mut Vec<KvReply>,
    ) {
        let mut requesting = std::mem::take(&mut self.requesting);
        for id in requesting.drain(..) {
            let Some(connection) = self.connections.get_mut(&id) else {
                continue;
            };
            for (dst, message) in connection.client.drain() {
                if dst == self.id {
                    self.replica.on_message(message);
                } else {
                    send(dst, message);
                }
            }
        }
        self.requesting = requesting;
        for (dst, message) in self.replica.drain_messages_before_persist() {
            send(dst, message);
        }
        replies.extend(self.replica.drain_replies());
    }

    /// The part of ending a batch of steps that comes after the write:
    /// what the write released goes to `send` and `replies`.
    fn after_write(
        &mut self,
        send: &mut impl FnMut(ReplicaID, KvMessage),
        replies: &mut Vec<KvReply>,
    ) {
        for (dst, message) in self.replica.drain_messages() {
            send(dst, message);
        }
        replies.extend(self.replica.drain_replies());
    }

    /// Ends a batch of steps the way the library requires: what need not
    /// wait for the write, see `before_write`, then the journal write,
    /// then what it released. Returns whether the journal was written.
    #[cfg(test)]
    pub(crate) fn step(
        &mut self,
        mut send: impl FnMut(ReplicaID, KvMessage),
        replies: &mut Vec<KvReply>,
    ) -> Result<bool, String> {
        self.before_write(&mut send, replies);
        let written = self.write()?;
        self.after_write(&mut send, replies);
        Ok(written)
    }

    /// Ends a batch of steps the way `step` does, but with the journal
    /// written on the thread behind `writes`, and sends what the node and
    /// its clients produced as soon as it may: protocol messages to the
    /// sender thread, and replies to the node that owns the client
    /// connection, which may be this one. What the last write released
    /// goes out first. Then, unless a write is out already, it takes the
    /// replica's write: one that needs a sync goes to `writes`, and comes
    /// back as an [`Event::Written`]; any other goes straight back to the
    /// replica, and what that released goes out too.
    pub(crate) fn deliver(
        &mut self,
        frames: &Sender<(ReplicaID, Frame)>,
        writes: &Sender<LogWrite<Op>>,
    ) {
        let mut send = |dst, message| {
            let _ = frames.send((dst, Frame::Message(message)));
        };
        let mut replies = Vec::new();
        self.before_write(&mut send, &mut replies);
        self.after_write(&mut send, &mut replies);
        self.answer(&mut replies, frames);
        if !self.writing {
            let write = self.replica.take_write();
            if write.sync && self.journaled {
                self.writing = true;
                if writes.send(write).is_err() {
                    fatal("the journal thread is gone");
                }
            } else {
                self.replica.persisted(write);
            }
        }
        self.after_write(&mut send, &mut replies);
        self.answer(&mut replies, frames);
    }

    /// Hands each of `replies` to the node that owns its client
    /// connection, which may be this one.
    fn answer(&mut self, replies: &mut Vec<KvReply>, frames: &Sender<(ReplicaID, Frame)>) {
        for reply in replies.drain(..) {
            let owner = node_of(reply.client_id);
            if owner == self.id {
                deliver_reply(&mut self.connections, reply);
            } else {
                let _ = frames.send((owner, Frame::Reply(reply)));
            }
        }
    }

    /// Steps the replica or a client with one event. Returns whether the
    /// store is due for a persist.
    pub(crate) fn handle(&mut self, event: Event) -> bool {
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
                self.requesting.push(id);
            }
            Event::Disconnect(id) => {
                self.connections.remove(&id);
            }
            Event::Tick => {
                self.ticks += 1;
                if self.writing {
                    self.write_ticks += 1;
                }
                self.replica.on_idle();
                if self.ticks.is_multiple_of(CLIENT_RESEND_TICKS) {
                    for (id, connection) in &mut self.connections {
                        connection.client.on_idle();
                        self.requesting.push(*id);
                    }
                }
                return self.ticks.is_multiple_of(FLUSH_TICKS);
            }
            Event::Written(write) => {
                self.writing = false;
                self.write_ticks = 0;
                self.replica.persisted(write);
            }
            Event::Stop => self.stopping = true,
        }
        false
    }

    /// Persists the store, which makes everything it has applied durable,
    /// and compacts the log up to there, less the retention window. The
    /// compaction reaches the journal with the next write.
    pub(crate) fn flush_store(&mut self) -> Result<(), String> {
        let applied = self.replica.applied();
        self.replica.state_machine().persist()?;
        self.replica
            .compact(applied.saturating_sub(self.log_retention));
        Ok(())
    }

    /// Runs the event loop until an [`Event::Stop`], stepping every batch
    /// of events already queued, see [`Node::batch`]. The journal is
    /// written on a thread of its own, which hands each write back through
    /// `wake`, a sender of `events`; the node steps the events that arrive
    /// meanwhile, and takes its next write once the last is back. The
    /// journal thread keeps `events` open, so only a stop ends the loop.
    pub(crate) fn run(
        &mut self,
        events: Receiver<Event>,
        wake: Sender<Event>,
        frames: Sender<(ReplicaID, Frame)>,
    ) {
        let journal = self
            .journal
            .take()
            .expect("the journal is with the event loop's thread");
        let (writes, writes_rx) = channel();
        let stats = self.stats.clone();
        let journal_thread = std::thread::Builder::new()
            .name(format!("journal {}", self.id))
            .spawn(move || run_journal(journal, writes_rx, wake, stats))
            .unwrap_or_else(|err| fatal(&format!("cannot start the journal thread: {err}")));
        // Whatever the replica produced on the way up goes out now.
        self.deliver(&frames, &writes);
        while let Ok(event) = events.recv() {
            let batch = std::iter::once(event)
                .chain(events.try_iter())
                .take(MAX_BATCH_EVENTS);
            if !self.batch(batch, &frames, &writes) {
                break;
            }
        }
        drop(writes);
        let journal = journal_thread
            .join()
            .unwrap_or_else(|_| fatal("the journal thread panicked"));
        self.journal = Some(journal);
    }

    /// Steps the node with a batch of events, then delivers what it
    /// produced, see [`Node::deliver`]. Returns false once the event loop
    /// is done: after an [`Event::Stop`], once no write is out.
    ///
    /// A write out for [`STALLED_WRITE_TICKS`] ticks means a stalled disk.
    /// Until it comes back the node delivers nothing, as if it wrote on its
    /// own thread: a primary that went on sending would keep the backups
    /// from electing another while it could answer no client.
    pub(crate) fn batch(
        &mut self,
        events: impl IntoIterator<Item = Event>,
        frames: &Sender<(ReplicaID, Frame)>,
        writes: &Sender<LogWrite<Op>>,
    ) -> bool {
        let mut flush_store = false;
        for event in events {
            flush_store |= self.handle(event);
        }
        if self.stopping {
            return self.writing;
        }
        if self.writing && self.write_ticks >= STALLED_WRITE_TICKS {
            return true;
        }
        self.deliver(frames, writes);
        if flush_store {
            self.flush_store().unwrap_or_else(|err| fatal(&err));
        }
        let stats = &self.stats;
        stats.batches.fetch_add(1, Ordering::Relaxed);
        let view_number = self.replica.view_number() as u64;
        stats.view_number.store(view_number, Ordering::Relaxed);
        let commit_number = self.replica.commit_number() as u64;
        stats.commit_number.store(commit_number, Ordering::Relaxed);
        let persists = self.replica.state_machine().persists.get();
        stats.store_persists.store(persists, Ordering::Relaxed);
        if self.replica.view_number() != self.view {
            self.view = self.replica.view_number();
            if self.announce_views {
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
        true
    }
}

/// Appends each write from `writes` to `journal` and hands it back to the
/// event loop through `events`, until `writes` closes. Returns the journal.
/// A write that never came back would stall the node for good, so a
/// journal that fails, or panics, ends the process.
fn run_journal(
    mut journal: Journal<Op>,
    writes: Receiver<LogWrite<Op>>,
    events: Sender<Event>,
    stats: Arc<Stats>,
) -> Journal<Op> {
    for write in writes {
        let appended =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| journal.append(&write)));
        match appended {
            Ok(Ok(())) => {}
            Ok(Err(err)) => fatal(&err),
            Err(_) => fatal("the journal thread panicked"),
        }
        stats.journal_writes.fetch_add(1, Ordering::Relaxed);
        if events.send(Event::Written(write)).is_err() {
            break;
        }
    }
    journal
}

/// Power losses for the tests. A node's journal keeps every write, since
/// it fsyncs each one; its store goes back to what it last persisted, a
/// copy of which every `Store::persist` keeps. fjall 3.1.10 also fsyncs
/// its journal when it recovers on open, which this model leaves out: it
/// holds the kvstore to its own contract with the library rather than to
/// what one version of fjall happens to do.
#[cfg(test)]
pub(crate) mod power {
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
