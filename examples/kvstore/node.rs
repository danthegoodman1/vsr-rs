//! A kvstore node: the replica, its store and journal, and the clients it
//! proxies, stepped by one event loop. `main.rs` puts it on the network;
//! the benchmark runs three of them in one process.

use crate::journal::{push_number, EntryCodec, Journal, Landing};
use fjall::{Database, Keyspace, KeyspaceCreateOptions, PersistMode, Readable, Snapshot};
use futures::future::{maybe_done, MaybeDone};
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, Sender, TryRecvError};
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::thread::JoinHandle;
use std::time::Duration;
use vsr_rs::{
    Client, ClientID, ClientRecord, Completion, Config, LogEntry, LogSegment, LogWrite, MessageFor,
    OpNumber, QueryNumber, Replica, ReplicaID, Reply, RequestNumber, StateMachine, ViewNumber,
};

/// How often the replica and the clients run their idle logic.
pub(crate) const TICK: Duration = Duration::from_millis(100);
/// Ticks between two re-sends of a request that got no reply.
pub(crate) const CLIENT_RESEND_TICKS: u64 = 5;
/// Idle periods without hearing from the primary before a view change.
pub(crate) const PRIMARY_TIMEOUT: usize = 5;
/// Sessions the client table holds, one per client connection.
pub(crate) const CLIENTS_MAX: usize = 16_384;
/// Commands a connection has in flight.
pub(crate) const IN_FLIGHT_MAX: usize = 16;
/// Ticks between two persists of the store, each of which lets the
/// replica compact its log.
pub(crate) const FLUSH_TICKS: u64 = 10;
/// The tick after a journal write went out at which the node takes its
/// disk for stalled if the write is still out, see [`Node::batch`].
pub(crate) const STALLED_WRITE_TICKS: u64 = 2;
/// The most events the event loop steps before it delivers, few enough
/// that the other nodes work on one batch while this node steps the next.
pub(crate) const MAX_BATCH_EVENTS: usize = 16;
/// The fewest replies worth handing to the answerer thread; the loop
/// wakes fewer connections itself, sooner than a handoff would.
pub(crate) const HANDED_OFF_ANSWERS: usize = 16;
/// Log entries kept behind what the store has persisted, so that a replica
/// a little behind catches up from the log rather than from a checkpoint.
pub(crate) const LOG_RETENTION: usize = 1_000;
/// Size of a write-ahead log file. Small, so that compaction reclaims
/// space in a short run.
pub(crate) const WAL_FILE_SIZE: u64 = 4 * 1024 * 1024;

// ---------------------------------------------------------------------------
// Operations

/// A write. A read is a query, a key, which goes in no log.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Op {
    Put(String, String),
}

pub(crate) fn write_op(out: &mut String, op: &Op) {
    match op {
        Op::Put(key, value) => {
            out.push_str("PUT ");
            out.push_str(key);
            out.push(' ');
            out.push_str(value);
        }
    }
}

/// Appends `entry` to `out`. The journal writes every entry this way on
/// the event loop, so it skips `format!`.
pub(crate) fn write_entry(out: &mut String, entry: &LogEntry<Op>) {
    match entry {
        LogEntry::Register { client_id } => {
            out.push_str("REG ");
            push_number(out, *client_id);
        }
        LogEntry::Request {
            client_id,
            session,
            request_number,
            answered,
            op,
        } => {
            out.push_str("REQ");
            for number in [client_id, session, request_number, answered] {
                out.push(' ');
                push_number(out, *number);
            }
            out.push(' ');
            write_op(out, op);
        }
    }
}

pub(crate) fn decode_entry(text: &str) -> Result<LogEntry<Op>, String> {
    let mut t = Tokens::new(text);
    let entry = t.entry()?;
    t.done()?;
    Ok(entry)
}

pub(crate) const ENTRY_CODEC: EntryCodec<Op> = EntryCodec {
    write: write_entry,
    decode: decode_entry,
};

pub(crate) fn encode_entries(log: &[LogEntry<Op>]) -> String {
    let mut out = log.len().to_string();
    for entry in log {
        out.push(' ');
        write_entry(&mut out, entry);
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
            kind => Err(format!("bad op {kind:?}")),
        }
    }

    pub(crate) fn entry(&mut self) -> Result<LogEntry<Op>, String> {
        match self.word()? {
            "REG" => Ok(LogEntry::Register {
                client_id: self.num()?,
            }),
            "REQ" => Ok(LogEntry::Request {
                client_id: self.num()?,
                session: self.num()?,
                request_number: self.num()?,
                answered: self.num()?,
                op: self.op()?,
            }),
            kind => Err(format!("bad entry {kind:?}")),
        }
    }

    /// A client record as `encode_client` wrote it.
    pub(crate) fn client(&mut self, client_id: ClientID) -> Result<KvClientRecord, String> {
        let session = self.num()?;
        let request_number = self.num()?;
        let op_number = self.num()?;
        let count = self.num()?;
        let mut replies = VecDeque::with_capacity(count.min(RESERVED_MAX));
        for _ in 0..count {
            replies.push_back(self.result()?);
        }
        Ok(ClientRecord {
            client_id,
            session,
            request_number,
            replies,
            op_number,
        })
    }

    pub(crate) fn entries(&mut self) -> Result<Vec<LogEntry<Op>>, String> {
        let count = self.num()?;
        let mut log = Vec::with_capacity(count.min(RESERVED_MAX));
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
// The store: a fjall database that never fsyncs on its own, behind the
// writes applied since the last flush

/// Key prefixes in a data keyspace.
pub(crate) const KEY_PREFIX: &str = "k/";
pub(crate) const CLIENT_PREFIX: &str = "c/";
/// The number of operations applied to the store, written in the same
/// batch as the operations' writes.
pub(crate) const APPLIED_KEY: &str = "applied";
/// The number of times the node has started, which keeps the client ids
/// of one run apart from those of the others.
pub(crate) const INCARNATION_KEY: &str = "incarnation";
/// The data keyspace that holds the state, 0 or 1.
const CURRENT_KEY: &str = "current";
/// The layout of the store's keyspaces and records, and of the journal's
/// entries.
pub(crate) const FORMAT_KEY: &str = "format";
pub(crate) const FORMAT: u64 = 3;
/// The one keyspace of formats before 3.
const OLD_FORMAT_KEYSPACE: &str = "kv";
/// The bytes of keys and values a checkpoint chunk holds, about.
pub(crate) const CHUNK_BYTES: usize = 256 * 1024;
/// The chunks a kept checkpoint reads ahead at most, one for each of a few
/// replicas fetching it at once.
const CHUNKS_AHEAD: usize = 4;
/// The most items decoding reserves room for up front, whatever count a
/// frame claims.
pub(crate) const RESERVED_MAX: usize = 1024;

/// The key-value store. The output of an op is the value read by a GET.
/// An op's writes stay in memory until a flush writes them to fjall, with
/// the op number they reach, in one batch, and fsyncs. The keys and the
/// client table live in one of two data keyspaces; the other stages a
/// checkpoint being fetched, and restoring it switches the two.
pub(crate) struct Store {
    pub(crate) path: PathBuf,
    pub(crate) db: Database,
    /// The counters, and which data keyspace is current.
    pub(crate) meta: Keyspace,
    data: [Keyspace; 2],
    current: usize,
    /// The number of operations applied.
    pub(crate) applied: OpNumber,
    /// The number of times the store was persisted, each with an fsync.
    pub(crate) persists: Arc<AtomicU64>,
    /// The writes of the operations applied since the last flush began.
    dirty: RefCell<Writes>,
    /// The flush out, if any.
    flushing: RefCell<Option<Flushing>>,
    /// The checkpoint kept for replicas that fell behind.
    kept: Option<Kept>,
    /// The op number of the checkpoint being staged, if any.
    staged: Option<OpNumber>,
}

/// The writes of a run of operations, which a flush encodes for fjall.
#[derive(Default)]
struct Writes {
    values: HashMap<String, String>,
    /// The client records handed over, `None` for an eviction.
    clients: HashMap<ClientID, Option<KvClientRecord>>,
}

/// A flush on a thread of its own.
struct Flushing {
    /// The writes it makes durable, which reads see until it lands.
    writes: Arc<Writes>,
    /// The op number it makes the store durable as of.
    op_number: OpNumber,
    /// Its result, sent before it wakes the event loop.
    done: Receiver<Result<(), String>>,
    thread: JoinHandle<()>,
}

/// A checkpoint kept: a snapshot of the current data keyspace, the key
/// each chunk found so far starts at, and the chunks after those served
/// last, each read ahead on a thread of its own while the replica that
/// asked stages the one before.
struct Kept {
    reader: ChunkReader,
    starts: RefCell<Vec<Vec<u8>>>,
    ahead: RefCell<VecDeque<(usize, JoinHandle<ReadChunk>)>>,
}

/// Reads chunks of a checkpoint.
#[derive(Clone)]
struct ChunkReader {
    snapshot: Snapshot,
    keyspace: Keyspace,
}

/// A chunk, and the key the next one starts at, if any.
type ReadChunk = (StoreChunk, Option<Vec<u8>>);

/// A run of a checkpoint's keys and values and client records, for a
/// replica that fell behind a compacted log.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct StoreChunk {
    pub(crate) pairs: Vec<(String, String)>,
    pub(crate) clients: Vec<KvClientRecord>,
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
        if db.keyspace_exists(OLD_FORMAT_KEYSPACE) {
            return Err(another_version(&format!("the store at {}", path.display())));
        }
        let keyspace = |name: &str| {
            db.keyspace(name, KeyspaceCreateOptions::default)
                .map_err(|err| format!("cannot open keyspace {name}: {err}"))
        };
        let meta = keyspace("meta")?;
        let data = [keyspace("data-a")?, keyspace("data-b")?];
        let mut store = Store {
            path: path.to_path_buf(),
            db,
            meta,
            data,
            current: 0,
            applied: 0,
            persists: Arc::default(),
            dirty: RefCell::default(),
            flushing: RefCell::default(),
            kept: None,
            staged: None,
        };
        store.applied = store.read_counter(APPLIED_KEY)?;
        match store.read_counter::<u64>(FORMAT_KEY)? {
            FORMAT => {}
            0 if store.applied == 0 => {}
            _ => return Err(another_version(&format!("the store at {}", path.display()))),
        }
        store.current = match store.read_counter(CURRENT_KEY)? {
            current @ (0 | 1) => current,
            _ => return Err(format!("bad {CURRENT_KEY} in store")),
        };
        // The other data keyspace holds a checkpoint staged before a crash,
        // or the state before a restore that crashed before clearing it.
        store.data[1 - store.current]
            .clear()
            .map_err(|err| format!("cannot write store: {err}"))?;
        Ok(store)
    }

    /// Whether the store records its format: one that holds nothing yet
    /// does not until [`Store::stamp_format`].
    pub(crate) fn has_format(&self) -> Result<bool, String> {
        Ok(self.read_counter::<u64>(FORMAT_KEY)? == FORMAT)
    }

    /// Records the store's format, durable with the next persist.
    pub(crate) fn stamp_format(&self) -> Result<(), String> {
        self.meta
            .insert(FORMAT_KEY, FORMAT.to_string())
            .map_err(|err| format!("cannot write store: {err}"))
    }

    /// The number the store keeps under `key`, or 0 if it keeps none.
    fn read_counter<T: std::str::FromStr + Default>(&self, key: &str) -> Result<T, String> {
        let value = self.meta.get(key).map_err(|err| err.to_string())?;
        parse_counter(key, value.as_deref())
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
        self.meta
            .insert(INCARNATION_KEY, incarnation.to_string())
            .map_err(|err| format!("cannot write store: {err}"))?;
        Ok(incarnation)
    }

    /// Makes everything applied so far durable.
    pub(crate) fn persist(&self) -> Result<(), String> {
        self.finish_flush()?;
        let (writes, op_number) = (self.dirty.take(), self.applied);
        let data = &self.data[self.current];
        make_durable(&self.db, &self.meta, data, &writes, op_number, &self.path)?;
        self.persists.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Starts a flush of the writes applied so far on a thread of its own,
    /// which calls `landed` once they are durable, unless a flush is out.
    /// Returns whether it started one.
    pub(crate) fn flush(&self, landed: impl FnOnce() + Send + 'static) -> bool {
        if self.flushing.borrow().is_some() {
            return false;
        }
        let writes = Arc::new(self.dirty.take());
        let op_number = self.applied;
        let (db, meta, data, persists) = (
            self.db.clone(),
            self.meta.clone(),
            self.data[self.current].clone(),
            self.persists.clone(),
        );
        let (flushed, path) = (writes.clone(), self.path.clone());
        let (result, done) = std::sync::mpsc::channel();
        let flush = move || {
            let flushed = make_durable(&db, &meta, &data, &flushed, op_number, &path);
            if flushed.is_ok() {
                persists.fetch_add(1, Ordering::Relaxed);
            }
            let _ = result.send(flushed);
            landed();
        };
        let thread = std::thread::Builder::new()
            .name("store flush".to_string())
            .spawn(flush)
            .unwrap_or_else(|err| fatal(&format!("cannot start the store's flush: {err}")));
        *self.flushing.borrow_mut() = Some(Flushing {
            writes,
            op_number,
            done,
            thread,
        });
        true
    }

    /// Writes what a flush would to fjall, without its fsync, as a flush
    /// the process dies in does.
    #[cfg(test)]
    pub(crate) fn write_unsynced(&self) -> Result<(), String> {
        self.finish_flush()?;
        let data = &self.data[self.current];
        write_batch(&self.db, &self.meta, data, &self.dirty.take(), self.applied)
    }

    /// Waits for the flush out, if any, and returns the op number it made
    /// the store durable as of.
    pub(crate) fn finish_flush(&self) -> Result<Option<OpNumber>, String> {
        self.take_flush(true)
    }

    /// `finish_flush`, for a flush that has landed; one still out stays out.
    pub(crate) fn landed_flush(&self) -> Result<Option<OpNumber>, String> {
        self.take_flush(false)
    }

    fn take_flush(&self, wait: bool) -> Result<Option<OpNumber>, String> {
        let mut out = self.flushing.borrow_mut();
        let Some(flushing) = out.as_ref() else {
            return Ok(None);
        };
        let result = if wait {
            flushing.done.recv().ok()
        } else {
            match flushing.done.try_recv() {
                Ok(result) => Some(result),
                Err(TryRecvError::Empty) => return Ok(None),
                Err(TryRecvError::Disconnected) => None,
            }
        };
        let flushing = out.take().expect("a flush is out");
        let joined = flushing.thread.join();
        match (result, joined) {
            (Some(result), Ok(())) => result.map(|()| Some(flushing.op_number)),
            _ => Err("the store's flush panicked".to_string()),
        }
    }

    pub(crate) fn get(&self, key: &str) -> Option<String> {
        if let Some(value) = self.dirty.borrow().values.get(key) {
            return Some(value.clone());
        }
        if let Some(flushing) = &*self.flushing.borrow() {
            if let Some(value) = flushing.writes.values.get(key) {
                return Some(value.clone());
            }
        }
        self.data[self.current]
            .get(format!("{KEY_PREFIX}{key}"))
            .unwrap_or_else(|err| fatal(&format!("cannot read store: {err}")))
            .map(|value| utf8(&value))
    }

    /// Every key and its value, less the prefix, in key order.
    #[cfg(test)]
    pub(crate) fn pairs(&self) -> Vec<(String, String)> {
        let mut pairs = self.committed(KEY_PREFIX);
        self.overlay(|writes| {
            for (key, value) in &writes.values {
                pairs.insert(key.clone(), value.clone());
            }
        });
        pairs.into_iter().collect()
    }

    /// Whether the data keyspace that is not current holds nothing.
    #[cfg(test)]
    pub(crate) fn staging_is_empty(&self) -> bool {
        self.data[1 - self.current].is_empty().unwrap()
    }

    /// The keys under `prefix` in fjall, less the prefix, and their values,
    /// in key order.
    fn committed(&self, prefix: &str) -> BTreeMap<String, String> {
        let mut pairs = BTreeMap::new();
        for guard in self.db.snapshot().prefix(&self.data[self.current], prefix) {
            let (key, value) = guard
                .into_inner()
                .unwrap_or_else(|err| fatal(&format!("cannot read store: {err}")));
            pairs.insert(utf8(&key[prefix.len()..]), utf8(&value));
        }
        pairs
    }

    /// Calls `apply` with the writes fjall may lack: those of the flush
    /// out, then those applied since.
    fn overlay(&self, mut apply: impl FnMut(&Writes)) {
        if let Some(flushing) = &*self.flushing.borrow() {
            apply(&flushing.writes);
        }
        apply(&self.dirty.borrow());
    }
}

/// The number stored under `key`, or 0 if there is none.
fn parse_counter<T: std::str::FromStr + Default>(
    key: &str,
    value: Option<&[u8]>,
) -> Result<T, String> {
    match value {
        Some(value) => utf8(value)
            .parse()
            .map_err(|_| format!("bad {key} in store")),
        None => Ok(T::default()),
    }
}

/// A client record as the store and the wire keep it, less its client id.
pub(crate) fn encode_client(record: &KvClientRecord) -> String {
    let mut out = format!(
        "{} {} {} {}",
        record.session,
        record.request_number,
        record.op_number,
        record.replies.len()
    );
    for reply in &record.replies {
        out.push(' ');
        out.push_str(&encode_result(reply));
    }
    out
}

/// Writes `writes` and the op number they reach to fjall in one batch, and
/// fsyncs.
fn make_durable(
    db: &Database,
    meta: &Keyspace,
    data: &Keyspace,
    writes: &Writes,
    op_number: OpNumber,
    path: &Path,
) -> Result<(), String> {
    write_batch(db, meta, data, writes, op_number)?;
    sync(db, path)
}

/// Writes `writes` to the data keyspace `data` and the op number they
/// reach to `meta`, in one batch.
fn write_batch(
    db: &Database,
    meta: &Keyspace,
    data: &Keyspace,
    writes: &Writes,
    op_number: OpNumber,
) -> Result<(), String> {
    let mut batch = db.batch();
    for (key, value) in &writes.values {
        batch.insert(data, format!("{KEY_PREFIX}{key}"), value.as_str());
    }
    for (client_id, record) in &writes.clients {
        let key = format!("{CLIENT_PREFIX}{client_id}");
        match record {
            Some(record) => batch.insert(data, key, encode_client(record)),
            None => batch.remove(data, key),
        }
    }
    batch.insert(meta, APPLIED_KEY, op_number.to_string());
    batch
        .commit()
        .map_err(|err| format!("cannot write store: {err}"))
}

/// Makes every write so far durable.
fn sync(db: &Database, path: &Path) -> Result<(), String> {
    db.persist(PersistMode::SyncData)
        .map_err(|err| format!("cannot persist store at {}: {err}", path.display()))?;
    #[cfg(test)]
    power::record_durable(path);
    Ok(())
}

impl Drop for Store {
    /// Waits for the flush out, so that the store is closed once dropped.
    fn drop(&mut self) {
        if let Err(err) = self.finish_flush() {
            eprintln!("{err}");
        }
    }
}

/// The error for data that another version of the kvstore wrote.
pub(crate) fn another_version(what: &str) -> String {
    format!("{what} was written by another version of the kvstore; start the node over with --init or --recover")
}

pub(crate) fn fatal(message: &str) -> ! {
    eprintln!("{message}");
    std::process::exit(1);
}

impl StateMachine for Store {
    type Input = Op;
    type Query = String;
    type Output = Option<String>;
    type Chunk = StoreChunk;

    fn apply(&mut self, op_number: OpNumber, op: &Op) -> Option<String> {
        match op {
            Op::Put(key, value) => {
                let values = &mut self.dirty.get_mut().values;
                values.insert(key.clone(), value.clone());
            }
        }
        self.applied = op_number;
        None
    }

    fn query(&self, key: &String) -> Option<String> {
        self.get(key)
    }

    /// Keeps the record in the writes since the last flush, reusing the
    /// copy kept there of the client's last one.
    fn record_client(
        &mut self,
        op_number: OpNumber,
        client_id: ClientID,
        record: Option<&KvClientRecord>,
    ) {
        let clients = &mut self.dirty.get_mut().clients;
        match (clients.get_mut(&client_id), record) {
            (Some(Some(kept)), Some(record)) => {
                kept.session = record.session;
                kept.request_number = record.request_number;
                kept.replies.clone_from(&record.replies);
                kept.op_number = record.op_number;
            }
            _ => {
                clients.insert(client_id, record.cloned());
            }
        }
        self.applied = op_number;
    }

    fn client_table(&self) -> Vec<KvClientRecord> {
        let mut table = BTreeMap::new();
        for (key, value) in self.committed(CLIENT_PREFIX) {
            let client_id = key
                .parse()
                .unwrap_or_else(|_| fatal(&format!("bad client id {key:?} in store")));
            let record = Tokens::new(&value)
                .client(client_id)
                .unwrap_or_else(|err| fatal(&format!("bad client record in store: {err}")));
            table.insert(client_id, record);
        }
        self.overlay(|writes| {
            for (client_id, record) in &writes.clients {
                match record {
                    Some(record) => table.insert(*client_id, record.clone()),
                    None => table.remove(client_id),
                };
            }
        });
        table.into_values().collect()
    }

    /// Keeps a snapshot of fjall, which holds the state as of the last
    /// flush written: no earlier than the replica's log start, since the
    /// node compacts only up to a flush that landed.
    fn checkpoint(&mut self) -> OpNumber {
        let snapshot = self.db.snapshot();
        let applied = snapshot
            .get(&self.meta, APPLIED_KEY)
            .map_err(|err| err.to_string())
            .and_then(|value| parse_counter(APPLIED_KEY, value.as_deref()))
            .unwrap_or_else(|err| fatal(&format!("cannot read store: {err}")));
        let reader = ChunkReader {
            snapshot,
            keyspace: self.data[self.current].clone(),
        };
        self.kept = Some(Kept {
            reader,
            starts: RefCell::new(vec![Vec::new()]),
            ahead: RefCell::default(),
        });
        applied
    }

    /// Chunk `index` holds the keys from where chunk `index - 1` stopped,
    /// up to about `CHUNK_BYTES` of them, so where each chunk starts
    /// depends only on the state.
    fn checkpoint_chunk(&self, index: usize) -> (StoreChunk, bool) {
        let kept = self.kept.as_ref().expect("a checkpoint kept");
        let (chunk, next) = kept.chunk(index);
        (chunk, next.is_none())
    }

    fn release_checkpoint(&mut self) {
        self.kept = None;
    }

    /// Writes the chunk to the data keyspace that is not current, which
    /// chunk 0 clears first. `restore` syncs it with the rest.
    fn stage_chunk(&mut self, op_number: OpNumber, index: usize, chunk: StoreChunk) {
        let staging = &self.data[1 - self.current];
        if index == 0 {
            staging
                .clear()
                .unwrap_or_else(|err| fatal(&format!("cannot write store: {err}")));
            self.staged = Some(op_number);
        }
        assert_eq!(Some(op_number), self.staged);
        let mut batch = self.db.batch();
        for (key, value) in chunk.pairs {
            batch.insert(staging, format!("{KEY_PREFIX}{key}"), value);
        }
        for record in chunk.clients {
            let key = format!("{CLIENT_PREFIX}{}", record.client_id);
            batch.insert(staging, key, encode_client(&record));
        }
        batch
            .commit()
            .unwrap_or_else(|err| fatal(&format!("cannot write store: {err}")));
    }

    /// Makes the staged checkpoint the state: one batch switches the data
    /// keyspaces and sets the op number, and a sync makes it durable with
    /// the chunks, as the library requires. The old state is cleared.
    fn restore(&mut self, op_number: OpNumber) {
        assert_eq!(Some(op_number), self.staged.take());
        assert!(self.kept.is_none(), "a checkpoint kept of the old state");
        self.finish_flush().unwrap_or_else(|err| fatal(&err));
        *self.dirty.get_mut() = Writes::default();
        let staging = 1 - self.current;
        let mut batch = self.db.batch();
        batch.insert(&self.meta, CURRENT_KEY, staging.to_string());
        batch.insert(&self.meta, APPLIED_KEY, op_number.to_string());
        batch
            .commit()
            .map_err(|err| format!("cannot write store: {err}"))
            .and_then(|()| sync(&self.db, &self.path))
            .unwrap_or_else(|err| fatal(&err));
        self.persists.fetch_add(1, Ordering::Relaxed);
        let old = std::mem::replace(&mut self.current, staging);
        self.data[old]
            .clear()
            .unwrap_or_else(|err| fatal(&format!("cannot write store: {err}")));
        self.applied = op_number;
    }
}

impl Kept {
    /// Chunk `index`: the one read ahead, or else read now; past the last
    /// chunk, an empty one. The chunk after it is then read ahead.
    fn chunk(&self, index: usize) -> ReadChunk {
        let ahead = {
            let mut ahead = self.ahead.borrow_mut();
            let position = ahead.iter().position(|(read, _)| *read == index);
            position.and_then(|position| ahead.remove(position))
        };
        let read = match ahead {
            Some((_, thread)) => thread
                .join()
                .unwrap_or_else(|_| fatal("a checkpoint read panicked")),
            None => match self.start(index) {
                Some(start) => self.reader.read(&start),
                None => (StoreChunk::default(), None),
            },
        };
        if let Some(next) = &read.1 {
            let mut starts = self.starts.borrow_mut();
            if starts.len() == index + 1 {
                starts.push(next.clone());
            }
            self.read_ahead(index + 1, next.clone());
        }
        read
    }

    /// Where chunk `index` starts, found from the last start known by the
    /// sizes of the keys and values alone, or `None` past the last chunk.
    fn start(&self, index: usize) -> Option<Vec<u8>> {
        let mut starts = self.starts.borrow_mut();
        while starts.len() <= index {
            let last = starts.last().expect("chunk 0's start");
            let next = self.reader.scan(last, |_, _| {})?;
            starts.push(next);
        }
        Some(starts[index].clone())
    }

    /// Reads chunk `index`, which starts at `start`, on a thread of its
    /// own, unless it is being read already; with `CHUNKS_AHEAD` being read,
    /// the oldest read goes first.
    fn read_ahead(&self, index: usize, start: Vec<u8>) {
        let mut ahead = self.ahead.borrow_mut();
        if ahead.iter().any(|(read, _)| *read == index) {
            return;
        }
        if ahead.len() == CHUNKS_AHEAD {
            if let Some((_, thread)) = ahead.pop_front() {
                let _ = thread.join();
            }
        }
        let reader = self.reader.clone();
        let thread = std::thread::Builder::new()
            .name("checkpoint read".to_string())
            .spawn(move || reader.read(&start))
            .unwrap_or_else(|err| fatal(&format!("cannot read a checkpoint: {err}")));
        ahead.push_back((index, thread));
    }
}

impl Drop for Kept {
    /// Waits for the chunks read ahead, whose keyspace a restore clears
    /// once the checkpoint is dropped.
    fn drop(&mut self) {
        for (_, thread) in self.ahead.get_mut().drain(..) {
            let _ = thread.join();
        }
    }
}

impl ChunkReader {
    /// The chunk that starts at key `start`, and the key the next one
    /// starts at, if any.
    fn read(&self, start: &[u8]) -> ReadChunk {
        let mut chunk = StoreChunk::default();
        let next = self.scan(start, |key, value| {
            if let Some(name) = key.strip_prefix(KEY_PREFIX.as_bytes()) {
                chunk.pairs.push((utf8(name), utf8(value)));
            } else if let Some(client_id) = key.strip_prefix(CLIENT_PREFIX.as_bytes()) {
                let client_id = utf8(client_id);
                let record = client_id
                    .parse()
                    .map_err(|_| format!("bad client id {client_id:?}"))
                    .and_then(|client_id| Tokens::new(&utf8(value)).client(client_id))
                    .unwrap_or_else(|err| fatal(&format!("bad client record in store: {err}")));
                chunk.clients.push(record);
            } else {
                fatal(&format!("unexpected key {:?} in store", utf8(key)));
            }
        });
        (chunk, next)
    }

    /// Calls `take` with each key and value of the chunk that starts at
    /// key `start`, and returns the key the next one starts at, if any.
    fn scan(&self, start: &[u8], mut take: impl FnMut(&[u8], &[u8])) -> Option<Vec<u8>> {
        let mut bytes = 0;
        for guard in self.snapshot.range(&self.keyspace, start..) {
            let (key, value) = guard
                .into_inner()
                .unwrap_or_else(|err| fatal(&format!("cannot read store: {err}")));
            if bytes >= CHUNK_BYTES {
                return Some(key.to_vec());
            }
            bytes += key.len() + value.len();
            take(&key, &value);
        }
        None
    }
}

// ---------------------------------------------------------------------------
// What nodes send each other

pub(crate) type KvReply = Reply<Option<String>>;
pub(crate) type KvMessage = MessageFor<Store>;
pub(crate) type KvSegment = LogSegment<Op>;
pub(crate) type KvClientRecord = ClientRecord<Option<String>>;

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

/// Where a node sends its frames for other nodes.
pub(crate) trait Outbox {
    fn send_to(&self, dst: ReplicaID, frame: Frame);
}

impl Outbox for Sender<(ReplicaID, Frame)> {
    fn send_to(&self, dst: ReplicaID, frame: Frame) {
        let _ = self.send((dst, frame));
    }
}

/// The configuration of a cluster of `replicas` nodes.
pub(crate) fn config(replicas: usize) -> Config {
    let mut config = Config::new();
    for _ in 0..replicas {
        config.add_replica();
    }
    config.set_primary_timeout(PRIMARY_TIMEOUT);
    config.set_clients_max(CLIENTS_MAX);
    config.set_in_flight_max(IN_FLIGHT_MAX);
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
/// Client ids must never repeat, or the primary answers a new connection's
/// registration with an old connection's session. The node id in the top
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
    /// writeahead's writer thread woke the event loop: the journal write
    /// that is out may have landed, and the loop polls it to see, see
    /// [`Node::run`].
    Written,
    /// The store's flush landed: the node compacts the log up to where it
    /// made the store durable, see [`Node::store_flushed`].
    Flushed,
    /// Ends [`Node::run`] once no write is out. The kvstore runs until it
    /// is killed; the benchmark and the tests stop their nodes.
    #[cfg_attr(not(test), allow(dead_code))]
    Stop,
}

/// A client connection's VSR client and the commands it has in flight.
pub(crate) struct Connection {
    pub(crate) client: Client<Op, String>,
    /// The commands in flight, in the order they came.
    pub(crate) pending: VecDeque<Pending>,
}

/// A command in flight on a connection.
pub(crate) struct Pending {
    pub(crate) ticket: Ticket,
    pub(crate) respond: Sender<String>,
    /// Its answer, held until the commands before it have theirs, so that
    /// a connection gets its answers in the order its commands came.
    pub(crate) answer: Option<String>,
}

/// What a command went out as: a request, or a query.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Ticket {
    Request(RequestNumber),
    Query(QueryNumber),
}

/// The answer to each command in flight when the connection's session was
/// evicted.
pub(crate) const EVICTED: &str = "-ERR session evicted; the command may or may not have run\r\n";

/// A reply as a client connection takes it, and where to send it.
pub(crate) type Answer = (Sender<String>, String);

pub(crate) fn format_reply(ticket: Ticket, result: Option<String>) -> String {
    match (ticket, result) {
        (Ticket::Request(_), _) => "+OK\r\n".to_string(),
        (Ticket::Query(_), Some(value)) => format!("${}\r\n{value}\r\n", value.len()),
        (Ticket::Query(_), None) => "$-1\r\n".to_string(),
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
    pub(crate) journal: Journal<Op>,
    /// The replica's write on its way to the journal, if one is out.
    pub(crate) writing: Option<Writing>,
    /// Ticks since the write that is out went out.
    pub(crate) write_ticks: u64,
    /// Whether writeahead's writer thread woke the event loop since the
    /// write that is out was last polled.
    pub(crate) woken: bool,
    /// Whether the event loop is to end, once no write is out.
    pub(crate) stopping: bool,
    /// What this node's clients send, taken from each as it has something.
    pub(crate) requests: Vec<(ReplicaID, KvMessage)>,
    /// The number of times this node has started, which its client ids
    /// carry.
    pub(crate) incarnation: u64,
    pub(crate) connections: HashMap<u64, Connection>,
    /// Replies for the connections, handed out after each batch, see
    /// [`Node::hand_out`].
    pub(crate) answers: Vec<Answer>,
    /// The thread that hands replies to the connections while
    /// [`Node::run`] runs, and the number of batches handed to it that it
    /// has not handed out yet.
    pub(crate) answerer: Option<Sender<Vec<Answer>>>,
    pub(crate) handed_off: Arc<AtomicU64>,
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
        // A store with no format of its own holds nothing yet, beside a
        // journal that may be of an earlier version, which fails to decode.
        let formatted = store.has_format()?;
        let (journal, state) = Journal::open(&journal_dir, wal_file_size, ENTRY_CODEC).map_err(
            |err| match formatted {
                true => err,
                false => format!(
                    "{}: {err}",
                    another_version(&format!("the journal in {dir}"))
                ),
            },
        )?;
        if !formatted {
            store.stamp_format()?;
        }
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
                let applied = store.applied;
                println!(
                    "restarting from view {} with {} ops, {} committed, {applied} applied by the store",
                    state.view_number,
                    state.log_start + state.log.len(),
                    state.commit_number
                );
                Replica::restart(id, config.clone(), store, applied, state, nonce)
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
            writing: None,
            write_ticks: 0,
            woken: false,
            stopping: false,
            requests: Vec::new(),
            incarnation,
            config,
            connections: HashMap::new(),
            answers: Vec::new(),
            answerer: None,
            handed_off: Arc::default(),
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
        let journal = &mut node.journal;
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
        let (journal, journaled) = (&mut self.journal, self.journaled);
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
        for (dst, message) in std::mem::take(&mut self.requests) {
            if dst == self.id {
                self.replica.on_message(message);
            } else {
                send(dst, message);
            }
        }
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

    /// Ends a batch of steps the way `step` does, stepping on while the
    /// journal writes, and sends what the node and its clients produced as
    /// soon as it may: protocol messages to `outbox`, and replies
    /// to the node that owns the client connection, which may be this one.
    /// What the last write released goes out first. Then, unless a write
    /// is out already, it takes the replica's write: one that needs a sync
    /// goes out to the journal, which wakes the event loop through `wake`
    /// with an [`Event::Written`]; any other goes straight back to the
    /// replica, and what that released goes out too. The replies to this
    /// node's clients can let them send more, which goes out in the same
    /// batch.
    pub(crate) fn deliver(&mut self, outbox: &impl Outbox, wake: &Sender<Event>) {
        let mut send = |dst, message| {
            outbox.send_to(dst, Frame::Message(message));
        };
        let mut replies = Vec::new();
        loop {
            self.before_write(&mut send, &mut replies);
            self.after_write(&mut send, &mut replies);
            self.answer(&mut replies, outbox);
            if self.writing.is_none() {
                let write = self.replica.take_write();
                if write.sync && self.journaled {
                    self.write_out(write, wake);
                } else {
                    self.replica.persisted(write);
                }
            }
            self.after_write(&mut send, &mut replies);
            self.answer(&mut replies, outbox);
            if self.requests.is_empty() {
                break;
            }
        }
        self.hand_out();
    }

    /// Sends the replies ready to their connections, in order: behind the
    /// batches the answerer thread has yet to hand out.
    fn hand_out(&mut self) {
        if self.answers.is_empty() {
            return;
        }
        let behind = self.handed_off.load(Ordering::Acquire) > 0;
        match &self.answerer {
            Some(answerer) if self.answers.len() >= HANDED_OFF_ANSWERS || behind => {
                self.handed_off.fetch_add(1, Ordering::AcqRel);
                if answerer.send(std::mem::take(&mut self.answers)).is_err() {
                    fatal("the answerer thread is gone");
                }
            }
            _ => {
                for (respond, answer) in self.answers.drain(..) {
                    let _ = respond.send(answer);
                }
            }
        }
    }

    /// Sends `write` to the journal: the first poll hands it to
    /// writeahead's writer thread. A write that lands on that poll stays
    /// out until the event loop handles its `Event::Written`, which the
    /// node sends itself, so that the node steps the same whatever the
    /// disk's speed.
    fn write_out(&mut self, write: LogWrite<Op>, wake: &Sender<Event>) {
        let mut landing = maybe_done(self.journal.submit(&write));
        let ready = poll_landing(&mut landing, wake).is_ready();
        self.writing = Some(Writing { write, landing });
        if ready && wake.send(Event::Written).is_err() {
            fatal("the event loop is gone");
        }
    }

    /// Polls the journal write that is out, after an [`Event::Written`]:
    /// once it has landed, records where, and hands it back to the
    /// replica, which releases what waited for it.
    fn landed(&mut self, wake: &Sender<Event>) {
        let Some(writing) = &mut self.writing else {
            return;
        };
        if poll_landing(&mut writing.landing, wake).is_pending() {
            return;
        }
        let Writing { write, mut landing } = self.writing.take().expect("a write is out");
        let ids = Pin::new(&mut landing)
            .take_output()
            .expect("a landed write has its output")
            .unwrap_or_else(|err| fatal(&err));
        self.journal
            .finish(&write, ids)
            .unwrap_or_else(|err| fatal(&err));
        self.stats.journal_writes.fetch_add(1, Ordering::Relaxed);
        self.write_ticks = 0;
        self.replica.persisted(write);
    }

    /// Hands each of `replies` to the node that owns its client
    /// connection, which may be this one.
    fn answer(&mut self, replies: &mut Vec<KvReply>, outbox: &impl Outbox) {
        for reply in replies.drain(..) {
            let owner = node_of(reply.client_id());
            if owner == self.id {
                self.deliver_reply(reply);
            } else {
                outbox.send_to(owner, Frame::Reply(reply));
            }
        }
    }

    /// Queues the answers a reply completes for its connection, if it is
    /// still there: a command's result, or, after an eviction, the failure
    /// of every command in flight. The connection's client may then have
    /// requests to send, with its session or with room in flight.
    fn deliver_reply(&mut self, reply: KvReply) {
        let id = reply.client_id() as u64;
        let Some(connection) = self.connections.get_mut(&id) else {
            return;
        };
        let completed = match connection.client.on_reply(reply) {
            Some(Completion::Executed(request_number, result)) => {
                Some((Ticket::Request(request_number), result))
            }
            Some(Completion::Queried(query_number, result)) => {
                Some((Ticket::Query(query_number), result))
            }
            Some(Completion::Evicted) => {
                for pending in &mut connection.pending {
                    pending.answer.get_or_insert_with(|| EVICTED.to_string());
                }
                None
            }
            None => None,
        };
        if let Some((ticket, result)) = completed {
            let pending = &mut connection.pending;
            if let Some(pending) = pending.iter_mut().find(|pending| pending.ticket == ticket) {
                pending.answer = Some(format_reply(ticket, result));
            }
        }
        while connection
            .pending
            .front()
            .is_some_and(|pending| pending.answer.is_some())
        {
            let Pending {
                respond, answer, ..
            } = connection.pending.pop_front().expect("a command in flight");
            self.answers
                .push((respond, answer.expect("the command's answer")));
        }
        self.requests.extend(connection.client.drain());
    }

    /// Steps the replica or a client with one event. Returns whether the
    /// store is due for a flush.
    pub(crate) fn handle(&mut self, event: Event) -> bool {
        match event {
            Event::Message(message) => self.replica.on_message(message),
            Event::Reply(reply) => self.deliver_reply(reply),
            Event::Command {
                connection: id,
                command,
                respond,
            } => {
                let config = &self.config;
                let connection = self.connections.entry(id).or_insert_with(|| Connection {
                    client: Client::new(id as ClientID, config.clone()),
                    pending: VecDeque::new(),
                });
                let ticket = match command {
                    Command::Set(key, value) => {
                        Ticket::Request(connection.client.on_request(Op::Put(key, value)))
                    }
                    Command::Get(key) => Ticket::Query(connection.client.on_query(key)),
                };
                connection.pending.push_back(Pending {
                    ticket,
                    respond,
                    answer: None,
                });
                self.requests.extend(connection.client.drain());
            }
            Event::Disconnect(id) => {
                self.connections.remove(&id);
            }
            Event::Tick => {
                self.ticks += 1;
                if self.writing.is_some() {
                    self.write_ticks += 1;
                }
                self.replica.on_idle();
                if self.ticks.is_multiple_of(CLIENT_RESEND_TICKS) {
                    for connection in self.connections.values_mut() {
                        connection.client.on_idle();
                        self.requests.extend(connection.client.drain());
                    }
                }
                return self.ticks.is_multiple_of(FLUSH_TICKS);
            }
            Event::Written => self.woken = true,
            Event::Flushed => self.store_flushed(),
            Event::Stop => self.stopping = true,
        }
        false
    }

    /// Persists the store, which makes everything it has applied durable,
    /// and compacts the log up to there, less the retention window. The
    /// compaction reaches the journal with the next write.
    #[cfg(test)]
    pub(crate) fn flush_store(&mut self) -> Result<(), String> {
        let applied = self.replica.applied();
        self.replica.state_machine().persist()?;
        self.replica
            .compact(applied.saturating_sub(self.log_retention));
        Ok(())
    }

    /// Starts a flush of the store on a thread of its own, which wakes the
    /// event loop through `wake` with an [`Event::Flushed`] when it lands,
    /// unless a flush is out.
    pub(crate) fn start_flush(&mut self, wake: &Sender<Event>) {
        let wake = wake.clone();
        self.replica.state_machine().flush(move || {
            let _ = wake.send(Event::Flushed);
        });
    }

    /// Compacts the log up to where the flush that landed made the store
    /// durable, less the retention window.
    fn store_flushed(&mut self) {
        let flushed = self.replica.state_machine().landed_flush();
        if let Some(op_number) = flushed.unwrap_or_else(|err| fatal(&err)) {
            self.replica
                .compact(op_number.saturating_sub(self.log_retention));
        }
    }

    /// Runs the event loop until an [`Event::Stop`], stepping every batch
    /// of events already queued, see [`Node::batch`]. The journal is
    /// written on writeahead's writer thread, which wakes the loop through
    /// `wake`, a sender of `events`, when each write lands; the node steps
    /// the events that arrive meanwhile, and takes its next write once the
    /// last is back. The writer holds `wake` while a write is out, so only
    /// a stop ends the loop.
    pub(crate) fn run(
        &mut self,
        events: Receiver<Event>,
        wake: Sender<Event>,
        outbox: impl Outbox,
    ) {
        let (answerer, batches) = std::sync::mpsc::channel::<Vec<Answer>>();
        let handed_off = self.handed_off.clone();
        let answering = std::thread::Builder::new()
            .name(format!("node {} answers", self.id))
            .spawn(move || {
                for batch in batches {
                    for (respond, answer) in batch {
                        let _ = respond.send(answer);
                    }
                    handed_off.fetch_sub(1, Ordering::AcqRel);
                }
            })
            .unwrap_or_else(|err| fatal(&format!("cannot start the answerer thread: {err}")));
        self.answerer = Some(answerer);
        // Whatever the replica produced on the way up goes out now.
        self.deliver(&outbox, &wake);
        while let Ok(event) = events.recv() {
            let batch = std::iter::once(event)
                .chain(events.try_iter())
                .take(MAX_BATCH_EVENTS);
            if !self.batch(batch, &outbox, &wake) {
                break;
            }
        }
        self.answerer = None;
        let _ = answering.join();
    }

    /// Steps the node with a batch of events, then delivers what it
    /// produced, see [`Node::deliver`]. Returns false once the event loop
    /// is done: after an [`Event::Stop`], once no write is out.
    ///
    /// A write still out at the [`STALLED_WRITE_TICKS`]th tick after it
    /// went out, one to two tick periods later, means a stalled disk.
    /// Until it comes back the node delivers nothing, as if it wrote on its
    /// own thread: a primary that went on sending would keep the backups
    /// from electing another while it could answer no client.
    pub(crate) fn batch(
        &mut self,
        events: impl IntoIterator<Item = Event>,
        outbox: &impl Outbox,
        wake: &Sender<Event>,
    ) -> bool {
        let mut flush_store = false;
        for event in events {
            flush_store |= self.handle(event);
        }
        self.hand_out();
        if std::mem::take(&mut self.woken) {
            self.landed(wake);
        }
        if self.stopping {
            return self.writing.is_some();
        }
        if self.writing.is_some() && self.write_ticks >= STALLED_WRITE_TICKS {
            return true;
        }
        self.deliver(outbox, wake);
        if flush_store {
            self.start_flush(wake);
        }
        let stats = &self.stats;
        stats.batches.fetch_add(1, Ordering::Relaxed);
        let view_number = self.replica.view_number() as u64;
        stats.view_number.store(view_number, Ordering::Relaxed);
        let commit_number = self.replica.commit_number() as u64;
        stats.commit_number.store(commit_number, Ordering::Relaxed);
        let persists = self
            .replica
            .state_machine()
            .persists
            .load(Ordering::Relaxed);
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

/// The replica's write on its way to the journal.
pub(crate) struct Writing {
    pub(crate) write: LogWrite<Op>,
    landing: MaybeDone<Landing>,
}

/// Polls `landing` with a waker that posts an [`Event::Written`] to `wake`
/// from writeahead's writer thread.
fn poll_landing(landing: &mut MaybeDone<Landing>, wake: &Sender<Event>) -> Poll<()> {
    let waker = Waker::from(Arc::new(JournalWaker(wake.clone())));
    Pin::new(landing).poll(&mut Context::from_waker(&waker))
}

/// Wakes the event loop when the journal write that is out lands.
struct JournalWaker(Sender<Event>);

impl Wake for JournalWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        let _ = self.0.send(Event::Written);
    }
}

/// Power losses for the tests. A node's journal keeps every write, since
/// it fsyncs each one; its store goes back to what it last made durable, a
/// copy of which every store flush and persist keeps. fjall 3.1.10 also fsyncs
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

    /// Keeps a copy of the store at `path` as it is right after an fsync.
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
