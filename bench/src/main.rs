//! Measures what the durable log costs.
//!
//! Three replicas and a set of closed-loop clients run in one process, each
//! replica on its own thread with the event loop of the kvstore example:
//! take every message already queued, step the replica, persist, send.
//! Messages travel over channels, so the numbers isolate what the replicas
//! do from the network. The state machine is the same fjall store in every
//! configuration, persisted once a second. Three configurations differ only
//! in the library and the journal:
//!
//! - `original`: the library as it was before the durable log, embedded
//!   from commit 0b64760 in `original.rs`, with the fjall store.
//! - `durable-nojournal`: the current library with the fjall store, the
//!   journal switched off. The difference from `original` is what the new
//!   bookkeeping costs in memory.
//! - `durable`: the current library with the fjall store and the journal:
//!   one write and one fsync per batch of events on every replica, and
//!   compaction after every store persist. The difference from
//!   `durable-nojournal` is the price of the log being on disk.
//!
//! Run with `cargo run --release -p vsr-bench`. Data goes under
//! `target/bench`, so fsync hits whatever disk the repository is on; a
//! tmpfs would make it free and the numbers meaningless.

#[allow(dead_code)]
#[path = "original.rs"]
mod original;

use fjall::{Database, Keyspace, KeyspaceCreateOptions, PersistMode, Readable};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{channel, Receiver, RecvTimeoutError, Sender};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use vsr_rs::{Checkpoint, ClientRecord, LogEntry, OpNumber, Replica, ReplicaID, StateMachine};

/// How often each replica runs its idle logic.
const TICK: Duration = Duration::from_millis(100);
/// Ticks between two persists of the store.
const FLUSH_TICKS: u64 = 10;
/// Log entries kept behind what the store has persisted.
const LOG_RETENTION: usize = 1_000;
const WAL_FILE_SIZE: u64 = 64 * 1024 * 1024;
const REPLICAS: usize = 3;
const KEY_SPACE: u64 = 100_000;

/// A write of `value` to `key`.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Op {
    key: u64,
    value: u64,
}

// ---------------------------------------------------------------------------
// The store, shared by every configuration

const APPLIED_KEY: &str = "m/applied";

struct Store {
    db: Database,
    keyspace: Keyspace,
    applied: OpNumber,
}

impl Store {
    fn open(path: &Path) -> Store {
        let db = Database::builder(path)
            .manual_journal_persist(true)
            .open()
            .expect("open store");
        let keyspace = db
            .keyspace("kv", KeyspaceCreateOptions::default)
            .expect("open keyspace");
        Store {
            db,
            keyspace,
            applied: 0,
        }
    }

    fn persist(&self) {
        self.db
            .persist(PersistMode::SyncData)
            .expect("persist store");
    }

    /// Writes `op` as op `op_number`, and the client's reply if given, in
    /// one batch.
    fn write(&mut self, op_number: OpNumber, op: &Op, client: Option<(usize, usize)>) {
        let mut batch = self.db.batch();
        batch.insert(
            &self.keyspace,
            format!("k/{}", op.key),
            op.value.to_le_bytes().to_vec(),
        );
        if let Some((client_id, request_number)) = client {
            batch.insert(
                &self.keyspace,
                format!("c/{client_id}"),
                format!("{request_number} -"),
            );
        }
        batch.insert(&self.keyspace, APPLIED_KEY, op_number.to_string());
        batch.commit().expect("write store");
        self.applied = op_number;
    }
}

/// The store behind the original library, which hands `apply` the op
/// alone: it writes the key and the op number.
struct OriginalStore(Store);

impl original::StateMachine for OriginalStore {
    type Input = Op;
    type Output = ();

    fn apply(&mut self, op: Op) {
        let op_number = self.0.applied + 1;
        self.0.write(op_number, &op, None);
    }
}

/// The store behind the current library: it also writes the client's
/// reply, which a restart needs.
struct DurableStore(Store);

impl StateMachine for DurableStore {
    type Input = Op;
    type Output = ();
    type Snapshot = Vec<(u64, u64)>;

    fn apply(&mut self, op_number: OpNumber, entry: &LogEntry<Op>) {
        self.0.write(
            op_number,
            &entry.op,
            Some((entry.client_id, entry.request_number)),
        );
    }

    fn snapshot(&self) -> Vec<(u64, u64)> {
        let snapshot = self.0.db.snapshot();
        snapshot
            .prefix(&self.0.keyspace, "k/")
            .map(|guard| {
                let (key, value) = guard.into_inner().expect("read store");
                let key = String::from_utf8_lossy(&key[2..]).parse().expect("key");
                let value = u64::from_le_bytes(value[..8].try_into().expect("value"));
                (key, value)
            })
            .collect()
    }

    fn restore(&mut self, checkpoint: Checkpoint<(), Vec<(u64, u64)>>) {
        let mut batch = self.0.db.batch();
        for (key, value) in checkpoint.state {
            batch.insert(
                &self.0.keyspace,
                format!("k/{key}"),
                value.to_le_bytes().to_vec(),
            );
        }
        for ClientRecord {
            client_id,
            request_number,
            ..
        } in checkpoint.client_table
        {
            batch.insert(
                &self.0.keyspace,
                format!("c/{client_id}"),
                format!("{request_number} -"),
            );
        }
        batch.insert(
            &self.0.keyspace,
            APPLIED_KEY,
            checkpoint.op_number.to_string(),
        );
        batch.commit().expect("write store");
        self.0
            .db
            .persist(PersistMode::SyncAll)
            .expect("persist store");
        self.0.applied = checkpoint.op_number;
    }
}

// ---------------------------------------------------------------------------
// The journal, shared with the kvstore example

#[path = "../../examples/kvstore/journal.rs"]
mod journal;

use journal::{EntryCodec, Journal};

fn encode_entry(entry: &LogEntry<Op>) -> String {
    format!(
        "{} {} {} {}",
        entry.client_id, entry.request_number, entry.op.key, entry.op.value
    )
}

fn decode_entry(text: &str) -> Result<LogEntry<Op>, String> {
    let mut words = text.split_whitespace();
    let mut number = || -> Result<u64, String> {
        words
            .next()
            .ok_or("truncated entry")?
            .parse()
            .map_err(|_| "bad number".to_string())
    };
    Ok(LogEntry {
        client_id: number()? as usize,
        request_number: number()? as usize,
        op: Op {
            key: number()?,
            value: number()?,
        },
    })
}

const ENTRY_CODEC: EntryCodec<Op> = EntryCodec {
    encode: encode_entry,
    decode: decode_entry,
};

// ---------------------------------------------------------------------------
// One harness for both libraries

/// A reply as the client thread sees it.
struct ReplyEvent {
    client_id: usize,
    request_number: usize,
    view_number: usize,
}

/// What a replica thread does with its replica, and a client thread with
/// its clients, for one library.
trait Version: Sized + 'static {
    type Msg: Send + 'static;
    type Replica: Send + 'static;
    type Client;

    fn replica(
        id: ReplicaID,
        dir: &Path,
        journaled: bool,
        batches: Arc<AtomicU64>,
    ) -> Self::Replica;
    fn client(id: usize) -> Self::Client;
    fn request(client: &mut Self::Client, op: Op) -> usize;
    fn client_messages(client: &mut Self::Client) -> Vec<(ReplicaID, Self::Msg)>;
    fn reply(client: &mut Self::Client, request_number: usize, view_number: usize) -> bool;
    fn client_idle(client: &mut Self::Client);
    fn on_message(replica: &mut Self::Replica, message: Self::Msg);
    fn on_idle(replica: &mut Self::Replica);
    fn persist(replica: &mut Self::Replica);
    fn flush_store(replica: &mut Self::Replica);
    fn drain(replica: &mut Self::Replica) -> (Vec<(ReplicaID, Self::Msg)>, Vec<ReplyEvent>);
    /// Messages that may go out before the persist. The original library
    /// has none.
    fn drain_early(_replica: &mut Self::Replica) -> Vec<(ReplicaID, Self::Msg)> {
        Vec::new()
    }
}

fn config<C>(new: fn() -> C, add: fn(&mut C)) -> C {
    let mut config = new();
    for _ in 0..REPLICAS {
        add(&mut config);
    }
    config
}

struct Original;

impl Version for Original {
    type Msg = original::Message<Op>;
    type Replica = original::Replica<OriginalStore>;
    type Client = original::Client<Op>;

    fn replica(
        id: ReplicaID,
        dir: &Path,
        _journaled: bool,
        _batches: Arc<AtomicU64>,
    ) -> Self::Replica {
        let store = OriginalStore(Store::open(&dir.join("store")));
        original::Replica::new(
            id,
            config(original::Config::new, |c| {
                c.add_replica();
            }),
            store,
        )
    }
    fn client(id: usize) -> Self::Client {
        original::Client::new(
            id,
            config(original::Config::new, |c| {
                c.add_replica();
            }),
        )
    }
    fn request(client: &mut Self::Client, op: Op) -> usize {
        client.on_request(op)
    }
    fn client_messages(client: &mut Self::Client) -> Vec<(ReplicaID, Self::Msg)> {
        client.drain().collect()
    }
    fn reply(client: &mut Self::Client, request_number: usize, view_number: usize) -> bool {
        client.on_reply(request_number, view_number)
    }
    fn client_idle(client: &mut Self::Client) {
        client.on_idle();
    }
    fn on_message(replica: &mut Self::Replica, message: Self::Msg) {
        replica.on_message(message);
    }
    fn on_idle(replica: &mut Self::Replica) {
        replica.on_idle();
    }
    fn persist(_replica: &mut Self::Replica) {}
    fn flush_store(replica: &mut Self::Replica) {
        replica.state_machine().0.persist();
    }
    fn drain(replica: &mut Self::Replica) -> (Vec<(ReplicaID, Self::Msg)>, Vec<ReplyEvent>) {
        let messages = replica.drain_messages().collect();
        let replies = replica
            .drain_replies()
            .map(|reply| ReplyEvent {
                client_id: reply.client_id,
                request_number: reply.request_number,
                view_number: reply.view_number,
            })
            .collect();
        (messages, replies)
    }
}

/// The current library, with or without the journal.
struct Durable {
    replica: Replica<DurableStore>,
    journal: Option<Journal<Op>>,
    batches: Arc<AtomicU64>,
}

struct Current;

impl Version for Current {
    type Msg = vsr_rs::MessageFor<DurableStore>;
    type Replica = Durable;
    type Client = vsr_rs::Client<Op>;

    fn replica(
        id: ReplicaID,
        dir: &Path,
        journaled: bool,
        batches: Arc<AtomicU64>,
    ) -> Self::Replica {
        let store = DurableStore(Store::open(&dir.join("store")));
        let replica = Replica::new(
            id,
            config(vsr_rs::Config::new, |c| {
                c.add_replica();
            }),
            store,
        );
        let journal = journaled.then(|| {
            Journal::open(&dir.join("journal"), WAL_FILE_SIZE, ENTRY_CODEC)
                .expect("open journal")
                .0
        });
        Durable {
            replica,
            journal,
            batches,
        }
    }
    fn client(id: usize) -> Self::Client {
        vsr_rs::Client::new(
            id,
            config(vsr_rs::Config::new, |c| {
                c.add_replica();
            }),
        )
    }
    fn request(client: &mut Self::Client, op: Op) -> usize {
        client.on_request(op)
    }
    fn client_messages(client: &mut Self::Client) -> Vec<(ReplicaID, Self::Msg)> {
        client.drain().collect()
    }
    fn reply(client: &mut Self::Client, request_number: usize, view_number: usize) -> bool {
        client.on_reply(request_number, view_number)
    }
    fn client_idle(client: &mut Self::Client) {
        client.on_idle();
    }
    fn on_message(replica: &mut Self::Replica, message: Self::Msg) {
        replica.replica.on_message(message);
    }
    fn on_idle(replica: &mut Self::Replica) {
        replica.replica.on_idle();
    }
    fn persist(replica: &mut Self::Replica) {
        let Durable {
            replica,
            journal,
            batches,
        } = replica;
        replica
            .persist(|write| {
                if let (Some(journal), true) = (journal.as_mut(), write.sync) {
                    journal.append(write)?;
                    batches.fetch_add(1, Ordering::Relaxed);
                }
                Ok::<(), String>(())
            })
            .expect("write journal");
    }
    fn flush_store(replica: &mut Self::Replica) {
        let applied = replica.replica.applied();
        replica.replica.state_machine().0.persist();
        replica
            .replica
            .compact(applied.saturating_sub(LOG_RETENTION));
    }
    fn drain_early(replica: &mut Self::Replica) -> Vec<(ReplicaID, Self::Msg)> {
        replica.replica.drain_messages_before_persist().collect()
    }
    fn drain(replica: &mut Self::Replica) -> (Vec<(ReplicaID, Self::Msg)>, Vec<ReplyEvent>) {
        let messages = replica.replica.drain_messages().collect();
        let replies = replica
            .replica
            .drain_replies()
            .map(|reply| ReplyEvent {
                client_id: reply.client_id,
                request_number: reply.request_number,
                view_number: reply.view_number,
            })
            .collect();
        (messages, replies)
    }
}

/// A replica's event loop: every message already queued, then the
/// messages that need not wait, then persist, then the rest. Idle logic
/// every tick, a store persist every `FLUSH_TICKS`.
fn run_replica<V: Version>(
    mut replica: V::Replica,
    inbox: Receiver<V::Msg>,
    peers: Vec<Sender<V::Msg>>,
    replies: Sender<ReplyEvent>,
    stop: Arc<AtomicBool>,
) {
    let mut next_tick = Instant::now() + TICK;
    let mut ticks = 0u64;
    while !stop.load(Ordering::Relaxed) {
        let timeout = next_tick.saturating_duration_since(Instant::now());
        match inbox.recv_timeout(timeout) {
            Ok(message) => V::on_message(&mut replica, message),
            Err(RecvTimeoutError::Timeout) => {
                // A tick that fell due while messages kept the loop busy
                // is not made up for: an idle period means the inbox was
                // empty for a while, as with the kvstore's timer thread.
                V::on_idle(&mut replica);
                ticks += 1;
                next_tick = Instant::now() + TICK;
                if ticks.is_multiple_of(FLUSH_TICKS) {
                    V::flush_store(&mut replica);
                }
            }
            Err(RecvTimeoutError::Disconnected) => return,
        }
        while let Ok(message) = inbox.try_recv() {
            V::on_message(&mut replica, message);
        }
        for (dst, message) in V::drain_early(&mut replica) {
            let _ = peers[dst].send(message);
        }
        V::persist(&mut replica);
        let (messages, reply_events) = V::drain(&mut replica);
        for (dst, message) in messages {
            let _ = peers[dst].send(message);
        }
        for reply in reply_events {
            let _ = replies.send(reply);
        }
    }
}

struct Sample {
    ops: u64,
    latencies: Vec<Duration>,
}

type Inboxes<V> = Vec<Sender<<V as Version>::Msg>>;

/// Runs `clients` closed-loop clients for `warmup` then `measure`, and
/// returns what happened during `measure`.
fn run_clients<V: Version>(
    clients: usize,
    peers: &[Sender<V::Msg>],
    replies: &Receiver<ReplyEvent>,
    warmup: Duration,
    measure: Duration,
) -> Sample {
    let mut prng = ChaCha8Rng::seed_from_u64(1);
    let mut states: Vec<V::Client> = (0..clients).map(V::client).collect();
    let mut sent_at: HashMap<usize, Instant> = HashMap::new();
    let mut send =
        |client_id: usize, states: &mut Vec<V::Client>, sent_at: &mut HashMap<usize, Instant>| {
            let op = Op {
                key: prng.gen_range(0..KEY_SPACE),
                value: prng.gen(),
            };
            V::request(&mut states[client_id], op);
            sent_at.insert(client_id, Instant::now());
            for (dst, message) in V::client_messages(&mut states[client_id]) {
                let _ = peers[dst].send(message);
            }
        };
    for client_id in 0..clients {
        send(client_id, &mut states, &mut sent_at);
    }
    let started = Instant::now();
    let measure_from = started + warmup;
    let end = measure_from + measure;
    let mut sample = Sample {
        ops: 0,
        latencies: Vec::new(),
    };
    let mut resend_at = Instant::now() + TICK * 5;
    loop {
        let now = Instant::now();
        if now >= end {
            return sample;
        }
        match replies.recv_timeout(resend_at.saturating_duration_since(now)) {
            Ok(reply) => {
                let client = &mut states[reply.client_id];
                if V::reply(client, reply.request_number, reply.view_number) {
                    let latency = sent_at[&reply.client_id].elapsed();
                    if Instant::now() >= measure_from {
                        sample.ops += 1;
                        sample.latencies.push(latency);
                    }
                    send(reply.client_id, &mut states, &mut sent_at);
                }
            }
            Err(RecvTimeoutError::Timeout) => {
                // A request that reached a replica during a view change
                // was dropped; re-send it, to every replica, as the kvstore
                // does every few ticks.
                for client in states.iter_mut() {
                    V::client_idle(client);
                    for (dst, message) in V::client_messages(client) {
                        let _ = peers[dst].send(message);
                    }
                }
                resend_at = Instant::now() + TICK * 5;
            }
            Err(RecvTimeoutError::Disconnected) => return sample,
        }
    }
}

/// Runs one configuration with `clients` clients and reports.
fn run<V: Version>(name: &str, journaled: bool, clients: usize, data: &Path) {
    let dir = data.join(format!("{name}-{clients}"));
    let _ = std::fs::remove_dir_all(&dir);
    let batches = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let (reply_tx, reply_rx) = channel::<ReplyEvent>();
    let (inboxes, receivers): (Inboxes<V>, Vec<Receiver<V::Msg>>) =
        (0..REPLICAS).map(|_| channel()).unzip();
    let mut threads = Vec::new();
    for (id, inbox) in receivers.into_iter().enumerate() {
        let replica = V::replica(
            id,
            &dir.join(format!("node{id}")),
            journaled,
            batches.clone(),
        );
        let peers = inboxes.clone();
        let replies = reply_tx.clone();
        let stop = stop.clone();
        threads.push(thread::spawn(move || {
            run_replica::<V>(replica, inbox, peers, replies, stop)
        }));
    }
    drop(reply_tx);
    let warmup = Duration::from_secs(2);
    let measure = Duration::from_secs(8);
    let batches_before = batches.load(Ordering::Relaxed);
    let sample = run_clients::<V>(clients, &inboxes, &reply_rx, warmup, measure);
    let batches_during = batches.load(Ordering::Relaxed) - batches_before;
    stop.store(true, Ordering::Relaxed);
    drop(inboxes);
    for thread in threads {
        let _ = thread.join();
    }
    let mut latencies = sample.latencies;
    latencies.sort();
    let percentile = |p: f64| -> Duration {
        if latencies.is_empty() {
            return Duration::ZERO;
        }
        let index = ((latencies.len() - 1) as f64 * p).round() as usize;
        latencies[index]
    };
    let seconds = measure.as_secs_f64();
    let ops_per_second = sample.ops as f64 / seconds;
    let fsyncs = batches_during as f64 / seconds / REPLICAS as f64;
    println!(
        "{name:<18} {clients:>7} {ops_per_second:>10.0} {:>9.2} {:>9.2} {:>9.2} {fsyncs:>10.0} {:>9.1}",
        percentile(0.5).as_secs_f64() * 1e3,
        percentile(0.99).as_secs_f64() * 1e3,
        percentile(1.0).as_secs_f64() * 1e3,
        if fsyncs > 0.0 { ops_per_second / fsyncs } else { 0.0 },
    );
    let _ = std::fs::remove_dir_all(&dir);
}

fn main() {
    let _ = env_logger::try_init();
    let data = PathBuf::from(
        std::env::args()
            .nth(1)
            .unwrap_or_else(|| "target/bench".to_string()),
    );
    let client_counts: Vec<usize> = match std::env::var("CLIENTS") {
        Ok(list) => list
            .split(',')
            .map(|n| n.parse().expect("CLIENTS"))
            .collect(),
        Err(_) => vec![1, 8, 64],
    };
    std::fs::create_dir_all(&data).expect("data directory");
    println!(
        "{} replicas on their own threads, channels between them, fjall store persisted every {}s, data in {}",
        REPLICAS,
        (TICK * FLUSH_TICKS as u32).as_secs_f64(),
        data.display()
    );
    println!();
    println!(
        "{:<18} {:>7} {:>10} {:>9} {:>9} {:>9} {:>10} {:>9}",
        "configuration", "clients", "ops/s", "p50 ms", "p99 ms", "max ms", "fsync/s", "ops/fsync"
    );
    let only = std::env::var("CONFIG").ok();
    for &clients in &client_counts {
        if only.as_deref().is_none_or(|name| name == "original") {
            run::<Original>("original", false, clients, &data);
        }
        if only
            .as_deref()
            .is_none_or(|name| name == "durable-nojournal")
        {
            run::<Current>("durable-nojournal", false, clients, &data);
        }
        if only.as_deref().is_none_or(|name| name == "durable") {
            run::<Current>("durable", true, clients, &data);
        }
    }
}
