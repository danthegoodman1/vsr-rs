//! Viewstamped Replication for Rust.
//!
//! A work-in-progress implementation of the protocol described in
//! "Viewstamped Replication Revisited" by Liskov and Cowling.
//!
//! The library does no I/O, keeps no clocks, and starts no threads. A
//! [`Replica`] and a [`Client`] are state machines that their owner steps:
//! hand them incoming messages with `on_message` and `on_reply`, tell them
//! time has passed with `on_idle`, persist the replica's write as described
//! below, and afterwards drain what they want sent with `drain_messages`,
//! `drain_replies`, and `drain`. The owner decides
//! how those get delivered, whether over sockets, through a simulated
//! network, or straight into another replica in a test.
//!
//! # Persistence
//!
//! The protocol keeps its state in memory, and the owner persists the part
//! of it that must survive a crash. After every step, the owner persists
//! the replica's [`LogWrite`]: the view state, and the log entries that
//! changed since the last write. Handing the write back releases what
//! waited for it: acknowledgements and view change messages. An
//! acknowledgement thus leaves only after the entry it covers is on disk,
//! and a view change is on disk before anything sent in the new view. A
//! committed operation executes once a handed-back write holds it: in the
//! step that commits it, if an earlier write does, or else when its own
//! write comes back. A state machine that persists what it executes thus
//! never gets ahead of the log. A replica comes back from a crash with what
//! the owner persisted, a [`PersistentState`], through
//! [`Replica::restart`].
//!
//! ```text
//! replica.on_message(message);                      // any number of steps
//! send(replica.drain_messages_before_persist());    // optional
//! reply(replica.drain_replies());                   // optional
//! replica.persist(|write| {
//!     if write.sync {
//!         disk.append(write)?;                      // durable before it returns
//!     }
//!     Ok(())
//! })?;
//! send(replica.drain_messages());
//! reply(replica.drain_replies());
//! ```
//!
//! [`Replica::persist`] takes the write with [`Replica::take_write`] and
//! hands it back with [`Replica::persisted`]. An owner that writes while
//! the replica goes on calls the two itself: one write is outstanding at a
//! time, and what the steps in between change waits for the next.
//!
//! Three things soften the ordering without weakening it. Messages that
//! promise nothing about the sender's durable state can leave before the
//! write, so that the primary's write overlaps the backups':
//! [`Replica::drain_messages_before_persist`] yields them. Replies can
//! leave before it too, since they answer operations already on disk. And
//! a write that changed nothing but the commit number need not be synced
//! at all: [`LogWrite::sync`] is false, the next write carries the commit
//! number, and a restart takes it from the state machine when the log is
//! behind it.
//!
//! A replica that lost its disk comes back through [`Replica::recover`]
//! instead, which fetches the state from the others. One thing must survive
//! even that: the view number. Without it a replica can forget that it asked
//! for a view change and let two views run at once, as shown by Michael et
//! al. in "Recovering Shared Objects Without Stable Storage". A cluster
//! needs at least three replicas: with fewer, a replica that lost its disk
//! has no quorum of others to recover from.
//!
//! # Compaction
//!
//! The log grows without bound until the owner compacts it with
//! [`Replica::compact`], which drops entries the state machine has made
//! durable. A replica that needs entries another one has compacted fetches
//! a checkpoint of that replica's state instead, one chunk at a time: the
//! sender keeps a copy of its state as of an op through
//! [`StateMachine::checkpoint`] and reads it out with
//! [`StateMachine::checkpoint_chunk`], and the receiver stages each chunk
//! with [`StateMachine::stage_chunk`] and switches to the whole with
//! [`StateMachine::restore`]. While the sender keeps a checkpoint it
//! compacts no entry after it, so the receiver finds the rest in the log;
//! it drops the checkpoint once no replica has asked for a chunk for twice
//! `Config::primary_timeout` idle periods. A fetch ends with the state
//! transfer, view change, or recovery that started it. One that gets no
//! chunk for as long stalls: the replica asks for state again, and goes on
//! from where it stopped if the answer names the same checkpoint. A new
//! primary that must fetch a checkpoint to start its view does so while
//! the others wait for it; if that takes longer than they wait, the next
//! view change moves on to another primary.
//!
//! # Sessions
//!
//! A client registers for a session through the log, and numbers its
//! requests within it from 1. The primary appends only a session's next
//! request, so a session's requests execute in order, and a client keeps up
//! to `Config::in_flight_max` of them in flight. The client table holds at
//! most `Config::clients_max` sessions: registering one more evicts the
//! session whose latest entry executed earliest, the same one on every
//! replica, since the log decides it. A request of an evicted session never
//! executes. Its client learns of the eviction from the reply, and
//! registers again.
//!
//! # Queries
//!
//! A query reads the state machine without going in the log. The primary
//! answers it once a quorum has confirmed its view in a round started after
//! the query arrived, from a state that holds every op that may have
//! completed by then: every write that completed before the query was
//! sent. A few rounds are out at a time, however many queries arrive. A
//! client keeps its operations in the order it issued them, so a query sees
//! every write its client issued before it.

use foldhash::{HashMap, HashSet};
use log::trace;
use std::{
    collections::{hash_map::Entry, BTreeMap, BTreeSet, VecDeque},
    fmt::Debug,
};

/// Identifies a client. Every client must have its own, and a client that
/// restarts must not reuse one: the primary answers its registration with
/// the session the old one had.
pub type ClientID = usize;

/// The number of log entries a replica has executed. The entries at
/// op numbers up to it are committed and never change.
pub type CommitID = usize;

/// The position of an entry in the log, counted from 1. A replica's op
/// number is that of its last entry.
pub type OpNumber = usize;

/// Identifies a replica: its index in the configuration's list of replicas.
pub type ReplicaID = usize;

/// Numbers the requests of one session, from 1, one more for every
/// request. The client table keeps the latest per session to spot
/// re-sends.
pub type RequestNumber = usize;

/// Numbers the queries of one client, from 1, one more for every query.
pub type QueryNumber = usize;

/// Numbers the views. The primary of view `v` is replica `v` modulo the
/// number of replicas, so the view number says who leads.
pub type ViewNumber = usize;

/// State machine.
///
/// The replica executes committed operations through it, in op number
/// order, once a write its owner handed back holds them: in the step that
/// commits them, or in [`Replica::persisted`]. It keeps the client table
/// too: the replica hands it every record that changes, with the op
/// number that changed it. A state machine that persists its state writes
/// the op numbers it got alongside, and the client table. After a crash
/// its owner makes whatever state it came back with durable, and hands
/// [`Replica::restart`] that state with its op number; the replica
/// executes the committed operations after it once more.
pub trait StateMachine {
    type Input: Clone + Debug;
    /// A read of the state, which changes nothing and goes in no log.
    type Query: Clone + Debug;
    /// The result of applying an input or answering a query. The client
    /// table keeps the latest results of each session to answer re-sent
    /// requests without running them again.
    type Output: Clone + Debug;
    /// A part of a checkpoint: a copy of the whole state, client table
    /// included, for a replica that fell behind a log this one has
    /// compacted.
    type Chunk: Clone + Debug;

    /// Executes `input`, committed as `op_number`, and returns its result.
    fn apply(&mut self, op_number: OpNumber, input: &Self::Input) -> Self::Output;

    /// Answers `query` from the state after every operation applied so
    /// far.
    fn query(&self, query: &Self::Query) -> Self::Output;

    /// Keeps `record` as the client table's record of `client_id`, or
    /// removes it if `None`, as of `op_number`. The replica calls it for
    /// every entry it executes, with the record of the entry's client after
    /// it, so every op number reaches the state machine, and first for a
    /// client an entry's registration evicts.
    fn record_client(
        &mut self,
        op_number: OpNumber,
        client_id: ClientID,
        record: Option<&ClientRecord<Self::Output>>,
    );

    /// The client table, as the records handed to `record_client` or a
    /// restored checkpoint left it.
    fn client_table(&self) -> Vec<ClientRecord<Self::Output>>;

    /// Keeps a copy of the state as of an op it has applied, one no earlier
    /// than the replica's log start, and returns that op number. It is
    /// kept until [`StateMachine::release_checkpoint`] for replicas that
    /// fell behind, which fetch it in chunks.
    fn checkpoint(&mut self) -> OpNumber;

    /// Chunk `index` of the checkpoint kept, and whether it is the last.
    /// Replicas ask for the chunks in order from 0. The chunks depend only
    /// on the state, so a checkpoint kept again at the same op splits the
    /// same way.
    fn checkpoint_chunk(&self, index: usize) -> (Self::Chunk, bool);

    /// Drops the checkpoint kept.
    fn release_checkpoint(&mut self);

    /// Stages chunk `index` of another replica's checkpoint at `op_number`,
    /// the chunks coming in order: chunk 0 drops what was staged before.
    fn stage_chunk(&mut self, op_number: OpNumber, index: usize, chunk: Self::Chunk);

    /// Replaces the state and the client table with the checkpoint staged
    /// at `op_number`, all of whose chunks are staged. A state machine that
    /// persists its state makes the checkpoint durable before it returns,
    /// together with its op number: the replica's log now starts at the
    /// checkpoint, so [`Replica::restart`] has nothing to apply before it.
    fn restore(&mut self, op_number: OpNumber);
}

/// Configuration.
#[derive(Clone, Debug)]
pub struct Config {
    /// IDs of all replicas, which are their indexes.
    replicas: Vec<ReplicaID>,
    /// Idle periods a backup waits without hearing from the primary before
    /// it starts a view change, and a view change may take before the next
    /// one starts.
    primary_timeout: usize,
    /// The most sessions the client table holds. Registering one more
    /// evicts the session whose latest entry executed earliest.
    clients_max: usize,
    /// The most requests a client has in flight, and the replies the
    /// client table keeps per session to answer re-sends.
    in_flight_max: usize,
}

impl Config {
    pub fn new() -> Config {
        Config {
            replicas: Vec::new(),
            primary_timeout: 3,
            clients_max: 4096,
            in_flight_max: 16,
        }
    }

    pub fn replicas(&self) -> &[ReplicaID] {
        &self.replicas
    }

    pub fn primary_id(&self, view_number: ViewNumber) -> ReplicaID {
        view_number % self.replicas.len()
    }

    pub fn add_replica(&mut self) -> ReplicaID {
        let id = self.replicas.len();
        self.replicas.push(id);
        id
    }

    pub fn quorum(&self) -> usize {
        self.replicas.len() / 2 + 1
    }

    pub fn primary_timeout(&self) -> usize {
        self.primary_timeout
    }

    pub fn set_primary_timeout(&mut self, idle_periods: usize) {
        assert!(idle_periods >= 1);
        self.primary_timeout = idle_periods;
    }

    pub fn clients_max(&self) -> usize {
        self.clients_max
    }

    pub fn set_clients_max(&mut self, clients: usize) {
        assert!(clients >= 1);
        self.clients_max = clients;
    }

    pub fn in_flight_max(&self) -> usize {
        self.in_flight_max
    }

    pub fn set_in_flight_max(&mut self, requests: usize) {
        assert!(requests >= 1);
        self.in_flight_max = requests;
    }
}

impl Default for Config {
    fn default() -> Config {
        Config::new()
    }
}

/// A log entry: a client's registration, or a request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LogEntry<Op> {
    /// Opens a session for the client, numbered with this entry's op
    /// number, unless the client has one.
    Register { client_id: ClientID },
    /// A request of the client's session `session`, executed only if the
    /// session is still in the client table.
    Request {
        client_id: ClientID,
        session: OpNumber,
        request_number: RequestNumber,
        /// The client has the replies to its session's requests up to this
        /// one, and re-sends none of them: the client table drops their
        /// results.
        answered: RequestNumber,
        op: Op,
    },
}

impl<Op> LogEntry<Op> {
    pub fn client_id(&self) -> ClientID {
        match self {
            LogEntry::Register { client_id } | LogEntry::Request { client_id, .. } => *client_id,
        }
    }
}

/// The primary's reply to a client.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Reply<Output> {
    /// The client's registration executed, and its session is `session`:
    /// that of the registration, or the one the client had already.
    Registered {
        view_number: ViewNumber,
        client_id: ClientID,
        session: OpNumber,
    },
    /// The request executed, with `result`.
    Executed {
        view_number: ViewNumber,
        client_id: ClientID,
        session: OpNumber,
        request_number: RequestNumber,
        result: Output,
    },
    /// The session is no longer in the client table. Each of the client's
    /// requests without a reply, this one included, may or may not have
    /// executed.
    Evicted {
        view_number: ViewNumber,
        client_id: ClientID,
        session: OpNumber,
    },
    /// The query executed, with `result`.
    Queried {
        view_number: ViewNumber,
        client_id: ClientID,
        query_number: QueryNumber,
        result: Output,
    },
}

impl<Output> Reply<Output> {
    pub fn client_id(&self) -> ClientID {
        match self {
            Reply::Registered { client_id, .. }
            | Reply::Executed { client_id, .. }
            | Reply::Evicted { client_id, .. }
            | Reply::Queried { client_id, .. } => *client_id,
        }
    }

    pub fn view_number(&self) -> ViewNumber {
        match self {
            Reply::Registered { view_number, .. }
            | Reply::Executed { view_number, .. }
            | Reply::Evicted { view_number, .. }
            | Reply::Queried { view_number, .. } => *view_number,
        }
    }
}

/// What the client table keeps of a session: its latest executed request,
/// the results of the last requests, so that a re-sent one is answered
/// without running it again, and when it last executed an entry, which
/// orders evictions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientRecord<Output> {
    pub client_id: ClientID,
    /// The op number of the registration that opened the session.
    pub session: OpNumber,
    pub request_number: RequestNumber,
    /// The results of the requests after the latest one's `answered`,
    /// oldest first, the last that of `request_number`: at least one, at
    /// most `Config::in_flight_max`.
    pub replies: VecDeque<Output>,
    /// The op number of the session's latest executed entry.
    pub op_number: OpNumber,
}

/// What a stretch of log starts from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogBase {
    /// The op number the entries start after. The receiver holds the
    /// entries up to it.
    Op(OpNumber),
    /// The op number of a checkpoint the sender keeps, which the entries
    /// follow. Sent when the sender has compacted entries the receiver
    /// needs, which fetches the checkpoint in chunks.
    Checkpoint(OpNumber),
}

impl LogBase {
    /// The op number the entries start after.
    pub fn start(&self) -> OpNumber {
        match self {
            LogBase::Op(op_number) | LogBase::Checkpoint(op_number) => *op_number,
        }
    }
}

/// A stretch of a replica's log: the entries after `base`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogSegment<Op> {
    pub base: LogBase,
    pub entries: Vec<LogEntry<Op>>,
}

impl<Op> LogSegment<Op> {
    /// The op number the entries start after.
    pub fn start(&self) -> OpNumber {
        self.base.start()
    }

    /// The op number of the last entry.
    pub fn end(&self) -> OpNumber {
        self.start() + self.entries.len()
    }
}

/// What the owner has persisted of a replica, which it hands back to
/// [`Replica::restart`]: the view state and the log. The client table and
/// the number of applied operations belong to the state machine, which
/// keeps them with its own durable state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PersistentState<Op> {
    pub view_number: ViewNumber,
    /// The latest view in which the replica's status was normal, which
    /// ranks its log in a view change.
    pub last_normal_view: ViewNumber,
    pub commit_number: CommitID,
    /// The op number of the last entry compacted away; `log` holds the
    /// entries after it.
    pub log_start: OpNumber,
    pub log: Vec<LogEntry<Op>>,
    /// Whether the replica was still recovering from a disk loss. Such a
    /// replica holds nothing it may act on, and must recover again.
    pub recovering: bool,
}

impl<Op: Clone> PersistentState<Op> {
    /// The state of a replica that has done nothing yet.
    pub fn empty() -> PersistentState<Op> {
        PersistentState {
            view_number: 0,
            last_normal_view: 0,
            commit_number: 0,
            log_start: 0,
            log: Vec::new(),
            recovering: false,
        }
    }

    /// Brings this copy of the persisted state up to date with `write`,
    /// for an owner that keeps the state in memory: the compacted prefix
    /// goes first, then the entries from [`LogWrite::entries_from`] on,
    /// then the counters.
    pub fn apply(&mut self, write: &LogWrite<Op>) {
        if write.log_start > self.log_start {
            let dropped = (write.log_start - self.log_start).min(self.log.len());
            self.log.drain(..dropped);
            self.log_start = write.log_start;
        }
        let end = self.log_start + self.log.len();
        assert!(
            (self.log_start + 1..=end + 1).contains(&write.entries_from),
            "write from op {} to a copy that holds ops {} to {end}",
            write.entries_from,
            self.log_start + 1,
        );
        let keep = write.entries_from - self.log_start - 1;
        self.log.truncate(keep);
        self.log.extend_from_slice(&write.entries);
        self.view_number = write.view_number;
        self.last_normal_view = write.last_normal_view;
        self.commit_number = write.commit_number;
        self.recovering = write.recovering;
    }
}

/// What the steps since the last write changed, for the owner to persist
/// before it hands the write back with [`Replica::persisted`]: the whole
/// view state, and the log entries that changed.
#[must_use = "the replica releases nothing until the write is handed back with `persisted`"]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogWrite<Op> {
    pub view_number: ViewNumber,
    pub last_normal_view: ViewNumber,
    pub recovering: bool,
    pub commit_number: CommitID,
    /// The op number of the last entry compacted away: the owner drops
    /// what it holds up to it.
    pub log_start: OpNumber,
    /// The op number of the first of `entries`: the owner drops what it
    /// holds from here on and appends `entries`. It is past the last entry
    /// when no entry changed.
    pub entries_from: OpNumber,
    pub entries: Vec<LogEntry<Op>>,
    /// Whether the write must be durable before [`Replica::persisted`]. It
    /// need not be when nothing but the commit number changed: the owner
    /// may then skip the disk, or write without waiting, and hands the
    /// write back either way.
    pub sync: bool,
    /// The op number of the last log entry, as of the write.
    op_number: OpNumber,
    /// The write's position among the replica's writes.
    sequence: u64,
}

impl<Op> LogWrite<Op> {
    /// The op number of the last log entry, as of the write.
    pub fn op_number(&self) -> OpNumber {
        self.op_number
    }
}

/// What the replica recorded of the write it has out.
#[derive(Clone, Copy, Debug)]
struct Outstanding {
    sequence: u64,
    /// The op number of the last entry the write holds.
    op_number: OpNumber,
    /// The view the replica was last normal in, as of the write.
    last_normal_view: ViewNumber,
}

/// What a write records besides the log, to tell what changed since.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Written {
    view_number: ViewNumber,
    last_normal_view: ViewNumber,
    recovering: bool,
    log_start: OpNumber,
}

/// A protocol message between replicas, or from a client to a replica.
///
/// Replies from the primary to a client are a [`Reply`], not a message.
/// Every message from one replica to another carries the sender's view
/// number, and a replica only acts on normal-case messages whose view
/// matches its own: a sender that is behind is ignored, one that is ahead
/// makes the replica catch up first. The variants follow the sections of
/// the paper: normal operation, state transfer, view changes, recovery.
///
/// The type is generic over the state machine's input, query, and
/// checkpoint chunk; [`MessageFor`] names it from the state machine.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Message<Op, Query, Chunk> {
    /// A client asks the primary for a session. Backups ignore it. A client
    /// that has one is answered with it.
    Register { client_id: ClientID },
    /// A client asks the primary to execute `op` in its session. Backups
    /// ignore it. The primary appends only the session's next request
    /// number, and answers a re-send of an executed request after
    /// `answered` from the client table.
    Request {
        client_id: ClientID,
        session: OpNumber,
        request_number: RequestNumber,
        /// See [`LogEntry::Request`].
        answered: RequestNumber,
        op: Op,
    },
    /// A client asks the primary to answer `query`. Backups ignore it. The
    /// primary answers it once a quorum has confirmed its view since the
    /// query arrived, from a state that holds every op that may have
    /// completed by then.
    Query {
        client_id: ClientID,
        query_number: QueryNumber,
        query: Query,
    },
    /// The primary asks the backups whether they are still in its view, to
    /// answer the queries that arrived before `round` started.
    ConfirmView { view_number: ViewNumber, round: u64 },
    /// A backup tells the primary that it was in the primary's view when
    /// `round` reached it: it had not moved on to a later view.
    ConfirmViewOk {
        view_number: ViewNumber,
        round: u64,
        replica_id: ReplicaID,
    },
    /// The primary replicates the entry it appended as `op_number` to the
    /// backups, and tells them how far it has committed so they can commit
    /// too. Backups accept it only in order: a gap means state transfer
    /// first. A `Prepare` for an op a backup already has is a re-send, and
    /// is acknowledged again.
    Prepare {
        view_number: ViewNumber,
        op_number: OpNumber,
        entry: LogEntry<Op>,
        /// The primary's commit number.
        commit_number: CommitID,
    },
    /// A backup tells the primary that it holds every op up to `op_number`.
    /// With a quorum of these for an op, counting itself and each backup
    /// once, the primary commits that op and everything before it.
    PrepareOk {
        view_number: ViewNumber,
        op_number: OpNumber,
        /// The backup sending the acknowledgement.
        replica_id: ReplicaID,
    },
    /// The primary's heartbeat while there are no requests: it carries the
    /// commit number so backups can commit, and its absence is how backups
    /// notice the primary is gone.
    Commit {
        view_number: ViewNumber,
        commit_number: CommitID,
    },
    /// A replica that is missing log entries asks for the entries after
    /// `op_number`. Within a view a backup asks the primary for everything
    /// after its own log; a replica catching up with a newer view asks its
    /// primary from its commit number, since its uncommitted suffix may not
    /// have survived the view change; and the primary of a view being
    /// started asks the replica whose log it chose for what that replica
    /// has compacted.
    GetState {
        replica_id: ReplicaID,
        view_number: ViewNumber,
        op_number: OpNumber,
    },
    /// The answer to `GetState`: the log after the requested op number, or
    /// a checkpoint and the log after it when the requested entries have
    /// been compacted. The paper sends only the op number of the last entry;
    /// the first is included too, so the receiver can tell a late reply to
    /// an earlier request from the one it is waiting for.
    NewState {
        /// The sender, which keeps the checkpoint the segment follows, if
        /// any.
        replica_id: ReplicaID,
        view_number: ViewNumber,
        segment: LogSegment<Op>,
        /// The sender's commit number.
        commit_number: CommitID,
    },
    /// Replica `replica_id` asks the replica that announced a checkpoint
    /// at `op_number` for its chunk `index`.
    GetChunk {
        replica_id: ReplicaID,
        op_number: OpNumber,
        index: usize,
    },
    /// Replica `replica_id`, which keeps the checkpoint at `op_number`,
    /// answers `GetChunk` with chunk `index`, and whether it is the last.
    NewChunk {
        replica_id: ReplicaID,
        op_number: OpNumber,
        index: usize,
        chunk: Chunk,
        last: bool,
    },
    /// A replica that suspects the primary has failed asks the others to
    /// move to `view_number`. A replica that receives one for a view ahead
    /// of its own adopts that view and sends its own.
    StartViewChange {
        view_number: ViewNumber,
        replica_id: ReplicaID,
    },
    /// A replica that has `StartViewChange` for `view_number` from f other
    /// replicas sends its state to the new view's primary. With a quorum of
    /// these the primary starts the view from the log with the latest
    /// `last_normal_view`, the longest if several.
    DoViewChange {
        view_number: ViewNumber,
        replica_id: ReplicaID,
        /// The latest view in which the sender's status was normal.
        last_normal_view: ViewNumber,
        /// The sender's log, after what it has compacted.
        segment: LogSegment<Op>,
        commit_number: CommitID,
    },
    /// The new primary starts `view_number` with the log it chose. Backups
    /// replace their log with it, commit up to `commit_number`, and
    /// acknowledge the rest. A backup that has committed less than the
    /// segment starts after fetches the primary's checkpoint first.
    StartView {
        view_number: ViewNumber,
        /// The primary's log, after what it has compacted.
        segment: LogSegment<Op>,
        commit_number: CommitID,
    },
    /// A replica back from a crash with no memory asks the others for the
    /// current state. The nonce tells this recovery's responses apart from
    /// an earlier one's.
    Recovery {
        replica_id: ReplicaID,
        nonce: u64,
        /// The view the replica persisted before the crash. A replica
        /// behind it takes this as the request for that view change, which
        /// the crashed replica may have started and forgotten.
        view_number: ViewNumber,
    },
    /// A replica in normal status answers `Recovery` with its view; the
    /// primary of that view also sends its state. The recovering replica
    /// needs a quorum of these, including one from the primary of the
    /// latest view among them, and that view must be at least the one it
    /// persisted.
    RecoveryResponse {
        view_number: ViewNumber,
        /// The nonce from the `Recovery` this answers.
        nonce: u64,
        replica_id: ReplicaID,
        /// The sender's state, if it is the primary.
        state: Option<RecoveryState<Op>>,
    },
}

/// [`Message`] for a given state machine.
pub type MessageFor<SM> =
    Message<<SM as StateMachine>::Input, <SM as StateMachine>::Query, <SM as StateMachine>::Chunk>;

/// The primary's state in a `RecoveryResponse`: its whole log, with a
/// checkpoint standing in for what it has compacted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoveryState<Op> {
    pub segment: LogSegment<Op>,
    pub commit_number: CommitID,
}

/// Client.
///
/// A client registers for a session with its first request, then keeps up
/// to `Config::in_flight_max` requests in flight: it sends request `n` once
/// every request up to `n - in_flight_max` has its reply. Queries need no
/// session, and go in flight the same way. The client keeps its operations
/// in the order it issued them: it sends a query only once every earlier
/// request has its reply, and a request only once every earlier query has
/// its reply, so a run of requests or of queries goes out together. It
/// queues the rest. Its owner delivers what [`Client::drain`] yields, feeds
/// replies to [`Client::on_reply`], and calls [`Client::on_idle`] at a
/// regular interval so that what goes a whole interval without a reply is
/// re-sent.
#[derive(Debug)]
pub struct Client<Op, Query> {
    config: Config,
    client_id: ClientID,
    /// The latest view this client has heard of, which tells it who the
    /// primary is.
    view_number: ViewNumber,
    /// The session, once the registration is answered.
    session: Option<OpNumber>,
    /// The idle period in which the client last asked for a session, while
    /// it awaits one.
    registering: Option<u64>,
    /// The last session evicted. A registration answered with it or an
    /// earlier one is a late reply to an earlier registration.
    evicted: OpNumber,
    next_request_number: RequestNumber,
    /// The number the next query gets, which never repeats: a late reply
    /// to an earlier query must not answer a later one.
    next_query_number: QueryNumber,
    /// Requests sent, from the oldest without a reply on. Their numbers
    /// follow each other.
    requests: VecDeque<InFlight<Op>>,
    /// Queries sent, from the oldest without a reply on. Their numbers
    /// follow each other.
    queries: VecDeque<InFlight<Query>>,
    /// The number of idle periods so far.
    idle_periods: u64,
    /// Operations waiting for the session, for room in flight, or for the
    /// replies to earlier operations of the other kind, in order.
    queued: VecDeque<Queued<Op, Query>>,
    /// Messages to send, with the replica each one goes to.
    outbox: Vec<(ReplicaID, Outgoing<Op, Query>)>,
}

/// A request or query a client sent.
#[derive(Debug)]
struct InFlight<T> {
    number: usize,
    /// The request's op or the query, kept to re-send until the reply
    /// comes.
    item: Option<T>,
    /// The idle period in which it was last sent.
    sent: u64,
}

/// An operation a client queued.
#[derive(Debug)]
enum Queued<Op, Query> {
    Request(RequestNumber, Op),
    Query(QueryNumber, Query),
}

/// A message a client sends.
#[derive(Clone, Debug)]
enum Outgoing<Op, Query> {
    Register,
    Request {
        session: OpNumber,
        request_number: RequestNumber,
        answered: RequestNumber,
        op: Op,
    },
    Query {
        query_number: QueryNumber,
        query: Query,
    },
}

/// What a reply completed, see [`Client::on_reply`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Completion<Output> {
    /// The request executed with this result.
    Executed(RequestNumber, Output),
    /// The query executed with this result.
    Queried(QueryNumber, Output),
    /// The session was evicted. Every request and query the client had no
    /// reply to failed, and each request may or may not have executed. The
    /// client registers again with its next request, and numbers requests
    /// from 1 again. It numbers queries on, since a reply to a query names
    /// no session.
    Evicted,
}

impl<Op: Clone + Debug, Query: Clone + Debug> Client<Op, Query> {
    pub fn new(client_id: ClientID, config: Config) -> Client<Op, Query> {
        assert!(
            config.replicas().len() >= 3,
            "a cluster needs at least three replicas"
        );
        Client {
            config,
            client_id,
            view_number: 0,
            session: None,
            registering: None,
            evicted: 0,
            next_request_number: 1,
            next_query_number: 1,
            requests: VecDeque::new(),
            queries: VecDeque::new(),
            idle_periods: 0,
            queued: VecDeque::new(),
            outbox: Vec::new(),
        }
    }

    pub fn client_id(&self) -> ClientID {
        self.client_id
    }

    pub fn view_number(&self) -> ViewNumber {
        self.view_number
    }

    /// The client's session, once registered.
    pub fn session(&self) -> Option<OpNumber> {
        self.session
    }

    /// Sends `op` to the primary, or queues it until the client has a
    /// session, room in flight, and the replies to the queries issued
    /// before it. Returns the request number it was given.
    pub fn on_request(&mut self, op: Op) -> RequestNumber {
        trace!("Client {} <- {:?}", self.client_id, op);
        let request_number = self.next_request_number;
        self.next_request_number += 1;
        match self.session {
            Some(session) if self.queued.is_empty() && self.request_may_go(request_number) => {
                self.send_request(session, request_number, op);
            }
            _ => {
                self.queued.push_back(Queued::Request(request_number, op));
                self.register();
            }
        }
        request_number
    }

    /// Sends `query` to the primary, or queues it until the client has room
    /// in flight and the replies to the requests issued before it. Returns
    /// the query number it was given.
    pub fn on_query(&mut self, query: Query) -> QueryNumber {
        trace!("Client {} <- {:?}", self.client_id, query);
        let query_number = self.next_query_number;
        self.next_query_number += 1;
        if self.queued.is_empty() && self.query_may_go(query_number) {
            self.send_query(query_number, query);
        } else {
            self.queued.push_back(Queued::Query(query_number, query));
        }
        query_number
    }

    /// Asks for a session, unless the client has one or has asked. The
    /// first request does too.
    pub fn register(&mut self) {
        if self.session.is_none() && self.registering.is_none() {
            self.registering = Some(self.idle_periods);
            let primary_id = self.config.primary_id(self.view_number);
            self.outbox.push((primary_id, Outgoing::Register));
        }
    }

    /// Sends the queued operations that may go, in order. The front of the
    /// queue waits between calls, so only a reply or a session lets any go.
    fn send_queued(&mut self) {
        loop {
            match self.queued.front() {
                Some(Queued::Request(request_number, _)) => {
                    let Some(session) = self.session else {
                        return;
                    };
                    if !self.request_may_go(*request_number) {
                        return;
                    }
                    let Some(Queued::Request(request_number, op)) = self.queued.pop_front() else {
                        unreachable!("a request is queued");
                    };
                    self.send_request(session, request_number, op);
                }
                Some(Queued::Query(query_number, _)) => {
                    if !self.query_may_go(*query_number) {
                        return;
                    }
                    let Some(Queued::Query(query_number, query)) = self.queued.pop_front() else {
                        unreachable!("a query is queued");
                    };
                    self.send_query(query_number, query);
                }
                None => return,
            }
        }
    }

    fn request_may_go(&self, request_number: RequestNumber) -> bool {
        let oldest = self
            .requests
            .front()
            .map_or(request_number, |sent| sent.number);
        self.queries.is_empty() && request_number < oldest + self.config.in_flight_max()
    }

    fn query_may_go(&self, query_number: QueryNumber) -> bool {
        let oldest = self
            .queries
            .front()
            .map_or(query_number, |sent| sent.number);
        self.requests.is_empty() && query_number < oldest + self.config.in_flight_max()
    }

    /// Sends a request to the primary, and keeps its op to re-send until
    /// the reply comes.
    fn send_request(&mut self, session: OpNumber, request_number: RequestNumber, op: Op) {
        let oldest = self
            .requests
            .front()
            .map_or(request_number, |sent| sent.number);
        self.requests.push_back(InFlight {
            number: request_number,
            item: Some(op.clone()),
            sent: self.idle_periods,
        });
        let outgoing = Outgoing::Request {
            session,
            request_number,
            answered: oldest - 1,
            op,
        };
        let primary_id = self.config.primary_id(self.view_number);
        self.outbox.push((primary_id, outgoing));
    }

    /// Sends a query to the primary, and keeps it to re-send until the
    /// reply comes.
    fn send_query(&mut self, query_number: QueryNumber, query: Query) {
        self.queries.push_back(InFlight {
            number: query_number,
            item: Some(query.clone()),
            sent: self.idle_periods,
        });
        let outgoing = Outgoing::Query {
            query_number,
            query,
        };
        let primary_id = self.config.primary_id(self.view_number);
        self.outbox.push((primary_id, outgoing));
    }

    /// Handles a reply to this client. Every reply tells the client the
    /// current view, and with it the primary to send to. Returns what the
    /// reply completed; a duplicate, or a reply for an earlier session,
    /// completes nothing.
    pub fn on_reply<Output>(&mut self, reply: Reply<Output>) -> Option<Completion<Output>> {
        if reply.client_id() != self.client_id {
            return None;
        }
        self.view_number = self.view_number.max(reply.view_number());
        let completion = match reply {
            Reply::Registered { session, .. } => {
                if self.registering.is_some() && session > self.evicted {
                    self.registering = None;
                    self.session = Some(session);
                    self.send_queued();
                }
                return None;
            }
            Reply::Executed {
                session,
                request_number,
                result,
                ..
            } => {
                if self.session != Some(session) || !answer(&mut self.requests, request_number) {
                    return None;
                }
                Completion::Executed(request_number, result)
            }
            Reply::Queried {
                query_number,
                result,
                ..
            } => {
                if !answer(&mut self.queries, query_number) {
                    return None;
                }
                Completion::Queried(query_number, result)
            }
            Reply::Evicted { session, .. } => {
                if self.session != Some(session) {
                    return None;
                }
                trace!("Client {} lost session {session}", self.client_id);
                self.evicted = session;
                self.session = None;
                self.next_request_number = 1;
                self.requests.clear();
                self.queries.clear();
                self.queued.clear();
                self.outbox
                    .retain(|(_, outgoing)| matches!(outgoing, Outgoing::Register));
                return Some(Completion::Evicted);
            }
        };
        self.send_queued();
        Some(completion)
    }

    /// Called at a regular interval. Re-sends the registration, and the
    /// requests and queries that have gone a whole interval without a
    /// reply, to every replica: the primary may have changed without this
    /// client knowing, and backups ignore clients.
    pub fn on_idle(&mut self) {
        self.idle_periods += 1;
        let now = self.idle_periods;
        let due = |sent: u64| sent + 1 < now;
        let replica_count = self.config.replicas().len();
        let mut resend = Vec::new();
        if self.registering.is_some_and(due) {
            self.registering = Some(now);
            resend.push(Outgoing::Register);
        }
        if let (Some(session), Some(oldest)) = (self.session, self.requests.front()) {
            let answered = oldest.number - 1;
            for sent in self.requests.iter_mut().filter(|sent| due(sent.sent)) {
                let Some(op) = &sent.item else {
                    continue;
                };
                sent.sent = now;
                resend.push(Outgoing::Request {
                    session,
                    request_number: sent.number,
                    answered,
                    op: op.clone(),
                });
            }
        }
        for sent in self.queries.iter_mut().filter(|sent| due(sent.sent)) {
            let Some(query) = &sent.item else {
                continue;
            };
            sent.sent = now;
            resend.push(Outgoing::Query {
                query_number: sent.number,
                query: query.clone(),
            });
        }
        for outgoing in resend {
            for replica_id in 0..replica_count {
                self.outbox.push((replica_id, outgoing.clone()));
            }
        }
    }

    /// Messages to send, with the replica each one goes to. A client sends
    /// no checkpoint chunk, so that type parameter is whatever the
    /// receiving replicas use.
    pub fn drain<Chunk>(
        &mut self,
    ) -> impl Iterator<Item = (ReplicaID, Message<Op, Query, Chunk>)> + '_ {
        let client_id = self.client_id;
        self.outbox.drain(..).map(move |(replica_id, outgoing)| {
            let message = match outgoing {
                Outgoing::Register => Message::Register { client_id },
                Outgoing::Request {
                    session,
                    request_number,
                    answered,
                    op,
                } => Message::Request {
                    client_id,
                    session,
                    request_number,
                    answered,
                    op,
                },
                Outgoing::Query {
                    query_number,
                    query,
                } => Message::Query {
                    client_id,
                    query_number,
                    query,
                },
            };
            (replica_id, message)
        })
    }
}

/// Takes the item numbered `number` from the requests or queries a client
/// has in flight, and drops those answered from the front. Returns whether
/// it was in flight without a reply.
fn answer<T>(in_flight: &mut VecDeque<InFlight<T>>, number: usize) -> bool {
    let Some(oldest) = in_flight.front().map(|sent| sent.number) else {
        return false;
    };
    let answered = number
        .checked_sub(oldest)
        .and_then(|index| in_flight.get_mut(index))
        .and_then(|sent| sent.item.take());
    while in_flight.front().is_some_and(|sent| sent.item.is_none()) {
        in_flight.pop_front();
    }
    answered.is_some()
}

/// What a replica is doing. See [`Replica::status`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Status {
    /// Taking part in normal operation.
    Normal,
    /// Waiting for `NewState` to fill a gap in the log, within the current
    /// view.
    StateTransfer,
    /// Back from a crash with no memory, and waiting for `RecoveryResponse`
    /// from a quorum before taking part in anything.
    Recovering,
    /// Taking part in a view change, or, once the new view is known to have
    /// started, waiting for `NewState` to catch up with it.
    ViewChange,
}

/// The most rounds of view confirmations the primary has out at a time,
/// which bounds their messages to that many per round trip, however many
/// queries arrive.
const ROUNDS_OUT: u64 = 32;

/// A checkpoint a replica fetches, one chunk at a time.
#[derive(Debug)]
struct Fetch<Op> {
    /// The replica that keeps it.
    from: ReplicaID,
    /// The chunk to ask for next.
    index: usize,
    /// Idle periods since the fetch started or the last chunk arrived.
    idle_periods: usize,
    /// Whether it waited too long for a chunk: the replica asks for state
    /// again, and goes on with this fetch if the answer names the same
    /// checkpoint.
    stalled: bool,
    /// The message that named the checkpoint, handled again once it is
    /// restored.
    then: Resume<Op>,
}

/// A message that named a checkpoint to fetch.
#[derive(Debug)]
enum Resume<Op> {
    NewState {
        view_number: ViewNumber,
        segment: LogSegment<Op>,
        commit_number: CommitID,
    },
    RecoveryResponse {
        view_number: ViewNumber,
        state: RecoveryState<Op>,
    },
}

impl<Op> Resume<Op> {
    /// The op number of the checkpoint the message named.
    fn checkpoint(&self) -> OpNumber {
        match self {
            Resume::NewState { segment, .. } => segment.start(),
            Resume::RecoveryResponse { state, .. } => state.segment.start(),
        }
    }
}

/// What installing a segment came to.
enum Installed<Op> {
    /// The segment extends or replaces the log.
    Log,
    /// The segment is of no use.
    Nothing,
    /// The segment follows a checkpoint that must be fetched first.
    Checkpoint(LogSegment<Op>),
}

/// A query waiting on the primary for its round and its op.
#[derive(Debug)]
struct WaitingQuery<Query> {
    client_id: ClientID,
    query_number: QueryNumber,
    query: Query,
    /// The last op that may have completed when the query arrived.
    op_number: OpNumber,
    /// The round of view confirmations it waits for, the first started
    /// after it arrived.
    round: u64,
}

/// What a replica knows of a client.
#[derive(Debug)]
struct ClientState<Output> {
    /// The client's session in the client table, from its registration's
    /// execution until its eviction.
    record: Option<ClientRecord<Output>>,
    /// The session and request number of the client's latest entry in the
    /// log after `applied`, request number 0 for a registration, whose
    /// session is its op number. A re-sent request or registration still in
    /// progress is not appended again.
    pending: Option<(OpNumber, RequestNumber)>,
}

impl<Output> Default for ClientState<Output> {
    fn default() -> ClientState<Output> {
        ClientState {
            record: None,
            pending: None,
        }
    }
}

/// What a replica reported in a `DoViewChange` message: the view it was
/// last normal in, its log, and its commit number.
#[derive(Debug)]
struct DoViewChange<Op> {
    last_normal_view: ViewNumber,
    segment: LogSegment<Op>,
    commit_number: CommitID,
}

type DoViewChangeFor<SM> = DoViewChange<<SM as StateMachine>::Input>;

/// What a recovering replica keeps of a `RecoveryResponse`: the sender's
/// view, and its state if it was the primary of that view.
#[derive(Debug)]
struct RecoveryResponse<Op> {
    view_number: ViewNumber,
    state: Option<RecoveryState<Op>>,
}

type RecoveryResponseFor<SM> = RecoveryResponse<<SM as StateMachine>::Input>;

/// Messages to send, with the replica each one goes to.
type Outbox<SM> = Vec<(ReplicaID, MessageFor<SM>)>;

/// Replica.
///
/// The owner feeds it messages with [`Replica::on_message`], calls
/// [`Replica::on_idle`] at a regular interval, and after each of those
/// persists what [`Replica::take_write`] returns, hands it back with
/// [`Replica::persisted`], and delivers what [`Replica::drain_messages`]
/// and [`Replica::drain_replies`] yield. See the module documentation.
#[derive(Debug)]
pub struct Replica<SM: StateMachine> {
    config: Config,
    self_id: ReplicaID,
    state_machine: SM,
    status: Status,
    view_number: ViewNumber,
    /// The latest view in which this replica's status was normal.
    last_normal_view: ViewNumber,
    commit_number: CommitID,
    /// The number of ops the state machine has executed, at most the
    /// commit number. Committed ops execute once a handed-back write holds
    /// them, see `durable`.
    applied: OpNumber,
    /// The committed ops still to be replied to, as inclusive ranges of op
    /// numbers: those this replica committed as the primary.
    reply_ranges: Vec<(OpNumber, OpNumber)>,
    /// The op number of the last entry compacted away. `log` holds the
    /// entries after it, so entry `i` of `log` is op `log_start + i + 1`.
    log_start: OpNumber,
    log: Vec<LogEntry<SM::Input>>,
    /// The lowest op number whose entry changed since the last write was
    /// taken.
    log_changed_from: Option<OpNumber>,
    /// What the last write recorded besides the log, or `None` before the
    /// first write of a replica that is new or recovering.
    written: Option<Written>,
    /// The number of writes taken, and the one out, if any.
    writes: u64,
    outstanding: Option<Outstanding>,
    /// The op number up to which the handed-back writes hold the log as
    /// it was when the last one was taken. Committed ops up to it, less
    /// any entry changed since, execute as soon as they commit.
    durable: OpNumber,
    /// The highest op number each replica has acknowledged in the current
    /// view, by replica id, this one included once its write is persisted.
    /// The primary commits up to the highest op number a quorum has
    /// acknowledged.
    acked: Vec<OpNumber>,
    /// The primary's op number as of its last idle period, or its commit
    /// number as the view started. The ops up to it have had a whole idle
    /// period to be acknowledged, and the next idle period re-sends to a
    /// backup that has not acknowledged them all.
    resend_up_to: OpNumber,
    /// The op number when the replica last entered normal status. As the
    /// primary, its log holds every op committed in an earlier view up to
    /// it, and it commits every later op itself.
    view_start_op: OpNumber,
    /// The queries waiting to execute on the primary, in the order they
    /// arrived, see `on_query`, and each by client and query number, so
    /// that a re-sent one waits once.
    queries: VecDeque<WaitingQuery<SM::Query>>,
    waiting: HashSet<(ClientID, QueryNumber)>,
    /// The last round of view confirmations the primary started in its
    /// view, the last a quorum has confirmed, and the last each replica
    /// confirmed, by replica id, this one's the last it started.
    round: u64,
    round_confirmed: u64,
    confirmed: Vec<u64>,
    /// Whether the last round's `ConfirmView` waits in the outbox: a query
    /// that arrives before it leaves joins that round.
    round_unsent: bool,
    /// What the replica knows of each client: its session in the client
    /// table, and its latest entry in the log after `applied`. Every entry
    /// looks its client up here, on every replica, so it is a hash map: with
    /// many clients at work, the paths of a tree fall out of the cache. It
    /// and `waiting` take their keys from clients; foldhash seeds each
    /// table, costs far less than SipHash, and resists chosen collisions
    /// only minimally.
    clients: HashMap<ClientID, ClientState<SM::Output>>,
    /// The number of sessions in the client table.
    sessions: usize,
    /// The oldest sessions as of the last scan of the client table, by the
    /// op number of their latest executed entry, oldest first: the first
    /// whose record still holds that op number is the session whose latest
    /// entry executed earliest, since op numbers only grow. A session that
    /// executed an entry since has a later one, and gives way.
    eviction_candidates: VecDeque<(OpNumber, ClientID)>,
    /// Whether the primary has been heard from since the last idle period.
    heard_from_primary: bool,
    /// Consecutive idle periods spent without hearing from the primary, or
    /// waiting for a view change to complete.
    idle_periods_waiting: usize,
    /// View changes entered without a stable stretch of normal status in
    /// between. Each one doubles the wait before the next, so that
    /// replicas whose timers fire faster than a view change can complete
    /// stop cutting each other off. Completing a view does not reset it:
    /// the replica that just did is the one whose timer would otherwise
    /// fire first in the next view change, and start yet another. It
    /// resets once the replica has been normal, hearing from the primary,
    /// for `Config::primary_timeout` idle periods.
    view_change_attempts: u32,
    /// Consecutive idle periods in normal status with the primary heard
    /// from, or as the primary.
    idle_periods_stable: usize,
    /// Replicas that sent `StartViewChange` for the current view.
    start_view_change_from: BTreeSet<ReplicaID>,
    /// Whether this replica has sent `DoViewChange` for the current view.
    do_view_change_sent: bool,
    /// Replicas that sent `DoViewChange` for the current view, with what they
    /// reported, if this replica is to be its primary.
    do_view_change_from: BTreeMap<ReplicaID, DoViewChangeFor<SM>>,
    /// The replica this one has asked for the log from its commit number
    /// on, and is waiting for: the primary of a view that started without
    /// this replica, or, for the primary of a view being started, the
    /// replica whose log it chose but which has compacted part of it.
    catching_up: Option<ReplicaID>,
    /// The checkpoint this replica keeps for replicas that fell behind, by
    /// op number, and the idle periods since one last asked for a chunk of
    /// it. The log keeps the entries after it meanwhile.
    serving: Option<(OpNumber, usize)>,
    /// The checkpoint this replica fetches, if any.
    fetch: Option<Fetch<SM::Input>>,
    /// The nonce of the recovery under way, if any.
    recovery_nonce: u64,
    /// `RecoveryResponse`s received for it, by sender.
    recovery_responses: BTreeMap<ReplicaID, RecoveryResponseFor<SM>>,
    /// Messages that may be sent before the step is persisted, see
    /// [`Replica::drain_messages_before_persist`].
    outbox_early: Outbox<SM>,
    /// Messages that must wait for the next write to be persisted.
    outbox: Outbox<SM>,
    /// Messages that wait for the write that is out.
    outbox_waiting: Outbox<SM>,
    /// Messages whose write is persisted.
    outbox_ready: Outbox<SM>,
    replies: Vec<Reply<SM::Output>>,
}

impl<SM: StateMachine> Replica<SM> {
    pub fn new(self_id: ReplicaID, config: Config, state_machine: SM) -> Replica<SM> {
        let replica_count = config.replicas().len();
        assert!(
            replica_count >= 3,
            "a cluster needs at least three replicas"
        );
        Replica {
            self_id,
            config,
            state_machine,
            status: Status::Normal,
            view_number: 0,
            last_normal_view: 0,
            commit_number: 0,
            applied: 0,
            reply_ranges: Vec::new(),
            log_start: 0,
            log: Vec::new(),
            log_changed_from: None,
            written: None,
            writes: 0,
            outstanding: None,
            durable: 0,
            acked: vec![0; replica_count],
            view_start_op: 0,
            queries: VecDeque::new(),
            waiting: HashSet::default(),
            round: 0,
            round_confirmed: 0,
            confirmed: vec![0; replica_count],
            round_unsent: false,
            resend_up_to: 0,
            clients: HashMap::default(),
            sessions: 0,
            eviction_candidates: VecDeque::new(),
            heard_from_primary: true,
            idle_periods_waiting: 0,
            view_change_attempts: 0,
            idle_periods_stable: 0,
            start_view_change_from: BTreeSet::new(),
            do_view_change_sent: false,
            do_view_change_from: BTreeMap::new(),
            catching_up: None,
            serving: None,
            fetch: None,
            recovery_nonce: 0,
            recovery_responses: BTreeMap::new(),
            outbox_early: Vec::new(),
            outbox: Vec::new(),
            outbox_waiting: Vec::new(),
            outbox_ready: Vec::new(),
            replies: Vec::new(),
        }
    }

    /// Creates a replica that is back from a crash with no memory. It
    /// starts in recovering status, in the view the owner had persisted
    /// for it, and asks the other replicas for the current state; until a
    /// quorum has answered, it takes no part in anything else. The nonce
    /// must differ from that of any earlier recovery of this replica, so a
    /// late response to an earlier one is not taken for a current one.
    pub fn recover(
        self_id: ReplicaID,
        config: Config,
        state_machine: SM,
        view_number: ViewNumber,
        nonce: u64,
    ) -> Replica<SM> {
        let mut replica = Replica::new(self_id, config, state_machine);
        replica.status = Status::Recovering;
        replica.view_number = view_number;
        replica.recovery_nonce = nonce;
        replica.send_recovery();
        replica
    }

    /// Creates a replica that is back from a crash with the state the
    /// owner persisted for it, and a state machine that has applied the
    /// first `applied` operations, with its client table as of then. The
    /// replica applies the committed ones after that once more, so
    /// `applied` must be at least `log_start`. Its first write records
    /// only what the restart changed.
    ///
    /// The state machine must be durable as of `applied`, since the replica
    /// may compact its log up to there and a later crash must not take the
    /// state machine back behind it. A state machine that writes through
    /// the page cache can come back from a process crash with operations
    /// it applied but never made durable; its owner makes them durable
    /// before calling this. So must `state` be: the replica treats all of
    /// it as on disk, and a log replayed after a process crash can hold a
    /// write that reached only the page cache, which its owner syncs.
    ///
    /// A state machine ahead of the log's commit number applied operations
    /// whose commit no write recorded: in steps that changed nothing else,
    /// which the owner need not write, or in a step whose write the crash
    /// cut off. Or it holds a checkpoint it made durable before the log was
    /// persisted after it, as [`StateMachine::restore`] requires. Either
    /// way the log's entries up to `applied` give way to the state machine,
    /// and the ones after it, which this replica acknowledged, stay.
    ///
    /// A backup that was normal in its view resumes there: its log is the
    /// one it acknowledged. A primary starts the next view instead: it may
    /// have sent a `Prepare` for an entry it never made durable, which a
    /// backup then holds under an op number the primary would use again.
    /// A replica that went down in the middle of a view change enters that
    /// view change again. One that was still recovering starts a new
    /// recovery with `nonce`, keeping the checkpoint its state machine may
    /// hold.
    pub fn restart(
        self_id: ReplicaID,
        config: Config,
        state_machine: SM,
        applied: OpNumber,
        state: PersistentState<SM::Input>,
        nonce: u64,
    ) -> Replica<SM> {
        let written = Written {
            view_number: state.view_number,
            last_normal_view: state.last_normal_view,
            recovering: state.recovering,
            log_start: state.log_start,
        };
        let mut replica = if state.recovering {
            let mut replica =
                Replica::recover(self_id, config, state_machine, state.view_number, nonce);
            replica.log_start = applied;
            replica.commit_number = applied;
            replica
        } else {
            let mut replica = Replica::new(self_id, config, state_machine);
            replica.view_number = state.view_number;
            replica.last_normal_view = state.last_normal_view;
            if applied > state.commit_number {
                replica.log_start = applied;
                replica.commit_number = applied;
                replica.log = state.log;
                replica
                    .log
                    .drain(..(applied - state.log_start).min(replica.log.len()));
            } else {
                assert!(
                    state.log_start <= applied,
                    "state machine applied {applied} ops but the log starts after {}",
                    state.log_start
                );
                replica.log_start = state.log_start;
                replica.commit_number = state.commit_number;
                replica.log = state.log;
            }
            replica
        };
        replica.applied = applied;
        replica.written = Some(written);
        replica.load_client_table();
        assert!(replica.commit_number <= replica.op_number());
        replica.durable = replica.op_number();
        replica.apply_committed(replica.durable);
        trace!(
            "Replica {self_id} restarts in view {} with {} ops, {} committed, {applied} applied",
            replica.view_number,
            replica.op_number(),
            replica.commit_number
        );
        replica.rebuild_pending();
        if replica.status == Status::Recovering {
            return replica;
        }
        if state.last_normal_view < state.view_number {
            replica.start_view_change(state.view_number);
        } else if replica.is_primary() {
            replica.start_view_change(state.view_number + 1);
        }
        replica
    }

    /// Takes the client table from the state machine.
    fn load_client_table(&mut self) {
        self.clients.clear();
        self.eviction_candidates.clear();
        let table = self.state_machine.client_table();
        self.sessions = table.len();
        for record in table {
            let client_id = record.client_id;
            let state = ClientState {
                record: Some(record),
                pending: None,
            };
            self.clients.insert(client_id, state);
        }
    }

    /// Everything the owner persists, as of now, see [`PersistentState`].
    pub fn persistent_state(&self) -> PersistentState<SM::Input> {
        PersistentState {
            view_number: self.view_number,
            last_normal_view: self.last_normal_view,
            commit_number: self.commit_number,
            log_start: self.log_start,
            log: self.log.clone(),
            recovering: self.status == Status::Recovering,
        }
    }

    /// Takes the replica's write, has `write_to_disk` persist it, and hands
    /// it back, which releases what waited for it: the persistence step of
    /// an owner that writes before it goes on. `write_to_disk` gets every
    /// write, and makes it durable when [`LogWrite::sync`] says so. If it
    /// fails, the write stays out, and the replica takes no other: the
    /// disk behind it is in doubt.
    pub fn persist<E>(
        &mut self,
        write_to_disk: impl FnOnce(&LogWrite<SM::Input>) -> Result<(), E>,
    ) -> Result<(), E> {
        let write = self.take_write();
        write_to_disk(&write)?;
        self.persisted(write);
        Ok(())
    }

    /// What the steps since the last write changed, for the owner to
    /// persist; see [`LogWrite`]. The messages those steps produced that
    /// must wait for it are released when the owner hands it back with
    /// [`Replica::persisted`]. One write is out at a time; steps may run
    /// before it is handed back, and what they change goes in the next.
    /// The first write of a new or recovering replica always needs a sync,
    /// so that a disk that holds anything holds the replica's view.
    pub fn take_write(&mut self) -> LogWrite<SM::Input> {
        assert!(
            self.outstanding.is_none(),
            "a write is out: hand it back with `persisted` before taking the next"
        );
        let op_number = self.op_number();
        // The entries the write replaces are in doubt until it is back.
        self.durable = self.durable_op();
        let changed = self.log_changed_from.take();
        let entries_from = changed.map_or(op_number + 1, |from| from.max(self.log_start + 1));
        let recovering = self.status == Status::Recovering;
        let sync = changed.is_some()
            || self.written.is_none_or(|written| {
                written.view_number != self.view_number
                    || written.last_normal_view != self.last_normal_view
                    || written.recovering != recovering
                    || written.log_start != self.log_start
            });
        self.written = Some(Written {
            view_number: self.view_number,
            last_normal_view: self.last_normal_view,
            recovering,
            log_start: self.log_start,
        });
        self.writes += 1;
        self.outstanding = Some(Outstanding {
            sequence: self.writes,
            op_number,
            last_normal_view: self.last_normal_view,
        });
        std::mem::swap(&mut self.outbox, &mut self.outbox_waiting);
        LogWrite {
            view_number: self.view_number,
            last_normal_view: self.last_normal_view,
            recovering,
            commit_number: self.commit_number,
            log_start: self.log_start,
            entries_from,
            entries: self.log_from(entries_from).to_vec(),
            sync,
            op_number,
            sequence: self.writes,
        }
    }

    /// Tells the replica that `write`, the one it has out, is durable, or
    /// needed no sync. The primary counts its own acknowledgement for the
    /// entries the write holds, the committed operations among them are
    /// executed, and the messages that waited for it are released to
    /// [`Replica::drain_messages`]. Entries that steps since changed are
    /// left for the next write.
    pub fn persisted(&mut self, write: LogWrite<SM::Input>) {
        let outstanding = self.outstanding.take().expect("no write is out");
        assert_eq!(
            outstanding.sequence, write.sequence,
            "a write handed back other than the one out"
        );
        self.durable = outstanding.op_number;
        // The entries the write holds and memory still has.
        let durable = self.durable_op();
        // Within a view the primary's log only grows, so a write taken
        // while normal in the current view holds a prefix of it.
        if self.status == Status::Normal
            && self.is_primary()
            && outstanding.last_normal_view == self.view_number
        {
            self.register_ack(self.self_id, durable);
        }
        self.apply_committed(durable);
        if self.outbox_ready.is_empty() {
            std::mem::swap(&mut self.outbox_ready, &mut self.outbox_waiting);
        } else {
            self.outbox_ready.append(&mut self.outbox_waiting);
        }
    }

    /// Drops the log entries up to `op_number` from memory. The op number
    /// must be at most what the state machine has made durable, since
    /// [`Replica::restart`] cannot apply an entry that is gone; anything
    /// beyond what the state machine has applied is not compacted, nor
    /// anything after the checkpoint kept for replicas that fell behind,
    /// while it is kept. A replica that needs the dropped entries fetches a
    /// checkpoint instead.
    pub fn compact(&mut self, op_number: OpNumber) {
        let mut op_number = op_number.min(self.applied);
        if let Some((kept, _)) = self.serving {
            op_number = op_number.min(kept);
        }
        if op_number <= self.log_start {
            return;
        }
        self.log.drain(..op_number - self.log_start);
        self.log_start = op_number;
    }

    /// Executes the committed entries up to `op_number` that the state
    /// machine has not yet, in order, and produces the replies for those
    /// this replica committed as the primary. The log must be durable up
    /// to `op_number`.
    fn apply_committed(&mut self, op_number: OpNumber) {
        while self.applied < self.commit_number.min(op_number) {
            let op_number = self.applied + 1;
            let reply = self.execute(op_number);
            self.applied = op_number;
            if self
                .reply_ranges
                .iter()
                .any(|(from, to)| (*from..=*to).contains(&op_number))
            {
                self.replies.push(reply);
            }
        }
        let applied = self.applied;
        self.reply_ranges.retain(|(_, to)| *to > applied);
        if !self.queries.is_empty() {
            self.execute_queries();
        }
    }

    /// Executes the entry at `op_number`, and returns the reply to it. A
    /// request executes only in its session, which holds every earlier
    /// request of the session, since the primary appends them in order.
    fn execute(&mut self, op_number: OpNumber) -> Reply<SM::Output> {
        let index = op_number - self.log_start - 1;
        let (client_id, session, request_number, answered) = match &self.log[index] {
            LogEntry::Register { client_id } => return self.register(op_number, *client_id),
            LogEntry::Request {
                client_id,
                session,
                request_number,
                answered,
                ..
            } => (*client_id, *session, *request_number, *answered),
        };
        let view_number = self.view_number;
        let state = self.clients.entry(client_id).or_default();
        if state.pending == Some((session, request_number)) {
            state.pending = None;
        }
        let in_session = state
            .record
            .as_ref()
            .is_some_and(|record| record.session == session);
        if !in_session {
            self.state_machine
                .record_client(op_number, client_id, state.record.as_ref());
            if state.record.is_none() && state.pending.is_none() {
                self.clients.remove(&client_id);
            }
            return Reply::Evicted {
                view_number,
                client_id,
                session,
            };
        }
        let record = state.record.as_mut().expect("the request's session");
        assert_eq!(
            record.request_number + 1,
            request_number,
            "client {client_id} skipped requests in session {session}"
        );
        let LogEntry::Request { op, .. } = &self.log[index] else {
            unreachable!("the entry is a request");
        };
        let result = self.state_machine.apply(op_number, op);
        record.request_number = request_number;
        record.replies.push_back(result.clone());
        let kept = request_number
            .saturating_sub(answered)
            .clamp(1, self.config.in_flight_max());
        let dropped = record.replies.len().saturating_sub(kept);
        record.replies.drain(..dropped);
        record.op_number = op_number;
        self.state_machine
            .record_client(op_number, client_id, Some(record));
        Reply::Executed {
            view_number,
            client_id,
            session,
            request_number,
            result,
        }
    }

    /// Opens a session numbered `op_number` for `client_id`, unless it has
    /// one, evicting the session whose latest entry executed earliest if
    /// the client table is full.
    fn register(&mut self, op_number: OpNumber, client_id: ClientID) -> Reply<SM::Output> {
        let view_number = self.view_number;
        let state = self.clients.entry(client_id).or_default();
        if state.pending == Some((op_number, 0)) {
            state.pending = None;
        }
        if let Some(record) = &state.record {
            self.state_machine
                .record_client(op_number, client_id, Some(record));
            return Reply::Registered {
                view_number,
                client_id,
                session: record.session,
            };
        }
        if self.sessions >= self.config.clients_max() {
            self.evict(op_number);
        }
        let record = ClientRecord {
            client_id,
            session: op_number,
            request_number: 0,
            replies: VecDeque::new(),
            op_number,
        };
        self.state_machine
            .record_client(op_number, client_id, Some(&record));
        self.clients.entry(client_id).or_default().record = Some(record);
        self.sessions += 1;
        Reply::Registered {
            view_number,
            client_id,
            session: op_number,
        }
    }

    /// Evicts the session whose latest entry executed earliest, at
    /// `op_number`.
    fn evict(&mut self, op_number: OpNumber) {
        let evicted = loop {
            let Some((latest, client_id)) = self.eviction_candidates.pop_front() else {
                self.find_eviction_candidates();
                continue;
            };
            let record = self
                .clients
                .get(&client_id)
                .and_then(|state| state.record.as_ref());
            if record.is_some_and(|record| record.op_number == latest) {
                break client_id;
            }
        };
        trace!(
            "Replica {} evicts client {evicted} at op {op_number}",
            self.self_id
        );
        if let Entry::Occupied(mut state) = self.clients.entry(evicted) {
            state.get_mut().record = None;
            if state.get().pending.is_none() {
                state.remove();
            }
        }
        self.sessions -= 1;
        self.state_machine.record_client(op_number, evicted, None);
    }

    /// Scans the client table for the oldest sessions, a sixteenth of them
    /// and at least one, so that a scan serves many evictions.
    fn find_eviction_candidates(&mut self) {
        let mut sessions: Vec<(OpNumber, ClientID)> = self
            .clients
            .values()
            .filter_map(|state| state.record.as_ref())
            .map(|record| (record.op_number, record.client_id))
            .collect();
        assert!(!sessions.is_empty(), "a full client table holds a session");
        let kept = (sessions.len() / 16).max(1);
        if kept < sessions.len() {
            sessions.select_nth_unstable(kept);
            sessions.truncate(kept);
        }
        sessions.sort_unstable();
        self.eviction_candidates = sessions.into();
    }

    /// Rebuilds the pending entries from the log after `applied`.
    fn rebuild_pending(&mut self) {
        self.clients.retain(|_, state| {
            state.pending = None;
            state.record.is_some()
        });
        let applied = self.applied;
        for (index, entry) in self.log[applied - self.log_start..].iter().enumerate() {
            let (client_id, key) = Self::pending_key(applied + index + 1, entry);
            self.clients.entry(client_id).or_default().pending = Some(key);
        }
    }

    /// What `pending` keeps for `entry`, appended as `op_number`: its
    /// client, and its session and request number, 0 for a registration.
    fn pending_key(
        op_number: OpNumber,
        entry: &LogEntry<SM::Input>,
    ) -> (ClientID, (OpNumber, RequestNumber)) {
        match entry {
            LogEntry::Register { client_id } => (*client_id, (op_number, 0)),
            LogEntry::Request {
                client_id,
                session,
                request_number,
                ..
            } => (*client_id, (*session, *request_number)),
        }
    }

    /// The main entry point to replica logic.
    pub fn on_message(&mut self, message: MessageFor<SM>) {
        trace!("Replica {} <- {:?}", self.self_id, message);
        // A recovering replica knows nothing it could safely act on: not
        // which view is current, not what it acknowledged before the crash.
        // Until it has recovered, only recovery responses matter, and the
        // chunks of a checkpoint one named.
        if self.status == Status::Recovering
            && !matches!(
                message,
                Message::RecoveryResponse { .. } | Message::NewChunk { .. }
            )
        {
            return;
        }
        match message {
            Message::Register { client_id } => {
                self.on_register(client_id);
            }
            Message::Request {
                client_id,
                session,
                request_number,
                answered,
                op,
            } => {
                self.on_request(client_id, session, request_number, answered, op);
            }
            Message::Query {
                client_id,
                query_number,
                query,
            } => {
                self.on_query(client_id, query_number, query);
            }
            Message::ConfirmView { view_number, round } => {
                self.on_confirm_view(view_number, round);
            }
            Message::ConfirmViewOk {
                view_number,
                round,
                replica_id,
            } => {
                self.on_confirm_view_ok(view_number, round, replica_id);
            }
            Message::Prepare {
                view_number,
                op_number,
                entry,
                commit_number,
            } => {
                self.on_prepare(view_number, op_number, entry, commit_number);
            }
            Message::PrepareOk {
                view_number,
                op_number,
                replica_id,
            } => {
                self.on_prepare_ok(view_number, op_number, replica_id);
            }
            Message::Commit {
                view_number,
                commit_number,
            } => {
                self.on_commit(view_number, commit_number);
            }
            Message::GetState {
                replica_id,
                view_number,
                op_number,
            } => {
                self.on_get_state(replica_id, view_number, op_number);
            }
            Message::NewState {
                replica_id,
                view_number,
                segment,
                commit_number,
            } => {
                self.on_new_state(replica_id, view_number, segment, commit_number);
            }
            Message::GetChunk {
                replica_id,
                op_number,
                index,
            } => {
                self.on_get_chunk(replica_id, op_number, index);
            }
            Message::NewChunk {
                replica_id,
                op_number,
                index,
                chunk,
                last,
            } => {
                self.on_new_chunk(replica_id, op_number, index, chunk, last);
            }
            Message::StartViewChange {
                view_number,
                replica_id,
            } => {
                self.on_start_view_change(view_number, replica_id);
            }
            Message::DoViewChange {
                view_number,
                replica_id,
                last_normal_view,
                segment,
                commit_number,
            } => {
                let dvc = DoViewChange {
                    last_normal_view,
                    segment,
                    commit_number,
                };
                self.on_do_view_change(view_number, replica_id, dvc);
            }
            Message::StartView {
                view_number,
                segment,
                commit_number,
            } => {
                self.on_start_view(view_number, segment, commit_number);
            }
            Message::Recovery {
                replica_id,
                nonce,
                view_number,
            } => {
                self.on_recovery(replica_id, nonce, view_number);
            }
            Message::RecoveryResponse {
                view_number,
                nonce,
                replica_id,
                state,
            } => {
                self.on_recovery_response(view_number, nonce, replica_id, state);
            }
        }
    }

    /// A client asks the primary for a session. One the client has is
    /// answered at once; otherwise the primary appends the registration,
    /// unless one is in the log already.
    fn on_register(&mut self, client_id: ClientID) {
        if !self.is_primary() || self.status != Status::Normal {
            return;
        }
        let state = self.clients.get(&client_id);
        if let Some(record) = state.and_then(|state| state.record.as_ref()) {
            self.replies.push(Reply::Registered {
                view_number: self.view_number,
                client_id,
                session: record.session,
            });
            return;
        }
        if state
            .and_then(|state| state.pending)
            .is_some_and(|(_, request_number)| request_number == 0)
        {
            return;
        }
        self.prepare(LogEntry::Register { client_id });
    }

    /// The client sends a `Request` message to the primary, which replicates
    /// the operation to the other replicas.
    fn on_request(
        &mut self,
        client_id: ClientID,
        session: OpNumber,
        request_number: RequestNumber,
        answered: RequestNumber,
        op: SM::Input,
    ) {
        // Backups ignore client requests; clients send to every replica
        // when they re-send, in case the primary has changed. A primary that
        // is not in normal status drops the request too, and the client's
        // re-send will find it once it is.
        if !self.is_primary() || self.status != Status::Normal {
            return;
        }
        let view_number = self.view_number;
        let state = self.clients.get(&client_id);
        let Some(record) = state
            .and_then(|state| state.record.as_ref())
            .filter(|record| record.session == session)
        else {
            // The session's registration has executed here, and its record
            // is gone: the session was evicted. A later one may not have
            // executed yet.
            if session <= self.applied {
                self.replies.push(Reply::Evicted {
                    view_number,
                    client_id,
                    session,
                });
            }
            return;
        };
        if request_number <= record.request_number {
            // A re-send of an executed request, answered while the table
            // keeps its result.
            let behind = record.request_number - request_number;
            if let Some(index) = record.replies.len().checked_sub(behind + 1) {
                self.replies.push(Reply::Executed {
                    view_number,
                    client_id,
                    session,
                    request_number,
                    result: record.replies[index].clone(),
                });
            }
            return;
        }
        // Only the session's next request is appended, so that its requests
        // execute in order: a later one arrived out of order, and an earlier
        // one is in the log already. A client has at most `in_flight_max`
        // requests past its last executed one. An entry of a later session
        // in the log means the client has left this one, whose requests
        // may be in the log too.
        let latest = match state.and_then(|state| state.pending) {
            Some((pending_session, pending)) if pending_session == session => pending,
            Some((pending_session, _)) if pending_session > session => return,
            _ => record.request_number,
        };
        if request_number != latest + 1
            || request_number > record.request_number + self.config.in_flight_max()
        {
            return;
        }
        self.prepare(LogEntry::Request {
            client_id,
            session,
            request_number,
            answered,
            op,
        });
    }

    /// Appends `entry` to the log and sends it to the backups.
    fn prepare(&mut self, entry: LogEntry<SM::Input>) {
        self.append_to_log(entry.clone());
        self.send_to_others(Message::Prepare {
            view_number: self.view_number,
            op_number: self.op_number(),
            entry,
            commit_number: self.commit_number,
        });
    }

    /// A client asks the primary to answer a query. It executes once a
    /// quorum, the primary included, has confirmed the primary's view in a
    /// round started after the query arrived, and the state machine has
    /// applied the ops that may have completed by then. A view that had
    /// started by then was started by a quorum past this view, which the
    /// confirming quorum meets, so no later view had committed anything.
    /// The ops committed in earlier views are in the log up to
    /// `view_start_op`, and the primary commits the later ones itself: no
    /// op past its commit number can have completed. A query joins the
    /// last round while its `ConfirmView` has not left; otherwise it starts
    /// a round, or, with `ROUNDS_OUT` out, waits for the next.
    fn on_query(&mut self, client_id: ClientID, query_number: QueryNumber, query: SM::Query) {
        if !self.is_primary()
            || self.status != Status::Normal
            || !self.waiting.insert((client_id, query_number))
        {
            return;
        }
        let round = if self.round_unsent {
            self.round
        } else if self.round - self.round_confirmed < ROUNDS_OUT {
            self.start_round();
            self.round
        } else {
            self.round + 1
        };
        self.queries.push_back(WaitingQuery {
            client_id,
            query_number,
            query,
            op_number: self.commit_number.max(self.view_start_op),
            round,
        });
    }

    /// Starts the next round of view confirmations.
    fn start_round(&mut self) {
        self.round += 1;
        self.round_unsent = true;
        self.confirmed[self.self_id] = self.round;
        self.send_to_others(Message::ConfirmView {
            view_number: self.view_number,
            round: self.round,
        });
    }

    /// A backup confirms a round of the primary's view while it is in that
    /// view. The confirmation promises nothing about its disk: a replica
    /// announces a later view only once a durable write holds it, and comes
    /// back from a crash in that view or a later one.
    fn on_confirm_view(&mut self, view_number: ViewNumber, round: u64) {
        if view_number != self.view_number || self.is_primary() {
            return;
        }
        self.send_to_primary(Message::ConfirmViewOk {
            view_number,
            round,
            replica_id: self.self_id,
        });
    }

    /// The primary counts a backup's confirmation, and once a quorum has
    /// confirmed a later round, starts the next round if queries wait for
    /// it and answers those that may go.
    fn on_confirm_view_ok(&mut self, view_number: ViewNumber, round: u64, replica_id: ReplicaID) {
        if view_number != self.view_number
            || !self.is_primary()
            || self.status != Status::Normal
            || replica_id >= self.confirmed.len()
            || round > self.round
            || round <= self.confirmed[replica_id]
        {
            return;
        }
        self.confirmed[replica_id] = round;
        let quorum = self.config.quorum();
        let confirmed = &self.confirmed;
        let round_confirmed = confirmed
            .iter()
            .copied()
            .filter(|&round| confirmed.iter().filter(|&&other| other >= round).count() >= quorum)
            .max()
            .unwrap_or(0);
        if round_confirmed <= self.round_confirmed {
            return;
        }
        self.round_confirmed = round_confirmed;
        if self
            .queries
            .back()
            .is_some_and(|query| query.round > self.round)
        {
            // A round that ended makes room for the next.
            self.start_round();
        }
        self.execute_queries();
    }

    /// Answers the waiting queries whose round a quorum has confirmed and
    /// whose op the state machine has applied, while this replica is the
    /// primary in normal status. Both grow with the order the queries
    /// arrived in, so those that may go come first.
    fn execute_queries(&mut self) {
        if !self.is_primary() || self.status != Status::Normal {
            return;
        }
        while let Some(query) = self.queries.front() {
            if query.round > self.round_confirmed || query.op_number > self.applied {
                return;
            }
            let query = self.queries.pop_front().expect("a query waits");
            self.waiting.remove(&(query.client_id, query.query_number));
            let result = self.state_machine.query(&query.query);
            self.replies.push(Reply::Queried {
                view_number: self.view_number,
                client_id: query.client_id,
                query_number: query.query_number,
                result,
            });
        }
    }

    /// The primary sends a `Prepare` message to replicate an operation to backup
    /// nodes. The nodes that receive a `Prepare` message will reply with `PrepareOk`
    /// when they have appended `op` to their logs. The message also contains the
    /// commit number of the primary, so that the backups can commit their logs up
    /// to that point.
    fn on_prepare(
        &mut self,
        view_number: ViewNumber,
        op_number: OpNumber,
        entry: LogEntry<SM::Input>,
        commit_number: CommitID,
    ) {
        if !self.accept_from_primary(view_number) {
            return;
        }
        // If we fell behind in the log, initiate state transfer.
        if op_number > self.op_number() + 1 {
            self.state_transfer();
            return;
        }
        if op_number == self.op_number() + 1 {
            // Append the request to our log.
            self.append_to_log(entry);
        }
        // Otherwise we already have the op: the primary re-sent it because
        // it has not seen our `PrepareOk`, so acknowledge it again below.
        //
        // Commit the log up to the commit number received in `Prepare`
        // message, which represents the committed state of the primary. A
        // re-sent `Prepare` can carry a commit number beyond our log; the
        // re-sent ops that follow close that gap.
        self.commit_up_to(commit_number.min(self.op_number()), false);
        // Acknowledge to the primary that we have every op up to our op
        // number.
        self.send_prepare_ok();
    }

    /// Backup nodes send `PrepareOk` message to the primary to acknowledge that
    /// they have appended an op to their logs. The acknowledgement covers
    /// every earlier op too: a backup only appends an op once it has every
    /// earlier one. When the primary has received `PrepareOk` messages from
    /// a quorum of replicas for an op, it commits that op and every earlier
    /// one, and replies to the clients.
    fn on_prepare_ok(
        &mut self,
        view_number: ViewNumber,
        op_number: OpNumber,
        replica_id: ReplicaID,
    ) {
        if view_number != self.view_number
            || !self.is_primary()
            || self.status != Status::Normal
            || replica_id >= self.acked.len()
        {
            return;
        }
        self.register_ack(replica_id, op_number);
    }

    /// Registers that `replica_id` holds every op up to `op_number`, and
    /// commits up to the highest op number a quorum of replicas holds. An
    /// acknowledgement covers every earlier op, so each replica's highest
    /// is all that counts: the same backup acknowledging twice, because the
    /// network replayed its message or because it answered a re-sent
    /// `Prepare`, still counts once, and an op whose own acknowledgements
    /// were lost or overtaken commits with the later ones that cover it.
    fn register_ack(&mut self, replica_id: ReplicaID, op_number: OpNumber) {
        let op_number = op_number.min(self.op_number());
        if op_number <= self.acked[replica_id] {
            return;
        }
        self.acked[replica_id] = op_number;
        // A quorum holds every op up to the quorum-th highest watermark: the
        // highest that at least a quorum of watermarks reach.
        let quorum = self.config.quorum();
        let acked = &self.acked;
        let committed = acked
            .iter()
            .copied()
            .filter(|&mark| acked.iter().filter(|&&other| other >= mark).count() >= quorum)
            .max()
            .unwrap_or(0);
        self.commit_up_to(committed, true);
    }

    /// A backup node typically commits its log as part of `Prepare`
    /// message handling because the primary uses that also to signal the
    /// current commit number. However, `Prepare` is sent only in
    /// reaction to a client `Request` message. If there are no client
    /// requests, then the primary sends a `Commit` message to backup
    /// nodes instead to give backup nodes the chance to commit.
    fn on_commit(&mut self, view_number: ViewNumber, commit_number: CommitID) {
        if !self.accept_from_primary(view_number) {
            return;
        }
        if commit_number > self.op_number() {
            self.state_transfer();
            return;
        }
        self.commit_up_to(commit_number, false);
    }

    /// Checks a `Prepare` or `Commit` from the primary of `view_number`
    /// against our own view and status, and returns whether to process it
    /// as a normal-case message.
    ///
    /// A message from a later view means a view change happened without
    /// us, and one for our own view while we are still changing to it means
    /// the view has started: either way we first catch up with the view's
    /// state from its primary.
    fn accept_from_primary(&mut self, view_number: ViewNumber) -> bool {
        if view_number < self.view_number {
            return false;
        }
        if view_number > self.view_number {
            self.catch_up_with_view(view_number);
            return false;
        }
        // The primary of our view is alive.
        self.heard_from_primary = true;
        match self.status {
            Status::Normal => !self.is_primary(),
            Status::StateTransfer | Status::Recovering => false,
            Status::ViewChange => {
                self.catch_up_with_view(view_number);
                false
            }
        }
    }

    /// Answers a `GetState` with the log after `op_number`, or with a
    /// checkpoint and the log after it when `op_number` is below what we
    /// have compacted. A replica answers requests for its view while its
    /// log is settled: in normal status, or in a view change once it has
    /// sent `DoViewChange`, when the primary of that view may need the
    /// part of our log we have compacted.
    fn on_get_state(
        &mut self,
        replica_id: ReplicaID,
        view_number: ViewNumber,
        op_number: OpNumber,
    ) {
        if view_number != self.view_number || op_number > self.op_number() {
            return;
        }
        let settled = match self.status {
            Status::Normal => true,
            Status::ViewChange => self.do_view_change_sent && self.catching_up.is_none(),
            Status::StateTransfer | Status::Recovering => false,
        };
        if !settled {
            return;
        }
        let message = Message::NewState {
            replica_id: self.self_id,
            view_number,
            segment: self.segment_from(op_number),
            commit_number: self.commit_number,
        };
        self.send(replica_id, message);
    }

    /// The log after `op_number`, or, if that has been compacted, the log
    /// after the checkpoint this replica keeps.
    fn segment_from(&mut self, op_number: OpNumber) -> LogSegment<SM::Input> {
        if op_number >= self.log_start {
            return LogSegment {
                base: LogBase::Op(op_number),
                entries: self.log_from(op_number + 1).to_vec(),
            };
        }
        let checkpoint = self.keep_checkpoint();
        LogSegment {
            base: LogBase::Checkpoint(checkpoint),
            entries: self.log_from(checkpoint + 1).to_vec(),
        }
    }

    /// The op number of the checkpoint this replica keeps for replicas that
    /// fell behind, which it keeps from now on if it kept none. Only
    /// requests for its chunks keep it longer, so a replica that keeps
    /// failing to fetch it holds compaction back no longer than it asks.
    fn keep_checkpoint(&mut self) -> OpNumber {
        if let Some((op_number, _)) = self.serving {
            return op_number;
        }
        let op_number = self.state_machine.checkpoint();
        assert!(
            (self.log_start..=self.applied).contains(&op_number),
            "a checkpoint at op {op_number} with the log starting after {} and {} ops applied",
            self.log_start,
            self.applied
        );
        self.serving = Some((op_number, 0));
        op_number
    }

    /// Answers a replica fetching the checkpoint this replica keeps with
    /// the chunk it asks for.
    fn on_get_chunk(&mut self, replica_id: ReplicaID, op_number: OpNumber, index: usize) {
        let Some((kept, idle_periods)) = &mut self.serving else {
            return;
        };
        if *kept != op_number {
            return;
        }
        *idle_periods = 0;
        let (chunk, last) = self.state_machine.checkpoint_chunk(index);
        let message = Message::NewChunk {
            replica_id: self.self_id,
            op_number,
            index,
            chunk,
            last,
        };
        self.send(replica_id, message);
    }

    /// A replica receives a `NewState` message in response to a
    /// `GetState` message it sent itself to catch up on its log.
    fn on_new_state(
        &mut self,
        replica_id: ReplicaID,
        view_number: ViewNumber,
        segment: LogSegment<SM::Input>,
        commit_number: CommitID,
    ) {
        if view_number != self.view_number {
            return;
        }
        self.heard_from_primary = true;
        let installed = match self.status {
            // We are filling a gap within our view. The reply may answer an
            // earlier `GetState` that the network delayed or replayed, in
            // which case it starts before our current op number. We can
            // still use whatever it has beyond our log, since within a view
            // the overlapping entries are identical. A reply that starts
            // past our log or that ends inside it is of no use, so keep
            // waiting for another.
            Status::StateTransfer => self.install_segment_in_view(segment),
            // We asked for everything after our commit number: what we have
            // beyond that is from an earlier view and never committed, so it
            // is replaced by what the reply holds.
            Status::ViewChange if self.catching_up.is_some() => {
                self.install_segment_from_commit(segment)
            }
            _ => return,
        };
        match installed {
            Installed::Log => self.new_state_installed(commit_number),
            Installed::Nothing => {}
            Installed::Checkpoint(segment) => {
                let resume = Resume::NewState {
                    view_number,
                    segment,
                    commit_number,
                };
                self.start_fetch(replica_id, resume);
            }
        }
    }

    /// Goes on from a `NewState` whose segment the log now holds: a state
    /// transfer is over, and a replica catching up with a view enters it,
    /// or starts it as its primary.
    fn new_state_installed(&mut self, commit_number: CommitID) {
        self.fetch = None;
        match self.status {
            Status::StateTransfer => {
                self.commit_up_to(commit_number, false);
                self.status = Status::Normal;
            }
            Status::ViewChange => {
                self.catching_up = None;
                if self.is_primary() && self.do_view_change_from.len() >= self.config.quorum() {
                    // We are the new primary, and asked the replica whose
                    // log we chose for what it had compacted.
                    self.try_start_view();
                    return;
                }
                self.commit_up_to(commit_number, false);
                self.enter_normal();
            }
            status => unreachable!("a NewState installed in status {status:?}"),
        }
        self.send_prepare_ok();
    }

    /// Installs a segment received within our view, which extends our log
    /// if it reaches beyond it. A checkpoint beyond our log replaces it,
    /// once fetched; one within our log covers ops we hold already, so
    /// only the entries matter.
    fn install_segment_in_view(&mut self, segment: LogSegment<SM::Input>) -> Installed<SM::Input> {
        let op_number = self.op_number();
        if segment.end() <= op_number {
            return Installed::Nothing;
        }
        match segment.base {
            LogBase::Checkpoint(checkpoint) if checkpoint > op_number => {
                Installed::Checkpoint(segment)
            }
            base if base.start() > op_number => Installed::Nothing,
            base => {
                self.merge_log(base.start(), segment.entries);
                Installed::Log
            }
        }
    }

    /// Installs a segment we asked for from our commit number, which
    /// replaces everything after it, after a checkpoint beyond our commit
    /// number once that is fetched. A segment that starts beyond our commit
    /// number otherwise answers a request we made with a higher commit
    /// number, and is of no use.
    fn install_segment_from_commit(
        &mut self,
        segment: LogSegment<SM::Input>,
    ) -> Installed<SM::Input> {
        match segment.base {
            LogBase::Checkpoint(checkpoint) if checkpoint > self.commit_number => {
                Installed::Checkpoint(segment)
            }
            base if base.start() > self.commit_number => Installed::Nothing,
            base => {
                self.merge_log(base.start(), segment.entries);
                Installed::Log
            }
        }
    }

    /// Fetches from `from` the checkpoint the segment in `then` follows,
    /// and handles `then` again once the checkpoint is restored. A fetch of
    /// the same checkpoint goes on from where it is, and one of a later
    /// checkpoint goes on instead unless it has stalled.
    fn start_fetch(&mut self, from: ReplicaID, then: Resume<SM::Input>) {
        let op_number = then.checkpoint();
        if let Some(fetch) = &mut self.fetch {
            let under_way = fetch.then.checkpoint();
            if (fetch.from, under_way) == (from, op_number) {
                fetch.then = then;
                if fetch.stalled {
                    fetch.stalled = false;
                    fetch.idle_periods = 0;
                    self.send_get_chunk();
                }
                return;
            }
            if under_way > op_number && !fetch.stalled {
                return;
            }
        }
        trace!(
            "Replica {} fetches the checkpoint at op {op_number} from replica {from}",
            self.self_id
        );
        self.fetch = Some(Fetch {
            from,
            index: 0,
            idle_periods: 0,
            stalled: false,
            then,
        });
        self.send_get_chunk();
    }

    /// Asks for the chunk the fetch under way waits for.
    fn send_get_chunk(&mut self) {
        let Some(fetch) = &self.fetch else {
            return;
        };
        let (from, op_number, index) = (fetch.from, fetch.then.checkpoint(), fetch.index);
        let message = Message::GetChunk {
            replica_id: self.self_id,
            op_number,
            index,
        };
        self.send(from, message);
    }

    /// Stages a chunk of the checkpoint under way and asks for the next,
    /// or, with the last one staged, restores the checkpoint and goes on
    /// with the message that named it, whose segment follows it. A chunk
    /// from the replica this one catches up with counts as hearing from it.
    fn on_new_chunk(
        &mut self,
        replica_id: ReplicaID,
        op_number: OpNumber,
        index: usize,
        chunk: SM::Chunk,
        last: bool,
    ) {
        let Some(fetch) = &mut self.fetch else {
            return;
        };
        if (fetch.from, fetch.then.checkpoint(), fetch.index) != (replica_id, op_number, index) {
            return;
        }
        fetch.idle_periods = 0;
        fetch.stalled = false;
        if !last {
            fetch.index += 1;
        }
        self.state_machine.stage_chunk(op_number, index, chunk);
        if self.catching_up == Some(replica_id) {
            self.idle_periods_waiting = 0;
        }
        if self.primary_id() == replica_id {
            self.heard_from_primary = true;
        }
        if !last {
            self.send_get_chunk();
            return;
        }
        let fetch = self.fetch.take().expect("a fetch under way");
        if op_number <= self.commit_number {
            return;
        }
        self.restore_checkpoint(op_number);
        match fetch.then {
            Resume::NewState {
                view_number,
                segment,
                commit_number,
            } => {
                if view_number == self.view_number {
                    self.merge_log(op_number, segment.entries);
                    self.new_state_installed(commit_number);
                }
            }
            Resume::RecoveryResponse { view_number, state } => {
                self.recover_into(view_number, state)
            }
        }
    }

    /// Makes the checkpoint staged at `op_number` the state, and starts the
    /// log after it. The checkpoint must be beyond our commit number, so
    /// that nothing executed here is undone.
    fn restore_checkpoint(&mut self, op_number: OpNumber) {
        assert!(op_number > self.commit_number);
        trace!(
            "Replica {} restores the checkpoint at op {op_number}",
            self.self_id
        );
        if self.serving.take().is_some() {
            self.state_machine.release_checkpoint();
        }
        self.commit_number = op_number;
        self.applied = op_number;
        self.reply_ranges.clear();
        self.log_start = op_number;
        self.log.clear();
        self.mark_log_changed(self.log_start + 1);
        self.state_machine.restore(op_number);
        self.load_client_table();
        self.rebuild_pending();
    }

    /// Asks the primary for the log after our op number, to fill a gap
    /// within the current view.
    fn state_transfer(&mut self) {
        self.status = Status::StateTransfer;
        self.send_get_state(self.op_number());
    }

    /// A replica that suspects the primary has failed, or that hears of a
    /// view change already under way, asks the others to move to a new
    /// view.
    fn on_start_view_change(&mut self, view_number: ViewNumber, replica_id: ReplicaID) {
        if view_number < self.view_number {
            return;
        }
        if view_number > self.view_number {
            self.start_view_change(view_number);
        } else if self.status != Status::ViewChange {
            // The view has already started. If we are its primary, the
            // sender missed `StartView`, so send it again.
            if self.status == Status::Normal && self.is_primary() {
                self.send_start_view(replica_id);
            }
            return;
        }
        self.start_view_change_from.insert(replica_id);
        self.maybe_send_do_view_change();
    }

    /// Replicas that know a majority wants the new view send their state to
    /// its primary, which starts the view once it has a quorum of them.
    fn on_do_view_change(
        &mut self,
        view_number: ViewNumber,
        replica_id: ReplicaID,
        dvc: DoViewChangeFor<SM>,
    ) {
        if view_number < self.view_number || self.config.primary_id(view_number) != self.self_id {
            return;
        }
        if view_number > self.view_number {
            self.start_view_change(view_number);
        } else if self.status == Status::Normal {
            // The view has already started; the sender missed `StartView`.
            self.send_start_view(replica_id);
            return;
        }
        self.record_do_view_change(replica_id, dvc);
    }

    /// The new primary starts the view with the log it chose. Backups
    /// replace their log with it, commit what it says is committed, and
    /// acknowledge whatever is not. A backup that has committed less than
    /// the primary has compacted cannot take the log as sent, and fetches
    /// the primary's checkpoint instead.
    fn on_start_view(
        &mut self,
        view_number: ViewNumber,
        segment: LogSegment<SM::Input>,
        commit_number: CommitID,
    ) {
        // A `StartView` for our own view is only new to us while we are
        // still changing to it; a replayed one after that must not replace
        // a log that has since grown.
        if view_number < self.view_number
            || (view_number == self.view_number && self.status != Status::ViewChange)
        {
            return;
        }
        if !matches!(self.install_segment_from_commit(segment), Installed::Log) {
            self.catch_up_with_view(view_number);
            return;
        }
        self.view_number = view_number;
        self.commit_up_to(commit_number, false);
        self.enter_normal();
        self.send_prepare_ok();
    }

    /// Moves to `view_number` and asks the other replicas to do the same.
    fn start_view_change(&mut self, view_number: ViewNumber) {
        trace!(
            "Replica {} starts view change to {view_number}",
            self.self_id
        );
        // A view change entered from another one, whether on our own
        // timer or because someone else's fired, means the previous one
        // did not complete: wait longer for this one.
        if self.status == Status::ViewChange {
            self.view_change_attempts += 1;
        }
        self.view_number = view_number;
        self.status = Status::ViewChange;
        self.idle_periods_waiting = 0;
        self.clear_view_change_state();
        self.send_to_others(Message::StartViewChange {
            view_number,
            replica_id: self.self_id,
        });
        self.maybe_send_do_view_change();
    }

    /// Forgets the view change under way and the queries waiting on the
    /// last view: a replica leaving normal status or entering it again
    /// starts both anew, and clients re-send their queries.
    fn clear_view_change_state(&mut self) {
        self.start_view_change_from.clear();
        self.do_view_change_sent = false;
        self.do_view_change_from.clear();
        self.catching_up = None;
        self.fetch = None;
        self.queries.clear();
        self.waiting.clear();
        self.round = 0;
        self.round_confirmed = 0;
        self.confirmed.fill(0);
        self.round_unsent = false;
    }

    /// Sends `DoViewChange` once `f` other replicas want the same view.
    fn maybe_send_do_view_change(&mut self) {
        if self.status != Status::ViewChange
            || self.catching_up.is_some()
            || self.do_view_change_sent
        {
            return;
        }
        let f = self.config.replicas().len() / 2;
        if self.start_view_change_from.len() < f {
            return;
        }
        self.do_view_change_sent = true;
        self.send_do_view_change();
    }

    /// Sends our state to the new view's primary, or records it directly if
    /// that is us.
    fn send_do_view_change(&mut self) {
        let view_number = self.view_number;
        let dvc = DoViewChange {
            last_normal_view: self.last_normal_view,
            segment: LogSegment {
                base: LogBase::Op(self.log_start),
                entries: self.log.clone(),
            },
            commit_number: self.commit_number,
        };
        let primary_id = self.config.primary_id(view_number);
        if primary_id == self.self_id {
            self.record_do_view_change(self.self_id, dvc);
        } else {
            self.send(
                primary_id,
                Message::DoViewChange {
                    view_number,
                    replica_id: self.self_id,
                    last_normal_view: dvc.last_normal_view,
                    segment: dvc.segment,
                    commit_number: dvc.commit_number,
                },
            );
        }
    }

    /// Records a `DoViewChange` and, with a quorum of them, starts the
    /// view.
    fn record_do_view_change(&mut self, replica_id: ReplicaID, dvc: DoViewChangeFor<SM>) {
        self.do_view_change_from.insert(replica_id, dvc);
        if self.do_view_change_from.len() < self.config.quorum() {
            return;
        }
        self.try_start_view();
    }

    /// Starts the view from the log with the latest normal view among the
    /// `DoViewChange` messages, the longest if several, which by quorum
    /// intersection holds every committed op. The sender may have compacted
    /// entries beyond our commit number, which its message does not carry;
    /// then we ask it for the log from our commit number on, checkpoint
    /// included, and come back here once that is installed.
    fn try_start_view(&mut self) {
        let (best_id, best) = self
            .do_view_change_from
            .iter()
            .max_by_key(|(_, dvc)| (dvc.last_normal_view, dvc.segment.end()))
            .map(|(id, dvc)| (*id, dvc))
            .unwrap();
        let segment = best.segment.clone();
        if !matches!(self.install_segment_from_commit(segment), Installed::Log) {
            trace!(
                "Replica {} needs the checkpoint of replica {best_id} to start view {}",
                self.self_id,
                self.view_number
            );
            self.catching_up = Some(best_id);
            let op_number = self.commit_number;
            self.send_get_state_to(best_id, op_number);
            return;
        }
        let commit_number = self
            .do_view_change_from
            .values()
            .map(|dvc| dvc.commit_number)
            .max()
            .unwrap();
        trace!(
            "Replica {} starts view {} with {} ops, {commit_number} committed",
            self.self_id,
            self.view_number,
            self.op_number()
        );
        // Execute what committed in earlier views but was not yet executed
        // here, and reply to the clients: the old primary may have failed
        // before it could.
        self.commit_up_to(commit_number, true);
        self.enter_normal();
        for replica_id in 0..self.config.replicas().len() {
            if replica_id != self.self_id {
                self.send_start_view(replica_id);
            }
        }
    }

    fn send_start_view(&mut self, replica_id: ReplicaID) {
        let message = Message::StartView {
            view_number: self.view_number,
            segment: LogSegment {
                base: LogBase::Op(self.log_start),
                entries: self.log.clone(),
            },
            commit_number: self.commit_number,
        };
        self.send(replica_id, message);
    }

    /// Learns that `view_number` has started, and asks its primary for the
    /// log from our commit number on.
    fn catch_up_with_view(&mut self, view_number: ViewNumber) {
        let primary_id = self.config.primary_id(view_number);
        if self.view_number == view_number && self.catching_up == Some(primary_id) {
            return;
        }
        trace!(
            "Replica {} catches up with view {view_number}",
            self.self_id
        );
        self.view_number = view_number;
        self.status = Status::ViewChange;
        self.idle_periods_waiting = 0;
        self.clear_view_change_state();
        self.catching_up = Some(primary_id);
        self.send_get_state(self.commit_number);
    }

    /// Returns to normal status in the current view. The view change
    /// backoff stays until the view has proved stable; see
    /// `view_change_attempts`.
    fn enter_normal(&mut self) {
        self.status = Status::Normal;
        self.last_normal_view = self.view_number;
        // Acknowledgements count within a view, and the first idle period
        // of a view re-sends nothing: `StartView` has just carried the log.
        self.acked.fill(0);
        self.resend_up_to = self.commit_number;
        self.view_start_op = self.op_number();
        self.heard_from_primary = true;
        self.idle_periods_waiting = 0;
        self.idle_periods_stable = 0;
        self.clear_view_change_state();
    }

    /// A replica in normal status answers a recovering replica with its
    /// view; if it is the primary, with its state too.
    ///
    /// The recovering replica persisted `view_number` before it crashed.
    /// If that is ahead of us, it had started a view change we never heard
    /// of, and it will not rejoin a view older than that, so start the
    /// change now as if its `StartViewChange` had just arrived.
    fn on_recovery(&mut self, replica_id: ReplicaID, nonce: u64, view_number: ViewNumber) {
        if view_number > self.view_number && self.status != Status::Recovering {
            self.start_view_change(view_number);
            return;
        }
        if self.status != Status::Normal {
            return;
        }
        // The recovering replica has nothing, so it needs the whole log,
        // or our checkpoint where the log has been compacted.
        let state = if self.is_primary() {
            Some(RecoveryState {
                segment: self.segment_from(0),
                commit_number: self.commit_number,
            })
        } else {
            None
        };
        let message = Message::RecoveryResponse {
            view_number: self.view_number,
            nonce,
            replica_id: self.self_id,
            state,
        };
        self.send(replica_id, message);
    }

    /// Collects recovery responses. With a quorum of them, including one
    /// from the primary of the latest view among them, the replica takes
    /// that primary's state and is back. The latest view must be at least
    /// the one persisted before the crash: a lower one means the cluster
    /// has not yet caught up with a view change this replica took part in
    /// and then forgot, and joining it could let that view change complete
    /// against a view already running.
    fn on_recovery_response(
        &mut self,
        view_number: ViewNumber,
        nonce: u64,
        replica_id: ReplicaID,
        state: Option<RecoveryState<SM::Input>>,
    ) {
        if self.status != Status::Recovering || nonce != self.recovery_nonce {
            return;
        }
        self.recovery_responses
            .insert(replica_id, RecoveryResponse { view_number, state });
        if self.recovery_responses.len() < self.config.quorum() {
            return;
        }
        let latest_view = self
            .recovery_responses
            .values()
            .map(|response| response.view_number)
            .max()
            .unwrap();
        if latest_view < self.view_number {
            return;
        }
        let primary_id = self.config.primary_id(latest_view);
        let Some(RecoveryResponse {
            view_number: primary_view,
            state: Some(_),
        }) = self.recovery_responses.get(&primary_id)
        else {
            return;
        };
        if *primary_view != latest_view {
            return;
        }
        let state = self
            .recovery_responses
            .remove(&primary_id)
            .and_then(|response| response.state)
            .unwrap();
        trace!(
            "Replica {} recovers into view {latest_view} with {} ops, {} committed",
            self.self_id,
            state.segment.end(),
            state.commit_number
        );
        self.recovery_responses.clear();
        self.recover_into(latest_view, state);
    }

    /// Takes the state the primary of `view_number` sent, fetching the
    /// checkpoint it names first if that is beyond ours, and is back in
    /// that view. The state machine may hold a checkpoint from an earlier
    /// attempt, which the log keeps up to; the primary's log covers it
    /// either way.
    fn recover_into(&mut self, view_number: ViewNumber, state: RecoveryState<SM::Input>) {
        let RecoveryState {
            segment,
            commit_number,
        } = state;
        match self.install_segment_from_commit(segment) {
            Installed::Log => {}
            Installed::Checkpoint(segment) => {
                let state = RecoveryState {
                    segment,
                    commit_number,
                };
                let resume = Resume::RecoveryResponse { view_number, state };
                self.start_fetch(self.config.primary_id(view_number), resume);
                return;
            }
            Installed::Nothing => {
                unreachable!("the primary's log reaches back to our commit number")
            }
        }
        self.view_number = view_number;
        self.commit_up_to(commit_number, false);
        self.enter_normal();
    }

    /// Asks the other replicas for their state: those that have not
    /// answered this recovery, or everyone again if a quorum has answered
    /// without completing it, since those answers are then stale.
    fn send_recovery(&mut self) {
        let message = Message::Recovery {
            replica_id: self.self_id,
            nonce: self.recovery_nonce,
            view_number: self.view_number,
        };
        let ask_everyone = self.recovery_responses.len() >= self.config.quorum();
        for replica_id in 0..self.config.replicas().len() {
            if replica_id != self.self_id
                && (ask_everyone || !self.recovery_responses.contains_key(&replica_id))
            {
                self.send(replica_id, message.clone());
            }
        }
    }

    /// When there are no client requests, the primary node sends a
    /// `Commit` message to backup nodes periodically to let them commit
    /// if needed.
    ///
    /// Idle periods also drive retransmission, which the paper leaves out of
    /// its description: a backup waiting for `NewState` asks again in case
    /// its `GetState` or the reply was lost, replicas in a view change
    /// re-send their view change messages, and a recovering replica
    /// re-sends `Recovery` to whoever has not answered. The primary
    /// re-sends one `Prepare` to each backup that has not acknowledged, a
    /// whole idle period after it was sent, an uncommitted op: that of the
    /// last such op. A backup that holds every op before it acknowledges
    /// them all; one that lacks some finds the gap and asks for them with a
    /// state transfer. A lost `Prepare` or `PrepareOk` is thus repaired one
    /// to two idle periods after it was sent.
    ///
    /// And they drive the timers: a backup that has not heard from the
    /// primary for `Config::primary_timeout` idle periods starts a view
    /// change, and a view change that takes as long to complete is
    /// followed by another, waiting twice as long each time.
    pub fn on_idle(&mut self) {
        self.serve_on_idle();
        match self.status {
            Status::Normal if self.is_primary() => {
                self.note_stable();
                let view_number = self.view_number;
                let commit_number = self.commit_number;
                self.send_to_others(Message::Commit {
                    view_number,
                    commit_number,
                });
                let last_op = self.op_number();
                let due = std::mem::replace(&mut self.resend_up_to, last_op).min(last_op);
                if due > commit_number {
                    let entry = self.entry(due).clone();
                    for replica_id in 0..self.config.replicas().len() {
                        if replica_id != self.self_id && self.acked[replica_id] < due {
                            self.send(
                                replica_id,
                                Message::Prepare {
                                    view_number,
                                    op_number: due,
                                    entry: entry.clone(),
                                    commit_number,
                                },
                            );
                        }
                    }
                }
                let round = self.round;
                if round > self.round_confirmed {
                    for replica_id in 0..self.config.replicas().len() {
                        if self.confirmed[replica_id] < round {
                            self.send(replica_id, Message::ConfirmView { view_number, round });
                        }
                    }
                }
            }
            Status::Recovering => {
                if !self.fetch_on_idle() {
                    self.send_recovery();
                }
            }
            Status::Normal | Status::StateTransfer => {
                if self.status == Status::StateTransfer && !self.fetch_on_idle() {
                    self.state_transfer();
                }
                if std::mem::replace(&mut self.heard_from_primary, false) {
                    self.idle_periods_waiting = 0;
                    self.note_stable();
                } else {
                    self.idle_periods_stable = 0;
                    if self.wait_timed_out() {
                        self.start_view_change(self.view_number + 1);
                    }
                }
            }
            Status::ViewChange => {
                if self.wait_timed_out() {
                    self.start_view_change(self.view_number + 1);
                    return;
                }
                if let Some(replica_id) = self.catching_up {
                    if !self.fetch_on_idle() {
                        let op_number = self.commit_number;
                        self.send_get_state_to(replica_id, op_number);
                    }
                }
                // A replica catching up with a view that has started has
                // nothing to say in the view change; the primary of a view
                // being started, waiting for a checkpoint, keeps the
                // others' `DoViewChange` coming.
                if self.catching_up.is_none() || self.do_view_change_sent {
                    self.send_to_others(Message::StartViewChange {
                        view_number: self.view_number,
                        replica_id: self.self_id,
                    });
                }
                if self.do_view_change_sent && self.catching_up.is_none() {
                    self.send_do_view_change();
                }
            }
        }
    }

    /// Counts an idle period of the checkpoint kept for replicas that fell
    /// behind, and drops it after twice `Config::primary_timeout` periods
    /// without a replica asking for it.
    fn serve_on_idle(&mut self) {
        let Some((_, idle_periods)) = &mut self.serving else {
            return;
        };
        *idle_periods += 1;
        if *idle_periods >= 2 * self.config.primary_timeout() {
            self.serving = None;
            self.state_machine.release_checkpoint();
        }
    }

    /// Counts an idle period of the fetch under way, and returns whether
    /// one is. A fetch that has waited a whole idle period for a chunk asks
    /// for it again, and one that has waited twice `Config::primary_timeout`
    /// periods, longer than a round trip takes, stalls: the replica asks
    /// for state again.
    fn fetch_on_idle(&mut self) -> bool {
        let Some(fetch) = &mut self.fetch else {
            return false;
        };
        if fetch.stalled {
            return false;
        }
        fetch.idle_periods += 1;
        if fetch.idle_periods >= 2 * self.config.primary_timeout() {
            trace!(
                "Replica {} stalls fetching the checkpoint at op {}",
                self.self_id,
                fetch.then.checkpoint()
            );
            fetch.stalled = true;
            return false;
        }
        if fetch.idle_periods > 1 {
            self.send_get_chunk();
        }
        true
    }

    /// Counts an idle period of stable normal operation. After
    /// `Config::primary_timeout` of them in a row the view changes are
    /// over, and the backoff is forgotten.
    fn note_stable(&mut self) {
        self.idle_periods_stable += 1;
        if self.idle_periods_stable >= self.config.primary_timeout() {
            self.view_change_attempts = 0;
        }
    }

    /// Counts an idle period spent waiting, and returns whether the wait
    /// has gone on for `Config::primary_timeout` periods, doubled for every
    /// view change entered without a stable stretch of normal status in
    /// between.
    fn wait_timed_out(&mut self) -> bool {
        self.idle_periods_waiting += 1;
        let backoff = self.view_change_attempts.min(10);
        self.idle_periods_waiting >= self.config.primary_timeout() << backoff
    }

    /// The entry with op number `op_number`, which must be in the log.
    fn entry(&self, op_number: OpNumber) -> &LogEntry<SM::Input> {
        &self.log[op_number - self.log_start - 1]
    }

    /// Records that the entries from `op_number` on have changed.
    fn mark_log_changed(&mut self, op_number: OpNumber) {
        self.log_changed_from = Some(match self.log_changed_from {
            Some(from) => from.min(op_number),
            None => op_number,
        });
    }

    /// Appends `entry` to the log.
    fn append_to_log(&mut self, entry: LogEntry<SM::Input>) {
        let op_number = self.op_number() + 1;
        self.mark_log_changed(op_number);
        let (client_id, key) = Self::pending_key(op_number, &entry);
        self.clients.entry(client_id).or_default().pending = Some(key);
        self.log.push(entry);
    }

    /// Replaces the log after `start` with `entries`, which must reach at
    /// least as far as our commit number. The caller guarantees that our
    /// entries up to `start` agree with the sender's: because they are
    /// committed, or because they came from the same primary in the same
    /// view. Our committed entries agree with the sender's too, since
    /// committed prefixes agree everywhere, so they stay as well, and the
    /// change is confined to the uncommitted suffix.
    fn merge_log(&mut self, start: OpNumber, entries: Vec<LogEntry<SM::Input>>) {
        let keep_up_to = start.max(self.commit_number);
        assert!(keep_up_to <= self.op_number());
        self.log.truncate(keep_up_to - self.log_start);
        self.log
            .extend(entries.into_iter().skip(keep_up_to - start));
        assert!(self.op_number() >= self.commit_number);
        self.mark_log_changed(keep_up_to + 1);
        self.rebuild_pending();
    }

    /// Commits every op up to `commit_number`, and executes them, replying
    /// if `reply` is set: at once those that a handed-back write holds as
    /// memory does, the rest once one does. The commit number never moves
    /// backwards.
    fn commit_up_to(&mut self, commit_number: CommitID, reply: bool) {
        if commit_number <= self.commit_number {
            return;
        }
        if reply {
            // A commit that follows the last extends its range, so that the
            // ranges `apply_committed` checks for every op stay few while a
            // write is out and nothing executes.
            let from = self.commit_number + 1;
            match self.reply_ranges.last_mut() {
                Some((_, to)) if *to + 1 == from => *to = commit_number,
                _ => self.reply_ranges.push((from, commit_number)),
            }
        }
        self.commit_number = commit_number;
        self.apply_committed(self.durable_op());
    }

    /// The op number up to which the handed-back writes hold the log as
    /// memory does: `durable`, less the entries changed since the last
    /// write was taken.
    fn durable_op(&self) -> OpNumber {
        self.log_changed_from
            .map_or(self.durable, |from| self.durable.min(from - 1))
    }

    /// Acknowledges to the primary that we have every op up to our op
    /// number. An acknowledgement covers every earlier one, so one still
    /// waiting to be sent for this view is dropped in favour of this one.
    fn send_prepare_ok(&mut self) {
        let primary_id = self.primary_id();
        let view_number = self.view_number;
        self.outbox.retain(|(replica_id, message)| {
            !(*replica_id == primary_id
                && matches!(message, Message::PrepareOk { view_number: v, .. } if *v == view_number))
        });
        let message = Message::PrepareOk {
            view_number,
            op_number: self.op_number(),
            replica_id: self.self_id,
        };
        self.send_to_primary(message);
    }

    fn send_get_state(&mut self, op_number: OpNumber) {
        let primary_id = self.primary_id();
        self.send_get_state_to(primary_id, op_number);
    }

    fn send_get_state_to(&mut self, replica_id: ReplicaID, op_number: OpNumber) {
        let message = Message::GetState {
            replica_id: self.self_id,
            view_number: self.view_number,
            op_number,
        };
        self.send(replica_id, message);
    }

    fn send_to_primary(&mut self, message: MessageFor<SM>) {
        let primary_id = self.primary_id();
        self.send(primary_id, message);
    }

    fn send_to_others(&mut self, message: MessageFor<SM>) {
        for replica_id in 0..self.config.replicas().len() {
            if replica_id != self.self_id {
                self.send(replica_id, message.clone());
            }
        }
    }

    /// Queues a message. One that promises nothing about our durable
    /// state may leave before the step is persisted: a `Prepare` or
    /// `NewState` asks the receiver to hold entries, and it is the
    /// receiver's acknowledgement that must wait; a `Commit` names ops
    /// that are committed wherever we go, and a `NewChunk` a checkpoint of
    /// ops as committed; a `GetState`, `GetChunk` or `ConfirmView` asks; a
    /// `ConfirmViewOk` says we have announced no later view, which
    /// we do only once a write holds it. The rest carry our view, our log,
    /// or our acknowledgement, and wait.
    fn send(&mut self, replica_id: ReplicaID, message: MessageFor<SM>) {
        let early = matches!(
            message,
            Message::Prepare { .. }
                | Message::Commit { .. }
                | Message::ConfirmView { .. }
                | Message::ConfirmViewOk { .. }
                | Message::GetState { .. }
                | Message::NewState { .. }
                | Message::GetChunk { .. }
                | Message::NewChunk { .. }
        );
        if early {
            self.outbox_early.push((replica_id, message));
        } else {
            self.outbox.push((replica_id, message));
        }
    }

    /// Returns the ID of this replica.
    pub fn id(&self) -> ReplicaID {
        self.self_id
    }

    /// Returns `true` if this replica is the primary of its current view.
    pub fn is_primary(&self) -> bool {
        self.self_id == self.primary_id()
    }

    /// Returns the ID of the primary of the current view.
    pub fn primary_id(&self) -> ReplicaID {
        self.config.primary_id(self.view_number)
    }

    /// Returns the current view number.
    pub fn view_number(&self) -> ViewNumber {
        self.view_number
    }

    /// The latest view in which this replica's status was normal.
    pub fn last_normal_view(&self) -> ViewNumber {
        self.last_normal_view
    }

    /// What this replica is doing right now.
    pub fn status(&self) -> Status {
        self.status
    }

    /// Whether this replica is still recovering from a crash.
    pub fn is_recovering(&self) -> bool {
        self.status == Status::Recovering
    }

    /// Returns the commit number, i.e. the number of log entries that are
    /// committed.
    pub fn commit_number(&self) -> CommitID {
        self.commit_number
    }

    /// The number of committed ops the state machine has executed, which
    /// happens once the log that holds them is durable.
    pub fn applied(&self) -> OpNumber {
        self.applied
    }

    /// Returns the op number, i.e. that of the last log entry.
    pub fn op_number(&self) -> OpNumber {
        self.log_start + self.log.len()
    }

    /// The op number of the last entry compacted away; the log holds the
    /// entries after it.
    pub fn log_start(&self) -> OpNumber {
        self.log_start
    }

    /// The log entries after [`Replica::log_start`], in op number order.
    pub fn log(&self) -> &[LogEntry<SM::Input>] {
        &self.log
    }

    /// The log entries from `op_number` on, or from the first one held if
    /// `op_number` has been compacted.
    fn log_from(&self, op_number: OpNumber) -> &[LogEntry<SM::Input>] {
        let skip = op_number
            .saturating_sub(self.log_start + 1)
            .min(self.log.len());
        &self.log[skip..]
    }

    /// The client table, in client id order.
    pub fn client_table(&self) -> Vec<ClientRecord<SM::Output>> {
        let mut table: Vec<_> = self
            .clients
            .values()
            .filter_map(|state| state.record.clone())
            .collect();
        table.sort_unstable_by_key(|record| record.client_id);
        table
    }

    /// Returns the state machine, as far as the persisted writes have
    /// taken it.
    pub fn state_machine(&self) -> &SM {
        &self.state_machine
    }

    /// Messages that may be sent before the step is persisted, because
    /// they promise nothing about this replica's durable state. An owner
    /// that sends them first overlaps its own write with the receivers';
    /// one that skips this gets them from [`Replica::drain_messages`].
    pub fn drain_messages_before_persist(
        &mut self,
    ) -> std::vec::Drain<'_, (ReplicaID, MessageFor<SM>)> {
        self.round_unsent = false;
        self.outbox_early.drain(..)
    }

    /// Messages to send to other replicas: those that need not wait for a
    /// write, and those whose write [`Replica::persisted`] has released.
    pub fn drain_messages(&mut self) -> impl Iterator<Item = (ReplicaID, MessageFor<SM>)> + '_ {
        self.round_unsent = false;
        self.outbox_early
            .drain(..)
            .chain(self.outbox_ready.drain(..))
    }

    /// Replies to send to clients: for registrations and requests
    /// executed, for re-sent ones answered from the client table, for
    /// requests of evicted sessions, and for queries. They answer entries a
    /// handed-back write holds, or read a state executed from them, so the
    /// owner may send them before the next write.
    pub fn drain_replies(&mut self) -> std::vec::Drain<'_, Reply<SM::Output>> {
        self.replies.drain(..)
    }
}

// Compiles and runs the README's example with the doctests.
#[cfg(doctest)]
#[doc = include_str!("README.md")]
struct ReadmeExample;
