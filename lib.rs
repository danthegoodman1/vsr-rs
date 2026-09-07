//! Viewstamped Replication for Rust.
//!
//! A work-in-progress implementation of the protocol described in
//! "Viewstamped Replication Revisited" by Liskov and Cowling.
//!
//! The library does no I/O, keeps no clocks, and starts no threads. A
//! [`Replica`] and a [`Client`] are state machines that their owner steps:
//! hand them incoming messages with `on_message` and `on_reply`, tell them
//! time has passed with `on_idle`, and afterwards drain what they want sent
//! with `drain_messages`, `drain_replies`, and `drain`. The owner decides
//! how those get delivered, whether over sockets, through a simulated
//! network, or straight into another replica in a test.
//!
//! # Persistence
//!
//! The protocol keeps its state in memory, and the owner persists the part
//! of it that must survive a crash: after every step, before it delivers
//! what the step produced. That ordering is what makes the persistence
//! sound: an acknowledgement leaves only after the entry it covers is on
//! disk, and a view change is on disk before anything sent in the new view.
//! What to persist is described by [`PersistentState`], and
//! [`Replica::take_log_changes`] says which log entries a step touched so
//! that the owner writes only those. A replica comes back from a crash with
//! that state through [`Replica::restart`].
//!
//! The state machine runs behind the same ordering: a replica executes
//! committed operations when its replies are drained, after the owner has
//! persisted the step that committed them, so a state machine that
//! persists what it executes never gets ahead of the log.
//!
//! A replica that lost its disk comes back through [`Replica::recover`]
//! instead, which fetches the state from the others. One thing must survive
//! even that: the view number. Without it a replica can forget that it asked
//! for a view change and let two views run at once, as shown by Michael et
//! al. in "Recovering Shared Objects Without Stable Storage".
//!
//! # Compaction
//!
//! The log grows without bound until the owner compacts it with
//! [`Replica::compact`], which drops entries the state machine has made
//! durable. A replica that needs entries another one has compacted gets a
//! [`Checkpoint`] of that replica's state instead, through
//! [`StateMachine::snapshot`] and [`StateMachine::restore`].

use log::trace;
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Debug,
};

/// Identifies a client. Every client must have its own, and a client that
/// restarts must not reuse one, or the primary's client table takes its
/// first request for a re-send of an old one.
pub type ClientID = usize;

/// The number of log entries a replica has executed. The entries at
/// op numbers up to it are committed and never change.
pub type CommitID = usize;

/// The position of an entry in the log, counted from 1. A replica's op
/// number is that of its last entry.
pub type OpNumber = usize;

/// Identifies a replica: its index in the configuration's list of replicas.
pub type ReplicaID = usize;

/// Numbers the requests of one client, increasing with every request. The
/// client table keeps the latest per client to spot re-sends.
pub type RequestNumber = usize;

/// Numbers the views. The primary of view `v` is replica `v` modulo the
/// number of replicas, so the view number says who leads.
pub type ViewNumber = usize;

/// State machine.
///
/// The replica executes committed operations through it, in op number
/// order, when [`Replica::drain_replies`] or [`Replica::apply_committed`]
/// is called: after the owner has persisted the step. A state machine
/// that persists its state writes the op number it got with each
/// operation alongside, and gives that number back to
/// [`Replica::restart`], which executes what the state machine had not yet
/// made durable once more.
pub trait StateMachine {
    type Input: Clone + Debug;
    /// The result of applying an input. Replicas keep the latest result per
    /// client to answer a re-sent request without running it again.
    type Output: Clone + Debug;
    /// A copy of the whole state, for a replica that fell behind a log this
    /// one has compacted.
    type Snapshot: Clone + Debug;

    /// Executes the request in `entry`, committed as `op_number`, and
    /// returns its result.
    fn apply(&mut self, op_number: OpNumber, entry: &LogEntry<Self::Input>) -> Self::Output;

    /// A copy of the state after every operation applied so far.
    fn snapshot(&self) -> Self::Snapshot;

    /// Replaces the state with the one in `checkpoint`. A state machine
    /// that persists its state makes the checkpoint durable before it
    /// returns, together with the checkpoint's client table and op number:
    /// the replica's log now starts at the checkpoint, so
    /// [`Replica::restart`] has nothing to apply before it, and takes the
    /// client table back from the owner.
    fn restore(&mut self, checkpoint: Checkpoint<Self::Output, Self::Snapshot>);
}

/// Configuration.
#[derive(Clone, Debug)]
pub struct Config {
    /// IDs of all replicas (in sorted order).
    replicas: Vec<ReplicaID>,
    /// Idle periods a backup waits without hearing from the primary before
    /// it starts a view change, and a view change may take before the next
    /// one starts.
    primary_timeout: usize,
}

impl Config {
    pub fn new() -> Config {
        Config {
            replicas: Vec::new(),
            primary_timeout: 3,
        }
    }

    pub fn replicas(&self) -> &[ReplicaID] {
        &self.replicas
    }

    pub fn primary_id(&self, view_number: ViewNumber) -> ReplicaID {
        self.replicas[view_number % self.replicas.len()]
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
}

impl Default for Config {
    fn default() -> Config {
        Config::new()
    }
}

/// A log entry: the client request that was assigned this op number.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogEntry<Op> {
    pub client_id: ClientID,
    pub request_number: RequestNumber,
    pub op: Op,
}

/// The primary's reply to a client request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Reply<Output> {
    pub view_number: ViewNumber,
    pub client_id: ClientID,
    pub request_number: RequestNumber,
    pub result: Output,
}

/// What a replica remembers about a client: its latest executed request,
/// and the result, so that a re-sent request is answered without running
/// it again.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientRecord<Output> {
    pub client_id: ClientID,
    pub request_number: RequestNumber,
    pub reply: Output,
}

/// A replica's state after `op_number` operations: the state machine's
/// snapshot and the client table. Sent to a replica that needs entries the
/// sender has compacted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Checkpoint<Output, Snapshot> {
    /// The number of operations the state reflects.
    pub op_number: OpNumber,
    pub state: Snapshot,
    pub client_table: Vec<ClientRecord<Output>>,
}

/// What a stretch of log starts from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LogBase<Output, Snapshot> {
    /// The op number the entries start after. The receiver holds the
    /// entries up to it.
    Op(OpNumber),
    /// A checkpoint of the sender's state, which the entries follow. Sent
    /// when the sender has compacted entries the receiver needs.
    Checkpoint(Checkpoint<Output, Snapshot>),
}

impl<Output, Snapshot> LogBase<Output, Snapshot> {
    /// The op number the entries start after.
    pub fn start(&self) -> OpNumber {
        match self {
            LogBase::Op(op_number) => *op_number,
            LogBase::Checkpoint(checkpoint) => checkpoint.op_number,
        }
    }
}

/// A stretch of a replica's log: the entries after `base`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogSegment<Op, Output, Snapshot> {
    pub base: LogBase<Output, Snapshot>,
    pub entries: Vec<LogEntry<Op>>,
}

impl<Op, Output, Snapshot> LogSegment<Op, Output, Snapshot> {
    /// The op number the entries start after.
    pub fn start(&self) -> OpNumber {
        self.base.start()
    }

    /// The op number of the last entry.
    pub fn end(&self) -> OpNumber {
        self.start() + self.entries.len()
    }
}

/// What the owner persists after every step, before delivering what the
/// step produced, and hands back to [`Replica::restart`].
///
/// The counters and the client table are small and change rarely; the log
/// is written incrementally: [`Replica::take_log_changes`] says from which
/// op number the entries changed, and [`Replica::log_start`] how far the
/// log has been compacted. [`PersistentState::update_from`] is that
/// procedure for an owner that keeps a copy in memory.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PersistentState<Op, Output> {
    pub view_number: ViewNumber,
    /// The latest view in which the replica's status was normal, which
    /// ranks its log in a view change.
    pub last_normal_view: ViewNumber,
    pub commit_number: CommitID,
    /// The op number of the last entry compacted away; `log` holds the
    /// entries after it.
    pub log_start: OpNumber,
    pub log: Vec<LogEntry<Op>>,
    /// The latest executed request of each client, with its result.
    pub client_table: Vec<ClientRecord<Output>>,
    /// Whether the replica was still recovering from a disk loss. Such a
    /// replica holds nothing it may act on, and must recover again.
    pub recovering: bool,
}

impl<Op: Clone, Output: Clone> PersistentState<Op, Output> {
    /// The state of a replica that has done nothing yet.
    pub fn empty() -> PersistentState<Op, Output> {
        PersistentState {
            view_number: 0,
            last_normal_view: 0,
            commit_number: 0,
            log_start: 0,
            log: Vec::new(),
            client_table: Vec::new(),
            recovering: false,
        }
    }

    /// Brings this copy of `replica`'s persistent state up to date after
    /// a step, and takes the log change marker: the compacted prefix goes
    /// first, then the entries from the marker on, then the counters and
    /// the client table.
    pub fn update_from<SM>(&mut self, replica: &mut Replica<SM>)
    where
        SM: StateMachine<Input = Op, Output = Output>,
    {
        let log_start = replica.log_start();
        if log_start > self.log_start {
            let dropped = (log_start - self.log_start).min(self.log.len());
            self.log.drain(..dropped);
            self.log_start = log_start;
        }
        if let Some(from) = replica.take_log_changes() {
            let keep = from.saturating_sub(self.log_start + 1).min(self.log.len());
            self.log.truncate(keep);
            self.log.extend_from_slice(replica.log_from(from));
        }
        self.view_number = replica.view_number();
        self.last_normal_view = replica.last_normal_view();
        self.commit_number = replica.commit_number();
        self.client_table = replica.client_table();
        self.recovering = replica.is_recovering();
    }
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
/// The type is generic over the state machine's input, output, and
/// snapshot; [`MessageFor`] names it from the state machine.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Message<Op, Output, Snapshot> {
    /// A client asks the primary to execute `op`. Backups ignore it. A
    /// request number no larger than the client's latest in the primary's
    /// client table is a re-send: it is answered from the table if it has
    /// executed, dropped otherwise.
    Request {
        client_id: ClientID,
        request_number: RequestNumber,
        op: Op,
    },
    /// The primary replicates the request it appended as `op_number` to
    /// the backups, and tells them how far it has committed so they can
    /// commit too. Backups accept it only in order: a gap means state
    /// transfer first. A `Prepare` for an op a backup already has is a
    /// re-send, and is acknowledged again.
    Prepare {
        view_number: ViewNumber,
        op_number: OpNumber,
        /// The client request being replicated.
        client_id: ClientID,
        request_number: RequestNumber,
        op: Op,
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
        view_number: ViewNumber,
        segment: LogSegment<Op, Output, Snapshot>,
        /// The sender's commit number.
        commit_number: CommitID,
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
        segment: LogSegment<Op, Output, Snapshot>,
        commit_number: CommitID,
    },
    /// The new primary starts `view_number` with the log it chose. Backups
    /// replace their log with it, commit up to `commit_number`, and
    /// acknowledge the rest. A backup that has committed less than the
    /// segment starts after fetches the primary's checkpoint first.
    StartView {
        view_number: ViewNumber,
        /// The primary's log, after what it has compacted.
        segment: LogSegment<Op, Output, Snapshot>,
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
        state: Option<RecoveryState<Op, Output, Snapshot>>,
    },
}

/// [`Message`] for a given state machine.
pub type MessageFor<SM> = Message<
    <SM as StateMachine>::Input,
    <SM as StateMachine>::Output,
    <SM as StateMachine>::Snapshot,
>;

/// The primary's state in a `RecoveryResponse`: its whole log, with a
/// checkpoint standing in for what it has compacted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoveryState<Op, Output, Snapshot> {
    pub segment: LogSegment<Op, Output, Snapshot>,
    pub commit_number: CommitID,
}

/// Client.
///
/// A client sends one request at a time to the primary and waits for the
/// reply. Its owner delivers what [`Client::drain`] yields, feeds replies
/// to [`Client::on_reply`], and calls [`Client::on_idle`] now and then so a
/// request that got no reply is re-sent.
#[derive(Debug)]
pub struct Client<Op> {
    config: Config,
    client_id: ClientID,
    /// The latest view this client has heard of, which tells it who the
    /// primary is.
    view_number: ViewNumber,
    next_request_number: RequestNumber,
    /// The request awaiting a reply, kept so it can be re-sent.
    pending: Option<(RequestNumber, Op)>,
    /// Requests to send: the replica, the request number, and the op.
    outbox: Vec<(ReplicaID, RequestNumber, Op)>,
}

impl<Op: Clone + Debug> Client<Op> {
    pub fn new(client_id: ClientID, config: Config) -> Client<Op> {
        Client {
            config,
            client_id,
            view_number: 0,
            next_request_number: 0,
            pending: None,
            outbox: Vec::new(),
        }
    }

    pub fn client_id(&self) -> ClientID {
        self.client_id
    }

    pub fn view_number(&self) -> ViewNumber {
        self.view_number
    }

    /// Sends `op` to the primary and returns the request number it was given.
    pub fn on_request(&mut self, op: Op) -> RequestNumber {
        trace!("Client {} <- {:?}", self.client_id, op);
        let request_number = self.next_request_number;
        self.next_request_number += 1;
        self.pending = Some((request_number, op.clone()));
        let primary_id = self.config.primary_id(self.view_number);
        self.outbox.push((primary_id, request_number, op));
        request_number
    }

    /// Handles a reply for `request_number`, sent in view `view_number`.
    /// Every reply tells the client the current view, and with it the
    /// primary to send the next request to. Returns whether the reply
    /// answers the pending request; a duplicate or a reply for an earlier
    /// request does not.
    pub fn on_reply(&mut self, request_number: RequestNumber, view_number: ViewNumber) -> bool {
        if view_number > self.view_number {
            self.view_number = view_number;
        }
        if self
            .pending
            .as_ref()
            .is_some_and(|(pending, _)| *pending == request_number)
        {
            self.pending = None;
            true
        } else {
            false
        }
    }

    /// Called when no reply has arrived in a while. Re-sends the pending
    /// request, if any, to every replica: the primary may have changed
    /// without this client knowing, and backups ignore client requests.
    pub fn on_idle(&mut self) {
        let Some((request_number, op)) = &self.pending else {
            return;
        };
        trace!(
            "Client {} re-sends request {request_number}",
            self.client_id
        );
        for replica_id in self.config.replicas() {
            self.outbox.push((*replica_id, *request_number, op.clone()));
        }
    }

    /// Messages to send, with the replica each one goes to. A client only
    /// sends requests, so the message's other type parameters are whatever
    /// the receiving replicas use.
    pub fn drain<Output, Snapshot>(
        &mut self,
    ) -> impl Iterator<Item = (ReplicaID, Message<Op, Output, Snapshot>)> + '_ {
        let client_id = self.client_id;
        self.outbox
            .drain(..)
            .map(move |(replica_id, request_number, op)| {
                (
                    replica_id,
                    Message::Request {
                        client_id,
                        request_number,
                        op,
                    },
                )
            })
    }
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

/// What a replica remembers about a client: its latest executed request
/// and the result. Requests still in the log's uncommitted suffix are
/// found there.
#[derive(Clone, Debug)]
struct ClientEntry<Output> {
    request_number: RequestNumber,
    reply: Output,
}

/// What a replica reported in a `DoViewChange` message: the view it was
/// last normal in, its log, and its commit number.
#[derive(Debug)]
struct DoViewChange<Op, Output, Snapshot> {
    last_normal_view: ViewNumber,
    segment: LogSegment<Op, Output, Snapshot>,
    commit_number: CommitID,
}

type DoViewChangeFor<SM> = DoViewChange<
    <SM as StateMachine>::Input,
    <SM as StateMachine>::Output,
    <SM as StateMachine>::Snapshot,
>;

/// What a recovering replica keeps of a `RecoveryResponse`: the sender's
/// view, and its state if it was the primary of that view.
#[derive(Debug)]
struct RecoveryResponse<Op, Output, Snapshot> {
    view_number: ViewNumber,
    state: Option<RecoveryState<Op, Output, Snapshot>>,
}

type RecoveryResponseFor<SM> = RecoveryResponse<
    <SM as StateMachine>::Input,
    <SM as StateMachine>::Output,
    <SM as StateMachine>::Snapshot,
>;

/// Replica.
///
/// The owner feeds it messages with [`Replica::on_message`], calls
/// [`Replica::on_idle`] at a regular interval, persists what
/// [`PersistentState`] describes, and after each of those delivers what
/// [`Replica::drain_messages`] and [`Replica::drain_replies`] yield.
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
    /// commit number. Committed ops are executed when the replies are
    /// drained, after the owner has persisted the step.
    applied: OpNumber,
    /// The committed ops still to be replied to, as inclusive ranges of op
    /// numbers: those this replica committed as the primary.
    reply_ranges: Vec<(OpNumber, OpNumber)>,
    /// The op number of the last entry compacted away. `log` holds the
    /// entries after it, so entry `i` of `log` is op `log_start + i + 1`.
    log_start: OpNumber,
    log: Vec<LogEntry<SM::Input>>,
    /// The lowest op number whose entry changed since the owner last
    /// asked, see [`Replica::take_log_changes`].
    log_changed_from: Option<OpNumber>,
    /// For each uncommitted op number, the replicas that have acknowledged it.
    acks: BTreeMap<OpNumber, BTreeSet<ReplicaID>>,
    /// The client table: the latest executed request of each client and
    /// its result, so that a re-sent request is not run twice. Requests
    /// not yet executed are in the log after `applied`.
    client_table: BTreeMap<ClientID, ClientEntry<SM::Output>>,
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
    /// The nonce of the recovery under way, if any.
    recovery_nonce: u64,
    /// `RecoveryResponse`s received for it, by sender.
    recovery_responses: BTreeMap<ReplicaID, RecoveryResponseFor<SM>>,
    outbox: Vec<(ReplicaID, MessageFor<SM>)>,
    replies: Vec<Reply<SM::Output>>,
}

impl<SM: StateMachine> Replica<SM> {
    pub fn new(self_id: ReplicaID, config: Config, state_machine: SM) -> Replica<SM> {
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
            acks: BTreeMap::new(),
            client_table: BTreeMap::new(),
            heard_from_primary: true,
            idle_periods_waiting: 0,
            view_change_attempts: 0,
            idle_periods_stable: 0,
            start_view_change_from: BTreeSet::new(),
            do_view_change_sent: false,
            do_view_change_from: BTreeMap::new(),
            catching_up: None,
            recovery_nonce: 0,
            recovery_responses: BTreeMap::new(),
            outbox: Vec::new(),
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
    /// first `applied` operations, with the client table as of then. The
    /// replica applies the committed ones after that once more, so
    /// `applied` must be at least `log_start`.
    ///
    /// A state machine ahead of the log's commit number holds a checkpoint
    /// it made durable before the log was persisted after it, as
    /// [`StateMachine::restore`] requires: the log's entries up to there
    /// give way to the checkpoint, and the ones after it, which this
    /// replica acknowledged, stay.
    ///
    /// A replica that was normal in its view resumes there: its log is the
    /// one it acknowledged, and if it was the primary the backups will
    /// acknowledge its uncommitted entries again. One that went down in
    /// the middle of a view change enters that view change again. One that
    /// was still recovering starts a new recovery with `nonce`, keeping
    /// the checkpoint its state machine may hold.
    pub fn restart(
        self_id: ReplicaID,
        config: Config,
        state_machine: SM,
        applied: OpNumber,
        state: PersistentState<SM::Input, SM::Output>,
        nonce: u64,
    ) -> Replica<SM> {
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
        replica.client_table = state
            .client_table
            .into_iter()
            .map(|record| {
                (
                    record.client_id,
                    ClientEntry {
                        request_number: record.request_number,
                        reply: record.reply,
                    },
                )
            })
            .collect();
        assert!(replica.commit_number <= replica.op_number());
        replica.apply_committed();
        trace!(
            "Replica {self_id} restarts in view {} with {} ops, {} committed, {applied} applied",
            replica.view_number,
            replica.op_number(),
            replica.commit_number
        );
        if replica.status == Status::Recovering {
            return replica;
        }
        if state.last_normal_view == state.view_number {
            if replica.is_primary() {
                for op_number in replica.commit_number + 1..=replica.op_number() {
                    replica.acks.insert(op_number, BTreeSet::from([self_id]));
                }
            }
        } else {
            replica.start_view_change(state.view_number);
        }
        replica
    }

    /// Everything the owner persists, see [`PersistentState`].
    pub fn persistent_state(&self) -> PersistentState<SM::Input, SM::Output> {
        PersistentState {
            view_number: self.view_number,
            last_normal_view: self.last_normal_view,
            commit_number: self.commit_number,
            log_start: self.log_start,
            log: self.log.clone(),
            client_table: self.client_table(),
            recovering: self.status == Status::Recovering,
        }
    }

    /// The lowest op number whose entry changed since the last call, if
    /// any changed. The owner drops what it has persisted from that op
    /// number on and writes [`Replica::log_from`] it instead. Compaction
    /// is separate: the owner also drops what is at or below
    /// [`Replica::log_start`].
    pub fn take_log_changes(&mut self) -> Option<OpNumber> {
        self.log_changed_from.take()
    }

    /// Drops the log entries up to `op_number` from memory. The op number
    /// must be at most what the state machine has made durable, since
    /// [`Replica::restart`] cannot apply an entry that is gone; anything
    /// beyond what the state machine has applied is not compacted. A
    /// replica that needs the dropped entries gets a checkpoint instead.
    pub fn compact(&mut self, op_number: OpNumber) {
        let op_number = op_number.min(self.applied);
        if op_number <= self.log_start {
            return;
        }
        self.log.drain(..op_number - self.log_start);
        self.log_start = op_number;
    }

    /// Executes the committed operations the state machine has not yet, in
    /// order, and produces the replies for those this replica committed as
    /// the primary. [`Replica::drain_replies`] calls it, so an owner that
    /// drains replies after every step, once the step is persisted, need
    /// not.
    pub fn apply_committed(&mut self) {
        while self.applied < self.commit_number {
            let op_number = self.applied + 1;
            let entry = &self.log[op_number - self.log_start - 1];
            let result = self.state_machine.apply(op_number, entry);
            let (client_id, request_number) = (entry.client_id, entry.request_number);
            self.applied = op_number;
            self.client_table.insert(
                client_id,
                ClientEntry {
                    request_number,
                    reply: result.clone(),
                },
            );
            if self
                .reply_ranges
                .iter()
                .any(|(from, to)| (*from..=*to).contains(&op_number))
            {
                self.replies.push(Reply {
                    view_number: self.view_number,
                    client_id,
                    request_number,
                    result,
                });
            }
        }
        self.reply_ranges.clear();
    }

    /// The main entry point to replica logic.
    pub fn on_message(&mut self, message: MessageFor<SM>) {
        trace!("Replica {} <- {:?}", self.self_id, message);
        // A recovering replica knows nothing it could safely act on: not
        // which view is current, not what it acknowledged before the crash.
        // Until it has recovered, only recovery responses matter.
        if self.status == Status::Recovering && !matches!(message, Message::RecoveryResponse { .. })
        {
            return;
        }
        match message {
            Message::Request {
                client_id,
                request_number,
                op,
            } => {
                self.on_request(client_id, request_number, op);
            }
            Message::Prepare {
                view_number,
                op_number,
                client_id,
                request_number,
                op,
                commit_number,
            } => {
                let entry = LogEntry {
                    client_id,
                    request_number,
                    op,
                };
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
                view_number,
                segment,
                commit_number,
            } => {
                self.on_new_state(view_number, segment, commit_number);
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

    /// The client sends a `Request` message to the primary, which replicates
    /// the operation to the other replicas.
    fn on_request(&mut self, client_id: ClientID, request_number: RequestNumber, op: SM::Input) {
        // Backups ignore client requests; clients send to every replica
        // when they re-send, in case the primary has changed. A primary that
        // is not in normal status drops the request too, and the client's
        // re-send will find it once it is.
        if !self.is_primary() || self.status != Status::Normal {
            return;
        }
        // Consult the client table. A request number no larger than the
        // latest executed one from this client is a re-send: if it is the
        // latest request, re-send the reply, otherwise drop it.
        if let Some(entry) = self.client_table.get(&client_id) {
            if request_number < entry.request_number {
                return;
            }
            if request_number == entry.request_number {
                let reply = Reply {
                    view_number: self.view_number,
                    client_id,
                    request_number,
                    result: entry.reply.clone(),
                };
                self.replies.push(reply);
                return;
            }
        }
        // A request already in the log but not yet executed is dropped
        // too; the reply will follow once it is.
        let in_progress = self.log[self.applied - self.log_start..]
            .iter()
            .any(|entry| entry.client_id == client_id && entry.request_number >= request_number);
        if in_progress {
            return;
        }
        self.append_to_log(LogEntry {
            client_id,
            request_number,
            op: op.clone(),
        });
        // And then register our own acknowledgement.
        let op_number = self.op_number();
        self.acks.insert(op_number, BTreeSet::from([self.self_id]));
        // Send a prepare message to all the replicas.
        self.send_to_others(Message::Prepare {
            view_number: self.view_number,
            op_number,
            client_id,
            request_number,
            op,
            commit_number: self.commit_number,
        });
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
        if view_number != self.view_number || !self.is_primary() || self.status != Status::Normal {
            return;
        }
        if op_number <= self.commit_number {
            return; // already committed
        }
        // Register the acknowledgement for every uncommitted op it covers.
        // A quorum is a set of distinct replicas: the same backup
        // acknowledging twice, because the network replayed its message or
        // because it answered a re-sent `Prepare`, still counts once.
        let quorum = self.config.quorum();
        let mut committed = None;
        for (acked_op_number, acked_by) in self.acks.range_mut(..=op_number) {
            acked_by.insert(replica_id);
            if acked_by.len() >= quorum {
                committed = Some(*acked_op_number);
            }
        }
        let Some(committed) = committed else {
            return;
        };
        // A quorum for an op means the op and all earlier ones are
        // committed. Earlier operations may not have reached a quorum on
        // their own, for example because their `PrepareOk` messages were
        // lost or overtaken, so commit everything up to it, in order.
        self.commit_up_to(committed, true);
        self.acks
            .retain(|acked_op_number, _| *acked_op_number > committed);
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
            view_number,
            segment: self.segment_from(op_number),
            commit_number: self.commit_number,
        };
        self.send(replica_id, message);
    }

    /// The log after `op_number`, or, if that has been compacted, a
    /// checkpoint of our state and the log after it.
    fn segment_from(&self, op_number: OpNumber) -> LogSegment<SM::Input, SM::Output, SM::Snapshot> {
        if op_number >= self.log_start {
            LogSegment {
                base: LogBase::Op(op_number),
                entries: self.log_from(op_number + 1).to_vec(),
            }
        } else {
            LogSegment {
                base: LogBase::Checkpoint(self.checkpoint()),
                entries: self.log_from(self.applied + 1).to_vec(),
            }
        }
    }

    /// A replica receives a `NewState` message in response to a
    /// `GetState` message it sent itself to catch up on its log.
    fn on_new_state(
        &mut self,
        view_number: ViewNumber,
        segment: LogSegment<SM::Input, SM::Output, SM::Snapshot>,
        commit_number: CommitID,
    ) {
        if view_number != self.view_number {
            return;
        }
        self.heard_from_primary = true;
        match self.status {
            Status::StateTransfer => {
                // We are filling a gap within our view. The reply may
                // answer an earlier `GetState` that the network delayed or
                // replayed, in which case it starts before our current op
                // number. We can still use whatever it has beyond our log,
                // since within a view the overlapping entries are
                // identical. A reply that starts past our log or that ends
                // inside it is of no use, so keep waiting for another.
                if !self.install_segment_in_view(segment) {
                    return;
                }
                self.commit_up_to(commit_number, false);
                self.status = Status::Normal;
            }
            Status::ViewChange if self.catching_up.is_some() => {
                // We asked for everything after our commit number: what we
                // have beyond that is from an earlier view and never
                // committed, so it is replaced by what the reply holds.
                if !self.install_segment_from_commit(segment) {
                    return;
                }
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
            _ => return,
        }
        self.send_prepare_ok();
    }

    /// Installs a segment received within our view, which extends our log
    /// if it reaches beyond it. Returns whether it did.
    fn install_segment_in_view(
        &mut self,
        segment: LogSegment<SM::Input, SM::Output, SM::Snapshot>,
    ) -> bool {
        let op_number = self.op_number();
        if segment.end() <= op_number {
            return false;
        }
        match segment.base {
            // A checkpoint beyond our log replaces it; one within our log
            // covers ops we hold already, so only the entries matter.
            LogBase::Checkpoint(checkpoint) if checkpoint.op_number > op_number => {
                self.install_checkpoint(checkpoint, segment.entries);
            }
            base => {
                if base.start() > op_number {
                    return false;
                }
                self.merge_log(base.start(), segment.entries);
            }
        }
        true
    }

    /// Installs a segment we asked for from our commit number, which
    /// replaces everything after it. Returns whether the segment reaches
    /// our commit number; one that starts beyond it answers a request we
    /// made with a higher commit number, and is of no use.
    fn install_segment_from_commit(
        &mut self,
        segment: LogSegment<SM::Input, SM::Output, SM::Snapshot>,
    ) -> bool {
        match segment.base {
            LogBase::Checkpoint(checkpoint) if checkpoint.op_number >= self.commit_number => {
                self.install_checkpoint(checkpoint, segment.entries);
            }
            base => {
                if base.start() > self.commit_number {
                    return false;
                }
                self.merge_log(base.start(), segment.entries);
            }
        }
        true
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
        segment: LogSegment<SM::Input, SM::Output, SM::Snapshot>,
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
        self.view_number = view_number;
        if !self.install_segment_from_commit(segment) {
            self.catch_up_with_view(view_number);
            return;
        }
        self.commit_up_to(commit_number, false);
        self.enter_normal();
        self.acks.clear();
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

    fn clear_view_change_state(&mut self) {
        self.start_view_change_from.clear();
        self.do_view_change_sent = false;
        self.do_view_change_from.clear();
        self.catching_up = None;
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
        if !self.install_segment_from_commit(segment) {
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
        self.acks.clear();
        for op_number in self.commit_number + 1..=self.op_number() {
            self.acks.insert(op_number, BTreeSet::from([self.self_id]));
        }
        for replica_id in self.config.replicas().to_vec() {
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
        let state = self.is_primary().then(|| RecoveryState {
            segment: self.segment_from(0),
            commit_number: self.commit_number,
        });
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
        state: Option<RecoveryState<SM::Input, SM::Output, SM::Snapshot>>,
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
        self.view_number = latest_view;
        // The state machine may hold a checkpoint from an earlier attempt,
        // which the log keeps up to; the primary's log covers it either
        // way.
        assert!(self.install_segment_from_commit(state.segment));
        self.commit_up_to(state.commit_number, false);
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
        for replica_id in self.config.replicas().to_vec() {
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
    /// its `GetState` or the reply was lost, the primary re-sends the
    /// `Prepare` for every op that has not committed yet in case a `Prepare`
    /// or its `PrepareOk` was lost, replicas in a view change re-send
    /// their view change messages, and a recovering replica re-sends
    /// `Recovery` to whoever has not answered.
    ///
    /// And they drive the timers: a backup that has not heard from the
    /// primary for `Config::primary_timeout` idle periods starts a view
    /// change, and a view change that takes as long to complete is
    /// followed by another, waiting twice as long each time.
    pub fn on_idle(&mut self) {
        match self.status {
            Status::Normal if self.is_primary() => {
                self.note_stable();
                let view_number = self.view_number;
                let commit_number = self.commit_number;
                self.send_to_others(Message::Commit {
                    view_number,
                    commit_number,
                });
                for op_number in commit_number + 1..=self.op_number() {
                    let entry = self.entry(op_number).clone();
                    self.send_to_others(Message::Prepare {
                        view_number,
                        op_number,
                        client_id: entry.client_id,
                        request_number: entry.request_number,
                        op: entry.op,
                        commit_number,
                    });
                }
            }
            Status::Recovering => self.send_recovery(),
            Status::Normal | Status::StateTransfer => {
                if self.status == Status::StateTransfer {
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
                    let op_number = self.commit_number;
                    self.send_get_state_to(replica_id, op_number);
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
        self.mark_log_changed(self.op_number() + 1);
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
    }

    /// Replaces our state with `checkpoint`, and the log with `entries`,
    /// which follow it. The checkpoint must be at or beyond our commit
    /// number, so that nothing executed here is undone.
    fn install_checkpoint(
        &mut self,
        checkpoint: Checkpoint<SM::Output, SM::Snapshot>,
        entries: Vec<LogEntry<SM::Input>>,
    ) {
        assert!(checkpoint.op_number >= self.commit_number);
        trace!(
            "Replica {} installs a checkpoint at op {} with {} entries after it",
            self.self_id,
            checkpoint.op_number,
            entries.len()
        );
        self.client_table = checkpoint
            .client_table
            .iter()
            .map(|record| {
                (
                    record.client_id,
                    ClientEntry {
                        request_number: record.request_number,
                        reply: record.reply.clone(),
                    },
                )
            })
            .collect();
        self.commit_number = checkpoint.op_number;
        self.applied = checkpoint.op_number;
        self.reply_ranges.clear();
        self.log_start = checkpoint.op_number;
        self.log = entries;
        self.mark_log_changed(self.log_start + 1);
        self.state_machine.restore(checkpoint);
    }

    /// Our state after every executed op, for a replica that has fallen
    /// behind what we have compacted.
    fn checkpoint(&self) -> Checkpoint<SM::Output, SM::Snapshot> {
        Checkpoint {
            op_number: self.applied,
            state: self.state_machine.snapshot(),
            client_table: self.client_table(),
        }
    }

    /// Commits every op up to `commit_number`; they are executed, and
    /// replied to if `reply` is set, when the replies are drained. The
    /// commit number never moves backwards.
    fn commit_up_to(&mut self, commit_number: CommitID, reply: bool) {
        if commit_number <= self.commit_number {
            return;
        }
        if reply {
            self.reply_ranges
                .push((self.commit_number + 1, commit_number));
        }
        self.commit_number = commit_number;
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
        for replica_id in self.config.replicas().to_vec() {
            if replica_id != self.self_id {
                self.send(replica_id, message.clone());
            }
        }
    }

    fn send(&mut self, replica_id: ReplicaID, message: MessageFor<SM>) {
        self.outbox.push((replica_id, message));
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

    /// The number of committed ops the state machine has executed, see
    /// [`Replica::apply_committed`].
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
    pub fn log_from(&self, op_number: OpNumber) -> &[LogEntry<SM::Input>] {
        let skip = op_number
            .saturating_sub(self.log_start + 1)
            .min(self.log.len());
        &self.log[skip..]
    }

    /// The client table: the latest executed request of each client, with
    /// its result.
    pub fn client_table(&self) -> Vec<ClientRecord<SM::Output>> {
        self.client_table
            .iter()
            .map(|(client_id, entry)| ClientRecord {
                client_id: *client_id,
                request_number: entry.request_number,
                reply: entry.reply.clone(),
            })
            .collect()
    }

    /// Returns the state machine, as far as [`Replica::apply_committed`]
    /// has taken it.
    pub fn state_machine(&self) -> &SM {
        &self.state_machine
    }

    /// Messages to send to other replicas.
    pub fn drain_messages(&mut self) -> std::vec::Drain<'_, (ReplicaID, MessageFor<SM>)> {
        self.outbox.drain(..)
    }

    /// Executes the committed operations, see [`Replica::apply_committed`],
    /// and returns the replies to send to clients.
    pub fn drain_replies(&mut self) -> std::vec::Drain<'_, Reply<SM::Output>> {
        self.apply_committed();
        self.replies.drain(..)
    }
}
