//! Properties are invariants checked after every tick and at the
//! end of the run.

use crate::disk::Disk;
use crate::network::message_kind;
use crate::state_machine::{Accumulator, Msg, Op};
use anyhow::{bail, ensure, Result};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use vsr_rs::{
    ClientID, ClientRecord, Config, LogEntry, LogSegment, Message, OpNumber, PersistentState,
    QueryNumber, Replica, ReplicaID, Reply, RequestNumber, StateMachine,
};

/// Read-only view of the simulated system handed to properties.
pub struct SimContext<'a> {
    pub tick: u64,
    pub config: &'a Config,
    /// What the committed log does, fed up to `committed`.
    pub model: &'a Model,
    pub replicas: &'a [Replica<Accumulator>],
    /// Each replica's disk.
    pub disks: &'a [Disk],
    /// Every op committed so far, in op number order: the simulator
    /// records each one from the log of the replica that committed it, at
    /// the tick it did, before any replica can compact it. Op `n` is at
    /// index `n - 1`.
    pub committed: &'a [LogEntry<Op>],
    /// Replies the clients have received, in order.
    pub replies: &'a [Reply<i64>],
    /// The queries the clients completed, in order.
    pub answers: &'a [Answer],
    /// The replicas that must converge: all of them during the safety
    /// phase, the liveness core afterwards.
    pub core: &'a [usize],
}

/// A query a client completed.
#[derive(Clone, Debug)]
pub struct Answer {
    pub tick: u64,
    pub client_id: ClientID,
    pub query_number: QueryNumber,
    /// The most requests executed in what any client had completed when
    /// the query was submitted: its requests, and the states its queries
    /// read.
    pub floor: i64,
    /// The most requests executed among those its client had completed
    /// when the query completed: those the client submitted before it.
    pub own: i64,
    /// The number of requests executed in the state the query read.
    pub result: i64,
}

pub trait Property {
    fn name(&self) -> &'static str;

    /// Called after every tick.
    fn check(&mut self, ctx: &SimContext) -> Result<()>;

    /// Called before each round of the replicas' steps, after whatever
    /// came before them in the tick: a step can lose power and take its
    /// replica's commit number with it.
    fn before_steps(&mut self, _ctx: &SimContext) -> Result<()> {
        Ok(())
    }

    /// Called once at the end of the run, after the network has been drained
    /// with faults disabled.
    fn finalize(&mut self, _ctx: &SimContext) -> Result<()> {
        Ok(())
    }

    /// Called when a replica is rebuilt as something other than what it
    /// was in memory: with no memory at all, or from a disk behind its
    /// commit number. Whatever the property tracked about that replica
    /// starts over.
    fn on_restart(&mut self, _replica_id: usize) {}

    /// Called as replica `id` sends `message`, with the replica and its
    /// disk as of then.
    fn on_send(
        &mut self,
        _id: ReplicaID,
        _replica: &Replica<Accumulator>,
        _disk: &PersistentState<Op>,
        _message: &Msg,
    ) -> Result<()> {
        Ok(())
    }
}

/// The default property set.
pub fn default_properties() -> Vec<Box<dyn Property>> {
    vec![
        Box::new(CommitNumberMonotonic::default()),
        Box::new(StateMatchesCommittedLog::default()),
        Box::new(CommittedPrefixAgreement::default()),
        Box::new(NoDuplicateOps::default()),
        Box::new(RepliesMatchCommits::default()),
        Box::new(LinearizableQueries::default()),
        Box::new(Durability::default()),
        Box::new(ExecutedOpsDurable::default()),
        Box::new(DurablePromise),
        Box::new(Convergence),
    ]
}

/// Every committed op is on enough disks to survive any view change: every
/// quorum that the replicas whose disks are not recovering could form must
/// include a disk that holds it. With nobody recovering that is a majority.
/// A recovering replica holds nothing and takes part in no quorum, so it
/// counts on neither side. A primary commits only on a quorum of
/// `PrepareOk` messages, and a replica sends one only once its disk holds
/// the op, so this must hold as soon as the commit happens, on whichever
/// replica committed it: it is checked before every round of steps, which
/// can lose power and the commit number with it, and after every tick.
/// Committed prefixes are never truncated, so each committed op needs
/// checking once per replica.
#[derive(Default)]
pub struct Durability {
    /// Per replica: number of committed ops already verified.
    verified: Vec<usize>,
}

impl Property for Durability {
    fn name(&self) -> &'static str {
        "durability"
    }

    fn on_restart(&mut self, replica_id: usize) {
        if let Some(verified) = self.verified.get_mut(replica_id) {
            *verified = 0;
        }
    }

    fn before_steps(&mut self, ctx: &SimContext) -> Result<()> {
        self.check(ctx)
    }

    fn check(&mut self, ctx: &SimContext) -> Result<()> {
        self.verified.resize(ctx.replicas.len(), 0);
        let quorum = ctx.replicas.len() / 2 + 1;
        let participants = ctx
            .disks
            .iter()
            .filter(|disk| !disk.state.recovering)
            .count();
        let needed = (participants + 1).saturating_sub(quorum);
        for (id, replica) in ctx.replicas.iter().enumerate() {
            let commit = replica.commit_number();
            for op_number in self.verified[id] + 1..=commit {
                let entry = &ctx.committed[op_number - 1];
                let copies = ctx
                    .disks
                    .iter()
                    .filter(|disk| !disk.state.recovering && disk.holds(op_number, entry))
                    .count();
                ensure!(
                    copies >= needed,
                    "tick {}: replica {id} committed op {op_number} held by {copies} of {participants} disks not recovering, {needed} needed to meet every quorum of {quorum}",
                    ctx.tick
                );
            }
            self.verified[id] = commit;
        }
        Ok(())
    }
}

/// Every op a replica has executed is on its disk: in the state machine's
/// flush, or in the log as memory holds it. Otherwise a state machine that
/// persists what it executes could hold an op that a restart cannot find
/// in the log. Once verified, an op stays so: the disk drops a log entry
/// only after the state machine has flushed it. It holds after each step,
/// not before: a replica that restored a checkpoint in a delivery has its
/// owner make it durable as its step begins.
#[derive(Default)]
pub struct ExecutedOpsDurable {
    /// Per replica: the ops already verified.
    verified: Vec<OpNumber>,
}

impl Property for ExecutedOpsDurable {
    fn name(&self) -> &'static str {
        "executed-ops-durable"
    }

    fn on_restart(&mut self, replica_id: usize) {
        if let Some(verified) = self.verified.get_mut(replica_id) {
            *verified = 0;
        }
    }

    fn check(&mut self, ctx: &SimContext) -> Result<()> {
        self.verified.resize(ctx.replicas.len(), 0);
        for (id, replica) in ctx.replicas.iter().enumerate() {
            let disk = &ctx.disks[id];
            let applied = replica.applied();
            for op_number in self.verified[id].max(disk.applied) + 1..=applied {
                let entry = op_number
                    .checked_sub(replica.log_start() + 1)
                    .and_then(|index| replica.log().get(index));
                ensure!(
                    entry.is_some_and(|entry| disk.holds(op_number, entry)),
                    "tick {}: replica {id} executed op {op_number}, which its disk does not hold",
                    ctx.tick
                );
            }
            self.verified[id] = applied;
        }
        Ok(())
    }
}

/// A replica sends these only for state on its disk, which each message is
/// checked against as it leaves:
///
/// - `PrepareOk` for op `n` in view `v`: the disk was last normal in `v`
///   and holds the replica's uncommitted entries up to op `n`, or was last
///   normal in a later view. A write can be out while the replica goes on
///   into a later view, whose log replaces the one the acknowledgement
///   covers; once the replica was last normal after `v`, the disk's log of
///   view `v` is the reference instead of the replica's.
/// - `StartViewChange`, `DoViewChange`, and `StartView` for view `v`: the
///   disk's view is at least `v`.
/// - `DoViewChange` and `StartView`: the disk holds the log they carry.
/// - `Recovery` for view `v`: the disk is recovering, in `v` or later.
/// - `RecoveryResponse` for view `v`: the disk is recovering no more, its
///   view is at least `v`, and it holds the log a primary's response
///   carries.
///
/// A later delivery in the same step can supersede a message the step
/// produced earlier and change what it described: a `DoViewChange` for `v`
/// once view `v` starts, a `StartView` or a primary's `RecoveryResponse`
/// for `v` once a later view does, a `Recovery` once the recovery
/// completes. The disk then holds the later state, which the check takes
/// instead. `Prepare`, `Commit`, `GetState`, and `NewState` ask for or
/// report state the receiver acts on, and may leave before the write.
pub struct DurablePromise;

impl Property for DurablePromise {
    fn name(&self) -> &'static str {
        "durable-promise"
    }

    fn check(&mut self, _ctx: &SimContext) -> Result<()> {
        Ok(())
    }

    fn on_send(
        &mut self,
        id: ReplicaID,
        replica: &Replica<Accumulator>,
        disk: &PersistentState<Op>,
        message: &Msg,
    ) -> Result<()> {
        let backed = match message {
            Message::PrepareOk {
                view_number,
                op_number,
                ..
            } => {
                !disk.recovering
                    && (disk.last_normal_view > *view_number
                        || (disk.last_normal_view == *view_number
                            && disk.log_start + disk.log.len() >= *op_number
                            && (replica.last_normal_view() > *view_number
                                || holds_uncommitted(disk, replica, *op_number))))
            }
            Message::StartViewChange { view_number, .. } => disk.view_number >= *view_number,
            Message::DoViewChange {
                view_number,
                last_normal_view,
                segment,
                ..
            } => {
                disk.view_number >= *view_number
                    && (disk.last_normal_view >= *view_number
                        || (disk.last_normal_view >= *last_normal_view
                            && holds_segment(disk, segment)))
            }
            Message::StartView {
                view_number,
                segment,
                ..
            } => {
                disk.view_number >= *view_number
                    && (disk.last_normal_view > *view_number
                        || (disk.last_normal_view == *view_number && holds_segment(disk, segment)))
            }
            Message::Recovery { view_number, .. } => {
                disk.view_number >= *view_number
                    && (disk.recovering || disk.last_normal_view >= *view_number)
            }
            Message::RecoveryResponse {
                view_number, state, ..
            } => {
                !disk.recovering
                    && disk.view_number >= *view_number
                    && (disk.last_normal_view > *view_number
                        || state
                            .as_ref()
                            .is_none_or(|state| holds_segment(disk, &state.segment)))
            }
            Message::Register { .. }
            | Message::Request { .. }
            | Message::Query { .. }
            | Message::ConfirmView { .. }
            | Message::ConfirmViewOk { .. }
            | Message::Prepare { .. }
            | Message::Commit { .. }
            | Message::GetState { .. }
            | Message::NewState { .. }
            | Message::GetChunk { .. }
            | Message::NewChunk { .. } => true,
        };
        ensure!(
            backed,
            "replica {id} sent {} for view {} that its disk does not back: view {}, last normal view {}, op number {}, recovering {}",
            message_kind(message),
            message_view(message),
            disk.view_number,
            disk.last_normal_view,
            disk.log_start + disk.log.len(),
            disk.recovering
        );
        Ok(())
    }
}

/// Whether the disk holds the entries the replica has in memory after its
/// commit number, up to op `op_number`, where both hold them. The
/// committed ones are what `Durability` and `CommittedPrefixAgreement`
/// look after.
fn holds_uncommitted(
    disk: &PersistentState<Op>,
    replica: &Replica<Accumulator>,
    op_number: OpNumber,
) -> bool {
    let from = replica
        .commit_number()
        .max(replica.log_start())
        .max(disk.log_start);
    (from + 1..=op_number.min(replica.op_number())).all(|op| {
        disk.log.get(op - disk.log_start - 1) == replica.log().get(op - replica.log_start() - 1)
    })
}

/// Whether the disk holds every entry of `segment`, or has compacted it,
/// which only committed entries are.
fn holds_segment(disk: &PersistentState<Op>, segment: &LogSegment<Op>) -> bool {
    let start = segment.start();
    let skip = disk.log_start.saturating_sub(start);
    if skip >= segment.entries.len() {
        return true;
    }
    let from = start + skip - disk.log_start;
    disk.log.get(from..from + segment.entries.len() - skip) == Some(&segment.entries[skip..])
}

/// The view a message names, for error messages.
fn message_view(message: &Msg) -> usize {
    match message {
        Message::Register { .. }
        | Message::Request { .. }
        | Message::Query { .. }
        | Message::GetChunk { .. }
        | Message::NewChunk { .. } => 0,
        Message::ConfirmView { view_number, .. } | Message::ConfirmViewOk { view_number, .. } => {
            *view_number
        }
        Message::Prepare { view_number, .. }
        | Message::PrepareOk { view_number, .. }
        | Message::Commit { view_number, .. }
        | Message::GetState { view_number, .. }
        | Message::NewState { view_number, .. }
        | Message::StartViewChange { view_number, .. }
        | Message::DoViewChange { view_number, .. }
        | Message::StartView { view_number, .. }
        | Message::Recovery { view_number, .. }
        | Message::RecoveryResponse { view_number, .. } => *view_number,
    }
}

/// A replica's commit number never decreases and never exceeds its op number,
/// its op number is the log start plus the log length, and the log start,
/// where the log has been compacted, never exceeds the commit number.
#[derive(Default)]
pub struct CommitNumberMonotonic {
    last_commit: Vec<usize>,
}

impl Property for CommitNumberMonotonic {
    fn name(&self) -> &'static str {
        "commit-number-monotonic"
    }

    fn on_restart(&mut self, replica_id: usize) {
        if let Some(last) = self.last_commit.get_mut(replica_id) {
            *last = 0;
        }
    }

    fn check(&mut self, ctx: &SimContext) -> Result<()> {
        self.last_commit.resize(ctx.replicas.len(), 0);
        for (id, replica) in ctx.replicas.iter().enumerate() {
            let commit = replica.commit_number();
            let op = replica.op_number();
            let log_start = replica.log_start();
            let len = replica.log().len();
            ensure!(
                op == log_start + len,
                "tick {}: replica {id} op_number {op} != log start {log_start} + log length {len}",
                ctx.tick
            );
            ensure!(
                commit <= op,
                "tick {}: replica {id} commit_number {commit} > op_number {op}",
                ctx.tick
            );
            ensure!(
                log_start <= commit,
                "tick {}: replica {id} compacted up to {log_start} but commit_number is {commit}",
                ctx.tick
            );
            ensure!(
                commit >= self.last_commit[id],
                "tick {}: replica {id} commit_number went backwards: {} -> {commit}",
                ctx.tick,
                self.last_commit[id]
            );
            self.last_commit[id] = commit;
        }
        Ok(())
    }
}

/// What the committed log does, fed it in order: which requests execute and
/// with what result, which sessions it opens, and which it evicts. The
/// simulator feeds it as ops commit, and the properties check the replicas
/// against it.
#[derive(Default)]
pub struct Model {
    /// Committed entries fed so far.
    fed: usize,
    value: i64,
    /// The client table after the entries fed.
    table: BTreeMap<ClientID, ClientRecord<i64>>,
    /// Sessions evicted, by client and session.
    evicted: BTreeSet<(ClientID, OpNumber)>,
    /// The sessions registrations were answered with, by client.
    sessions: BTreeSet<(ClientID, OpNumber)>,
    /// The ops executed, with their op numbers, in order.
    executed: Vec<(OpNumber, Op)>,
    /// The result of each request that executed, the number of requests
    /// executed up to it, by client, session, and request number.
    results: BTreeMap<(ClientID, OpNumber, RequestNumber), i64>,
}

impl Model {
    /// Feeds the committed entries not fed yet. A request executes only as
    /// its session's next one.
    pub fn feed(&mut self, config: &Config, tick: u64, committed: &[LogEntry<Op>]) -> Result<()> {
        for entry in &committed[self.fed..] {
            self.fed += 1;
            let op_number = self.fed;
            let (client_id, session, request_number, answered, op) = match entry {
                LogEntry::Register { client_id } => {
                    let session = self.register(config, op_number, *client_id);
                    self.sessions.insert((*client_id, session));
                    continue;
                }
                LogEntry::Request {
                    client_id,
                    session,
                    request_number,
                    answered,
                    op,
                } => (*client_id, *session, *request_number, *answered, op),
            };
            let Some(record) = self
                .table
                .get_mut(&client_id)
                .filter(|record| record.session == session)
            else {
                continue;
            };
            ensure!(
                request_number == record.request_number + 1,
                "tick {tick}: op {op_number} is request {request_number} of client {client_id} in session {session}, after request {}",
                record.request_number
            );
            self.value = op.kind.apply(self.value);
            self.executed.push((op_number, op.clone()));
            let position = self.executed.len() as i64;
            record.request_number = request_number;
            record.replies.push_back(position);
            let kept = request_number
                .saturating_sub(answered)
                .clamp(1, config.in_flight_max());
            while record.replies.len() > kept {
                record.replies.pop_front();
            }
            record.op_number = op_number;
            self.results
                .insert((client_id, session, request_number), position);
        }
        Ok(())
    }

    /// Registers `client_id` at `op_number`, and returns its session.
    fn register(&mut self, config: &Config, op_number: OpNumber, client_id: ClientID) -> OpNumber {
        if let Some(record) = self.table.get(&client_id) {
            return record.session;
        }
        if self.table.len() >= config.clients_max() {
            let (&evicted, record) = self
                .table
                .iter()
                .min_by_key(|(_, record)| record.op_number)
                .expect("a full table holds a session");
            self.evicted.insert((evicted, record.session));
            self.table.remove(&evicted);
        }
        let record = ClientRecord {
            client_id,
            session: op_number,
            request_number: 0,
            replies: VecDeque::new(),
            op_number,
        };
        self.table.insert(client_id, record);
        op_number
    }
}

/// A replica's state machine has applied the requests the committed log
/// executes, in order, up to the replica's applied op number, and its value
/// is their fold. That is every committed op unless the replica has a write
/// out: an op executes only once a landed write holds it. The state
/// machine's client table is the replica's, and, once it has applied every
/// committed op, the model's.
#[derive(Default)]
pub struct StateMatchesCommittedLog {
    /// Per replica: (number of executed ops already verified, expected value).
    verified: Vec<(usize, i64)>,
}

impl Property for StateMatchesCommittedLog {
    fn name(&self) -> &'static str {
        "state-matches-committed-log"
    }

    fn on_restart(&mut self, replica_id: usize) {
        if let Some(verified) = self.verified.get_mut(replica_id) {
            *verified = (0, 0);
        }
    }

    fn check(&mut self, ctx: &SimContext) -> Result<()> {
        self.verified.resize(ctx.replicas.len(), (0, 0));
        for (id, replica) in ctx.replicas.iter().enumerate() {
            let commit = replica.commit_number();
            let applied = replica.applied();
            ensure!(
                applied <= commit,
                "tick {}: replica {id} applied {applied} ops but commit_number is {commit}",
                ctx.tick
            );
            ensure!(
                applied == commit || ctx.disks[id].outstanding.is_some(),
                "tick {}: replica {id} applied {applied} of {commit} committed ops with no write out",
                ctx.tick
            );
            let state = replica.state_machine();
            let executed = &ctx.model.executed;
            let expected = executed.partition_point(|(op_number, _)| *op_number <= applied);
            ensure!(
                state.applied.len() == expected,
                "tick {}: replica {id} executed {} requests up to op {applied}, but the log executes {expected}",
                ctx.tick,
                state.applied.len()
            );
            let (verified, value) = &mut self.verified[id];
            let checking = state.applied[*verified..expected]
                .iter()
                .zip(&executed[*verified..expected]);
            for (applied, executed) in checking {
                ensure!(
                    applied == executed,
                    "tick {}: replica {id} executed {applied:?}, but the log executes {executed:?}",
                    ctx.tick
                );
                *value = executed.1.kind.apply(*value);
            }
            *verified = expected;
            ensure!(
                state.value == *value,
                "tick {}: replica {id} value {} != expected {value}",
                ctx.tick,
                state.value
            );
            ensure!(
                replica.client_table() == state.client_table(),
                "tick {}: replica {id} holds client table {:?}, its state machine {:?}",
                ctx.tick,
                replica.client_table(),
                state.client_table()
            );
            ensure!(
                applied < ctx.model.fed || state.clients == ctx.model.table,
                "tick {}: replica {id} holds client table {:?}, the log makes {:?}",
                ctx.tick,
                state.clients,
                ctx.model.table
            );
        }
        Ok(())
    }
}

/// All replicas agree on the committed prefix of the log: every committed
/// entry a replica holds in its log is the one recorded as committed at
/// that op number.
#[derive(Default)]
pub struct CommittedPrefixAgreement {
    /// Per replica: number of committed ops already verified.
    verified: Vec<usize>,
}

impl Property for CommittedPrefixAgreement {
    fn name(&self) -> &'static str {
        "committed-prefix-agreement"
    }

    fn on_restart(&mut self, replica_id: usize) {
        if let Some(verified) = self.verified.get_mut(replica_id) {
            *verified = 0;
        }
    }

    fn check(&mut self, ctx: &SimContext) -> Result<()> {
        self.verified.resize(ctx.replicas.len(), 0);
        for (id, replica) in ctx.replicas.iter().enumerate() {
            let commit = replica.commit_number();
            let log_start = replica.log_start();
            for op_number in self.verified[id].max(log_start) + 1..=commit {
                let entry = &replica.log()[op_number - log_start - 1];
                let canonical = &ctx.committed[op_number - 1];
                ensure!(
                    *canonical == *entry,
                    "tick {}: replica {id} committed {entry:?} as op {op_number} but another replica committed {canonical:?}",
                    ctx.tick
                );
            }
            self.verified[id] = commit;
        }
        Ok(())
    }
}

/// No operation commits twice. Entries beyond the commit number can be
/// replaced by a view change, so only the committed log, which is
/// append-only, is checked.
#[derive(Default)]
pub struct NoDuplicateOps {
    /// Committed ops already verified, and the op IDs seen.
    verified: usize,
    seen: BTreeSet<u64>,
}

impl Property for NoDuplicateOps {
    fn name(&self) -> &'static str {
        "no-duplicate-ops"
    }

    fn check(&mut self, ctx: &SimContext) -> Result<()> {
        for (i, entry) in ctx.committed.iter().enumerate().skip(self.verified) {
            if let LogEntry::Request { op, .. } = entry {
                ensure!(
                    self.seen.insert(op.id),
                    "tick {}: op {op:?} committed again as op {}",
                    ctx.tick,
                    i + 1
                );
            }
        }
        self.verified = ctx.committed.len();
        Ok(())
    }
}

/// Every reply answers what committed: a registration with a session the
/// log gave the client, a request with the accumulator value right after
/// it executed, an eviction for a session the log evicted. Replies may be
/// duplicated, since the primary answers a re-sent request from its client
/// table, and by the end every request that executed has a reply, unless
/// its session was evicted.
#[derive(Default)]
pub struct RepliesMatchCommits {
    /// Requests that have received a reply.
    replied: BTreeSet<(ClientID, OpNumber, RequestNumber)>,
    /// Replies already checked.
    verified: usize,
}

impl Property for RepliesMatchCommits {
    fn name(&self) -> &'static str {
        "replies-match-commits"
    }

    fn check(&mut self, ctx: &SimContext) -> Result<()> {
        let model = ctx.model;
        for reply in &ctx.replies[self.verified..] {
            match reply {
                Reply::Registered {
                    client_id, session, ..
                } => ensure!(
                    model.sessions.contains(&(*client_id, *session)),
                    "tick {}: reply {reply:?} for a session the log did not give the client",
                    ctx.tick
                ),
                Reply::Executed {
                    client_id,
                    session,
                    request_number,
                    result,
                    ..
                } => {
                    let key = (*client_id, *session, *request_number);
                    let Some(expected) = model.results.get(&key) else {
                        bail!(
                            "tick {}: reply {reply:?} for a request that has not executed",
                            ctx.tick
                        );
                    };
                    ensure!(
                        result == expected,
                        "tick {}: reply {reply:?} but expected result {expected}",
                        ctx.tick
                    );
                    self.replied.insert(key);
                }
                Reply::Evicted {
                    client_id, session, ..
                } => ensure!(
                    model.evicted.contains(&(*client_id, *session)),
                    "tick {}: reply {reply:?} for a session the log did not evict",
                    ctx.tick
                ),
                Reply::Queried { .. } => {}
            }
        }
        self.verified = ctx.replies.len();
        Ok(())
    }

    fn finalize(&mut self, ctx: &SimContext) -> Result<()> {
        for (client_id, session, request_number) in ctx.model.results.keys() {
            ensure!(
                self.replied
                    .contains(&(*client_id, *session, *request_number))
                    || ctx.model.evicted.contains(&(*client_id, *session)),
                "client {client_id} got no reply for request {request_number} of session {session}"
            );
        }
        Ok(())
    }
}

/// Every query read a state in the committed history no earlier than what
/// was completed before the query was submitted, every request and the
/// state every query read, nor than every earlier request of its own
/// client, and no later than what has executed.
#[derive(Default)]
pub struct LinearizableQueries {
    /// Answers already checked.
    verified: usize,
}

impl Property for LinearizableQueries {
    fn name(&self) -> &'static str {
        "linearizable-queries"
    }

    fn check(&mut self, ctx: &SimContext) -> Result<()> {
        let executed = ctx.model.executed.len() as i64;
        for answer in &ctx.answers[self.verified..] {
            ensure!(
                answer.result >= answer.floor
                    && answer.result >= answer.own
                    && answer.result <= executed,
                "tick {}: query {} of client {} read the state after {} requests; requests completed before it reach {}, its client's {}, and {executed} have executed",
                answer.tick,
                answer.query_number,
                answer.client_id,
                answer.result,
                answer.floor,
                answer.own
            );
        }
        self.verified = ctx.answers.len();
        Ok(())
    }
}

/// Once the network is drained, every core replica has committed the same
/// number of ops, all of its log, holds the committed entries, and holds
/// the same state machine value.
pub struct Convergence;

impl Property for Convergence {
    fn name(&self) -> &'static str {
        "convergence"
    }

    fn check(&mut self, _ctx: &SimContext) -> Result<()> {
        Ok(())
    }

    fn finalize(&mut self, ctx: &SimContext) -> Result<()> {
        let reference_id = ctx.core[0];
        let reference_op_number = ctx.replicas[reference_id].op_number();
        let reference_value = ctx.replicas[reference_id].state_machine().value;
        for &id in ctx.core {
            let replica = &ctx.replicas[id];
            let op_number = replica.op_number();
            ensure!(
                op_number == reference_op_number,
                "replica {id} op_number {op_number} != replica {reference_id} op_number {reference_op_number}"
            );
            ensure!(
                replica.commit_number() == op_number,
                "replica {id} committed {} of {op_number} ops",
                replica.commit_number()
            );
            let log_start = replica.log_start();
            for (i, entry) in replica.log().iter().enumerate() {
                let op_number = log_start + i + 1;
                ensure!(
                    ctx.committed.get(op_number - 1) == Some(entry),
                    "replica {id} holds {entry:?} as op {op_number}, which is not what committed"
                );
            }
            let value = replica.state_machine().value;
            ensure!(
                value == reference_value,
                "replica {id} value {value} != replica {reference_id} value {reference_value}"
            );
        }
        Ok(())
    }
}
