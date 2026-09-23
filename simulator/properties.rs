//! Properties are invariants checked after every tick and at the
//! end of the run.

use crate::disk::Disk;
use crate::network::message_kind;
use crate::state_machine::{Accumulator, Msg, Op};
use anyhow::{ensure, Result};
use std::collections::{BTreeMap, BTreeSet};
use vsr_rs::{
    ClientID, LogEntry, LogSegment, Message, OpNumber, PersistentState, Replica, ReplicaID, Reply,
    RequestNumber,
};

/// Read-only view of the simulated system handed to properties.
pub struct SimContext<'a> {
    pub tick: u64,
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
    /// The replicas that must converge: all of them during the safety
    /// phase, the liveness core afterwards.
    pub core: &'a [usize],
}

pub trait Property {
    fn name(&self) -> &'static str;

    /// Called after every tick.
    fn check(&mut self, ctx: &SimContext) -> Result<()>;

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
        Box::new(Durability::default()),
        Box::new(DurablePromise),
        Box::new(Convergence),
    ]
}

/// Every committed op is on enough disks to survive any view change:
/// every quorum that the replicas whose disks are not recovering could form
/// must include a disk that holds it. With nobody recovering that is a
/// majority. A recovering replica holds nothing and takes part in no
/// quorum, so it counts on neither side. A primary commits only on a
/// quorum of `PrepareOk` messages, and a replica sends one only once its
/// disk holds the op, so this must hold at the tick the commit happens, on
/// whichever replica committed it. Committed prefixes are never truncated,
/// so each committed op needs checking once per replica.
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

/// A replica sends these only for state on its disk, which each message is
/// checked against as it leaves:
///
/// - `PrepareOk` for op `n` in view `v`: the disk was last normal in `v`
///   and holds the replica's uncommitted entries up to op `n`, or was last
///   normal in a later view.
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
                            && holds_uncommitted(disk, replica, *op_number)))
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
            Message::Request { .. }
            | Message::Prepare { .. }
            | Message::Commit { .. }
            | Message::GetState { .. }
            | Message::NewState { .. } => true,
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
fn holds_segment(disk: &PersistentState<Op>, segment: &LogSegment<Op, i64, Accumulator>) -> bool {
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
        Message::Request { .. } => 0,
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

/// A replica's state machine has applied exactly the committed ops, in
/// order, and its value is the fold of those operations.
#[derive(Default)]
pub struct StateMatchesCommittedLog {
    /// Per replica: (number of committed ops already verified, expected value).
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
            let state = replica.state_machine();
            let (verified, value) = &mut self.verified[id];
            ensure!(
                state.applied.len() == commit,
                "tick {}: replica {id} applied {} ops but commit_number is {commit}",
                ctx.tick,
                state.applied.len()
            );
            for (i, entry) in ctx
                .committed
                .iter()
                .enumerate()
                .take(commit)
                .skip(*verified)
            {
                ensure!(
                    state.applied[i] == entry.op,
                    "tick {}: replica {id} applied {:?} as op {} but the committed op is {:?}",
                    ctx.tick,
                    state.applied[i],
                    i + 1,
                    entry.op
                );
                *value = entry.op.kind.apply(*value);
            }
            *verified = commit;
            ensure!(
                state.value == *value,
                "tick {}: replica {id} value {} != expected {value}",
                ctx.tick,
                state.value
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
            ensure!(
                self.seen.insert(entry.op.id),
                "tick {}: op {:?} committed again as op {}",
                ctx.tick,
                entry.op,
                i + 1
            );
        }
        self.verified = ctx.committed.len();
        Ok(())
    }
}

/// Every reply is for a request that has committed, and carries the
/// accumulator value right after that request's op. Replies may be
/// duplicated, since the primary answers a re-sent request from its client
/// table, but every committed request gets at least one reply by the end.
#[derive(Default)]
pub struct RepliesMatchCommits {
    /// Expected result per committed request.
    expected: BTreeMap<(ClientID, RequestNumber), i64>,
    /// Requests that have received a reply.
    replied: BTreeSet<(ClientID, RequestNumber)>,
    /// Committed entries and replies already processed.
    committed: usize,
    verified: usize,
    value: i64,
}

impl Property for RepliesMatchCommits {
    fn name(&self) -> &'static str {
        "replies-match-commits"
    }

    fn check(&mut self, ctx: &SimContext) -> Result<()> {
        for entry in &ctx.committed[self.committed..] {
            self.value = entry.op.kind.apply(self.value);
            self.expected
                .insert((entry.client_id, entry.request_number), self.value);
        }
        self.committed = ctx.committed.len();
        for reply in &ctx.replies[self.verified..] {
            let key = (reply.client_id, reply.request_number);
            let Some(expected) = self.expected.get(&key) else {
                anyhow::bail!(
                    "tick {}: reply {reply:?} for a request that has not committed",
                    ctx.tick
                );
            };
            ensure!(
                reply.result == *expected,
                "tick {}: reply {reply:?} but expected result {expected}",
                ctx.tick
            );
            self.replied.insert(key);
        }
        self.verified = ctx.replies.len();
        Ok(())
    }

    fn finalize(&mut self, _ctx: &SimContext) -> Result<()> {
        for key in self.expected.keys() {
            ensure!(
                self.replied.contains(key),
                "client {} got no reply for request {}",
                key.0,
                key.1
            );
        }
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
