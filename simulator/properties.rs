//! Properties are invariants checked after every tick and at the
//! end of the run.

use crate::state_machine::{Accumulator, Op};
use anyhow::{ensure, Result};
use std::collections::{BTreeMap, BTreeSet};
use vsr_rs::{ClientID, LogEntry, OpNumber, Replica, Reply, RequestNumber};

/// Read-only view of the simulated system handed to properties.
pub struct SimContext<'a> {
    pub tick: u64,
    pub replicas: &'a [Replica<Accumulator>],
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

    /// Called when a replica comes back from a crash as something other
    /// than what it was in memory: with no memory at all, or from a disk
    /// that lost the last step. Whatever the property tracked about that
    /// replica starts over.
    fn on_restart(&mut self, _replica_id: usize) {}
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
        Box::new(Convergence),
    ]
}

/// Whether `replica` holds the committed op `op_number`, which is `entry`:
/// in its log, or in its state if it has compacted the entry.
fn holds(replica: &Replica<Accumulator>, op_number: OpNumber, entry: &LogEntry<Op>) -> bool {
    let log_start = replica.log_start();
    op_number <= log_start || replica.log().get(op_number - log_start - 1) == Some(entry)
}

/// Every committed op is held by enough replicas to survive any view
/// change: every quorum the replicas that are not recovering could form
/// must include one that holds it. With nobody recovering that is a
/// majority. A recovering replica holds nothing and takes part in no quorum,
/// so it counts on neither side. A primary only commits on a quorum of
/// `PrepareOk` messages, and a backup only acknowledges an op once it is in
/// its log, so this must hold at the tick the commit happens, on whichever
/// replica committed it. A crashed replica still holds what is on its disk,
/// which the simulator keeps equal to its log. Committed prefixes are never
/// truncated, so each committed op needs checking once per replica.
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
            .replicas
            .iter()
            .filter(|replica| !replica.is_recovering())
            .count();
        let needed = (participants + 1).saturating_sub(quorum);
        for (id, replica) in ctx.replicas.iter().enumerate() {
            let commit = replica.commit_number();
            for op_number in self.verified[id] + 1..=commit {
                let entry = &ctx.committed[op_number - 1];
                let copies = ctx
                    .replicas
                    .iter()
                    .filter(|other| !other.is_recovering() && holds(other, op_number, entry))
                    .count();
                ensure!(
                    copies >= needed,
                    "tick {}: replica {id} committed op {op_number} held by {copies} of {participants} replicas not recovering, {needed} needed to meet every quorum of {quorum}",
                    ctx.tick
                );
            }
            self.verified[id] = commit;
        }
        Ok(())
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
