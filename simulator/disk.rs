//! A replica's disk, and the owner's step that writes it.
//!
//! A disk holds what a real owner's would: the replica's persistent state
//! as of its last landed write, and the state machine as of its last
//! flush. [`Disk::step`] is the owner's side of a step, in the order the
//! library requires: send what need not wait, persist the replica's write,
//! hand it back, then deliver the rest. The write may also stay out while
//! the replica steps on, as with an owner that writes on another thread,
//! and land at a later step. A power loss can cut a step off before its
//! write, a crash lands a write that is out whole or not at all, and a
//! replica that lost power is rebuilt from its disk with
//! [`Disk::restart`].

use crate::state_machine::{Accumulator, Msg, Op};
use anyhow::Result;
use rand::Rng;
use rand_chacha::ChaCha8Rng;
use vsr_rs::{
    ClientRecord, Config, LogEntry, LogWrite, OpNumber, PersistentState, Replica, ReplicaID, Reply,
    StateMachine, ViewNumber,
};

/// The chance that a step loses power before its write.
#[derive(Clone, Copy, Debug, Default)]
pub struct PowerLossOdds {
    /// For a step in which the state machine restored a checkpoint, which
    /// it makes durable at once.
    pub after_checkpoint: f64,
    /// For any other step.
    pub otherwise: f64,
}

impl PowerLossOdds {
    /// A power loss in the step, whatever it did.
    pub fn certain() -> PowerLossOdds {
        PowerLossOdds {
            after_checkpoint: 1.0,
            otherwise: 1.0,
        }
    }
}

/// The chances that a write stays out after the step that took it, and
/// that a write that is out lands at a later step.
#[derive(Clone, Copy, Debug, Default)]
pub struct WriteOdds {
    pub out: f64,
    pub land: f64,
}

/// How a step ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Step {
    /// The step is on disk, and everything it produced was delivered.
    Persisted,
    /// The replica lost power before the write. What need not wait was
    /// sent, and the replies ready before the write delivered; the rest of
    /// the step is lost.
    PowerLost,
    /// The step is done, but a write is out: the replica's memory is ahead
    /// of its disk until the write lands.
    WriteOut,
}

/// A replica's disk.
#[derive(Clone, Debug)]
pub struct Disk {
    /// The replica's persistent state as of its last landed write.
    pub state: PersistentState<Op>,
    /// The write the owner has taken and not yet landed, if any.
    pub outstanding: Option<LogWrite<Op>>,
    /// The state machine as of its last flush, the number of ops it had
    /// applied then, and the client table as of then, which a durable
    /// state machine keeps alongside what it applies.
    pub state_machine: Accumulator,
    pub applied: OpNumber,
    pub client_table: Vec<ClientRecord<i64>>,
}

impl Default for Disk {
    fn default() -> Disk {
        Disk {
            state: PersistentState::empty(),
            outstanding: None,
            state_machine: Accumulator::default(),
            applied: 0,
            client_table: Vec::new(),
        }
    }
}

impl Disk {
    /// The disk of a replica that lost its old one: it holds the view
    /// number the replica recovers in, and says it is recovering, which
    /// its owner writes before it starts the recovery.
    pub fn for_recovery(view_number: ViewNumber) -> Disk {
        Disk {
            state: PersistentState {
                view_number,
                recovering: true,
                ..PersistentState::empty()
            },
            ..Disk::default()
        }
    }

    /// The owner's side of one step: sends what need not wait and delivers
    /// the replies ready so far to `replies`; lands the write that is out
    /// with probability `writes.land`, and sends and delivers what it
    /// released; takes the next write if none is out, and keeps it out with
    /// probability `writes.out`, or else lands it at once and sends and
    /// delivers what that released. What a write released thus leaves
    /// before the next write lands. `send` gets each
    /// message as it leaves, with the disk and the replica as of then. The
    /// replica loses power before any write with `odds`; what left before
    /// is out, and the rest of the step is lost with the replica's memory.
    pub fn step(
        &mut self,
        replica: &mut Replica<Accumulator>,
        prng: &mut ChaCha8Rng,
        odds: PowerLossOdds,
        writes: WriteOdds,
        mut send: impl FnMut(&PersistentState<Op>, &Replica<Accumulator>, ReplicaID, Msg) -> Result<()>,
        replies: &mut Vec<Reply<i64>>,
    ) -> Result<Step> {
        let early: Vec<_> = replica.drain_messages_before_persist().collect();
        for (to, message) in early {
            send(&self.state, replica, to, message)?;
        }
        replies.extend(replica.drain_replies());
        // A checkpoint the replica installed moved its log start past what
        // the state machine had flushed: the state machine made the
        // checkpoint durable when it restored it, as the library requires.
        let odds = if replica.log_start() > self.applied {
            self.flush(replica);
            odds.after_checkpoint
        } else {
            odds.otherwise
        };
        if odds > 0.0 && prng.gen_bool(odds) {
            return Ok(Step::PowerLost);
        }
        if self.outstanding.is_some() && prng.gen_bool(writes.land) {
            self.land(replica);
            self.release(replica, &mut send, replies)?;
        }
        if self.outstanding.is_none() {
            // A write that needs no sync has nothing to wait for.
            let write = replica.take_write();
            let keep_out = write.sync && writes.out > 0.0 && prng.gen_bool(writes.out);
            self.outstanding = Some(write);
            if !keep_out {
                self.land(replica);
                self.release(replica, &mut send, replies)?;
            }
        }
        Ok(if self.outstanding.is_some() {
            Step::WriteOut
        } else {
            Step::Persisted
        })
    }

    /// Sends the messages and delivers the replies that the writes handed
    /// back so far released.
    fn release(
        &self,
        replica: &mut Replica<Accumulator>,
        send: &mut impl FnMut(&PersistentState<Op>, &Replica<Accumulator>, ReplicaID, Msg) -> Result<()>,
        replies: &mut Vec<Reply<i64>>,
    ) -> Result<()> {
        let released: Vec<_> = replica.drain_messages().collect();
        for (to, message) in released {
            send(&self.state, replica, to, message)?;
        }
        replies.extend(replica.drain_replies());
        Ok(())
    }

    /// The write that is out reaches the disk, whole, and goes back to the
    /// replica. A write that changed only the commit number needs no sync,
    /// and this owner skips it: the disk keeps its prior image.
    fn land(&mut self, replica: &mut Replica<Accumulator>) {
        let write = self.outstanding.take().expect("a write is out");
        if write.sync {
            self.state.apply(&write);
        }
        replica.persisted(write);
    }

    /// Settles the write that is out as a crash leaves it: on disk whole,
    /// or not at all. Returns whether it landed, if one was out.
    pub fn crash(&mut self, prng: &mut ChaCha8Rng) -> Option<bool> {
        let write = self.outstanding.take()?;
        let landed = prng.gen_bool(0.5);
        if landed && write.sync {
            self.state.apply(&write);
        }
        Some(landed)
    }

    /// The state machine writes its state to disk.
    pub fn flush(&mut self, replica: &Replica<Accumulator>) {
        self.applied = replica.applied();
        self.state_machine = replica.state_machine().clone();
        self.client_table = replica.client_table();
    }

    /// The state machine as a process crash leaves it, made durable by its
    /// owner before the restart: what it had flushed, and the operations
    /// after it up to `op_number`, which it had applied and which reached
    /// the page cache. The replica's log still holds them, since it
    /// compacts only what the state machine flushed.
    pub fn flush_up_to(&mut self, replica: &Replica<Accumulator>, op_number: OpNumber) {
        assert!(op_number <= replica.applied());
        while self.applied < op_number {
            let op = self.applied + 1;
            let entry = &replica.log()[op - replica.log_start() - 1];
            let reply = self.state_machine.apply(op, entry);
            match self
                .client_table
                .iter_mut()
                .find(|record| record.client_id == entry.client_id)
            {
                Some(record) => {
                    record.request_number = entry.request_number;
                    record.reply = reply;
                }
                None => self.client_table.push(ClientRecord {
                    client_id: entry.client_id,
                    request_number: entry.request_number,
                    reply,
                }),
            }
            self.applied = op;
        }
    }

    /// The replica its owner rebuilds from this disk after a power loss,
    /// with the state machine and its client table as of its last flush.
    pub fn restart(&self, id: ReplicaID, config: Config, nonce: u64) -> Replica<Accumulator> {
        Replica::restart(
            id,
            config,
            self.state_machine.clone(),
            self.applied,
            self.client_table.clone(),
            self.state.clone(),
            nonce,
        )
    }

    /// Whether the disk holds what `replica` does in memory, but for the
    /// commit number, which a write may skip, and the client table, which
    /// comes back from the state machine.
    pub fn matches(&self, replica: &Replica<Accumulator>) -> bool {
        self.state.view_number == replica.view_number()
            && self.state.last_normal_view == replica.last_normal_view()
            && self.state.recovering == replica.is_recovering()
            && self.state.log_start == replica.log_start()
            && self.state.log.as_slice() == replica.log()
    }

    /// Whether the disk holds the committed op `op_number`, which is
    /// `entry`: in the state machine's flush, or in the log.
    pub fn holds(&self, op_number: OpNumber, entry: &LogEntry<Op>) -> bool {
        op_number <= self.applied
            || op_number
                .checked_sub(self.state.log_start + 1)
                .and_then(|index| self.state.log.get(index))
                == Some(entry)
    }
}
