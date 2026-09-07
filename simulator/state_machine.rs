//! The state machine replicated by the simulator.
//!
//! The simulator uses a simple accumulator that also records every operation
//! it has applied. Properties use the recorded history to check that the
//! state machine state matches the committed log.

use vsr_rs::{Checkpoint, LogEntry, MessageFor, OpNumber, StateMachine};

/// The kind of an operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OpKind {
    Add(i64),
    Sub(i64),
}

impl OpKind {
    /// Applies this operation to `value`.
    pub fn apply(self, value: i64) -> i64 {
        match self {
            OpKind::Add(v) => value.wrapping_add(v),
            OpKind::Sub(v) => value.wrapping_sub(v),
        }
    }
}

/// An operation submitted by a client.
///
/// Every operation carries a unique `id` so that properties can detect
/// duplicate or missing operations in replica logs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Op {
    pub id: u64,
    pub kind: OpKind,
}

/// An accumulator state machine that records its history.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Accumulator {
    /// The current value of the accumulator.
    pub value: i64,
    /// Every operation applied so far, in application order.
    pub applied: Vec<Op>,
}

impl StateMachine for Accumulator {
    type Input = Op;
    type Output = i64;
    /// The whole state, history included, so that a replica that installs
    /// a checkpoint can still be checked against the committed log.
    type Snapshot = Accumulator;

    fn apply(&mut self, op_number: OpNumber, entry: &LogEntry<Op>) -> i64 {
        assert_eq!(op_number, self.applied.len() + 1);
        self.value = entry.op.kind.apply(self.value);
        self.applied.push(entry.op.clone());
        self.value
    }

    fn snapshot(&self) -> Accumulator {
        self.clone()
    }

    fn restore(&mut self, checkpoint: Checkpoint<i64, Accumulator>) {
        assert_eq!(checkpoint.op_number, checkpoint.state.applied.len());
        *self = checkpoint.state;
    }
}

/// A protocol message in the simulator.
pub type Msg = MessageFor<Accumulator>;
