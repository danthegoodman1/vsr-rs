//! The state machine replicated by the simulator.
//!
//! The simulator uses a simple accumulator that also records every operation
//! it has applied and every client record it was handed. Properties use the
//! recorded history to check that the state machine state matches the
//! committed log.

use std::collections::BTreeMap;
use vsr_rs::{ClientID, ClientRecord, MessageFor, OpNumber, StateMachine};

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
    /// Every operation applied so far, in order, with its op number.
    pub applied: Vec<(OpNumber, Op)>,
    /// The client table, as the records handed over left it.
    pub clients: BTreeMap<ClientID, ClientRecord<i64>>,
    /// Every client record handed over so far, in order, with its op
    /// number: `None` for an eviction.
    pub recorded: Vec<(OpNumber, ClientID, Option<ClientRecord<i64>>)>,
    /// The last op applied, recorded, or restored.
    op_number: OpNumber,
    /// The checkpoint kept for replicas that fell behind, which lives in
    /// memory only.
    kept: Option<Kept>,
    /// The chunks staged of another replica's checkpoint, with its op
    /// number.
    staged: Vec<(OpNumber, Chunk)>,
}

/// A checkpoint: the history up to its op, which only grows while the
/// checkpoint is kept, and what the history came to.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Kept {
    op_number: OpNumber,
    applied: usize,
    recorded: usize,
    value: i64,
    clients: BTreeMap<ClientID, ClientRecord<i64>>,
}

/// A part of a checkpoint's history, and in the last part, what the
/// history came to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Chunk {
    applied: Vec<(OpNumber, Op)>,
    recorded: Vec<(OpNumber, ClientID, Option<ClientRecord<i64>>)>,
    last: Option<(i64, BTreeMap<ClientID, ClientRecord<i64>>)>,
}

impl Accumulator {
    /// A copy of the state as a flush writes it, which leaves out the
    /// checkpoint kept.
    pub fn flushed(&self) -> Accumulator {
        Accumulator {
            kept: None,
            ..self.clone()
        }
    }

    /// Applies what `later` applied and recorded after op `from`, up to op
    /// `to`: `later` is this state machine further on.
    pub fn catch_up(&mut self, later: &Accumulator, from: OpNumber, to: OpNumber) {
        let within = |op_number: &OpNumber| (from + 1..=to).contains(op_number);
        for (op_number, op) in later.applied.iter().filter(|(op, _)| within(op)) {
            self.apply(*op_number, op);
        }
        for (op_number, client_id, record) in later.recorded.iter().filter(|(op, ..)| within(op)) {
            self.record_client(*op_number, *client_id, record.as_ref());
        }
    }
}

/// The result of an operation or a query is the number of operations the
/// state reflects, which places it in the committed history.
impl StateMachine for Accumulator {
    type Input = Op;
    type Query = ();
    type Output = i64;
    /// The history comes along, so that a replica that restores a
    /// checkpoint can still be checked against the committed log.
    type Chunk = Chunk;

    fn apply(&mut self, op_number: OpNumber, op: &Op) -> i64 {
        assert!(self
            .applied
            .last()
            .is_none_or(|(last, _)| *last < op_number));
        self.value = op.kind.apply(self.value);
        self.applied.push((op_number, op.clone()));
        self.op_number = op_number;
        self.applied.len() as i64
    }

    fn query(&self, _query: &()) -> i64 {
        self.applied.len() as i64
    }

    fn record_client(
        &mut self,
        op_number: OpNumber,
        client_id: ClientID,
        record: Option<&ClientRecord<i64>>,
    ) {
        match record {
            Some(record) => self.clients.insert(client_id, record.clone()),
            None => self.clients.remove(&client_id),
        };
        self.recorded.push((op_number, client_id, record.cloned()));
        self.op_number = op_number;
    }

    fn client_table(&self) -> Vec<ClientRecord<i64>> {
        self.clients.values().cloned().collect()
    }

    fn checkpoint(&mut self) -> OpNumber {
        self.kept = Some(Kept {
            op_number: self.op_number,
            applied: self.applied.len(),
            recorded: self.recorded.len(),
            value: self.value,
            clients: self.clients.clone(),
        });
        self.op_number
    }

    /// A checkpoint comes in one to four chunks, as its op number says, so
    /// that transfers take a few round trips however long the history.
    fn checkpoint_chunk(&self, index: usize) -> (Chunk, bool) {
        let kept = self.kept.as_ref().expect("a checkpoint kept");
        let count = 1 + kept.op_number % 4;
        let part = |len: usize| {
            let size = len.div_ceil(count);
            (index * size).min(len)..((index + 1) * size).min(len)
        };
        let last = index + 1 == count;
        let chunk = Chunk {
            applied: self.applied[part(kept.applied)].to_vec(),
            recorded: self.recorded[part(kept.recorded)].to_vec(),
            last: last.then(|| (kept.value, kept.clients.clone())),
        };
        (chunk, last)
    }

    fn release_checkpoint(&mut self) {
        assert!(self.kept.take().is_some());
    }

    fn stage_chunk(&mut self, op_number: OpNumber, index: usize, chunk: Chunk) {
        if index == 0 {
            self.staged.clear();
        }
        assert_eq!(index, self.staged.len());
        self.staged.push((op_number, chunk));
    }

    fn restore(&mut self, op_number: OpNumber) {
        let staged = std::mem::take(&mut self.staged);
        self.applied.clear();
        self.recorded.clear();
        for (staged_at, chunk) in staged {
            assert_eq!(op_number, staged_at);
            self.applied.extend(chunk.applied);
            self.recorded.extend(chunk.recorded);
            if let Some((value, clients)) = chunk.last {
                self.value = value;
                self.clients = clients;
            }
        }
        assert!(self
            .applied
            .last()
            .is_none_or(|(last, _)| *last <= op_number));
        self.op_number = op_number;
    }
}

/// A protocol message in the simulator.
pub type Msg = MessageFor<Accumulator>;
