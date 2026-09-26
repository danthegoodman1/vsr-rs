//! Simulated network with fault injection.
//!
//! The simulator hands the network everything the replicas and clients want
//! sent. The network decides for each message whether to lose, replay, or
//! delay it, and every tick hands back whatever is due.

use crate::state_machine::Msg;
use log::trace;
use rand::Rng;
use rand_chacha::ChaCha8Rng;
use std::collections::BTreeMap;
use vsr_rs::{Message, Reply};

pub type ReplicaId = usize;

/// Who sent a message.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Origin {
    Replica(ReplicaId),
    Client(usize),
}

/// A message in the network.
#[derive(Clone, Debug)]
pub struct Envelope {
    pub from: Origin,
    pub to: ReplicaId,
    pub sent_at: u64,
    pub due_at: u64,
    pub message: Msg,
}

/// The name of a message's kind, for display.
pub fn message_kind(message: &Msg) -> &'static str {
    match message {
        Message::Register { .. } => "Register",
        Message::Request { .. } => "Request",
        Message::Query { .. } => "Query",
        Message::ConfirmView { .. } => "ConfirmView",
        Message::ConfirmViewOk { .. } => "ConfirmViewOk",
        Message::Prepare { .. } => "Prepare",
        Message::PrepareOk { .. } => "PrepareOk",
        Message::Commit { .. } => "Commit",
        Message::GetState { .. } => "GetState",
        Message::NewState { .. } => "NewState",
        Message::GetChunk { .. } => "GetChunk",
        Message::NewChunk { .. } => "NewChunk",
        Message::StartViewChange { .. } => "StartViewChange",
        Message::DoViewChange { .. } => "DoViewChange",
        Message::StartView { .. } => "StartView",
        Message::Recovery { .. } => "Recovery",
        Message::RecoveryResponse { .. } => "RecoveryResponse",
    }
}

/// Network fault injection options.
#[derive(Clone, Debug)]
pub struct NetworkOptions {
    /// Probability per message that it is lost.
    pub packet_loss_probability: f64,
    /// Probability per message that it is delivered twice.
    pub packet_replay_probability: f64,
    /// Minimum one-way delay in ticks: the transit time every message
    /// takes.
    pub one_way_delay_min: u64,
    /// Mean one-way delay in ticks. Delay above the minimum is a fault:
    /// exponentially distributed, so messages can be reordered. Equal to
    /// the minimum, there is none.
    pub one_way_delay_mean: u64,
    /// Whether faults also apply to messages sent by clients.
    pub fault_client_messages: bool,
}

impl NetworkOptions {
    /// A perfect network: no loss, no replay, no delay.
    pub fn perfect() -> NetworkOptions {
        NetworkOptions {
            packet_loss_probability: 0.0,
            packet_replay_probability: 0.0,
            one_way_delay_min: 0,
            one_way_delay_mean: 0,
            fault_client_messages: false,
        }
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        for (name, p) in [
            ("packet_loss_probability", self.packet_loss_probability),
            ("packet_replay_probability", self.packet_replay_probability),
        ] {
            anyhow::ensure!(
                (0.0..=1.0).contains(&p),
                "{name} must be in 0.0..=1.0, got {p}"
            );
        }
        anyhow::ensure!(
            self.one_way_delay_min <= self.one_way_delay_mean,
            "one_way_delay_min must not exceed one_way_delay_mean"
        );
        Ok(())
    }
}

/// Message counters.
#[derive(Clone, Debug, Default)]
pub struct MessageSummary {
    pub sent: usize,
    pub delivered: usize,
    pub lost: usize,
    pub replayed: usize,
    /// Messages delayed beyond the minimum transit time.
    pub delayed: usize,
}

pub struct Network {
    options: NetworkOptions,
    /// Messages in flight, keyed by (delivery tick, sequence number).
    queue: BTreeMap<(u64, u64), Envelope>,
    /// Replies in flight to the clients, keyed the same way.
    replies: BTreeMap<(u64, u64), Reply<i64>>,
    seq: u64,
    pub summary: MessageSummary,
}

impl Network {
    pub fn new(options: NetworkOptions) -> Network {
        Network {
            options,
            queue: BTreeMap::new(),
            replies: BTreeMap::new(),
            seq: 0,
            summary: MessageSummary::default(),
        }
    }

    pub fn options(&self) -> &NetworkOptions {
        &self.options
    }

    /// Replaces the fault options. Messages already in flight keep their
    /// scheduled delivery tick.
    pub fn set_options(&mut self, options: NetworkOptions) {
        self.options = options;
    }

    /// Number of messages and replies in flight.
    pub fn pending(&self) -> usize {
        self.queue.len() + self.replies.len()
    }

    /// Messages in flight, in delivery order.
    pub fn in_flight(&self) -> impl Iterator<Item = &Envelope> {
        self.queue.values()
    }

    /// Accepts a message sent at tick `now`, applying faults.
    pub fn send(&mut self, now: u64, from: Origin, dst: ReplicaId, msg: Msg, rng: &mut ChaCha8Rng) {
        self.summary.sent += 1;
        let from_client = matches!(
            msg,
            Message::Register { .. } | Message::Request { .. } | Message::Query { .. }
        );
        if from_client && !self.options.fault_client_messages {
            self.enqueue_at(now, now, from, dst, msg);
            return;
        }
        if self.options.packet_loss_probability > 0.0
            && rng.gen_bool(self.options.packet_loss_probability)
        {
            trace!("tick {now}: lost {msg:?} to {dst}");
            self.summary.lost += 1;
            return;
        }
        if self.options.packet_replay_probability > 0.0
            && rng.gen_bool(self.options.packet_replay_probability)
        {
            trace!("tick {now}: replaying {msg:?} to {dst}");
            self.summary.replayed += 1;
            self.enqueue(now, from, dst, msg.clone(), rng);
        }
        self.enqueue(now, from, dst, msg, rng);
    }

    /// Accepts a reply to a client at tick `now`. It is returned for
    /// delivery at once unless faults apply to clients' messages, which
    /// lose, replay, and delay replies as they do messages.
    pub fn send_reply(
        &mut self,
        now: u64,
        reply: Reply<i64>,
        rng: &mut ChaCha8Rng,
    ) -> Option<Reply<i64>> {
        self.summary.sent += 1;
        if !self.options.fault_client_messages {
            self.summary.delivered += 1;
            return Some(reply);
        }
        if self.options.packet_loss_probability > 0.0
            && rng.gen_bool(self.options.packet_loss_probability)
        {
            trace!("tick {now}: lost {reply:?}");
            self.summary.lost += 1;
            return None;
        }
        let copies = if self.options.packet_replay_probability > 0.0
            && rng.gen_bool(self.options.packet_replay_probability)
        {
            trace!("tick {now}: replaying {reply:?}");
            self.summary.replayed += 1;
            2
        } else {
            1
        };
        for _ in 0..copies {
            let delay = self.sample_delay(rng);
            if delay > self.options.one_way_delay_min {
                self.summary.delayed += 1;
            }
            self.seq += 1;
            self.replies.insert((now + delay, self.seq), reply.clone());
        }
        None
    }

    /// Returns every reply due at tick `now` in delivery order.
    pub fn take_due_replies(&mut self, now: u64) -> Vec<Reply<i64>> {
        let later = self.replies.split_off(&(now + 1, 0));
        let due = std::mem::replace(&mut self.replies, later);
        self.summary.delivered += due.len();
        due.into_values().collect()
    }

    /// Returns every message due at tick `now` in delivery order.
    pub fn take_due(&mut self, now: u64) -> Vec<Envelope> {
        let later = self.queue.split_off(&(now + 1, 0));
        let due = std::mem::replace(&mut self.queue, later);
        self.summary.delivered += due.len();
        due.into_values().collect()
    }

    fn enqueue(&mut self, now: u64, from: Origin, dst: ReplicaId, msg: Msg, rng: &mut ChaCha8Rng) {
        let delay = self.sample_delay(rng);
        if delay > self.options.one_way_delay_min {
            trace!("tick {now}: delaying {msg:?} to {dst} by {delay}");
            self.summary.delayed += 1;
        }
        self.enqueue_at(now, now + delay, from, dst, msg);
    }

    fn sample_delay(&self, rng: &mut ChaCha8Rng) -> u64 {
        let min = self.options.one_way_delay_min;
        let mean = self.options.one_way_delay_mean;
        if mean <= min {
            return min;
        }
        // Exponential distribution with mean `mean - min` on top of the minimum.
        let u: f64 = 1.0 - rng.gen::<f64>();
        let extra = -u.ln() * (mean - min) as f64;
        min + extra.round() as u64
    }

    fn enqueue_at(&mut self, now: u64, at: u64, from: Origin, dst: ReplicaId, msg: Msg) {
        self.seq += 1;
        self.queue.insert(
            (at, self.seq),
            Envelope {
                from,
                to: dst,
                sent_at: now,
                due_at: at,
                message: msg,
            },
        );
    }
}
