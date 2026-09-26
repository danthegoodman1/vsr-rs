//! A single-threaded benchmark of the replica logic alone: three replicas
//! and a set of closed-loop clients, with messages delivered in rounds and
//! no I/O. For each number of operations in flight it reports the time the
//! replicas take per operation, which leaves out the harness that moves
//! messages and runs the clients, and the messages between replicas per
//! operation. Each runs without idle periods, and with one every
//! fifty-one rounds, when the primary sends its heartbeat and re-sends the
//! `Prepare` of an op a backup has not acknowledged; the last column counts
//! those re-sends per thousand operations. Every number is the median of
//! `REPEAT` runs, three unless set, after a warm-up run.
//!
//! ```console
//! cargo run --release -p vsr-bench --bin vsr-micro
//! OPS=1000000 REPEAT=5 cargo run --release -p vsr-bench --bin vsr-micro
//! ```

use std::collections::VecDeque;
use std::time::{Duration, Instant};
use vsr_rs::{
    Client, ClientID, ClientRecord, Completion, Config, Message, MessageFor, OpNumber, Replica,
    StateMachine,
};

/// Adds up the ops. It keeps no client table: the benchmark never restarts
/// a replica or transfers state, and a copy of each record would count
/// against the library.
struct Sum(u64);

impl StateMachine for Sum {
    type Input = u64;
    type Query = ();
    type Output = u64;
    type Chunk = ();

    fn apply(&mut self, _op_number: OpNumber, op: &u64) -> u64 {
        self.0 = self.0.wrapping_add(*op);
        self.0
    }

    fn query(&self, _query: &()) -> u64 {
        self.0
    }

    fn record_client(
        &mut self,
        _op_number: OpNumber,
        _client_id: ClientID,
        _record: Option<&ClientRecord<u64>>,
    ) {
    }

    fn client_table(&self) -> Vec<ClientRecord<u64>> {
        Vec::new()
    }

    fn checkpoint(&mut self) -> OpNumber {
        unreachable!("the benchmark transfers no state")
    }

    fn checkpoint_chunk(&self, _index: usize) -> ((), bool) {
        unreachable!("the benchmark transfers no state")
    }

    fn release_checkpoint(&mut self) {
        unreachable!("the benchmark transfers no state")
    }

    fn stage_chunk(&mut self, _op_number: OpNumber, _index: usize, _chunk: ()) {
        unreachable!("the benchmark transfers no state")
    }

    fn restore(&mut self, _op_number: OpNumber) {
        unreachable!("the benchmark transfers no state")
    }
}

const REPLICAS: usize = 3;
/// Rounds between two idle periods, when idle periods are on. An op takes
/// two rounds from request to acknowledgement, so the number is odd, for
/// idle periods to fall in both.
const IDLE_ROUNDS: usize = 51;
/// Idle periods every run with them goes through at least, which sets how
/// many ops it takes at depth.
const IDLE_PERIODS: usize = 5;
/// Entries a replica keeps behind what it has applied.
const LOG_RETENTION: usize = 1_000;

/// Replica time and messages between replicas, per operation.
struct Cost {
    ns_per_op: f64,
    messages_per_op: f64,
    /// `Prepare`s sent to a backup again, per thousand operations.
    resends_per_kop: f64,
}

/// Runs `in_flight` closed-loop clients until `total` requests are
/// answered.
fn run(in_flight: usize, total: usize, idle: bool) -> Cost {
    let mut config = Config::new();
    for _ in 0..REPLICAS {
        config.add_replica();
    }
    config.set_clients_max(in_flight);
    let mut replicas: Vec<Replica<Sum>> = (0..REPLICAS)
        .map(|id| Replica::new(id, config.clone(), Sum(0)))
        .collect();
    let mut clients: Vec<Client<u64, ()>> = (0..in_flight)
        .map(|id| Client::new(id, config.clone()))
        .collect();
    let mut queues: Vec<VecDeque<MessageFor<Sum>>> =
        (0..REPLICAS).map(|_| VecDeque::new()).collect();
    for client in &mut clients {
        client.on_request(1);
        for (to, message) in client.drain() {
            queues[to].push_back(message);
        }
    }
    let mut done = 0;
    let mut messages = 0;
    let mut resends = 0;
    // The highest op number whose `Prepare` each replica was sent.
    let mut prepared = [0; REPLICAS];
    let mut round = 0;
    let mut elapsed = Duration::ZERO;
    while done < total {
        round += 1;
        for id in 0..REPLICAS {
            let replica = &mut replicas[id];
            let inbox = std::mem::take(&mut queues[id]);
            let start = Instant::now();
            for message in inbox {
                replica.on_message(message);
            }
            if idle && round % IDLE_ROUNDS == 0 {
                replica.on_idle();
            }
            let mut sent: Vec<_> = replica.drain_messages_before_persist().collect();
            // Nothing to make durable: the write goes straight back.
            let write = replica.take_write();
            replica.persisted(write);
            let applied = replica.applied();
            if applied > replica.log_start() + 2 * LOG_RETENTION {
                replica.compact(applied - LOG_RETENTION);
            }
            sent.extend(replica.drain_messages());
            let replies: Vec<_> = replica.drain_replies().collect();
            elapsed += start.elapsed();
            for (to, message) in sent {
                messages += 1;
                if let Message::Prepare { op_number, .. } = message {
                    if op_number <= prepared[to] {
                        resends += 1;
                    }
                    prepared[to] = prepared[to].max(op_number);
                }
                queues[to].push_back(message);
            }
            for reply in replies {
                let client = &mut clients[reply.client_id()];
                if let Some(Completion::Executed(..)) = client.on_reply(reply) {
                    done += 1;
                    client.on_request(1);
                }
                for (to, message) in client.drain() {
                    queues[to].push_back(message);
                }
            }
        }
    }
    Cost {
        ns_per_op: elapsed.as_nanos() as f64 / done as f64,
        messages_per_op: messages as f64 / done as f64,
        resends_per_kop: resends as f64 * 1000.0 / done as f64,
    }
}

/// The median of `runs`, by `value`.
fn median(runs: &[Cost], value: impl Fn(&Cost) -> f64) -> f64 {
    let mut values: Vec<f64> = runs.iter().map(value).collect();
    values.sort_by(f64::total_cmp);
    values[values.len() / 2]
}

fn env(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn main() {
    let ops = env("OPS", 400_000);
    let repeat = env("REPEAT", 3).max(1);
    println!(
        "{REPLICAS} replicas, at least {ops} ops and {IDLE_PERIODS} idle periods per run, median of {repeat} runs"
    );
    println!(
        "{:>9} {:>9} {:>9} {:>14} {:>14} {:>17}",
        "in flight", "ns/op", "msgs/op", "ns/op idle", "msgs/op idle", "re-sends/kop idle"
    );
    run(16, ops, true);
    for in_flight in [1, 16, 64, 256, 1024, 4096, 16384] {
        // An op takes two rounds, so half the ops in flight complete in
        // each, and this many take the run through the idle periods.
        let total = ops.max(in_flight * IDLE_ROUNDS * IDLE_PERIODS / 2);
        let busy: Vec<Cost> = (0..repeat).map(|_| run(in_flight, total, false)).collect();
        let idle: Vec<Cost> = (0..repeat).map(|_| run(in_flight, total, true)).collect();
        println!(
            "{in_flight:>9} {:>9.0} {:>9.2} {:>14.0} {:>14.2} {:>17.1}",
            median(&busy, |cost| cost.ns_per_op),
            median(&busy, |cost| cost.messages_per_op),
            median(&idle, |cost| cost.ns_per_op),
            median(&idle, |cost| cost.messages_per_op),
            median(&idle, |cost| cost.resends_per_kop),
        );
    }
}
