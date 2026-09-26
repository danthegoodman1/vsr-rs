# Viewstamped Replication

[![CI](https://github.com/penberg/vsr-rs/actions/workflows/smoke_test.yml/badge.svg)](https://github.com/penberg/vsr-rs/actions/workflows/smoke_test.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE.md)

Viewstamped Replication (VSR) is a replication protocol that enables a group of
servers to act as a single reliable service, remaining consistent even when
some of them crash. It was introduced by Brian Oki and Barbara Liskov in 1988
[^oki88], but later restated in a cleaner, standalone form by Liskov and
Cowling in the 2012 paper Viewstamped Replication Revisited[^liskov12].
Recently, VSR has been popularized by the [TigerBeetle](https://github.com/tigerbeetle/tigerbeetle) project.

**Status:** experimental. The protocol is complete and simulator-tested,
but not formally verified nor known to run in production.

## Motivation

**Paxos is notoriously hard.** Lamport's original paper took eight years
to reach publication, and the follow-up was titled *Paxos Made Simple*.
Multi-Paxos, the version anyone actually runs, has no canonical
specification: the engineers who built Google's Chubby on it reported
"significant gaps between the description of the Paxos algorithm and the
needs of a real-world system" and had to fill in the leader, log, and
reconfiguration machinery themselves[^chandra07]. Raft exists because of
this; its paper is titled *In Search of an Understandable Consensus
Algorithm*[^ongaro14].

**Raft is derived from Viewstamped Replication.** The Raft paper says it
is "similar in many ways to existing consensus algorithms (most notably,
Oki and Liskov's Viewstamped Replication)"[^ongaro14]. Both have one
primary, a log, and a majority; Raft renamed views to terms and the primary
to a leader. The main difference is leader election: Raft uses randomized
timeouts, VSR rotates the primary round-robin with each view, which Joran
Dirk Greef of TigerBeetle argues gives better stability and availability,
lower latency, and no dueling leaders[^greef25].

**Viewstamped Replication is easy to simulate deterministically.**
Nothing in the 2012 protocol is random. The next primary is fixed in
advance, so a replica's behaviour is a pure function of the messages it
received and the ticks it counted. Raft's randomized election timeouts are
one more source of nondeterminism a simulator has to seed and reproduce;
VSR has none, so a run is determined by the network schedule alone.

## Design

The library provides the consensus state machine and nothing else. It does
no I/O, keeps no clocks, and starts no threads, the way TigerBeetle
[^tigerbeetle] structures its replica.

A `Replica` and a `Client` are state machines that their owner steps:

1. Hand a replica incoming messages with `on_message`, and a client
   incoming replies with `on_reply`.
2. Tell each one that time has passed with `on_idle`, at a regular
   interval. That drives heartbeats, retransmission, and the view change
   timer.
3. Afterwards, persist the replica's write with `persist`. Then drain
   what they want sent with `drain_messages`, `drain_replies`, and
   `drain`, and deliver it however you like.

You provide the rest:

- **State machine.** Implement `StateMachine`: `apply`, which the library
  calls in order for every committed operation with its op number; `query`,
  which reads the state;
  `record_client` and `client_table`, which keep the client table with the
  state; and `checkpoint`, `checkpoint_chunk`, `release_checkpoint`,
  `stage_chunk`, and `restore`, which move the state a chunk at a time to
  a replica that fell behind a compacted log.
- **Transport.** Serialize `Message` values and move them between
  replicas and clients. The library does not care how, or whether they
  arrive, are duplicated, or are reordered.
- **Timers.** Call `on_idle` at a fixed period. The library measures time
  in idle periods, not seconds.
- **Persistence.** After each step, call `persist` with a function that
  makes the replica's `LogWrite` durable: a few counters and the log
  entries that changed. Handing the write back releases what waited for
  it. An owner that writes while the replica goes on takes the write with
  `take_write` and hands it back with `persisted` itself. Rebuild the
  replica's `PersistentState` from the writes when it restarts, and pass
  it to `Replica::restart` with a state machine made durable as of the ops
  it had applied, and the client table it keeps with them; the replica
  applies the rest again. That ordering is the whole durability argument:
  an acknowledgement leaves only after the entry it covers is on disk, and
  the replica executes a committed operation only once a handed-back write
  holds it, so a state machine that persists what it executes never gets
  ahead of the log. Three things cost nothing in that argument, and the
  library exposes all three: messages that promise nothing about the
  sender's durable state, a `Prepare` above all, can leave before the
  write, which overlaps the primary's write with the backups'; replies
  can leave before it too, since an earlier write holds what they answer;
  and a write that changed only the commit number needs no sync.
- **Compaction.** Once the state machine has made its state durable, call
  `compact` with the op number it reached. A replica that needs entries
  another one has compacted fetches a checkpoint of its state instead, a
  chunk at a time, and the sender compacts no entry after the checkpoint
  while replicas ask for its chunks.

A client registers for a session through the log, then keeps up to
`Config::in_flight_max` requests in flight. The primary appends only a
session's next request, so a session's requests execute in order. The
client table holds at most `Config::clients_max` sessions: registering one
more evicts the session whose latest entry executed earliest, the same one
on every replica. An evicted session's requests never execute, and its
client learns so from the reply, fails what it had in flight, and
registers again.

A query reads the state machine without a log entry. The primary answers
it once a quorum has confirmed its view in a round started after the query
arrived, from a state that holds every op that may have completed by then:
the ops committed in earlier views, and those up to its commit number. So
the query sees every write completed before it. No clocks are involved. At
most 32 rounds are out at a time, whatever the number of queries. A client
keeps its operations in the order it issued them: a query goes out only
once every earlier request has its reply, and a request only once every
earlier query has its reply.

A replica whose disk is gone comes back through `Replica::recover`, which
fetches the state from the others. It needs the view number it had, or it
can forget it asked for a view change and let two views run at
once[^michael17].

Reconfiguration, which changes the membership of a running cluster, is out
of scope. The membership is fixed when the cluster is created. TigerBeetle
runs the same protocol in production without it, replacing a lost machine
with a new one that recovers into the same replica id, and the same works
here.

| Feature | Paper section | Status |
|---|---|---|
| Normal operation | 4.1 | done |
| Client table and request retransmission | 4.1 | done, with sessions registered through the log, requests in flight, and a bounded table |
| Reads | 6.3 | done, as queries the primary answers once a quorum confirms its view, rather than with leases |
| View changes | 4.2 | done, with exponential backoff |
| Recovery | 4.3 | done, with a persisted view number[^michael17] |
| State transfer | 5.2 | done, without the truncation defect[^vanlightly22] |
| Checkpoints and log compaction | 5.1 | done, checkpoints come from the state machine a chunk at a time |
| Durable log | | done, the owner persists a write after every step |
| Reconfiguration | 7 | out of scope, membership is fixed |

## Getting started

A replicated counter, with three replicas and one client in a single
process. A test or a simulator delivers the messages like this; a real
program persists each write and puts the messages on the wire.

```rust
use vsr_rs::{
    Client, ClientID, ClientRecord, Completion, Config, OpNumber, Replica, StateMachine,
};
use std::collections::BTreeMap;

/// The value and the client table: the whole state, in one chunk.
type State = (i64, Vec<ClientRecord<i64>>);

#[derive(Clone, Debug, Default)]
struct Counter {
    value: i64,
    clients: BTreeMap<ClientID, ClientRecord<i64>>,
    op_number: OpNumber,
    kept: Option<State>,
    staged: Option<State>,
}

impl StateMachine for Counter {
    type Input = i64;
    type Query = ();
    type Output = i64;
    type Chunk = State;

    fn apply(&mut self, op_number: OpNumber, op: &i64) -> i64 {
        self.value += op;
        self.op_number = op_number;
        self.value
    }

    fn query(&self, _query: &()) -> i64 {
        self.value
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
        self.op_number = op_number;
    }

    fn client_table(&self) -> Vec<ClientRecord<i64>> {
        self.clients.values().cloned().collect()
    }

    fn checkpoint(&mut self) -> OpNumber {
        self.kept = Some((self.value, self.client_table()));
        self.op_number
    }

    fn checkpoint_chunk(&self, _index: usize) -> (State, bool) {
        (self.kept.clone().expect("a checkpoint kept"), true)
    }

    fn release_checkpoint(&mut self) {
        self.kept = None;
    }

    fn stage_chunk(&mut self, _op_number: OpNumber, _index: usize, chunk: State) {
        self.staged = Some(chunk);
    }

    fn restore(&mut self, op_number: OpNumber) {
        let (value, table) = self.staged.take().expect("a staged checkpoint");
        self.value = value;
        self.clients = table.into_iter().map(|record| (record.client_id, record)).collect();
        self.op_number = op_number;
    }
}

let mut config = Config::new();
for _ in 0..3 {
    config.add_replica();
}
let mut replicas: Vec<_> = (0..3)
    .map(|id| Replica::new(id, config.clone(), Counter::default()))
    .collect();
let mut client: Client<i64, ()> = Client::new(0, config);

client.on_request(5);
loop {
    let mut queue: Vec<_> = client.drain().collect();
    for replica in &mut replicas {
        // Nothing to make durable in memory: hand the write straight back.
        let write = replica.take_write();
        replica.persisted(write);
        queue.extend(replica.drain_messages());
        for reply in replica.drain_replies() {
            if let Some(Completion::Executed(request, result)) = client.on_reply(reply) {
                println!("request {request} -> {result}");
            }
        }
    }
    queue.extend(client.drain());
    if queue.is_empty() {
        break;
    }
    for (to, message) in queue {
        replicas[to].on_message(message);
    }
}
```

For a complete program, [`examples/kvstore`](examples/kvstore) is a
replicated key-value store over TCP that speaks a Redis-like protocol. It
persists the replica's log with the `writeahead` crate, one fsync per
journal write, which holds every batch of events stepped while the last
write was out, and keeps the store in a `fjall` database that is persisted
once a second, after which the log is compacted. Each client connection is
a session, and its commands pipeline: they are read as they come and
answered in order. Start three nodes of a new cluster, each in its own
terminal:

```console
cargo build --example kvstore
./target/debug/examples/kvstore --init --id 0 --replicas 127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002 --listen 127.0.0.1:6379
./target/debug/examples/kvstore --init --id 1 --replicas 127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002 --listen 127.0.0.1:6380
./target/debug/examples/kvstore --init --id 2 --replicas 127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002 --listen 127.0.0.1:6381
```

Talk to any of them:

```console
$ nc localhost 6379
SET foo bar
+OK
GET foo
$3
bar
```

Stop node 0 with Ctrl-C, or kill it. The others pick a new primary within
a second and keep serving. Start node 0 again, without `--init`, and it
comes back from its disk and rejoins as a backup. Kill all three and start
them again, and they come back with everything they had committed.

## Verification

To run the integration tests, the simulator's tests, and the kvstore
example's, type:

```console
cargo test --workspace --all-targets
```

### Simulator

[`simulator/`](simulator) is a deterministic simulator modeled on
TigerBeetle's VOPR. It runs a cluster and its clients in one thread, passes
every message through a network that loses, replays, and delays them,
crashes and restarts replicas, sometimes from what they persisted after a
power loss, sometimes after their process died with a state machine
ahead of its last flush, sometimes with their disk wiped, cuts the power
of every replica at once, or of a replica between sending what need not
wait and persisting the step, makes state machines flush and replicas
compact their logs, and checks a set of safety properties after every
tick:

- committed prefixes agree on every replica,
- committed operations survive on enough disks,
- every operation a replica executed is on its disk,
- every reply matches what committed: a request's result, a session the
  log opened, an eviction the log made,
- a session's requests execute in order, and only in their session,
- every query reads a state that holds every request completed before the
  query was issued, and every earlier request of its own client,
- no request runs twice.

It also checks every message as it leaves: a replica acknowledges an op,
takes part in a view change, or answers a recovery only with state its
disk holds.

Each replica has a disk that the simulator writes the way an owner would,
from the replica's write after every step. In most runs a write can also
stay out for several steps while the replica goes on, as with an owner
that writes on another thread. A replica that loses power loses its memory
at once, with any step it had not yet written, lands a write that was out
whole or not at all, and is rebuilt from its disk.

The seed determines the whole configuration, from cluster size to fault
rates. Once the requests are done, faults stop and a random majority of
replicas must converge, or the run fails.

```console
cargo run --release -p vsr-simulator
```

Every run prints its seed; pass it back to reproduce the run exactly. A git
commit hash works as a seed too, which is how CI runs it.

```console
cargo run --release -p vsr-simulator -- 10693013600028533629
cargo run --release -p vsr-simulator -- --lite      # small cluster, fewer requests
cargo run --release -p vsr-simulator -- --help      # overrides for every fault
```

To run it on every core with random seeds for a while, and get the commands
to reproduce whatever failed:

```console
scripts/simulate --budget 1h
scripts/simulate --report          # the runs of the current commit
scripts/simulate --report --all    # every commit ever run
```

To check that the simulator catches persistence bugs, `scripts/mutants`
plants each of a few known ones in a scratch copy, such as an
acknowledgement sent before its write, and reports how many of 400 seeds
catch it:

```console
scripts/mutants
```

### Benchmark

[`bench/`](bench) holds two benchmarks.

`vsr-micro` measures the library alone: three replicas and a set of
closed-loop clients in one thread, with no I/O, reporting the time the
replicas take and the messages between them per operation, by the number
of operations in flight. On the development machine, the library before
the durable log (commit `0b64760`) and now, through the same loop:

| ops in flight | 1 | 16 | 64 | 256 | 1,024 | 4,096 | 16,384 |
|---|---|---|---|---|---|---|---|
| before, ns/op | 314 | 166 | 189 | 273 | 627 | 2,152 | 9,493 |
| now, ns/op | 484 | 181 | 170 | 172 | 177 | 200 | 267 |

With one operation in flight the library now spends more on each, on the
write it builds and on holding back what waits for it; with more, it
spends less, and the cost no longer grows with the operations in flight.
Messages per operation fell from four to two once several are in flight:
a backup acknowledges a batch of `Prepare`s with one `PrepareOk`.

`vsr-bench` measures what durability costs in the kvstore's event loop.
Three kvstore nodes run in one process, each on its own thread with the
kvstore's event loop, store, journal, and timer, with channels between
them in place of TCP, and closed-loop clients send commands to node 0.
It runs the nodes with the journal on and off, several times each, and
reports the median and the spread of throughput, latency percentiles, and
fsyncs per node per second, journal writes and store persists alike.

It can also emulate other hardware: with the data on a tmpfs, where
fsync costs nothing, `FSYNC_US` adds that many microseconds to every
journal fsync, and `NET_US` delays every frame between nodes by that many
microseconds each way. On the development machine, with a 150 µs round
trip between nodes (`NET_US=75`) and the fsync of an NVMe drive with
power-loss protection, 15 to 100 µs as load grows, the median ops/s of
three rounds of SETs from clients with one command in flight each:

| fsync | 1 client | 16 | 64 | 256 | 1,024 |
|---|---|---|---|---|---|
| 15 µs | 4.9k | 76.4k | 286.7k | 635.4k | 655.4k |
| 30 µs | 4.6k | 69.9k | 252.8k | 639.5k | 659.4k |
| 60 µs | 4.0k | 57.2k | 206.3k | 574.2k | 653.4k |
| 100 µs | 3.4k | 47.1k | 163.7k | 500.8k | 647.9k |

With enough requests in flight the primary's event loop sets the limit,
whatever the fsync. With few, each request waits out a round trip and a
backup's fsync; the primary's own fsync overlaps them.

A GET is a query, which costs no log entry and no fsync. At 30 µs, with
`READS` percent of the commands GETs:

| reads | 1 client | 16 | 64 | 256 | 1,024 |
|---|---|---|---|---|---|
| 50% | 4.9k | 78.9k | 297.7k | 752.3k | 759.9k |
| 90% | 5.3k | 88.4k | 310.1k | 679.8k | 668.3k |

A connection with 16 SETs in flight (`PIPELINE=16`) goes further: 638.9k
ops/s from 16 connections, 827.6k from 64.

```console
cargo run --release -p vsr-bench --bin vsr-micro
cargo run --release -p vsr-bench
CLIENTS=1,16,256 REPEAT=5 cargo run --release -p vsr-bench
NET_US=100 FSYNC_US=50 cargo run --release -p vsr-bench -- /dev/shm/vsr-bench
```

### Coverage

`scripts/coverage` runs a batch of random seeds under LLVM instrumentation
to show which lines of the implementation the simulator reaches. It needs
the LLVM tools of the toolchain, once:

```console
rustup component add llvm-tools-preview
scripts/coverage --runs 100
```

It prints a per-file summary and writes an HTML report to
`target/coverage/html/index.html`:

```console
open target/coverage/html/index.html      # xdg-open on Linux
```

### Interactive simulation

The same simulator has a terminal viewer that draws the replicas, every
message in flight, each replica's state, view, log and commit progress,
and an event log. It takes faults from the keyboard: crash, power loss,
blackout, restart, reboot without memory, flush, partition, packet loss.

```console
cargo run --release -p vsr-simulator --bin vsr-simulator-tui -- --interactive
cargo run --release -p vsr-simulator --bin vsr-simulator-tui -- 10693013600028533629 --until 40000
```

`--interactive` is a perfect cluster with no seed: nothing goes wrong until
you inject a fault. On quit it prints every fault you injected as a script,
and `--script FILE` replays one. A seed instead replays the run the
headless simulator does for it, and `--until` runs at full speed to a tick
and pauses there, for stepping into the failure a seed reproduces.

## License

This project is licensed under the [MIT license](LICENSE.md).

### Contribution

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in `vsr-rs` by you shall be licensed as MIT,
without any additional terms or conditions.

[^oki88]: Oki, B. M., & Liskov, B. H. (1988). *Viewstamped Replication: A New
    Primary Copy Method to Support Highly-Available Distributed Systems.*
    PODC '88. https://www.cs.princeton.edu/courses/archive/fall11/cos518/papers/viewstamped.pdf

[^liskov12]: Liskov, B., & Cowling, J. (2012). *Viewstamped Replication
    Revisited.* MIT-CSAIL-TR-2012-021. https://dspace.mit.edu/entities/publication/80846d94-fcd3-40e6-87fb-8d91fe99a5d1

[^michael17]: Michael, E., Ports, D. R. K., Sharma, N., & Szekeres, A. (2017).
    *Recovering Shared Objects Without Stable Storage.* DISC 2017.
    https://drkp.net/papers/recovery-tr17.pdf

[^vanlightly22]: Vanlightly, J. (2022). *VR Revisited: An Analysis with TLA+.*
    https://jack-vanlightly.com/analyses/2022/12/20/vr-revisited-an-analysis-with-tlaplus

[^tigerbeetle]: TigerBeetle. https://github.com/tigerbeetle/tigerbeetle.
    Its license is in [licenses/tigerbeetle.md](licenses/tigerbeetle.md).

[^ongaro14]: Ongaro, D., & Ousterhout, J. (2014). *In Search of an
    Understandable Consensus Algorithm.* USENIX ATC '14.
    https://raft.github.io/raft.pdf

[^chandra07]: Chandra, T. D., Griesemer, R., & Redstone, J. (2007). *Paxos
    Made Live: An Engineering Perspective.* PODC '07.
    https://www.cs.utexas.edu/users/lorenzo/corsi/cs380d/papers/paper2-1.pdf

[^greef25]: Greef, J. D. (2025). Comment on Hacker News.
    https://news.ycombinator.com/item?id=44929576
