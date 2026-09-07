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
3. Afterwards, drain what they want sent with `drain_messages`,
   `drain_replies`, and `drain`, and deliver it however you like.

You provide the rest:

- **State machine.** Implement `StateMachine`: `apply`, which the library
  calls in order for every committed operation with its op number, and
  `snapshot` and `restore`, which move the whole state to a replica that
  fell behind a compacted log.
- **Transport.** Serialize `Message` values and move them between
  replicas and clients. The library does not care how, or whether they
  arrive, are duplicated, or are reordered.
- **Timers.** Call `on_idle` at a fixed period. The library measures time
  in idle periods, not seconds.
- **Persistence.** After each step, before delivering what the step
  produced, write what `PersistentState` describes: a few counters, the
  client table, and the log entries from the change marker on. Pass it to
  `Replica::restart` when the replica restarts, with the number of ops the
  state machine had made durable; the replica applies the rest again. That
  ordering is the whole durability argument: an acknowledgement leaves only
  after the entry it covers is on disk, and the replica executes committed
  operations only when its replies are drained, after the step is
  persisted, so a state machine that persists what it executes never gets
  ahead of the log. Two things cost nothing in that argument, and the
  library exposes both: messages that promise nothing about the sender's
  durable state, a `Prepare` above all, can leave before the write, which
  overlaps the primary's write with the backups', and a step that changed
  only the commit number needs no write before delivery.
- **Compaction.** Once the state machine has made its state durable, call
  `compact` with the op number it reached. A replica that needs entries
  another one has compacted gets a checkpoint of its state instead.

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
| Client table and request retransmission | 4.1 | done |
| View changes | 4.2 | done, with exponential backoff |
| Recovery | 4.3 | done, with a persisted view number[^michael17] |
| State transfer | 5.2 | done, without the truncation defect[^vanlightly22] |
| Checkpoints and log compaction | 5.1 | done, checkpoints come from the state machine |
| Durable log | | done, the owner persists after every step |
| Reconfiguration | 7 | out of scope, membership is fixed |

## Getting started

A replicated counter, with three replicas and one client in a single
process. A test or a simulator delivers the messages like this; a real
program puts them on the wire.

```rust
use vsr_rs::{Checkpoint, Client, Config, LogEntry, OpNumber, Replica, StateMachine};

struct Counter(i64);

impl StateMachine for Counter {
    type Input = i64;
    type Output = i64;
    type Snapshot = i64;

    fn apply(&mut self, _op_number: OpNumber, entry: &LogEntry<i64>) -> i64 {
        self.0 += entry.op;
        self.0
    }

    fn snapshot(&self) -> i64 {
        self.0
    }

    fn restore(&mut self, checkpoint: Checkpoint<i64, i64>) {
        self.0 = checkpoint.state;
    }
}

let mut config = Config::new();
for _ in 0..3 {
    config.add_replica();
}
let mut replicas: Vec<_> = (0..3)
    .map(|id| Replica::new(id, config.clone(), Counter(0)))
    .collect();
let mut client = Client::new(0, config);

client.on_request(5);
loop {
    let mut queue: Vec<_> = client.drain().collect();
    for replica in &mut replicas {
        queue.extend(replica.drain_messages());
        for reply in replica.drain_replies() {
            println!("request {} -> {}", reply.request_number, reply.result);
        }
    }
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
persists the replica's log with the `writeahead` crate, one fsync per batch
of events, and keeps the store in a `fjall` database that is persisted once
a second, after which the log is compacted. Start three nodes, each in its
own terminal:

```console
cargo build --example kvstore
./target/debug/examples/kvstore --id 0 --replicas 127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002 --listen 127.0.0.1:6379
./target/debug/examples/kvstore --id 1 --replicas 127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002 --listen 127.0.0.1:6380
./target/debug/examples/kvstore --id 2 --replicas 127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002 --listen 127.0.0.1:6381
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
a second and keep serving. Start node 0 again and it comes back from its
disk and rejoins as a backup. Kill all three and start them again, and
they come back with everything they had committed.

## Verification

To run the integration tests, type:


```console
cargo test --workspace
```

### Simulator

[`simulator/`](simulator) is a deterministic simulator modeled on
TigerBeetle's VOPR. It runs a cluster and its clients in one thread, passes
every message through a network that loses, replays, and delays them,
crashes and restarts replicas, sometimes from what they persisted after a
power loss, sometimes with their disk wiped, cuts the power of every
replica at once, or of a replica between sending what need not wait and
persisting the step, makes state machines flush and replicas compact
their logs, and checks a set of safety properties after every tick:

- committed prefixes agree on every replica,
- committed operations survive on enough replicas,
- every reply matches a committed request,
- no request runs twice.

Each replica has a disk that the simulator writes the way an owner would,
from the change marker after every step, and a replica that lost power
is rebuilt from it.

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

### Benchmark

[`bench/`](bench) measures what the durable log costs. Three replicas run
on their own threads with the event loop of the kvstore example and a
fjall store, and a set of closed-loop clients drives them over channels.
It compares the library as it was before the durable log, embedded from
the commit that preceded it, with the current library with the journal
off and on, so the cost of the new bookkeeping and the cost of the fsync
per batch can be read apart.

```console
cargo run --release -p vsr-bench
CLIENTS=1,16,256 cargo run --release -p vsr-bench
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
