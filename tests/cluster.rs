//! Cluster tests: a few replicas and a client driven by hand through a
//! tick function that decides which messages get delivered.
//!
//! Every replica has a disk: a copy of its persistent state that the
//! cluster maintains the way an owner would, from the replica's write
//! after every step, and checks against the replica. Restarts rebuild a
//! replica from its disk.

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use vsr_rs::{
    Checkpoint, Client, ClientRecord, Config, LogBase, LogEntry, LogSegment, Message, MessageFor,
    OpNumber, PersistentState, Replica, ReplicaID, Reply, RequestNumber, StateMachine, Status,
};

#[derive(Clone, Debug, PartialEq, Eq)]
enum Op {
    Add(i32),
    Sub(i32),
}

#[derive(Default)]
struct Accumulator {
    value: i32,
}

impl StateMachine for Accumulator {
    type Input = Op;
    type Output = ();
    type Snapshot = i32;

    fn apply(&mut self, _op_number: OpNumber, entry: &LogEntry<Op>) {
        match entry.op {
            Op::Add(value) => self.value += value,
            Op::Sub(value) => self.value -= value,
        }
    }

    fn snapshot(&self) -> i32 {
        self.value
    }

    fn restore(&mut self, checkpoint: Checkpoint<(), i32>) {
        self.value = checkpoint.state;
    }
}

type Msg = MessageFor<Accumulator>;
type Disk = PersistentState<Op>;

/// Applies the replica's write to a persisted copy of its state, the way
/// an owner persists after every step, checks the copy, and hands the
/// write back. The copy takes every write, those that need no sync too.
/// The check sits between the write and its hand-back, which may commit,
/// so this owner takes and hands back the write itself.
fn persist(disk: &mut Disk, replica: &mut Replica<Accumulator>) {
    let write = replica.take_write();
    disk.apply(&write);
    assert_eq!(
        *disk,
        replica.persistent_state(),
        "disk of replica {}",
        replica.id()
    );
    replica.persisted(write);
}

fn empty_disk() -> Disk {
    PersistentState::empty()
}

/// Replicas and one client, with the messages between them held in a queue
/// that the test delivers as it sees fit.
struct Cluster {
    config: Config,
    replicas: Vec<Replica<Accumulator>>,
    disks: Vec<Disk>,
    client: Client<Op>,
    queue: VecDeque<(ReplicaID, Msg)>,
    replies: Vec<Reply<()>>,
}

impl Cluster {
    fn new(replica_count: usize) -> Cluster {
        Cluster::with_primary_timeout(replica_count, Config::new().primary_timeout())
    }

    /// A cluster whose replicas wait `primary_timeout` idle periods for the
    /// primary, and for a view change to complete.
    fn with_primary_timeout(replica_count: usize, primary_timeout: usize) -> Cluster {
        let _ = env_logger::try_init();
        let mut config = Config::new();
        for _ in 0..replica_count {
            config.add_replica();
        }
        config.set_primary_timeout(primary_timeout);
        let replicas = (0..replica_count)
            .map(|id| Replica::new(id, config.clone(), Accumulator::default()))
            .collect();
        Cluster {
            replicas,
            disks: (0..replica_count).map(|_| empty_disk()).collect(),
            client: Client::new(0, config.clone()),
            config,
            queue: VecDeque::new(),
            replies: Vec::new(),
        }
    }

    fn request(&mut self, op: Op) -> RequestNumber {
        self.client.on_request(op)
    }

    /// Persists replica `id`'s steps, which releases what waited for them.
    fn persist(&mut self, id: ReplicaID) {
        persist(&mut self.disks[id], &mut self.replicas[id]);
    }

    /// Persists every replica, then moves everything the replicas and the
    /// client want sent into the queue, and collects the replies.
    fn collect(&mut self) {
        for (replica, disk) in self.replicas.iter_mut().zip(&mut self.disks) {
            persist(disk, replica);
            self.queue.extend(replica.drain_messages());
            self.replies.extend(replica.drain_replies());
        }
        self.queue.extend(self.client.drain());
    }

    /// Delivers every queued message for which `deliver` returns true, and
    /// whatever those deliveries produce, until nothing is left.
    fn tick_with(&mut self, deliver: &dyn Fn(ReplicaID, &Msg) -> bool) {
        loop {
            self.collect();
            if self.queue.is_empty() {
                return;
            }
            for (replica_id, message) in std::mem::take(&mut self.queue) {
                if deliver(replica_id, &message) {
                    self.replicas[replica_id].on_message(message);
                }
            }
        }
    }

    fn tick(&mut self) {
        self.tick_with(&|_, _| true);
    }

    /// Delivers everything except messages to `dead`.
    fn tick_without(&mut self, dead: ReplicaID) {
        self.tick_with(&|replica_id, _| replica_id != dead);
    }

    fn idle(&mut self) {
        for replica in &mut self.replicas {
            replica.on_idle();
        }
    }

    /// Runs the idle logic of every replica but `dead`.
    fn idle_without(&mut self, dead: ReplicaID) {
        for replica in &mut self.replicas {
            if replica.id() != dead {
                replica.on_idle();
            }
        }
    }

    /// One tick of a cluster whose messages take one tick to arrive, in
    /// the simulator's order: every replica gets an idle period, then
    /// everything queued so far is delivered, and what those deliveries
    /// produce waits for the next step. Returns whether the deliveries
    /// produced anything.
    fn step(&mut self) -> bool {
        self.idle();
        self.collect();
        for (replica_id, message) in std::mem::take(&mut self.queue) {
            self.replicas[replica_id].on_message(message);
        }
        self.collect();
        !self.queue.is_empty()
    }

    /// Whether every replica is in normal status in the same view.
    fn settled(&self) -> bool {
        let view = self.replicas[0].view_number();
        self.replicas
            .iter()
            .all(|replica| replica.status() == Status::Normal && replica.view_number() == view)
    }

    fn take_replies(&mut self) -> Vec<Reply<()>> {
        self.collect();
        std::mem::take(&mut self.replies)
    }

    /// The client table after the first `applied` ops, from the log of a
    /// replica that holds them all.
    fn client_table_as_of(&self, applied: OpNumber) -> Vec<ClientRecord<()>> {
        let log = self
            .replicas
            .iter()
            .find(|replica| replica.log_start() == 0 && replica.commit_number() >= applied)
            .expect("a replica with the first ops in its log")
            .log();
        let mut table = BTreeMap::new();
        for entry in &log[..applied] {
            table.insert(entry.client_id, entry.request_number);
        }
        table
            .into_iter()
            .map(|(client_id, request_number)| ClientRecord {
                client_id,
                request_number,
                reply: (),
            })
            .collect()
    }

    fn value(&self, replica_id: ReplicaID) -> i32 {
        self.replicas[replica_id].state_machine().value
    }

    /// Replaces a replica with one rebuilt from its disk, as after a power
    /// loss: the state machine starts over and applies the committed log
    /// again. Whatever the replica had not sent yet is lost.
    fn restart(&mut self, replica_id: ReplicaID) {
        self.restart_with_applied(replica_id, 0, Accumulator::default());
    }

    /// Like `restart`, with a state machine that had made the first
    /// `applied` ops durable.
    fn restart_with_applied(&mut self, replica_id: ReplicaID, applied: OpNumber, sm: Accumulator) {
        let disk = self.disks[replica_id].clone();
        self.restart_with_state(replica_id, applied, sm, disk);
    }

    /// Like `restart`, from the given persisted state. The client table
    /// comes back with the state machine, as of `applied`.
    fn restart_with_state(
        &mut self,
        replica_id: ReplicaID,
        applied: OpNumber,
        sm: Accumulator,
        state: Disk,
    ) {
        let client_table = self.client_table_as_of(applied);
        self.disks[replica_id] = state.clone();
        self.replicas[replica_id] = Replica::restart(
            replica_id,
            self.config.clone(),
            sm,
            applied,
            client_table,
            state,
            7,
        );
    }
}

#[test]
fn test_normal_operation() {
    let mut cluster = Cluster::new(3);
    cluster.request(Op::Add(10));
    cluster.tick();
    assert_eq!(10, cluster.value(0));
    cluster.request(Op::Sub(5));
    cluster.tick();
    assert_eq!(5, cluster.value(0));
}

#[test]
fn test_idle() {
    let mut cluster = Cluster::new(3);
    cluster.request(Op::Add(10));
    cluster.tick();
    cluster.idle();
    cluster.tick();
    assert_eq!(10, cluster.value(0));
    cluster.request(Op::Sub(5));
    cluster.tick();
    cluster.idle();
    cluster.tick();
    for id in 0..3 {
        assert_eq!(5, cluster.value(id));
    }
}

/// Replica 1 misses everything about the first op and catches up by state
/// transfer when the second arrives.
#[test]
fn test_recovery() {
    let mut cluster = Cluster::new(3);
    cluster.request(Op::Add(10));
    cluster.tick_without(1);
    assert_eq!(10, cluster.value(0));
    cluster.request(Op::Sub(5));
    cluster.tick();
    assert_eq!(5, cluster.value(0));
    cluster.request(Op::Add(7));
    cluster.tick();
    assert_eq!(12, cluster.value(0));
}

/// Prepare messages to one backup arrive in reverse order. The first
/// one it sees has a gap, so it starts state transfer; the earlier ones
/// then arrive while it is still waiting for `NewState` and must be
/// dropped, otherwise its log moves past the point it asked to be
/// repaired from.
#[test]
fn test_prepare_reordered_during_state_transfer() {
    let mut cluster = Cluster::new(3);
    cluster.request(Op::Add(10));
    cluster.request(Op::Add(20));
    cluster.request(Op::Add(30));
    // Delivers everything, but `Prepare` messages to replica 1 in
    // descending op-number order.
    loop {
        cluster.collect();
        if cluster.queue.is_empty() {
            break;
        }
        let mut batch: Vec<_> = std::mem::take(&mut cluster.queue).into_iter().collect();
        batch.sort_by_key(|(replica_id, message)| match message {
            Message::Prepare { op_number, .. } if *replica_id == 1 => std::cmp::Reverse(*op_number),
            _ => std::cmp::Reverse(0),
        });
        for (replica_id, message) in batch {
            cluster.replicas[replica_id].on_message(message);
        }
    }
    assert_eq!(60, cluster.value(0));
    assert_eq!(3, cluster.replicas[1].op_number());
    // A commit heartbeat lets the backups catch up.
    cluster.idle();
    cluster.tick();
    for id in 0..3 {
        assert_eq!(60, cluster.value(id));
    }
}

/// The acknowledgements for op 1 are lost, but both backups acknowledge
/// op 2. A `PrepareOk` for op n acknowledges n and all earlier ops, so
/// the primary must commit ops 1 and 2, in order, once op 2 reaches a
/// quorum, instead of executing op 2 in op 1's slot.
#[test]
fn test_prepare_ok_quorum_commits_prefix() {
    let mut cluster = Cluster::new(3);
    cluster.request(Op::Add(10));
    cluster.request(Op::Add(20));
    cluster.tick_with(&|_, message| !matches!(message, Message::PrepareOk { op_number: 1, .. }));
    assert_eq!(2, cluster.replicas[0].commit_number());
    assert_eq!(30, cluster.value(0));
    cluster.idle();
    cluster.tick();
    for id in 0..3 {
        assert_eq!(30, cluster.value(id));
    }
}

/// A backup's `GetState` is replayed by the network. The reply to the
/// replayed copy arrives during a later state transfer, when the backup
/// already has more entries than the reply starts at. The stale reply
/// overlaps the backup's log and carries the entries it is missing, so
/// the backup must use the suffix instead of treating the mismatch as
/// a fatal error.
#[test]
fn test_stale_new_state_during_state_transfer() {
    let mut cluster = Cluster::new(3);
    let replayed = RefCell::new(Vec::new());

    // Replica 1 misses op 2, so op 3 makes it ask for state transfer.
    // The network replays its `GetState`; keep the copy for later.
    cluster.request(Op::Add(10));
    cluster.request(Op::Add(20));
    cluster.request(Op::Add(30));
    cluster.tick_with(&|replica_id, message| match message {
        Message::Prepare { op_number: 2, .. } if replica_id == 1 => false,
        Message::GetState { .. } => {
            replayed.borrow_mut().push((replica_id, message.clone()));
            true
        }
        _ => true,
    });
    assert_eq!(3, cluster.replicas[1].op_number());

    // Replica 1 misses op 4, so op 5 makes it ask for state transfer
    // again, but this time the `GetState` is lost.
    cluster.request(Op::Add(40));
    cluster.request(Op::Add(50));
    cluster.tick_with(&|replica_id, message| match message {
        Message::Prepare { op_number: 4, .. } if replica_id == 1 => false,
        Message::GetState { .. } => false,
        _ => true,
    });
    assert_eq!(3, cluster.replicas[1].op_number());

    // The replayed `GetState` from the first transfer now reaches the
    // primary. Its reply starts at op 1 and covers ops 1 to 5, and it
    // reaches replica 1 while it waits for a reply starting at op 3.
    for (replica_id, message) in replayed.borrow_mut().drain(..) {
        cluster.replicas[replica_id].on_message(message);
    }
    cluster.tick();
    assert_eq!(5, cluster.replicas[1].op_number());

    cluster.idle();
    cluster.tick();
    for id in 0..3 {
        assert_eq!(150, cluster.value(id));
    }
}

/// A backup's `GetState` is lost. On the next idle period it must ask
/// again instead of staying in state transfer forever.
#[test]
fn test_get_state_retried_on_idle() {
    let mut cluster = Cluster::new(3);
    cluster.request(Op::Add(10));
    cluster.request(Op::Add(20));
    cluster.request(Op::Add(30));
    cluster.tick_with(&|replica_id, message| match message {
        Message::Prepare { op_number: 2, .. } if replica_id == 1 => false,
        Message::GetState { .. } => false,
        _ => true,
    });
    assert_eq!(1, cluster.replicas[1].op_number());
    cluster.idle();
    cluster.tick();
    assert_eq!(3, cluster.replicas[1].op_number());
    cluster.idle();
    cluster.tick();
    for id in 0..3 {
        assert_eq!(60, cluster.value(id));
    }
}

/// Every `PrepareOk` for the last ops is lost. On the next idle period
/// the primary must re-send the uncommitted `Prepare` messages so the
/// backups acknowledge them again.
#[test]
fn test_prepare_resent_on_idle_when_prepare_ok_lost() {
    let mut cluster = Cluster::new(3);
    cluster.request(Op::Add(10));
    cluster.request(Op::Add(20));
    cluster.tick_with(&|_, message| !matches!(message, Message::PrepareOk { .. }));
    assert_eq!(2, cluster.replicas[1].op_number());
    assert_eq!(2, cluster.replicas[2].op_number());
    assert_eq!(0, cluster.replicas[0].commit_number());
    cluster.idle();
    cluster.tick();
    assert_eq!(2, cluster.replicas[0].commit_number());
    cluster.idle();
    cluster.tick();
    for id in 0..3 {
        assert_eq!(30, cluster.value(id));
    }
}

/// The `Prepare` for the last op never reaches any backup, so no backup
/// can notice a gap. On the next idle period the primary must re-send
/// it.
#[test]
fn test_prepare_resent_on_idle_when_prepare_lost() {
    let mut cluster = Cluster::new(3);
    cluster.request(Op::Add(10));
    cluster.request(Op::Add(20));
    cluster.tick_with(&|_, message| !matches!(message, Message::Prepare { op_number: 2, .. }));
    assert_eq!(1, cluster.replicas[1].op_number());
    assert_eq!(1, cluster.replicas[2].op_number());
    assert_eq!(1, cluster.replicas[0].commit_number());
    cluster.idle();
    cluster.tick();
    assert_eq!(2, cluster.replicas[0].commit_number());
    cluster.idle();
    cluster.tick();
    for id in 0..3 {
        assert_eq!(30, cluster.value(id));
    }
}

/// Four replicas, so a quorum is three. Only one backup receives the
/// `Prepare`, and the network delivers its `PrepareOk` twice. Two acks from
/// the same backup are still one backup, so the primary must not commit
/// until another backup acknowledges.
#[test]
fn test_duplicate_prepare_ok_is_not_a_quorum() {
    let mut cluster = Cluster::new(4);
    cluster.request(Op::Add(10));
    loop {
        cluster.collect();
        if cluster.queue.is_empty() {
            break;
        }
        for (replica_id, message) in std::mem::take(&mut cluster.queue) {
            match message {
                Message::Prepare { .. } if replica_id != 3 => continue,
                Message::PrepareOk { .. } => {
                    cluster.replicas[replica_id].on_message(message.clone());
                    cluster.replicas[replica_id].on_message(message);
                }
                _ => cluster.replicas[replica_id].on_message(message),
            }
        }
    }
    assert_eq!(1, cluster.replicas[3].op_number());
    assert_eq!(0, cluster.replicas[1].op_number());
    assert_eq!(0, cluster.replicas[2].op_number());
    assert_eq!(0, cluster.replicas[0].commit_number());
    // Once the other backups get the re-sent Prepare, it commits.
    cluster.idle();
    cluster.tick();
    assert_eq!(1, cluster.replicas[0].commit_number());
    cluster.idle();
    cluster.tick();
    for id in 0..4 {
        assert_eq!(10, cluster.value(id));
    }
}

/// The same client request reaches the primary twice: once more while it
/// is still being prepared, and once more after it has committed. The
/// primary must execute it once. It drops the duplicate that arrives while
/// the request is in progress, and answers the one that arrives after the
/// commit by re-sending the reply it already has.
#[test]
fn test_duplicate_request_executes_once() {
    let mut cluster = Cluster::new(3);
    cluster.request(Op::Add(10));
    // The request is delivered twice before anything else happens.
    cluster.collect();
    let (replica_id, request) = cluster.queue.pop_front().unwrap();
    assert!(cluster.queue.is_empty());
    cluster.replicas[replica_id].on_message(request.clone());
    cluster.replicas[replica_id].on_message(request.clone());
    cluster.tick();
    assert_eq!(1, cluster.replicas[0].op_number());
    assert_eq!(1, cluster.replicas[0].commit_number());
    assert_eq!(10, cluster.value(0));
    assert_eq!(1, cluster.take_replies().len());
    // The request is delivered once more after it committed: the primary
    // re-sends the reply without running it again.
    cluster.replicas[replica_id].on_message(request);
    cluster.tick();
    assert_eq!(1, cluster.replicas[0].op_number());
    assert_eq!(10, cluster.value(0));
    assert_eq!(1, cluster.take_replies().len());
}

/// The client's request is lost. On its next idle period the client must
/// re-send it, to every replica since the primary may have changed, and
/// backups must ignore it.
#[test]
fn test_lost_request_resent_on_idle() {
    let mut cluster = Cluster::new(3);
    cluster.request(Op::Add(10));
    cluster.tick_with(&|_, message| !matches!(message, Message::Request { .. }));
    assert_eq!(0, cluster.replicas[0].op_number());
    // The idle period must make the client re-send the request.
    cluster.client.on_idle();
    cluster.tick();
    assert_eq!(1, cluster.replicas[0].commit_number());
    assert_eq!(10, cluster.value(0));
    let replies = cluster.take_replies();
    assert_eq!(1, replies.len());
    // Once replied to, the request is not re-sent again.
    assert!(cluster
        .client
        .on_reply(replies[0].request_number, replies[0].view_number));
    cluster.client.on_idle();
    cluster.tick();
    assert_eq!(1, cluster.replicas[0].op_number());
    assert_eq!(0, cluster.take_replies().len());
}

/// The primary crashes with an op prepared on the backups but not yet
/// committed. The backups must notice the silence, move to view 1 with
/// replica 1 as primary, and the new primary must commit the op it found
/// in the logs and reply to the client. The client learns the new view from
/// the reply and sends its next request to the new primary.
#[test]
fn test_view_change_after_primary_crash() {
    let mut cluster = Cluster::new(3);

    // Two ops commit everywhere in view 0.
    cluster.request(Op::Add(10));
    cluster.request(Op::Add(20));
    cluster.tick();
    cluster.idle();
    cluster.tick();
    for id in 0..3 {
        assert_eq!(30, cluster.value(id));
    }
    assert_eq!(2, cluster.take_replies().len());

    // A third op reaches the backups, but the primary crashes before it
    // sees their acknowledgements.
    let request_number = cluster.request(Op::Add(30));
    cluster.tick_with(&|replica_id, message| {
        replica_id != 0 || matches!(message, Message::Request { .. })
    });
    assert_eq!(3, cluster.replicas[1].op_number());
    assert_eq!(3, cluster.replicas[2].op_number());
    assert_eq!(2, cluster.replicas[0].commit_number());

    // The backups stop hearing from the primary and change view.
    for _ in 0..10 {
        cluster.idle_without(0);
        cluster.tick_without(0);
    }
    assert_eq!(1, cluster.replicas[1].view_number());
    assert_eq!(1, cluster.replicas[2].view_number());
    assert!(cluster.replicas[1].is_primary());
    assert!(!cluster.replicas[2].is_primary());
    // The new primary commits the op it found and replies to the client.
    assert_eq!(3, cluster.replicas[1].commit_number());
    assert_eq!(3, cluster.replicas[2].commit_number());
    assert_eq!(60, cluster.value(1));
    assert_eq!(60, cluster.value(2));
    let replies = cluster.take_replies();
    assert_eq!(1, replies.len());
    assert_eq!(request_number, replies[0].request_number);
    assert_eq!(1, replies[0].view_number);

    // The client learns the view from the reply and sends the next request
    // to the new primary.
    cluster
        .client
        .on_reply(replies[0].request_number, replies[0].view_number);
    cluster.request(Op::Add(40));
    cluster.tick_without(0);
    cluster.idle_without(0);
    cluster.tick_without(0);
    assert_eq!(4, cluster.replicas[1].commit_number());
    assert_eq!(4, cluster.replicas[2].commit_number());
    assert_eq!(100, cluster.value(1));
    assert_eq!(100, cluster.value(2));
    assert_eq!(1, cluster.take_replies().len());
    // The crashed primary never moved.
    assert_eq!(0, cluster.replicas[0].view_number());
    assert_eq!(30, cluster.value(0));
}

/// A backup that cannot reach anyone keeps starting view changes. The first
/// waits the primary timeout, and each one after that waits twice as long
/// as the last, otherwise replicas whose timers fire faster than a view
/// change can complete keep interrupting each other forever.
#[test]
fn test_view_change_timeout_backs_off() {
    let mut cluster = Cluster::new(3);
    // Replica 1 is alone: nothing it sends is delivered. Record the idle
    // period at which it first asks for each new view.
    let mut first_asked_at: Vec<usize> = Vec::new();
    for idle_period in 1..=200 {
        cluster.replicas[1].on_idle();
        cluster.persist(1);
        for (_, message) in cluster.replicas[1].drain_messages() {
            if let Message::StartViewChange { view_number, .. } = message {
                if view_number > first_asked_at.len() {
                    first_asked_at.push(idle_period);
                }
            }
        }
        if first_asked_at.len() == 4 {
            break;
        }
    }
    assert_eq!(
        4,
        first_asked_at.len(),
        "asked for views at {first_asked_at:?}"
    );
    let timeout = Config::new().primary_timeout();
    let gaps: Vec<usize> = first_asked_at.windows(2).map(|w| w[1] - w[0]).collect();
    assert_eq!(
        vec![timeout, 2 * timeout, 4 * timeout],
        gaps,
        "asked for views at {first_asked_at:?}"
    );
}

/// Replica 1 crashes and comes back with no memory. Until it has recovered
/// it must take no part in the protocol: not acknowledge, not join a view
/// change. Once the others answer its Recovery, it must hold the primary's
/// log and state, and take part again.
#[test]
fn test_recovery_after_reboot() {
    let mut cluster = Cluster::new(3);
    cluster.request(Op::Add(10));
    cluster.request(Op::Add(20));
    cluster.tick();
    cluster.idle();
    cluster.tick();
    for id in 0..3 {
        assert_eq!(30, cluster.value(id));
    }

    // Replica 1 reboots. It last persisted view 0, and picks nonce 42.
    cluster.replicas[1] =
        Replica::recover(1, cluster.config.clone(), Accumulator::default(), 0, 42);
    cluster.disks[1] = empty_disk();
    assert!(cluster.replicas[1].is_recovering());
    assert_eq!(0, cluster.replicas[1].op_number());

    // A new op arrives while it is still recovering: it must not touch it.
    // Nothing it sent has been delivered yet, only the primary's messages.
    cluster.request(Op::Add(30));
    cluster.tick_with(&|_, message| !matches!(message, Message::Recovery { .. }));
    assert!(cluster.replicas[1].is_recovering());
    assert_eq!(0, cluster.replicas[1].op_number());
    assert_eq!(3, cluster.replicas[0].commit_number());
    assert_eq!(60, cluster.value(0));

    // Its Recovery reaches the others, and their responses bring it back.
    cluster.idle();
    cluster.tick();
    assert!(!cluster.replicas[1].is_recovering());
    assert_eq!(3, cluster.replicas[1].op_number());
    assert_eq!(3, cluster.replicas[1].commit_number());
    assert_eq!(60, cluster.value(1));
    assert_eq!(0, cluster.replicas[1].view_number());

    // And it takes part in the next op.
    cluster.request(Op::Add(40));
    cluster.tick_with(&|replica_id, message| {
        replica_id != 2 || matches!(message, Message::Request { .. })
    });
    assert_eq!(4, cluster.replicas[0].commit_number());
    assert_eq!(100, cluster.value(0));
    cluster.idle();
    cluster.tick();
    assert_eq!(100, cluster.value(1));
}

/// A view change must not start the next one. With a primary timeout of two
/// idle periods and messages that take a tick, a view change takes exactly
/// as long as a replica is willing to wait for it: a replica that joins
/// the change from normal status, with no backoff, times out one tick
/// before `StartView` reaches it and starts another view. The previous
/// view's primary is always such a replica, since completing a view reset
/// its backoff, so every view change starts the next one, round the ring,
/// forever. The backoff must survive a completed view long enough to break
/// the ring.
#[test]
fn test_view_change_does_not_start_the_next() {
    let mut cluster = Cluster::with_primary_timeout(3, 2);
    cluster.request(Op::Add(10));
    cluster.tick();
    cluster.idle();
    cluster.tick();
    for id in 0..3 {
        assert_eq!(10, cluster.value(id));
    }
    // Replica 2 hears nothing from the primary for two idle periods and
    // starts a view change. From here on the network is perfect.
    for _ in 0..3 {
        cluster.replicas[2].on_idle();
    }
    assert_eq!(cluster.replicas[2].status(), Status::ViewChange);
    let mut quiet = 0;
    for _ in 0..500 {
        let busy = cluster.step();
        if !busy && cluster.settled() {
            quiet += 1;
            if quiet == 10 {
                break;
            }
        } else {
            quiet = 0;
        }
    }
    let views: Vec<_> = cluster.replicas.iter().map(|r| r.view_number()).collect();
    assert_eq!(
        quiet, 10,
        "the cluster never settled: replicas are in views {views:?}"
    );
}

/// A backup restarts from its disk with a fresh state machine. It applies
/// the committed log again, comes back in normal status in its view, and
/// takes part in the next op.
#[test]
fn test_restart_backup_from_disk() {
    let mut cluster = Cluster::new(3);
    cluster.request(Op::Add(10));
    cluster.request(Op::Add(20));
    cluster.tick();
    cluster.idle();
    cluster.tick();
    for id in 0..3 {
        assert_eq!(30, cluster.value(id));
    }

    cluster.restart(1);
    assert_eq!(Status::Normal, cluster.replicas[1].status());
    assert_eq!(0, cluster.replicas[1].view_number());
    assert_eq!(2, cluster.replicas[1].commit_number());
    assert_eq!(30, cluster.value(1));

    // Only replica 1 acknowledges the next op, so its acknowledgement is
    // what commits it.
    cluster.request(Op::Add(30));
    cluster.tick_with(&|replica_id, message| {
        replica_id != 2 || matches!(message, Message::Request { .. })
    });
    assert_eq!(3, cluster.replicas[0].commit_number());
    assert_eq!(60, cluster.value(0));
}

/// A backup restarts with a state machine that had made some of the
/// committed ops durable: only the rest are applied again.
#[test]
fn test_restart_applies_only_what_the_state_machine_lacks() {
    let mut cluster = Cluster::new(3);
    cluster.request(Op::Add(10));
    cluster.request(Op::Add(20));
    cluster.request(Op::Add(30));
    cluster.tick();
    cluster.idle();
    cluster.tick();
    assert_eq!(60, cluster.value(1));

    // The state machine had applied op 1 when replica 1 went down, and
    // that op alone must not be applied again.
    cluster.restart_with_applied(1, 1, Accumulator { value: 10 });
    assert_eq!(60, cluster.value(1));
    assert_eq!(3, cluster.replicas[1].commit_number());
}

/// The primary restarts with an op in its log that the backups never
/// acknowledged. It does not resume as the primary: it starts the next
/// view. The view starts from the backups' logs, which lack the op, so
/// the client's re-send is what gets it executed.
#[test]
fn test_restart_primary_starts_next_view() {
    let mut cluster = Cluster::new(3);
    cluster.request(Op::Add(10));
    cluster.tick();
    cluster.request(Op::Add(20));
    // The `Prepare` for op 2 is lost, and the primary goes down.
    cluster.tick_with(&|_, message| !matches!(message, Message::Prepare { op_number: 2, .. }));
    assert_eq!(2, cluster.replicas[0].op_number());
    assert_eq!(1, cluster.replicas[0].commit_number());

    cluster.restart(0);
    assert_eq!(Status::ViewChange, cluster.replicas[0].status());
    assert_eq!(1, cluster.replicas[0].view_number());
    assert_eq!(2, cluster.replicas[0].op_number());
    assert_eq!(10, cluster.value(0));

    for _ in 0..10 {
        cluster.idle();
        cluster.tick();
    }
    assert!(cluster.settled());
    assert!(cluster.replicas[1].is_primary());
    for id in 0..3 {
        assert_eq!(1, cluster.replicas[id].commit_number());
        assert_eq!(1, cluster.replicas[id].op_number());
    }
    cluster.client.on_idle();
    cluster.tick();
    cluster.idle();
    cluster.tick();
    for id in 0..3 {
        assert_eq!(2, cluster.replicas[id].commit_number());
        assert_eq!(30, cluster.value(id));
    }
}

/// The primary sends the `Prepare` for an op before its own entry is
/// durable, and loses power before it is. The backups hold the op under
/// op number 2; the primary's disk does not. On restart the primary must
/// not resume and assign op number 2 again: it starts the next view, and
/// the op the backups hold commits there.
#[test]
fn test_prepare_sent_before_persist_survives_primary_crash() {
    let mut cluster = Cluster::new(3);
    cluster.request(Op::Add(10));
    cluster.tick();
    cluster.idle();
    cluster.tick();
    // The request reaches the primary, whose `Prepare` goes out before
    // its disk has op 2.
    let request_number = cluster.request(Op::Add(20));
    cluster.collect();
    for (replica_id, message) in std::mem::take(&mut cluster.queue) {
        cluster.replicas[replica_id].on_message(message);
    }
    let early: Vec<_> = cluster.replicas[0]
        .drain_messages_before_persist()
        .collect();
    assert_eq!(2, early.len());
    for (replica_id, message) in early {
        cluster.replicas[replica_id].on_message(message);
    }
    assert_eq!(1, cluster.disks[0].log.len());
    // The primary restarts from that disk. The backups' acknowledgements
    // reach nobody.
    cluster.restart(0);
    assert_eq!(1, cluster.replicas[0].op_number());
    assert_eq!(Status::ViewChange, cluster.replicas[0].status());
    for id in [1, 2] {
        cluster.persist(id);
        cluster.replicas[id].drain_messages().for_each(drop);
    }
    assert_eq!(2, cluster.replicas[1].op_number());

    for _ in 0..10 {
        cluster.idle();
        cluster.tick();
    }
    assert!(cluster.settled());
    let replies = cluster.take_replies();
    assert!(replies
        .iter()
        .any(|reply| reply.request_number == request_number));
    for id in 0..3 {
        assert_eq!(2, cluster.replicas[id].commit_number());
        assert_eq!(30, cluster.value(id));
    }
}

/// A backup that is down leaves the primary with itself and one backup,
/// which is a quorum of three only if the primary counts its own
/// acknowledgement, which it does once its write is persisted.
#[test]
fn test_primary_counts_own_acknowledgement_after_persist() {
    let mut cluster = Cluster::new(3);
    cluster.request(Op::Add(10));
    cluster.collect();
    for (replica_id, message) in std::mem::take(&mut cluster.queue) {
        cluster.replicas[replica_id].on_message(message);
    }
    // Before the persist, the primary has appended but acknowledged
    // nothing; a backup's acknowledgement alone is no quorum.
    let prepares: Vec<_> = cluster.replicas[0]
        .drain_messages_before_persist()
        .collect();
    for (replica_id, message) in prepares {
        if replica_id == 1 {
            cluster.replicas[replica_id].on_message(message);
        }
    }
    cluster.persist(1);
    let acks: Vec<_> = cluster.replicas[1].drain_messages().collect();
    for (_, message) in acks {
        cluster.replicas[0].on_message(message);
    }
    assert_eq!(0, cluster.replicas[0].commit_number());
    // Draining releases only the messages that need not wait; the commit
    // waits for the write.
    cluster.replicas[0].drain_messages().for_each(drop);
    assert_eq!(0, cluster.replicas[0].commit_number());
    cluster.persist(0);
    assert_eq!(1, cluster.replicas[0].commit_number());
}

/// The primary counts its own acknowledgement for the op number its write
/// holds, not for entries appended after the write was taken.
#[test]
fn test_primary_acknowledges_what_it_wrote() {
    let mut cluster = Cluster::new(3);
    cluster.request(Op::Add(10));
    cluster.collect();
    for (replica_id, message) in std::mem::take(&mut cluster.queue) {
        cluster.replicas[replica_id].on_message(message);
    }
    let write = cluster.replicas[0].take_write();
    assert_eq!(1, write.op_number());
    // Op 2 arrives, and a backup acknowledges both ops, before the write
    // of op 1 is persisted.
    cluster.replicas[0].on_message(Message::Request {
        client_id: 5,
        request_number: 0,
        op: Op::Add(20),
    });
    assert_eq!(2, cluster.replicas[0].op_number());
    cluster.replicas[0].on_message(Message::PrepareOk {
        view_number: 0,
        op_number: 2,
        replica_id: 1,
    });
    assert_eq!(0, cluster.replicas[0].commit_number());
    cluster.disks[0].apply(&write);
    cluster.replicas[0].persisted(write);
    assert_eq!(1, cluster.replicas[0].commit_number());
    assert_eq!(10, cluster.value(0));
    cluster.persist(0);
    assert_eq!(2, cluster.replicas[0].commit_number());
    assert_eq!(30, cluster.value(0));
}

/// A backup's write is out when a `StartView` replaces the entries it
/// holds and commits them. Handing it back executes none of them: memory
/// holds entries the disk lacks, and they wait for the next write.
#[test]
fn test_persisted_executes_only_what_the_write_holds() {
    let mut cluster = Cluster::new(3);
    cluster.request(Op::Add(10));
    cluster.request(Op::Add(20));
    cluster.tick_with(&|_, message| !matches!(message, Message::PrepareOk { .. }));
    assert_eq!(2, cluster.replicas[2].op_number());
    assert_eq!(0, cluster.replicas[2].commit_number());
    let write = cluster.replicas[2].take_write();
    let replaced = |op| LogEntry {
        client_id: 9,
        request_number: 0,
        op,
    };
    cluster.replicas[2].on_message(Message::StartView {
        view_number: 1,
        segment: LogSegment {
            base: LogBase::Op(0),
            entries: vec![replaced(Op::Add(1)), replaced(Op::Add(2))],
        },
        commit_number: 2,
    });
    assert_eq!(2, cluster.replicas[2].commit_number());
    cluster.disks[2].apply(&write);
    cluster.replicas[2].persisted(write);
    assert_eq!(0, cluster.replicas[2].applied());
    assert_eq!(0, cluster.value(2));
    cluster.persist(2);
    assert_eq!(2, cluster.replicas[2].applied());
    assert_eq!(3, cluster.value(2));
}

/// One write is out at a time.
#[test]
#[should_panic(expected = "a write is out")]
fn test_one_write_out_at_a_time() {
    let mut cluster = Cluster::new(3);
    let _write = cluster.replicas[0].take_write();
    let _next = cluster.replicas[0].take_write();
}

/// Until its write is persisted, a replica releases only the messages
/// that promise nothing about its disk, and executes nothing.
#[test]
fn test_drain_before_persisted_releases_only_early_messages() {
    let mut cluster = Cluster::new(3);
    cluster.request(Op::Add(10));
    cluster.tick();
    assert_eq!(0, cluster.replicas[1].commit_number());
    // Backup 1 hears that op 1 committed, times out on the primary and
    // starts a view change, then hears of a later view and asks for its
    // state.
    cluster.replicas[1].on_message(Message::Commit {
        view_number: 0,
        commit_number: 1,
    });
    assert_eq!(1, cluster.replicas[1].commit_number());
    for _ in 0..=Config::new().primary_timeout() {
        cluster.replicas[1].on_idle();
    }
    cluster.replicas[1].on_message(Message::Commit {
        view_number: 5,
        commit_number: 1,
    });
    let released: Vec<Msg> = cluster.replicas[1]
        .drain_messages()
        .map(|(_, message)| message)
        .collect();
    assert!(!released.is_empty());
    assert!(
        released
            .iter()
            .all(|message| matches!(message, Message::GetState { .. })),
        "{released:?}"
    );
    assert_eq!(0, cluster.replicas[1].applied());
    assert_eq!(0, cluster.value(1));
    cluster.persist(1);
    assert_eq!(10, cluster.value(1));
    assert!(cluster.replicas[1]
        .drain_messages()
        .any(|(_, message)| matches!(message, Message::StartViewChange { .. })));
}

/// A replica restarts in the middle of a view change. Its last normal
/// view is behind its view number, so it must not resume as if the view
/// had started: it enters the view change again, and the view completes.
#[test]
fn test_restart_during_view_change() {
    let mut cluster = Cluster::new(3);
    cluster.request(Op::Add(10));
    cluster.tick();
    cluster.idle();
    cluster.tick();

    // The primary goes silent. Replica 2 starts a view change; replica 1
    // joins it and goes down right after.
    for _ in 0..4 {
        cluster.replicas[2].on_idle();
    }
    cluster.tick_with(&|replica_id, message| {
        replica_id == 1 && matches!(message, Message::StartViewChange { .. })
    });
    assert_eq!(Status::ViewChange, cluster.replicas[1].status());
    assert_eq!(1, cluster.replicas[1].view_number());
    assert_eq!(0, cluster.replicas[1].last_normal_view());

    cluster.restart(1);
    assert_eq!(Status::ViewChange, cluster.replicas[1].status());
    assert_eq!(1, cluster.replicas[1].view_number());
    assert!(cluster.replicas[1].is_primary());

    for _ in 0..10 {
        cluster.idle_without(0);
        cluster.tick_without(0);
    }
    assert_eq!(Status::Normal, cluster.replicas[1].status());
    assert_eq!(Status::Normal, cluster.replicas[2].status());
    assert_eq!(1, cluster.replicas[1].view_number());
    assert_eq!(1, cluster.replicas[2].view_number());
    cluster.request(Op::Add(20));
    cluster.client.on_idle();
    cluster.tick_without(0);
    assert_eq!(30, cluster.value(1));
    cluster.idle_without(0);
    cluster.tick_without(0);
    assert_eq!(30, cluster.value(2));
}

/// A replica that was still recovering when it went down holds nothing
/// it may act on, so a restart from its disk recovers again.
#[test]
fn test_restart_while_recovering() {
    let mut cluster = Cluster::new(3);
    cluster.request(Op::Add(10));
    cluster.tick();
    cluster.replicas[1] =
        Replica::recover(1, cluster.config.clone(), Accumulator::default(), 0, 42);
    cluster.disks[1] = empty_disk();
    cluster.collect();
    assert!(cluster.disks[1].recovering);

    cluster.restart(1);
    assert!(cluster.replicas[1].is_recovering());
    cluster.tick();
    assert!(!cluster.replicas[1].is_recovering());
    assert_eq!(10, cluster.value(1));
}

/// The primary compacts its log, and a backup that missed an op cannot get
/// it as entries any more. State transfer brings the primary's checkpoint
/// instead, and the backup carries on from it.
#[test]
fn test_state_transfer_with_checkpoint() {
    let mut cluster = Cluster::new(3);
    cluster.request(Op::Add(10));
    cluster.request(Op::Add(20));
    cluster.tick_without(1);
    assert_eq!(2, cluster.replicas[0].commit_number());
    cluster.replicas[0].compact(2);
    assert_eq!(2, cluster.replicas[0].log_start());
    assert_eq!(0, cluster.replicas[0].log().len());

    cluster.request(Op::Add(30));
    cluster.tick();
    assert_eq!(2, cluster.replicas[1].log_start());
    assert_eq!(3, cluster.replicas[1].op_number());
    cluster.idle();
    cluster.tick();
    for id in 0..3 {
        assert_eq!(60, cluster.value(id));
    }
    // The backup restarts from what it persisted: a checkpoint's worth of
    // state, which the state machine must have kept, and the entries after
    // it.
    cluster.restart_with_applied(1, 2, Accumulator { value: 30 });
    assert_eq!(60, cluster.value(1));
    assert_eq!(3, cluster.replicas[1].commit_number());
}

/// Compaction stops at the commit number: uncommitted entries can still
/// be replaced by a view change.
#[test]
fn test_compaction_stops_at_commit_number() {
    let mut cluster = Cluster::new(3);
    cluster.request(Op::Add(10));
    cluster.tick();
    cluster.request(Op::Add(20));
    cluster.tick_with(&|_, message| !matches!(message, Message::PrepareOk { .. }));
    assert_eq!(1, cluster.replicas[0].commit_number());
    assert_eq!(2, cluster.replicas[0].op_number());
    cluster.replicas[0].compact(2);
    assert_eq!(1, cluster.replicas[0].log_start());
    assert_eq!(1, cluster.replicas[0].log().len());
    cluster.collect();
}

/// The new primary's commit number is behind what the replica with the
/// best log has compacted, so that replica's `DoViewChange` cannot carry
/// everything the primary needs. The primary fetches its checkpoint,
/// starts the view from it, and answers a re-sent request from the client
/// table that came with the checkpoint.
#[test]
fn test_view_change_fetches_checkpoint() {
    let mut cluster = Cluster::new(3);
    // Ops 1 to 3 commit on replicas 0 and 2; replica 1 misses everything.
    cluster.request(Op::Add(10));
    cluster.request(Op::Add(20));
    let request_number = cluster.request(Op::Add(30));
    cluster.tick_without(1);
    cluster.idle_without(1);
    cluster.tick_without(1);
    assert_eq!(3, cluster.replicas[2].commit_number());
    assert_eq!(0, cluster.replicas[1].op_number());
    cluster.replicas[2].compact(3);

    // The primary goes down. Replica 1 becomes the primary of view 1 with
    // replica 2's log, whose entries it has compacted.
    for _ in 0..10 {
        cluster.idle_without(0);
        cluster.tick_without(0);
    }
    assert_eq!(1, cluster.replicas[1].view_number());
    assert_eq!(Status::Normal, cluster.replicas[1].status());
    assert!(cluster.replicas[1].is_primary());
    assert_eq!(3, cluster.replicas[1].log_start());
    assert_eq!(3, cluster.replicas[1].commit_number());
    assert_eq!(60, cluster.value(1));
    assert_eq!(Status::Normal, cluster.replicas[2].status());
    assert_eq!(1, cluster.replicas[2].view_number());

    // The client's latest request, executed before the checkpoint, is
    // answered from the client table.
    cluster.take_replies();
    cluster.replicas[1].on_message(Message::Request {
        client_id: 0,
        request_number,
        op: Op::Add(30),
    });
    let replies = cluster.take_replies();
    assert_eq!(1, replies.len());
    assert_eq!(request_number, replies[0].request_number);

    cluster.client.on_reply(request_number, 1);
    cluster.request(Op::Add(40));
    cluster.tick_without(0);
    assert_eq!(100, cluster.value(1));
    cluster.idle_without(0);
    cluster.tick_without(0);
    assert_eq!(100, cluster.value(2));
}

/// The new primary has compacted past a backup's commit number, so the
/// backup cannot take the log in `StartView`. It fetches the primary's
/// checkpoint and joins the view from it.
#[test]
fn test_start_view_beyond_backup_commit() {
    let mut cluster = Cluster::new(3);
    // Ops 1 to 3 commit on replicas 0 and 1; replica 2 misses everything.
    cluster.request(Op::Add(10));
    cluster.request(Op::Add(20));
    cluster.request(Op::Add(30));
    cluster.tick_without(2);
    cluster.idle_without(2);
    cluster.tick_without(2);
    assert_eq!(3, cluster.replicas[1].commit_number());
    assert_eq!(0, cluster.replicas[2].op_number());
    cluster.replicas[1].compact(3);

    for _ in 0..10 {
        cluster.idle_without(0);
        cluster.tick_without(0);
    }
    assert!(cluster.replicas[1].is_primary());
    assert_eq!(Status::Normal, cluster.replicas[1].status());
    assert_eq!(Status::Normal, cluster.replicas[2].status());
    assert_eq!(1, cluster.replicas[2].view_number());
    assert_eq!(3, cluster.replicas[2].log_start());
    assert_eq!(3, cluster.replicas[2].commit_number());
    assert_eq!(60, cluster.value(2));

    cluster.request(Op::Add(40));
    cluster.client.on_idle();
    cluster.tick_without(0);
    assert_eq!(100, cluster.value(1));
    cluster.idle_without(0);
    cluster.tick_without(0);
    assert_eq!(100, cluster.value(2));
}

/// A replica recovers with no memory from a primary that has compacted
/// its log: the recovery response carries the primary's checkpoint.
#[test]
fn test_recovery_with_checkpoint() {
    let mut cluster = Cluster::new(3);
    cluster.request(Op::Add(10));
    cluster.request(Op::Add(20));
    cluster.tick();
    cluster.idle();
    cluster.tick();
    cluster.replicas[0].compact(2);
    cluster.request(Op::Add(30));
    cluster.tick_with(&|_, message| !matches!(message, Message::PrepareOk { .. }));
    assert_eq!(2, cluster.replicas[0].commit_number());
    assert_eq!(3, cluster.replicas[0].op_number());

    cluster.replicas[1] =
        Replica::recover(1, cluster.config.clone(), Accumulator::default(), 0, 42);
    cluster.disks[1] = empty_disk();
    cluster.tick();
    assert!(!cluster.replicas[1].is_recovering());
    assert_eq!(2, cluster.replicas[1].log_start());
    assert_eq!(2, cluster.replicas[1].commit_number());
    assert_eq!(3, cluster.replicas[1].op_number());
    assert_eq!(30, cluster.value(1));
    // The re-sent `Prepare` for op 3 commits it, and the next heartbeat
    // tells the backups.
    cluster.idle();
    cluster.tick();
    cluster.idle();
    cluster.tick();
    for id in 0..3 {
        assert_eq!(60, cluster.value(id));
    }
}

/// A backup that handles several `Prepare` messages before its owner
/// sends anything acknowledges once, for the last op: an acknowledgement
/// covers every earlier op.
#[test]
fn test_prepare_ok_coalesced() {
    let mut cluster = Cluster::new(3);
    cluster.request(Op::Add(10));
    cluster.request(Op::Add(20));
    cluster.request(Op::Add(30));
    cluster.collect();
    for (replica_id, message) in std::mem::take(&mut cluster.queue) {
        assert_eq!(0, replica_id);
        cluster.replicas[0].on_message(message);
    }
    cluster.collect();
    let prepares: Vec<_> = std::mem::take(&mut cluster.queue)
        .into_iter()
        .filter(|(replica_id, message)| {
            *replica_id == 1 && matches!(message, Message::Prepare { .. })
        })
        .collect();
    assert_eq!(3, prepares.len());
    for (_, message) in prepares {
        cluster.replicas[1].on_message(message);
    }
    cluster.persist(1);
    let acks: Vec<_> = cluster.replicas[1]
        .drain_messages()
        .filter_map(|(_, message)| match message {
            Message::PrepareOk { op_number, .. } => Some(op_number),
            _ => None,
        })
        .collect();
    assert_eq!(vec![3], acks);
}

/// A replica restored a checkpoint, which its state machine made durable
/// at once, and lost power before the log was persisted after it. The
/// state machine is ahead of the log: the checkpoint stands and the older
/// log is dropped, then the replica catches up from the primary.
#[test]
fn test_restart_with_state_machine_ahead_of_log() {
    let mut cluster = Cluster::new(3);
    cluster.request(Op::Add(10));
    cluster.tick();
    cluster.request(Op::Add(20));
    cluster.tick_with(&|_, message| !matches!(message, Message::PrepareOk { .. }));
    // What replica 1 had persisted before the checkpoint: op 1 committed,
    // op 2 still uncommitted.
    let stale = cluster.disks[1].clone();
    assert_eq!((1, 2), (stale.commit_number, stale.log.len()));
    cluster.request(Op::Add(30));
    cluster.tick();
    cluster.idle();
    cluster.tick();
    assert_eq!(3, cluster.replicas[1].commit_number());
    cluster.restart_with_state(1, 3, Accumulator { value: 60 }, stale);
    assert_eq!(3, cluster.replicas[1].commit_number());
    assert_eq!(3, cluster.replicas[1].log_start());
    assert_eq!(0, cluster.replicas[1].log().len());
    assert_eq!(60, cluster.value(1));

    cluster.request(Op::Add(40));
    cluster.tick();
    cluster.idle();
    cluster.tick();
    for id in 0..3 {
        assert_eq!(100, cluster.value(id));
    }
}

/// Five replicas, so a quorum is three. One backup gets three ops at once
/// and acknowledges only the last, another gets only the first op. An
/// acknowledgement covers every earlier op, so the first op has a quorum
/// and must commit at once.
#[test]
fn test_prepare_ok_covers_earlier_ops() {
    let mut cluster = Cluster::new(5);
    cluster.request(Op::Add(10));
    cluster.request(Op::Add(20));
    cluster.request(Op::Add(30));
    cluster.tick_with(&|replica_id, message| match message {
        Message::Prepare { op_number, .. } => {
            replica_id == 1 || (replica_id == 2 && *op_number == 1)
        }
        _ => true,
    });
    assert_eq!(1, cluster.replicas[0].commit_number());
    assert_eq!(10, cluster.value(0));
    cluster.idle();
    cluster.tick();
    assert_eq!(3, cluster.replicas[0].commit_number());
}

/// A backup restored a checkpoint, which its state machine made durable at
/// once, and lost power before the log was persisted after it. The log it
/// had persisted holds an op beyond the checkpoint that it had
/// acknowledged, and that op must survive the restart: the primary is
/// gone, and the op commits in the next view from this replica's log.
#[test]
fn test_restart_keeps_acknowledged_entries_beyond_checkpoint() {
    let mut cluster = Cluster::new(3);
    cluster.request(Op::Add(10));
    cluster.request(Op::Add(20));
    cluster.request(Op::Add(30));
    cluster.tick();
    cluster.idle();
    cluster.tick();
    // Op 4 reaches the backups; the primary never sees their
    // acknowledgements.
    cluster.request(Op::Add(40));
    cluster.tick_with(&|_, message| !matches!(message, Message::PrepareOk { op_number: 4, .. }));
    assert_eq!(4, cluster.replicas[1].op_number());
    assert_eq!(3, cluster.replicas[1].commit_number());
    // Replica 1's disk is behind its state machine: it says op 1 is the
    // last committed, while the state machine holds a checkpoint at op 3.
    let mut stale = cluster.disks[1].clone();
    stale.commit_number = 1;
    cluster.restart_with_state(1, 3, Accumulator { value: 60 }, stale);
    assert_eq!(3, cluster.replicas[1].log_start());
    assert_eq!(3, cluster.replicas[1].commit_number());
    assert_eq!(4, cluster.replicas[1].op_number());
    assert_eq!(60, cluster.value(1));

    for _ in 0..10 {
        cluster.idle_without(0);
        cluster.tick_without(0);
    }
    assert!(cluster.replicas[1].is_primary());
    assert_eq!(4, cluster.replicas[1].commit_number());
    assert_eq!(100, cluster.value(1));
    assert_eq!(100, cluster.value(2));
}

/// A recovering replica restored the primary's checkpoint and lost power
/// before it was persisted as recovered. It recovers again with its state
/// machine at the checkpoint, and applies only what comes after it.
#[test]
fn test_restart_recovering_with_checkpoint() {
    let mut cluster = Cluster::new(3);
    cluster.request(Op::Add(10));
    cluster.request(Op::Add(20));
    cluster.request(Op::Add(30));
    cluster.tick();
    cluster.replicas[1] =
        Replica::recover(1, cluster.config.clone(), Accumulator::default(), 0, 42);
    cluster.disks[1] = empty_disk();
    cluster.collect();
    assert!(cluster.disks[1].recovering);
    cluster.restart_with_applied(1, 2, Accumulator { value: 30 });
    assert!(cluster.replicas[1].is_recovering());
    assert_eq!(2, cluster.replicas[1].log_start());
    cluster.tick();
    assert!(!cluster.replicas[1].is_recovering());
    assert_eq!(3, cluster.replicas[1].commit_number());
    assert_eq!(60, cluster.value(1));
}

/// Committed ops are executed once the write that holds them is
/// persisted, so a state machine that persists what it executes never
/// gets ahead of the log.
#[test]
fn test_state_machine_applies_once_persisted() {
    let mut cluster = Cluster::new(3);
    cluster.request(Op::Add(10));
    cluster.collect();
    for (replica_id, message) in std::mem::take(&mut cluster.queue) {
        cluster.replicas[replica_id].on_message(message);
    }
    cluster.collect();
    for (replica_id, message) in std::mem::take(&mut cluster.queue) {
        cluster.replicas[replica_id].on_message(message);
    }
    cluster.collect();
    for (replica_id, message) in std::mem::take(&mut cluster.queue) {
        cluster.replicas[replica_id].on_message(message);
    }
    // The primary has a quorum of acknowledgements and has committed op
    // 1, but has not executed it.
    assert_eq!(1, cluster.replicas[0].commit_number());
    assert_eq!(0, cluster.replicas[0].applied());
    assert_eq!(0, cluster.value(0));
    assert_eq!(0, cluster.replicas[0].drain_replies().count());
    assert_eq!(0, cluster.replicas[0].applied());
    cluster.persist(0);
    let replies: Vec<_> = cluster.replicas[0].drain_replies().collect();
    assert_eq!(1, replies.len());
    assert_eq!(1, cluster.replicas[0].applied());
    assert_eq!(10, cluster.value(0));
}
