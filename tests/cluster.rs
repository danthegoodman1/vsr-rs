//! Cluster tests: a few replicas and a client driven by hand through a
//! tick function that decides which messages get delivered.
//!
//! Every replica has a disk: a copy of its persistent state that the
//! cluster maintains the way an owner would, from the replica's write
//! after every step, and checks against the replica. Restarts rebuild a
//! replica from its disk.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use vsr_rs::{
    Client, ClientID, ClientRecord, Completion, Config, LogBase, LogEntry, LogSegment, Message,
    MessageFor, OpNumber, PersistentState, Replica, ReplicaID, Reply, RequestNumber, StateMachine,
    Status,
};

#[derive(Clone, Debug, PartialEq, Eq)]
enum Op {
    Add(i32),
    Sub(i32),
}

impl Op {
    fn apply(&self, value: i32) -> i32 {
        match self {
            Op::Add(operand) => value + operand,
            Op::Sub(operand) => value - operand,
        }
    }
}

#[derive(Clone, Debug, Default)]
struct Accumulator {
    value: i32,
    clients: BTreeMap<ClientID, ClientRecord<i32>>,
    /// The last op number handed over.
    last_op: OpNumber,
    /// The checkpoint kept for replicas that fell behind.
    kept: Option<Box<Accumulator>>,
    /// The chunks staged of another replica's checkpoint, with its op
    /// number.
    staged: Vec<(OpNumber, Chunk)>,
}

/// A checkpoint comes as a chunk per client record, then its value.
#[derive(Clone, Debug)]
enum Chunk {
    Client(ClientRecord<i32>),
    Value(i32),
}

impl StateMachine for Accumulator {
    type Input = Op;
    type Query = ();
    type Output = i32;
    type Chunk = Chunk;

    fn apply(&mut self, op_number: OpNumber, op: &Op) -> i32 {
        self.value = op.apply(self.value);
        self.last_op = op_number;
        self.value
    }

    fn query(&self, _query: &()) -> i32 {
        self.value
    }

    fn record_client(
        &mut self,
        op_number: OpNumber,
        client_id: ClientID,
        record: Option<&ClientRecord<i32>>,
    ) {
        match record {
            Some(record) => self.clients.insert(client_id, record.clone()),
            None => self.clients.remove(&client_id),
        };
        self.last_op = op_number;
    }

    fn client_table(&self) -> Vec<ClientRecord<i32>> {
        self.clients.values().cloned().collect()
    }

    fn checkpoint(&mut self) -> OpNumber {
        let kept = Accumulator {
            value: self.value,
            clients: self.clients.clone(),
            last_op: self.last_op,
            ..Accumulator::default()
        };
        self.kept = Some(Box::new(kept));
        self.last_op
    }

    fn checkpoint_chunk(&self, index: usize) -> (Chunk, bool) {
        let kept = self.kept.as_ref().expect("a checkpoint kept");
        match kept.clients.values().nth(index) {
            Some(record) => (Chunk::Client(record.clone()), false),
            None => (Chunk::Value(kept.value), true),
        }
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
        self.clients.clear();
        for (staged_at, chunk) in std::mem::take(&mut self.staged) {
            assert_eq!(op_number, staged_at);
            match chunk {
                Chunk::Client(record) => {
                    self.clients.insert(record.client_id, record);
                }
                Chunk::Value(value) => self.value = value,
            }
        }
        self.last_op = op_number;
    }
}

type Msg = MessageFor<Accumulator>;
type TestClient = Client<Op, ()>;
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

/// Replicas and a client, with the messages between them held in a queue
/// that the test delivers as it sees fit. More clients can join.
struct Cluster {
    config: Config,
    replicas: Vec<Replica<Accumulator>>,
    disks: Vec<Disk>,
    /// Client 0.
    client: TestClient,
    /// Clients 1 and up, and what their replies completed.
    others: Vec<(TestClient, Vec<Completion<i32>>)>,
    queue: VecDeque<(ReplicaID, Msg)>,
    replies: Vec<Reply<i32>>,
}

impl Cluster {
    fn new(replica_count: usize) -> Cluster {
        Cluster::with_primary_timeout(replica_count, Config::new().primary_timeout())
    }

    /// A cluster whose replicas wait `primary_timeout` idle periods for the
    /// primary, and for a view change to complete.
    fn with_primary_timeout(replica_count: usize, primary_timeout: usize) -> Cluster {
        let mut config = Config::new();
        config.set_primary_timeout(primary_timeout);
        Cluster::with_config(replica_count, config)
    }

    /// A cluster of `replica_count` replicas configured as `config` says.
    fn with_config(replica_count: usize, mut config: Config) -> Cluster {
        let _ = env_logger::try_init();
        for _ in 0..replica_count {
            config.add_replica();
        }
        let replicas = (0..replica_count)
            .map(|id| Replica::new(id, config.clone(), Accumulator::default()))
            .collect();
        let mut cluster = Cluster {
            replicas,
            disks: (0..replica_count).map(|_| empty_disk()).collect(),
            client: Client::new(0, config.clone()),
            others: Vec::new(),
            config,
            queue: VecDeque::new(),
            replies: Vec::new(),
        };
        // The client's session is op 1, committed everywhere.
        cluster.client.register();
        cluster.tick();
        cluster.idle();
        cluster.tick();
        assert_eq!(Some(1), cluster.client.session());
        cluster.replies.clear();
        cluster
    }

    fn request(&mut self, op: Op) -> RequestNumber {
        self.client.on_request(op)
    }

    /// Adds a client, and returns its id.
    fn add_client(&mut self) -> ClientID {
        let client_id = self.others.len() + 1;
        let client = Client::new(client_id, self.config.clone());
        self.others.push((client, Vec::new()));
        client_id
    }

    /// Client `id`, one of those added.
    fn other(&mut self, id: ClientID) -> &mut TestClient {
        &mut self.others[id - 1].0
    }

    /// What client `id`'s replies completed so far.
    fn completed(&mut self, id: ClientID) -> Vec<Completion<i32>> {
        std::mem::take(&mut self.others[id - 1].1)
    }

    /// Runs the client's idle logic for a whole idle period, after which it
    /// re-sends what got no reply.
    fn resend(&mut self) {
        self.client.on_idle();
        self.client.on_idle();
    }

    /// Hands what the client wants sent straight to the replicas.
    fn deliver_client(&mut self) {
        for (replica_id, message) in self.client.drain() {
            self.replicas[replica_id].on_message(message);
        }
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
            for reply in replica.drain_replies() {
                match reply.client_id() {
                    0 => {
                        self.client.on_reply(reply.clone());
                    }
                    id => {
                        if let Some((client, completed)) = self.others.get_mut(id - 1) {
                            completed.extend(client.on_reply(reply.clone()));
                        }
                    }
                }
                self.replies.push(reply);
            }
        }
        self.queue.extend(self.client.drain());
        for (client, _) in &mut self.others {
            self.queue.extend(client.drain());
        }
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

    fn take_replies(&mut self) -> Vec<Reply<i32>> {
        self.collect();
        std::mem::take(&mut self.replies)
    }

    /// The state machine after the first `applied` ops, which reach
    /// `value`, from the log of a replica that holds them all.
    fn state_machine_as_of(&self, applied: OpNumber, value: i32) -> Accumulator {
        let log = self
            .replicas
            .iter()
            .find(|replica| replica.log_start() == 0 && replica.commit_number() >= applied)
            .expect("a replica with the first ops in its log")
            .log();
        let mut clients: BTreeMap<ClientID, ClientRecord<i32>> = BTreeMap::new();
        let mut folded = 0;
        for (index, entry) in log[..applied].iter().enumerate() {
            let op_number = index + 1;
            match entry {
                LogEntry::Register { client_id } if !clients.contains_key(client_id) => {
                    if clients.len() == self.config.clients_max() {
                        let evicted = *clients
                            .iter()
                            .min_by_key(|(_, record)| record.op_number)
                            .unwrap()
                            .0;
                        clients.remove(&evicted);
                    }
                    let record = ClientRecord {
                        client_id: *client_id,
                        session: op_number,
                        request_number: 0,
                        replies: VecDeque::new(),
                        op_number,
                    };
                    clients.insert(*client_id, record);
                }
                LogEntry::Register { .. } => {}
                LogEntry::Request {
                    client_id,
                    session,
                    request_number,
                    answered,
                    op,
                } => {
                    let Some(record) = clients
                        .get_mut(client_id)
                        .filter(|record| record.session == *session)
                    else {
                        continue;
                    };
                    folded = op.apply(folded);
                    record.request_number = *request_number;
                    record.replies.push_back(folded);
                    let kept = (request_number - answered).min(self.config.in_flight_max());
                    while record.replies.len() > kept {
                        record.replies.pop_front();
                    }
                    record.op_number = op_number;
                }
            }
        }
        assert_eq!(value, folded);
        Accumulator {
            value,
            clients,
            last_op: applied,
            ..Accumulator::default()
        }
    }

    fn value(&self, replica_id: ReplicaID) -> i32 {
        self.replicas[replica_id].state_machine().value
    }

    /// Replaces a replica with one rebuilt from its disk, as after a power
    /// loss: the state machine starts over and applies the committed log
    /// again. Whatever the replica had not sent yet is lost.
    fn restart(&mut self, replica_id: ReplicaID) {
        self.restart_with_applied(replica_id, 0, 0);
    }

    /// Like `restart`, with a state machine that had made the first
    /// `applied` ops durable, reaching `value`.
    fn restart_with_applied(&mut self, replica_id: ReplicaID, applied: OpNumber, value: i32) {
        let disk = self.disks[replica_id].clone();
        self.restart_with_state(replica_id, applied, value, disk);
    }

    /// Like `restart_with_applied`, from the given persisted state.
    fn restart_with_state(
        &mut self,
        replica_id: ReplicaID,
        applied: OpNumber,
        value: i32,
        state: Disk,
    ) {
        let state_machine = self.state_machine_as_of(applied, value);
        self.disks[replica_id] = state.clone();
        self.replicas[replica_id] = Replica::restart(
            replica_id,
            self.config.clone(),
            state_machine,
            applied,
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
    assert_eq!(4, cluster.replicas[1].op_number());
    // A commit heartbeat lets the backups catch up.
    cluster.idle();
    cluster.tick();
    for id in 0..3 {
        assert_eq!(60, cluster.value(id));
    }
}

/// The acknowledgements for op 2 are lost, but both backups acknowledge
/// op 3. A `PrepareOk` for op n acknowledges n and all earlier ops, so
/// the primary must commit ops 2 and 3, in order, once op 3 reaches a
/// quorum, instead of executing op 3 in op 2's slot.
#[test]
fn test_prepare_ok_quorum_commits_prefix() {
    let mut cluster = Cluster::new(3);
    cluster.request(Op::Add(10));
    cluster.request(Op::Add(20));
    cluster.tick_with(&|_, message| !matches!(message, Message::PrepareOk { op_number: 2, .. }));
    assert_eq!(3, cluster.replicas[0].commit_number());
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

    // Replica 1 misses op 3, so op 4 makes it ask for state transfer.
    // The network replays its `GetState`; keep the copy for later.
    cluster.request(Op::Add(10));
    cluster.request(Op::Add(20));
    cluster.request(Op::Add(30));
    cluster.tick_with(&|replica_id, message| match message {
        Message::Prepare { op_number: 3, .. } if replica_id == 1 => false,
        Message::GetState { .. } => {
            replayed.borrow_mut().push((replica_id, message.clone()));
            true
        }
        _ => true,
    });
    assert_eq!(4, cluster.replicas[1].op_number());

    // Replica 1 misses op 5, so op 6 makes it ask for state transfer
    // again, but this time the `GetState` is lost.
    cluster.request(Op::Add(40));
    cluster.request(Op::Add(50));
    cluster.tick_with(&|replica_id, message| match message {
        Message::Prepare { op_number: 5, .. } if replica_id == 1 => false,
        Message::GetState { .. } => false,
        _ => true,
    });
    assert_eq!(4, cluster.replicas[1].op_number());

    // The replayed `GetState` from the first transfer now reaches the
    // primary. Its reply starts at op 2 and covers ops 2 to 6, and it
    // reaches replica 1 while it waits for a reply starting at op 4.
    for (replica_id, message) in replayed.borrow_mut().drain(..) {
        cluster.replicas[replica_id].on_message(message);
    }
    cluster.tick();
    assert_eq!(6, cluster.replicas[1].op_number());

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
        Message::Prepare { op_number: 3, .. } if replica_id == 1 => false,
        Message::GetState { .. } => false,
        _ => true,
    });
    assert_eq!(2, cluster.replicas[1].op_number());
    cluster.idle();
    cluster.tick();
    assert_eq!(4, cluster.replicas[1].op_number());
    cluster.idle();
    cluster.tick();
    for id in 0..3 {
        assert_eq!(60, cluster.value(id));
    }
}

/// Every `PrepareOk` for the last ops is lost. Once the ops have gone a
/// whole idle period unacknowledged, the primary must re-send their
/// `Prepare` messages so the backups acknowledge them again.
#[test]
fn test_prepare_resent_on_idle_when_prepare_ok_lost() {
    let mut cluster = Cluster::new(3);
    cluster.request(Op::Add(10));
    cluster.request(Op::Add(20));
    cluster.tick_with(&|_, message| !matches!(message, Message::PrepareOk { .. }));
    assert_eq!(3, cluster.replicas[1].op_number());
    assert_eq!(3, cluster.replicas[2].op_number());
    assert_eq!(1, cluster.replicas[0].commit_number());
    cluster.idle();
    cluster.tick();
    assert_eq!(1, cluster.replicas[0].commit_number());
    cluster.idle();
    cluster.tick();
    assert_eq!(3, cluster.replicas[0].commit_number());
    cluster.idle();
    cluster.tick();
    for id in 0..3 {
        assert_eq!(30, cluster.value(id));
    }
}

/// The `Prepare` for the last op never reaches any backup, so no backup
/// can notice a gap. Once the op has gone a whole idle period
/// unacknowledged, the primary must re-send it.
#[test]
fn test_prepare_resent_on_idle_when_prepare_lost() {
    let mut cluster = Cluster::new(3);
    cluster.request(Op::Add(10));
    cluster.request(Op::Add(20));
    cluster.tick_with(&|_, message| !matches!(message, Message::Prepare { op_number: 3, .. }));
    assert_eq!(2, cluster.replicas[1].op_number());
    assert_eq!(2, cluster.replicas[2].op_number());
    assert_eq!(2, cluster.replicas[0].commit_number());
    cluster.idle();
    cluster.tick();
    assert_eq!(2, cluster.replicas[0].commit_number());
    cluster.idle();
    cluster.tick();
    assert_eq!(3, cluster.replicas[0].commit_number());
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
    assert_eq!(2, cluster.replicas[3].op_number());
    assert_eq!(1, cluster.replicas[1].op_number());
    assert_eq!(1, cluster.replicas[2].op_number());
    assert_eq!(1, cluster.replicas[0].commit_number());
    // Once the other backups get the re-sent Prepare, a whole idle period
    // later, it commits.
    cluster.idle();
    cluster.tick();
    assert_eq!(1, cluster.replicas[0].commit_number());
    cluster.idle();
    cluster.tick();
    assert_eq!(2, cluster.replicas[0].commit_number());
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
    assert_eq!(2, cluster.replicas[0].op_number());
    assert_eq!(2, cluster.replicas[0].commit_number());
    assert_eq!(10, cluster.value(0));
    assert_eq!(1, cluster.take_replies().len());
    // The request is delivered once more after it committed: the primary
    // re-sends the reply without running it again.
    cluster.replicas[replica_id].on_message(request);
    cluster.tick();
    assert_eq!(2, cluster.replicas[0].op_number());
    assert_eq!(10, cluster.value(0));
    assert_eq!(1, cluster.take_replies().len());
}

/// The client's request is lost. Once it has gone a whole idle period
/// without a reply, the client must re-send it, to every replica since the
/// primary may have changed, and backups must ignore it.
#[test]
fn test_lost_request_resent_on_idle() {
    let mut cluster = Cluster::new(3);
    cluster.request(Op::Add(10));
    cluster.tick_with(&|_, message| !matches!(message, Message::Request { .. }));
    assert_eq!(1, cluster.replicas[0].op_number());
    // The idle period must make the client re-send the request.
    cluster.resend();
    cluster.tick();
    assert_eq!(2, cluster.replicas[0].commit_number());
    assert_eq!(10, cluster.value(0));
    assert_eq!(1, cluster.take_replies().len());
    // Once replied to, the request is not re-sent again.
    cluster.resend();
    cluster.tick();
    assert_eq!(2, cluster.replicas[0].op_number());
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
    assert_eq!(4, cluster.replicas[1].op_number());
    assert_eq!(4, cluster.replicas[2].op_number());
    assert_eq!(3, cluster.replicas[0].commit_number());

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
    assert_eq!(4, cluster.replicas[1].commit_number());
    assert_eq!(4, cluster.replicas[2].commit_number());
    assert_eq!(60, cluster.value(1));
    assert_eq!(60, cluster.value(2));
    let replies = cluster.take_replies();
    assert_eq!(1, replies.len());
    assert_eq!(request_number, executed(&replies[0]));
    assert_eq!(1, replies[0].view_number());

    // The client learns the view from the reply and sends the next request
    // to the new primary.
    cluster.request(Op::Add(40));
    cluster.tick_without(0);
    cluster.idle_without(0);
    cluster.tick_without(0);
    assert_eq!(5, cluster.replicas[1].commit_number());
    assert_eq!(5, cluster.replicas[2].commit_number());
    assert_eq!(100, cluster.value(1));
    assert_eq!(100, cluster.value(2));
    assert_eq!(1, cluster.take_replies().len());
    // The crashed primary never moved.
    assert_eq!(0, cluster.replicas[0].view_number());
    assert_eq!(30, cluster.value(0));
}

/// The request number of an `Executed` reply.
fn executed(reply: &Reply<i32>) -> RequestNumber {
    match reply {
        Reply::Executed { request_number, .. } => *request_number,
        reply => panic!("not an executed request: {reply:?}"),
    }
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
    assert_eq!(4, cluster.replicas[0].commit_number());
    assert_eq!(60, cluster.value(0));

    // Its Recovery reaches the others, and their responses bring it back.
    cluster.idle();
    cluster.tick();
    assert!(!cluster.replicas[1].is_recovering());
    assert_eq!(4, cluster.replicas[1].op_number());
    assert_eq!(4, cluster.replicas[1].commit_number());
    assert_eq!(60, cluster.value(1));
    assert_eq!(0, cluster.replicas[1].view_number());

    // And it takes part in the next op.
    cluster.request(Op::Add(40));
    cluster.tick_with(&|replica_id, message| {
        replica_id != 2 || matches!(message, Message::Request { .. })
    });
    assert_eq!(5, cluster.replicas[0].commit_number());
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
    assert_eq!(3, cluster.replicas[1].commit_number());
    assert_eq!(30, cluster.value(1));

    // Only replica 1 acknowledges the next op, so its acknowledgement is
    // what commits it.
    cluster.request(Op::Add(30));
    cluster.tick_with(&|replica_id, message| {
        replica_id != 2 || matches!(message, Message::Request { .. })
    });
    assert_eq!(4, cluster.replicas[0].commit_number());
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

    // The state machine had applied ops 1 and 2 when replica 1 went down,
    // and those must not be applied again.
    cluster.restart_with_applied(1, 2, 10);
    assert_eq!(60, cluster.value(1));
    assert_eq!(4, cluster.replicas[1].commit_number());
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
    // The `Prepare` for op 3 is lost, and the primary goes down.
    cluster.tick_with(&|_, message| !matches!(message, Message::Prepare { op_number: 3, .. }));
    assert_eq!(3, cluster.replicas[0].op_number());
    assert_eq!(2, cluster.replicas[0].commit_number());

    cluster.restart(0);
    assert_eq!(Status::ViewChange, cluster.replicas[0].status());
    assert_eq!(1, cluster.replicas[0].view_number());
    assert_eq!(3, cluster.replicas[0].op_number());
    assert_eq!(10, cluster.value(0));

    for _ in 0..10 {
        cluster.idle();
        cluster.tick();
    }
    assert!(cluster.settled());
    assert!(cluster.replicas[1].is_primary());
    for id in 0..3 {
        assert_eq!(2, cluster.replicas[id].commit_number());
        assert_eq!(2, cluster.replicas[id].op_number());
    }
    cluster.resend();
    cluster.tick();
    cluster.idle();
    cluster.tick();
    for id in 0..3 {
        assert_eq!(3, cluster.replicas[id].commit_number());
        assert_eq!(30, cluster.value(id));
    }
}

/// The primary sends the `Prepare` for an op before its own entry is
/// durable, and loses power before it is. The backups hold the op under
/// op number 3; the primary's disk does not. On restart the primary must
/// not resume and assign op number 3 again: it starts the next view, and
/// the op the backups hold commits there.
#[test]
fn test_prepare_sent_before_persist_survives_primary_crash() {
    let mut cluster = Cluster::new(3);
    cluster.request(Op::Add(10));
    cluster.tick();
    cluster.idle();
    cluster.tick();
    // The request reaches the primary, whose `Prepare` goes out before
    // its disk has op 3.
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
    assert_eq!(2, cluster.disks[0].log.len());
    // The primary restarts from that disk. The backups' acknowledgements
    // reach nobody.
    cluster.restart(0);
    assert_eq!(2, cluster.replicas[0].op_number());
    assert_eq!(Status::ViewChange, cluster.replicas[0].status());
    for id in [1, 2] {
        cluster.persist(id);
        cluster.replicas[id].drain_messages().for_each(drop);
    }
    assert_eq!(3, cluster.replicas[1].op_number());

    for _ in 0..10 {
        cluster.idle();
        cluster.tick();
    }
    assert!(cluster.settled());
    let replies = cluster.take_replies();
    assert!(replies
        .iter()
        .any(|reply| executed(reply) == request_number));
    for id in 0..3 {
        assert_eq!(3, cluster.replicas[id].commit_number());
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
    assert_eq!(1, cluster.replicas[0].commit_number());
    // Draining releases only the messages that need not wait; the commit
    // waits for the write.
    cluster.replicas[0].drain_messages().for_each(drop);
    assert_eq!(1, cluster.replicas[0].commit_number());
    cluster.persist(0);
    assert_eq!(2, cluster.replicas[0].commit_number());
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
    assert_eq!(2, write.op_number());
    // Op 3 arrives, and a backup acknowledges both ops, before the write
    // of op 2 is persisted.
    cluster.request(Op::Add(20));
    cluster.deliver_client();
    assert_eq!(3, cluster.replicas[0].op_number());
    cluster.replicas[0].on_message(Message::PrepareOk {
        view_number: 0,
        op_number: 3,
        replica_id: 1,
    });
    assert_eq!(1, cluster.replicas[0].commit_number());
    cluster.disks[0].apply(&write);
    cluster.replicas[0].persisted(write);
    assert_eq!(2, cluster.replicas[0].commit_number());
    assert_eq!(10, cluster.value(0));
    cluster.persist(0);
    assert_eq!(3, cluster.replicas[0].commit_number());
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
    assert_eq!(3, cluster.replicas[2].op_number());
    assert_eq!(1, cluster.replicas[2].commit_number());
    let write = cluster.replicas[2].take_write();
    cluster.replicas[2].on_message(Message::StartView {
        view_number: 1,
        segment: LogSegment {
            base: LogBase::Op(1),
            entries: vec![add(1, 1), add(2, 2)],
        },
        commit_number: 3,
    });
    assert_eq!(3, cluster.replicas[2].commit_number());
    cluster.disks[2].apply(&write);
    cluster.replicas[2].persisted(write);
    assert_eq!(1, cluster.replicas[2].applied());
    assert_eq!(0, cluster.value(2));
    cluster.persist(2);
    assert_eq!(3, cluster.replicas[2].applied());
    assert_eq!(3, cluster.value(2));
}

/// Request `request_number` of the client's session, which adds `value`.
fn add(request_number: RequestNumber, value: i32) -> LogEntry<Op> {
    LogEntry::Request {
        client_id: 0,
        session: 1,
        request_number,
        answered: request_number - 1,
        op: Op::Add(value),
    }
}

/// An op that a handed-back write holds executes in the step that commits
/// it, and its reply is ready before that step's write, which here holds
/// the next request.
#[test]
fn test_reply_ready_before_the_next_write() {
    let mut cluster = Cluster::new(3);
    let request_number = cluster.request(Op::Add(10));
    cluster.tick_with(&|_, message| !matches!(message, Message::PrepareOk { .. }));
    assert_eq!(1, cluster.replicas[0].commit_number());
    cluster.replicas[0].on_message(Message::PrepareOk {
        view_number: 0,
        op_number: 2,
        replica_id: 1,
    });
    cluster.request(Op::Add(20));
    cluster.deliver_client();
    assert_eq!(2, cluster.replicas[0].commit_number());
    assert_eq!(10, cluster.value(0));
    let replies: Vec<_> = cluster.replicas[0]
        .drain_replies()
        .map(|reply| (reply.client_id(), executed(&reply)))
        .collect();
    assert_eq!(vec![(0, request_number)], replies);
    let write = cluster.replicas[0].take_write();
    assert!(write.sync);
    assert_eq!(3, write.op_number());
    cluster.disks[0].apply(&write);
    cluster.replicas[0].persisted(write);
}

/// A primary whose own write of an op is still out commits the op on the
/// backups' acknowledgements, but executes it, and replies, only once the
/// write comes back.
#[test]
fn test_execution_waits_for_the_write_that_holds_the_op() {
    let mut cluster = Cluster::new(3);
    cluster.request(Op::Add(10));
    cluster.collect();
    for (replica_id, message) in std::mem::take(&mut cluster.queue) {
        cluster.replicas[replica_id].on_message(message);
    }
    let write = cluster.replicas[0].take_write();
    let prepares: Vec<_> = cluster.replicas[0]
        .drain_messages_before_persist()
        .collect();
    for (replica_id, message) in prepares {
        cluster.replicas[replica_id].on_message(message);
        cluster.persist(replica_id);
        let acks: Vec<_> = cluster.replicas[replica_id].drain_messages().collect();
        for (_, ack) in acks {
            cluster.replicas[0].on_message(ack);
        }
    }
    assert_eq!(2, cluster.replicas[0].commit_number());
    assert_eq!(1, cluster.replicas[0].applied());
    assert_eq!(0, cluster.replicas[0].drain_replies().count());
    cluster.disks[0].apply(&write);
    cluster.replicas[0].persisted(write);
    assert_eq!(2, cluster.replicas[0].applied());
    assert_eq!(1, cluster.replicas[0].drain_replies().count());
}

/// A backup executes an op in the step that commits it only if its disk
/// holds the op as memory does: a `StartView` that replaces op 3 and
/// commits it leaves op 3 for the write that holds the new entry.
#[test]
fn test_commit_executes_only_ops_on_disk_unchanged() {
    let mut cluster = Cluster::new(3);
    cluster.request(Op::Add(10));
    cluster.tick();
    cluster.request(Op::Add(20));
    cluster.tick_with(&|_, message| !matches!(message, Message::PrepareOk { .. }));
    assert_eq!(3, cluster.replicas[2].op_number());
    assert_eq!(2, cluster.replicas[2].applied());
    cluster.replicas[2].on_message(Message::StartView {
        view_number: 1,
        segment: LogSegment {
            base: LogBase::Op(2),
            entries: vec![add(2, 2)],
        },
        commit_number: 3,
    });
    assert_eq!(3, cluster.replicas[2].commit_number());
    assert_eq!(2, cluster.replicas[2].applied());
    assert_eq!(10, cluster.value(2));
    cluster.persist(2);
    assert_eq!(3, cluster.replicas[2].applied());
    assert_eq!(12, cluster.value(2));
}

/// A write taken over entries a view change replaced leaves them in doubt
/// until it comes back: a commit that arrives while it is out executes
/// only the ops the disk held unchanged before it.
#[test]
fn test_commit_during_write_of_replaced_entries_waits_for_it() {
    let mut cluster = Cluster::new(3);
    cluster.request(Op::Add(10));
    cluster.tick();
    cluster.request(Op::Add(20));
    cluster.tick_with(&|_, message| !matches!(message, Message::PrepareOk { .. }));
    assert_eq!(2, cluster.replicas[2].applied());
    cluster.replicas[2].on_message(Message::StartView {
        view_number: 1,
        segment: LogSegment {
            base: LogBase::Op(2),
            entries: vec![add(2, 2)],
        },
        commit_number: 2,
    });
    let write = cluster.replicas[2].take_write();
    cluster.replicas[2].on_message(Message::Commit {
        view_number: 1,
        commit_number: 3,
    });
    assert_eq!(3, cluster.replicas[2].commit_number());
    assert_eq!(2, cluster.replicas[2].applied());
    cluster.disks[2].apply(&write);
    cluster.replicas[2].persisted(write);
    assert_eq!(3, cluster.replicas[2].applied());
    assert_eq!(12, cluster.value(2));
}

/// A primary counts acknowledgements within its view: one it counted when
/// it led an earlier view commits nothing once it leads again. Replica 0
/// leads view 0, where backup 1 acknowledges op 1 before the primary's own
/// write is persisted, then view 3, whose log holds another op 1.
#[test]
fn test_reelected_primary_counts_only_new_acknowledgements() {
    let mut config = Config::new();
    for _ in 0..3 {
        config.add_replica();
    }
    let mut replica = Replica::new(0, config, Accumulator::default());
    replica.on_message(Message::Register { client_id: 5 });
    replica.on_message(Message::PrepareOk {
        view_number: 0,
        op_number: 1,
        replica_id: 1,
    });
    assert_eq!(0, replica.commit_number());
    let entry = |client_id| LogEntry::<Op>::Register { client_id };
    replica.on_message(Message::StartViewChange {
        view_number: 3,
        replica_id: 2,
    });
    replica.on_message(Message::DoViewChange {
        view_number: 3,
        replica_id: 2,
        last_normal_view: 0,
        segment: LogSegment {
            base: LogBase::Op(0),
            entries: vec![entry(6), entry(7)],
        },
        commit_number: 0,
    });
    assert_eq!(Status::Normal, replica.status());
    assert_eq!(3, replica.view_number());
    assert_eq!(entry(6), replica.log()[0]);
    let write = replica.take_write();
    replica.persisted(write);
    assert_eq!(0, replica.commit_number());
}

/// The primary re-sends a `Prepare` only to the backups that have not
/// acknowledged, once an idle period has passed since it was sent.
#[test]
fn test_prepare_resent_only_to_backups_that_did_not_acknowledge() {
    let mut cluster = Cluster::new(5);
    cluster.request(Op::Add(10));
    cluster.request(Op::Add(20));
    cluster.tick_with(
        &|_, message| !matches!(message, Message::PrepareOk { replica_id, .. } if *replica_id != 1),
    );
    assert_eq!(1, cluster.replicas[0].commit_number());
    let mut resent_to = Vec::new();
    for _ in 0..2 {
        cluster.replicas[0].on_idle();
        cluster.persist(0);
        for (to, message) in cluster.replicas[0].drain_messages() {
            if let Message::Prepare { op_number, .. } = message {
                assert_eq!(3, op_number);
                resent_to.push(to);
            }
        }
    }
    assert_eq!(vec![2, 3, 4], resent_to);
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
/// that promise nothing about its disk.
#[test]
fn test_drain_before_persisted_releases_only_early_messages() {
    let mut cluster = Cluster::new(3);
    cluster.request(Op::Add(10));
    cluster.tick();
    assert_eq!(1, cluster.replicas[1].commit_number());
    // Backup 1 hears that op 2 committed, times out on the primary and
    // starts a view change, then hears of a later view and asks for its
    // state.
    cluster.replicas[1].on_message(Message::Commit {
        view_number: 0,
        commit_number: 2,
    });
    assert_eq!(2, cluster.replicas[1].commit_number());
    for _ in 0..=Config::new().primary_timeout() {
        cluster.replicas[1].on_idle();
    }
    cluster.replicas[1].on_message(Message::Commit {
        view_number: 5,
        commit_number: 2,
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
    cluster.persist(1);
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
    cluster.resend();
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
    assert_eq!(3, cluster.replicas[0].commit_number());
    cluster.replicas[0].compact(3);
    assert_eq!(3, cluster.replicas[0].log_start());
    assert_eq!(0, cluster.replicas[0].log().len());

    cluster.request(Op::Add(30));
    cluster.tick();
    assert_eq!(3, cluster.replicas[1].log_start());
    assert_eq!(4, cluster.replicas[1].op_number());
    cluster.idle();
    cluster.tick();
    for id in 0..3 {
        assert_eq!(60, cluster.value(id));
    }
    // The backup restarts from what it persisted: a checkpoint's worth of
    // state, which the state machine must have kept, and the entries after
    // it.
    cluster.restart_with_applied(1, 3, 30);
    assert_eq!(60, cluster.value(1));
    assert_eq!(4, cluster.replicas[1].commit_number());
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
    assert_eq!(2, cluster.replicas[0].commit_number());
    assert_eq!(3, cluster.replicas[0].op_number());
    cluster.replicas[0].compact(3);
    assert_eq!(2, cluster.replicas[0].log_start());
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
    // Ops 2 to 4 commit on replicas 0 and 2; replica 1 misses them.
    cluster.request(Op::Add(10));
    cluster.request(Op::Add(20));
    let request_number = cluster.request(Op::Add(30));
    cluster.tick_without(1);
    cluster.idle_without(1);
    cluster.tick_without(1);
    assert_eq!(4, cluster.replicas[2].commit_number());
    assert_eq!(1, cluster.replicas[1].op_number());
    cluster.replicas[2].compact(4);

    // The primary goes down. Replica 1 becomes the primary of view 1 with
    // replica 2's log, whose entries it has compacted.
    for _ in 0..10 {
        cluster.idle_without(0);
        cluster.tick_without(0);
    }
    assert_eq!(1, cluster.replicas[1].view_number());
    assert_eq!(Status::Normal, cluster.replicas[1].status());
    assert!(cluster.replicas[1].is_primary());
    assert_eq!(4, cluster.replicas[1].log_start());
    assert_eq!(4, cluster.replicas[1].commit_number());
    assert_eq!(60, cluster.value(1));
    assert_eq!(Status::Normal, cluster.replicas[2].status());
    assert_eq!(1, cluster.replicas[2].view_number());

    // The client's latest request, executed before the checkpoint, is
    // answered from the client table.
    cluster.take_replies();
    cluster.replicas[1].on_message(Message::Request {
        client_id: 0,
        session: 1,
        request_number,
        answered: 0,
        op: Op::Add(30),
    });
    let replies = cluster.take_replies();
    assert_eq!(1, replies.len());
    assert_eq!(request_number, executed(&replies[0]));

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
    // Ops 2 to 4 commit on replicas 0 and 1; replica 2 misses them.
    cluster.request(Op::Add(10));
    cluster.request(Op::Add(20));
    cluster.request(Op::Add(30));
    cluster.tick_without(2);
    cluster.idle_without(2);
    cluster.tick_without(2);
    assert_eq!(4, cluster.replicas[1].commit_number());
    assert_eq!(1, cluster.replicas[2].op_number());
    cluster.replicas[1].compact(4);

    for _ in 0..10 {
        cluster.idle_without(0);
        cluster.tick_without(0);
    }
    assert!(cluster.replicas[1].is_primary());
    assert_eq!(Status::Normal, cluster.replicas[1].status());
    assert_eq!(Status::Normal, cluster.replicas[2].status());
    assert_eq!(1, cluster.replicas[2].view_number());
    assert_eq!(4, cluster.replicas[2].log_start());
    assert_eq!(4, cluster.replicas[2].commit_number());
    assert_eq!(60, cluster.value(2));

    cluster.request(Op::Add(40));
    cluster.resend();
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
    cluster.replicas[0].compact(3);
    cluster.request(Op::Add(30));
    cluster.tick_with(&|_, message| !matches!(message, Message::PrepareOk { .. }));
    assert_eq!(3, cluster.replicas[0].commit_number());
    assert_eq!(4, cluster.replicas[0].op_number());

    cluster.replicas[1] =
        Replica::recover(1, cluster.config.clone(), Accumulator::default(), 0, 42);
    cluster.disks[1] = empty_disk();
    cluster.tick();
    assert!(!cluster.replicas[1].is_recovering());
    assert_eq!(3, cluster.replicas[1].log_start());
    assert_eq!(3, cluster.replicas[1].commit_number());
    assert_eq!(4, cluster.replicas[1].op_number());
    assert_eq!(30, cluster.value(1));
    // The `Prepare` for op 4, re-sent once it has gone a whole idle
    // period unacknowledged, commits it, and the next heartbeat tells the
    // backups.
    for _ in 0..3 {
        cluster.idle();
        cluster.tick();
    }
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
    assert_eq!(vec![4], acks);
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
    // What replica 1 had persisted before the checkpoint: op 2 committed,
    // op 3 still uncommitted.
    let stale = cluster.disks[1].clone();
    assert_eq!((2, 3), (stale.commit_number, stale.log.len()));
    cluster.request(Op::Add(30));
    cluster.tick();
    cluster.idle();
    cluster.tick();
    assert_eq!(4, cluster.replicas[1].commit_number());
    cluster.restart_with_state(1, 4, 60, stale);
    assert_eq!(4, cluster.replicas[1].commit_number());
    assert_eq!(4, cluster.replicas[1].log_start());
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
            replica_id == 1 || (replica_id == 2 && *op_number == 2)
        }
        _ => true,
    });
    assert_eq!(2, cluster.replicas[0].commit_number());
    assert_eq!(10, cluster.value(0));
    cluster.idle();
    cluster.tick();
    assert_eq!(4, cluster.replicas[0].commit_number());
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
    // Op 5 reaches the backups; the primary never sees their
    // acknowledgements.
    cluster.request(Op::Add(40));
    cluster.tick_with(&|_, message| !matches!(message, Message::PrepareOk { op_number: 5, .. }));
    assert_eq!(5, cluster.replicas[1].op_number());
    assert_eq!(4, cluster.replicas[1].commit_number());
    // Replica 1's disk is behind its state machine: it says op 2 is the
    // last committed, while the state machine holds a checkpoint at op 4.
    let mut stale = cluster.disks[1].clone();
    stale.commit_number = 2;
    cluster.restart_with_state(1, 4, 60, stale);
    assert_eq!(4, cluster.replicas[1].log_start());
    assert_eq!(4, cluster.replicas[1].commit_number());
    assert_eq!(5, cluster.replicas[1].op_number());
    assert_eq!(60, cluster.value(1));

    for _ in 0..10 {
        cluster.idle_without(0);
        cluster.tick_without(0);
    }
    assert!(cluster.replicas[1].is_primary());
    assert_eq!(5, cluster.replicas[1].commit_number());
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
    cluster.restart_with_applied(1, 3, 30);
    assert!(cluster.replicas[1].is_recovering());
    assert_eq!(3, cluster.replicas[1].log_start());
    cluster.tick();
    assert!(!cluster.replicas[1].is_recovering());
    assert_eq!(4, cluster.replicas[1].commit_number());
    assert_eq!(60, cluster.value(1));
}

/// A backup that missed the ops the primary compacted fetches the
/// primary's checkpoint a chunk at a time, one per client record and one
/// for the value, and restores the same client table from them.
#[test]
fn test_state_transfer_fetches_every_chunk() {
    let mut cluster = Cluster::new(3);
    for _ in 0..2 {
        let id = cluster.add_client();
        cluster.other(id).register();
    }
    cluster.request(Op::Add(10));
    cluster.tick_without(1);
    cluster.idle_without(1);
    cluster.tick_without(1);
    assert_eq!(4, cluster.replicas[0].commit_number());
    cluster.replicas[0].compact(4);

    cluster.request(Op::Add(20));
    let chunks = Cell::new(Vec::new());
    cluster.tick_with(&|_, message| {
        if let Message::NewChunk { index, last, .. } = message {
            let mut seen = chunks.take();
            seen.push((*index, *last));
            chunks.set(seen);
        }
        true
    });
    assert_eq!(
        vec![(0, false), (1, false), (2, false), (3, true)],
        chunks.take()
    );
    assert_eq!(Status::Normal, cluster.replicas[1].status());
    assert_eq!(4, cluster.replicas[1].log_start());
    cluster.idle();
    cluster.tick();
    assert_eq!(30, cluster.value(1));
    assert_eq!(
        cluster.replicas[0].state_machine().clients,
        cluster.replicas[1].state_machine().clients
    );
}

/// The primary has compacted its whole log, so its checkpoint is all a
/// lagging backup gets. Restoring it brings the backup level with the
/// primary, and back to normal status.
#[test]
fn test_state_transfer_to_the_checkpoint_alone() {
    let mut cluster = Cluster::new(3);
    cluster.request(Op::Add(10));
    cluster.request(Op::Add(20));
    cluster.tick_without(1);
    cluster.idle_without(1);
    cluster.tick_without(1);
    cluster.replicas[0].compact(3);
    assert_eq!(0, cluster.replicas[0].log().len());

    cluster.idle();
    cluster.tick();
    assert_eq!(Status::Normal, cluster.replicas[1].status());
    assert_eq!(3, cluster.replicas[1].commit_number());
    assert_eq!(30, cluster.value(1));
}

/// A lost chunk is asked for again once the fetch has waited a whole idle
/// period for it, and the fetch carries on from there.
#[test]
fn test_lost_chunk_is_asked_for_again() {
    let mut cluster = lagging_backup();
    cluster.request(Op::Add(30));
    cluster.tick_with(&|_, message| !matches!(message, Message::NewChunk { index: 1, .. }));
    assert_eq!(Status::StateTransfer, cluster.replicas[1].status());
    cluster.idle();
    cluster.tick();
    assert_eq!(Status::StateTransfer, cluster.replicas[1].status());
    cluster.idle();
    cluster.tick();
    assert_eq!(Status::Normal, cluster.replicas[1].status());
    cluster.idle();
    cluster.tick();
    assert_eq!(60, cluster.value(1));
}

/// Replica 1 misses ops 2 and 3, which commit on the others, and the
/// primary compacts them.
fn lagging_backup() -> Cluster {
    let mut cluster = Cluster::new(3);
    cluster.request(Op::Add(10));
    cluster.request(Op::Add(20));
    cluster.tick_without(1);
    cluster.idle_without(1);
    cluster.tick_without(1);
    cluster.replicas[0].compact(3);
    assert_eq!(3, cluster.replicas[0].log_start());
    assert_eq!(1, cluster.replicas[1].op_number());
    cluster
}

/// A fetch that gets no chunk for twice `primary_timeout` idle periods is
/// given up, and the backup starts over by asking for the state again.
#[test]
fn test_stalled_fetch_starts_over() {
    let mut cluster = lagging_backup();
    cluster.request(Op::Add(30));
    let no_chunks = |_: ReplicaID, message: &Msg| !matches!(message, Message::NewChunk { .. });
    cluster.tick_with(&no_chunks);
    let get_states = Cell::new(0);
    let counting = |_: ReplicaID, message: &Msg| {
        if matches!(message, Message::GetState { .. }) {
            get_states.set(get_states.get() + 1);
        }
        no_chunks(0, message)
    };
    for _ in 0..5 {
        cluster.idle();
        cluster.tick_with(&counting);
    }
    assert_eq!(0, get_states.get());
    cluster.idle();
    cluster.tick_with(&counting);
    assert_eq!(1, get_states.get());
    assert_eq!(Status::StateTransfer, cluster.replicas[1].status());

    // The new fetch lost its first chunk too, and asks again.
    for _ in 0..2 {
        cluster.idle();
        cluster.tick();
    }
    assert_eq!(Status::Normal, cluster.replicas[1].status());
    cluster.idle();
    cluster.tick();
    assert_eq!(60, cluster.value(1));
}

/// A stalled fetch goes on from the chunk it stopped at when the state it
/// asks for again names the same checkpoint.
#[test]
fn test_stalled_fetch_goes_on_where_it_stopped() {
    let mut cluster = lagging_backup();
    cluster.request(Op::Add(30));
    let asked = RefCell::new(Vec::new());
    let no_second_chunk = |_: ReplicaID, message: &Msg| {
        if let Message::GetChunk { index, .. } = message {
            asked.borrow_mut().push(*index);
        }
        !matches!(message, Message::NewChunk { index: 1, .. })
    };
    cluster.tick_with(&no_second_chunk);
    for _ in 0..8 {
        cluster.idle();
        cluster.tick_with(&no_second_chunk);
    }
    assert_eq!(Status::StateTransfer, cluster.replicas[1].status());
    let asked = asked.take();
    assert_eq!(0, asked[0]);
    assert!(asked[1..].iter().all(|index| *index == 1), "{asked:?}");

    for _ in 0..2 {
        cluster.idle();
        cluster.tick();
    }
    assert_eq!(Status::Normal, cluster.replicas[1].status());
    cluster.idle();
    cluster.tick();
    assert_eq!(60, cluster.value(1));
}

/// A late `NewState` that ends at the backup's op number adds nothing, and
/// leaves the state transfer and its fetch under way; one that names an
/// earlier checkpoint leaves the fetch of the later one under way.
#[test]
fn test_late_new_state_leaves_the_fetch() {
    let mut cluster = lagging_backup();
    cluster.request(Op::Add(30));
    cluster.tick_with(&|_, message| !matches!(message, Message::NewChunk { .. }));
    assert_eq!(Status::StateTransfer, cluster.replicas[1].status());
    let registration = cluster.replicas[1].log().to_vec();
    let late = [
        LogSegment {
            base: LogBase::Op(0),
            entries: registration,
        },
        LogSegment {
            base: LogBase::Checkpoint(2),
            entries: cluster.replicas[0].log().to_vec(),
        },
    ];
    for segment in late {
        cluster.replicas[1].on_message(Message::NewState {
            replica_id: 0,
            view_number: 0,
            segment,
            commit_number: 1,
        });
    }
    assert_eq!(Status::StateTransfer, cluster.replicas[1].status());
    cluster.persist(1);
    let fetches: Vec<_> = cluster.replicas[1]
        .drain_messages()
        .filter(|(_, message)| matches!(message, Message::GetChunk { .. }))
        .collect();
    assert!(fetches.is_empty(), "{fetches:?}");

    for _ in 0..2 {
        cluster.idle();
        cluster.tick();
    }
    assert_eq!(Status::Normal, cluster.replicas[1].status());
    cluster.idle();
    cluster.tick();
    assert_eq!(60, cluster.value(1));
}

/// Only requests for its chunks keep a checkpoint: one that replicas only
/// hear of in `NewState` goes after twice `primary_timeout` idle periods,
/// and compaction moves past it.
#[test]
fn test_kept_checkpoint_goes_unless_its_chunks_are_asked_for() {
    let mut cluster = lagging_backup();
    let get_state = Message::GetState {
        replica_id: 1,
        view_number: 0,
        op_number: 1,
    };
    cluster.replicas[0].on_message(get_state.clone());
    cluster.request(Op::Add(30));
    cluster.tick_without(1);
    assert_eq!(4, cluster.replicas[0].commit_number());
    for idle in 1..=6 {
        cluster.replicas[0].on_message(get_state.clone());
        cluster.idle_without(1);
        cluster.tick_without(1);
        cluster.replicas[0].compact(4);
        let log_start = if idle < 6 { 3 } else { 4 };
        assert_eq!(
            log_start,
            cluster.replicas[0].log_start(),
            "idle period {idle}"
        );
    }
}

/// The primary keeps the checkpoint a backup fetches, and sends it as of
/// the op it was taken at while later ops execute. Compaction stops at it
/// until it has gone unasked for twice `primary_timeout` idle periods.
#[test]
fn test_kept_checkpoint_holds_compaction() {
    let mut cluster = lagging_backup();
    cluster.request(Op::Add(30));
    cluster.tick_with(&|_, message| !matches!(message, Message::NewChunk { .. }));
    assert_eq!(4, cluster.replicas[0].commit_number());
    cluster.replicas[0].compact(4);
    assert_eq!(3, cluster.replicas[0].log_start());

    // The fetch asks again after a whole idle period, and completes.
    for _ in 0..2 {
        cluster.idle();
        cluster.tick();
    }
    assert_eq!(Status::Normal, cluster.replicas[1].status());

    for _ in 0..5 {
        cluster.idle();
        cluster.tick();
    }
    assert_eq!(60, cluster.value(1));
    cluster.replicas[0].compact(4);
    assert_eq!(3, cluster.replicas[0].log_start());
    cluster.idle();
    cluster.replicas[0].compact(4);
    assert_eq!(4, cluster.replicas[0].log_start());
}

/// A recovering replica fetches a checkpoint whose chunks take two idle
/// periods to come back, as long as `primary_timeout`: the fetch waits
/// longer than that before it gives up, so it completes. Found by the
/// simulator, `--lite 11068199783341970918`.
#[test]
fn test_fetch_outlasts_round_trips_as_long_as_the_timeout() {
    let mut cluster = Cluster::with_primary_timeout(3, 2);
    cluster.request(Op::Add(10));
    cluster.request(Op::Add(20));
    cluster.tick();
    cluster.idle();
    cluster.tick();
    cluster.replicas[0].compact(3);
    cluster.replicas[1] =
        Replica::recover(1, cluster.config.clone(), Accumulator::default(), 0, 42);
    cluster.disks[1] = empty_disk();
    for _ in 0..20 {
        cluster.step();
    }
    assert!(!cluster.replicas[1].is_recovering());
    assert_eq!(30, cluster.value(1));
}

/// A backup changes view in the middle of a fetch. The fetch ends with
/// the view it was for: a chunk that arrives late changes nothing, and the
/// backup takes the new view's log.
#[test]
fn test_view_change_drops_the_fetch() {
    let mut cluster = lagging_backup();
    cluster.request(Op::Add(30));
    let late = RefCell::new(Vec::new());
    cluster.tick_with(&|replica_id, message| {
        if matches!(message, Message::NewChunk { .. }) {
            late.borrow_mut().push((replica_id, message.clone()));
            return false;
        }
        true
    });
    assert_eq!(Status::StateTransfer, cluster.replicas[1].status());

    for _ in 0..10 {
        cluster.idle_without(0);
        cluster.tick_without(0);
    }
    assert!(cluster.replicas[1].is_primary());
    assert_eq!(Status::Normal, cluster.replicas[1].status());
    assert_eq!(0, cluster.replicas[1].log_start());
    assert_eq!(60, cluster.value(1));

    for (replica_id, message) in late.take() {
        cluster.replicas[replica_id].on_message(message);
    }
    cluster.tick_without(0);
    assert_eq!(Status::Normal, cluster.replicas[1].status());
    assert_eq!(0, cluster.replicas[1].log_start());
    assert_eq!(60, cluster.value(1));
}

/// A replica catching up with a view hears, from a `StartView` whose log
/// it cannot take, that a later view with the same primary has started:
/// it catches up with the later view.
#[test]
fn test_start_view_of_a_later_view_with_the_same_primary() {
    let mut cluster = Cluster::new(3);
    cluster.replicas[2].on_message(Message::Commit {
        view_number: 3,
        commit_number: 1,
    });
    assert_eq!(Status::ViewChange, cluster.replicas[2].status());
    cluster.replicas[2].on_message(Message::StartView {
        view_number: 6,
        segment: LogSegment {
            base: LogBase::Op(5),
            entries: Vec::new(),
        },
        commit_number: 5,
    });
    assert_eq!(6, cluster.replicas[2].view_number());
    cluster.persist(2);
    let get_states: Vec<_> = cluster.replicas[2]
        .drain_messages()
        .filter_map(|(to, message)| match message {
            Message::GetState { view_number, .. } => Some((to, view_number)),
            _ => None,
        })
        .collect();
    assert_eq!(vec![(0, 3), (0, 6)], get_states);
}

/// Regression test case for https://github.com/penberg/vsr-rs/issues/14
#[test]
#[should_panic(expected = "at least three replicas")]
fn test_one_replica_is_rejected() {
    Cluster::new(1);
}

/// Regression test case for https://github.com/penberg/vsr-rs/issues/14
#[test]
#[should_panic(expected = "at least three replicas")]
fn test_two_replicas_are_rejected() {
    Cluster::new(2);
}

/// A client's requests reach the primary in reverse order. Only the
/// session's next request is appended, so they execute in the order the
/// client issued them once its re-sends fill the gap.
#[test]
fn test_requests_in_flight_execute_in_order() {
    let mut cluster = Cluster::new(3);
    cluster.request(Op::Add(1));
    cluster.request(Op::Sub(2));
    cluster.request(Op::Add(3));
    let mut requests: Vec<_> = cluster.client.drain::<Chunk>().collect();
    assert_eq!(3, requests.len());
    requests.reverse();
    for (replica_id, message) in requests {
        cluster.replicas[replica_id].on_message(message);
    }
    assert_eq!(2, cluster.replicas[0].op_number());
    cluster.tick();
    cluster.resend();
    cluster.tick();
    let numbers: Vec<_> = cluster.replicas[0].log()[1..]
        .iter()
        .map(|entry| match entry {
            LogEntry::Request { request_number, .. } => *request_number,
            entry => panic!("not a request: {entry:?}"),
        })
        .collect();
    assert_eq!(vec![1, 2, 3], numbers);
    assert_eq!(2, cluster.value(0));
}

/// A client sends a request only once every request `in_flight_max`
/// before it has its reply, and the primary appends none beyond that.
#[test]
fn test_in_flight_window() {
    let mut config = Config::new();
    config.set_in_flight_max(2);
    let mut cluster = Cluster::with_config(3, config);
    for value in 1..=4 {
        cluster.request(Op::Add(value));
    }
    let sent: Vec<_> = cluster
        .client
        .drain::<Chunk>()
        .map(|(_, message)| match message {
            Message::Request { request_number, .. } => request_number,
            message => panic!("not a request: {message:?}"),
        })
        .collect();
    assert_eq!(vec![1, 2], sent);
    // A request past the window is dropped.
    cluster.replicas[0].on_message(Message::Request {
        client_id: 0,
        session: 1,
        request_number: 1,
        answered: 0,
        op: Op::Add(1),
    });
    cluster.replicas[0].on_message(Message::Request {
        client_id: 0,
        session: 1,
        request_number: 2,
        answered: 0,
        op: Op::Add(2),
    });
    cluster.replicas[0].on_message(Message::Request {
        client_id: 0,
        session: 1,
        request_number: 3,
        answered: 0,
        op: Op::Add(3),
    });
    assert_eq!(3, cluster.replicas[0].op_number());
    // Replies open the window for the rest.
    cluster.tick();
    cluster.idle();
    cluster.tick();
    assert_eq!(10, cluster.value(0));
    assert_eq!(5, cluster.replicas[0].commit_number());
}

/// A re-sent request is answered from the replies the client table keeps,
/// those after what the client said it has replies to, and is not run
/// again; one before those gets no reply. With requests 1 and 2 in flight,
/// reply 1 lets the client send request 3, which says it has replies up to
/// request 1.
#[test]
fn test_resend_answered_from_the_reply_window() {
    let mut config = Config::new();
    config.set_in_flight_max(2);
    let mut cluster = Cluster::with_config(3, config);
    for value in 1..=3 {
        cluster.request(Op::Add(value));
    }
    cluster.tick();
    assert_eq!(6, cluster.value(0));
    cluster.take_replies();
    let resend = |request_number| Message::Request {
        client_id: 0,
        session: 1,
        request_number,
        answered: 0,
        op: Op::Add(100),
    };
    for request_number in 1..=3 {
        cluster.replicas[0].on_message(resend(request_number));
    }
    let answered: Vec<_> = cluster.take_replies().iter().map(executed).collect();
    assert_eq!(vec![2, 3], answered);
    assert_eq!(6, cluster.value(0));
    assert_eq!(4, cluster.replicas[0].op_number());
}

/// A registration with the client table full evicts the session whose
/// latest entry executed earliest. Its next request is answered
/// `Evicted`, which fails what the client had in flight, and the client
/// registers again for the next one.
#[test]
fn test_register_evicts_least_recently_active() {
    let mut config = Config::new();
    config.set_clients_max(2);
    let mut cluster = Cluster::with_config(3, config);
    // Client 0 holds session 1. Client 1 registers, then client 0 runs a
    // request, so client 1 is the least recently active.
    let one = cluster.add_client();
    cluster.other(one).register();
    cluster.tick();
    cluster.request(Op::Add(1));
    cluster.tick();
    let two = cluster.add_client();
    cluster.other(two).register();
    cluster.tick();
    let table: Vec<_> = cluster.replicas[0]
        .client_table()
        .iter()
        .map(|record| record.client_id)
        .collect();
    assert_eq!(vec![0, two], table);
    for id in 0..3 {
        cluster.idle();
        cluster.tick();
        assert_eq!(
            table,
            cluster.replicas[id]
                .state_machine()
                .client_table()
                .iter()
                .map(|record| record.client_id)
                .collect::<Vec<_>>()
        );
    }

    cluster.other(one).on_request(Op::Add(10));
    cluster.tick();
    assert_eq!(vec![Completion::Evicted], cluster.completed(one));
    assert_eq!(1, cluster.value(0));
    assert_eq!(None, cluster.other(one).session());

    let request_number = cluster.other(one).on_request(Op::Add(20));
    assert_eq!(1, request_number);
    cluster.tick();
    assert_eq!(vec![Completion::Executed(1, 21)], cluster.completed(one));
    assert_eq!(21, cluster.value(0));
}

/// A reply to a registration from before an eviction reaches a client that
/// registers again: the client takes only a session opened after the one
/// it lost.
#[test]
fn test_late_registration_reply_after_eviction() {
    let mut cluster = Cluster::new(3);
    cluster.request(Op::Add(1));
    cluster.tick();
    assert_eq!(
        Some(Completion::Evicted),
        cluster.client.on_reply(Reply::<()>::Evicted {
            view_number: 0,
            client_id: 0,
            session: 1,
        })
    );
    cluster.request(Op::Add(2));
    let late = Reply::<()>::Registered {
        view_number: 0,
        client_id: 0,
        session: 1,
    };
    assert_eq!(None, cluster.client.on_reply(late));
    assert_eq!(None, cluster.client.session());
}

/// A request appended while its session was in the client table, and
/// executed after a registration evicted the session, does not run: every
/// replica skips it.
#[test]
fn test_request_of_a_session_evicted_in_between_does_not_execute() {
    let mut config = Config::new();
    config.set_clients_max(2);
    let mut cluster = Cluster::with_config(3, config);
    let one = cluster.add_client();
    cluster.other(one).register();
    cluster.tick();
    cluster.request(Op::Add(1));
    cluster.tick();
    // Client 2's registration is appended, then client 1's request, before
    // either executes.
    let two = cluster.add_client();
    let unacknowledged =
        |_: ReplicaID, message: &Msg| !matches!(message, Message::PrepareOk { .. });
    cluster.other(two).register();
    cluster.tick_with(&unacknowledged);
    cluster.other(one).on_request(Op::Add(10));
    cluster.tick_with(&unacknowledged);
    assert_eq!(5, cluster.replicas[0].op_number());
    assert_eq!(3, cluster.replicas[0].commit_number());
    for _ in 0..3 {
        cluster.idle();
        cluster.tick();
    }
    for id in 0..3 {
        assert_eq!(5, cluster.replicas[id].commit_number());
        assert_eq!(1, cluster.value(id));
    }
    assert_eq!(vec![Completion::Evicted], cluster.completed(one));
}

/// A client asks for a session twice. The second request is dropped while
/// the first is in the log, and answered with the same session once it
/// executed.
/// A new primary holds a client's request and its registration after an
/// eviction it has yet to execute. A replayed copy of the request, from the
/// session the client has left, is not appended again. Found by the
/// simulator, `--lite 4242660838422493032`.
#[test]
fn test_request_of_a_left_session_is_appended_once() {
    let mut config = Config::new();
    config.set_clients_max(1);
    let mut cluster = Cluster::with_config(3, config);
    // Client 1's registration evicts client 0's session, and client 0's
    // request, appended behind it, is skipped. Client 0 registers again
    // with its next request. Replica 1 misses all of it.
    let one = cluster.add_client();
    cluster.other(one).register();
    let registration: Vec<_> = cluster.other(one).drain::<Chunk>().collect();
    for (replica_id, message) in registration {
        cluster.replicas[replica_id].on_message(message);
    }
    cluster.request(Op::Add(1));
    cluster.deliver_client();
    cluster.tick_without(1);
    cluster.idle_without(1);
    cluster.tick_without(1);
    cluster.request(Op::Add(2));
    cluster.tick_without(1);
    cluster.idle_without(1);
    cluster.tick_without(1);
    assert_eq!(5, cluster.replicas[2].op_number());
    assert_eq!(1, cluster.replicas[1].op_number());

    // Replica 0 goes down, and replica 1 starts view 1 from replica 2's
    // log. Before its write lands, a replayed copy of the request arrives.
    let dvcs = RefCell::new(Vec::new());
    while dvcs.borrow().is_empty() {
        cluster.idle_without(0);
        cluster.tick_with(&|replica_id, message| {
            if replica_id == 1 && matches!(message, Message::DoViewChange { .. }) {
                dvcs.borrow_mut().push(message.clone());
                return false;
            }
            replica_id != 0
        });
    }
    for message in dvcs.take() {
        cluster.replicas[1].on_message(message);
    }
    assert!(cluster.replicas[1].is_primary());
    cluster.replicas[1].on_message(Message::Request {
        client_id: 0,
        session: 1,
        request_number: 1,
        answered: 0,
        op: Op::Add(1),
    });
    assert_eq!(5, cluster.replicas[1].op_number());
}

#[test]
fn test_duplicate_registration_answers_the_session() {
    let mut cluster = Cluster::new(3);
    let one = cluster.add_client();
    let register = Message::Register { client_id: one };
    cluster.replicas[0].on_message(register.clone());
    cluster.replicas[0].on_message(register.clone());
    assert_eq!(2, cluster.replicas[0].op_number());
    cluster.tick();
    let sessions: Vec<_> = cluster
        .take_replies()
        .into_iter()
        .map(|reply| match reply {
            Reply::Registered { session, .. } => session,
            reply => panic!("not a registration: {reply:?}"),
        })
        .collect();
    assert_eq!(vec![2], sessions);
    cluster.replicas[0].on_message(register);
    assert_eq!(2, cluster.replicas[0].op_number());
    let replies = cluster.take_replies();
    assert!(matches!(
        replies.as_slice(),
        [Reply::Registered { session: 2, .. }]
    ));
}

/// A request of an evicted session changes nothing, and still reaches the
/// state machine, so that its count of op numbers is the replica's and a
/// compaction up to it is safe. The second registration is dropped while
/// the first is in the log.
#[test]
fn test_every_executed_entry_reaches_the_state_machine() {
    let mut config = Config::new();
    config.set_clients_max(1);
    let mut cluster = Cluster::with_config(3, config);
    let one = cluster.add_client();
    cluster.replicas[0].on_message(Message::Register { client_id: one });
    cluster.replicas[0].on_message(Message::Register { client_id: one });
    cluster.replicas[0].on_message(Message::Request {
        client_id: 0,
        session: 1,
        request_number: 1,
        answered: 0,
        op: Op::Add(1),
    });
    cluster.tick();
    cluster.idle();
    cluster.tick();
    for replica in &cluster.replicas {
        assert_eq!(3, replica.applied());
        assert_eq!(replica.applied(), replica.state_machine().last_op);
        assert_eq!(0, replica.state_machine().value);
    }
}

/// A replica restarts with a state machine that kept the client table,
/// and evicts the same session as the others at the next registration: the
/// order of evictions comes back with the records.
#[test]
fn test_restart_keeps_the_eviction_order() {
    let mut config = Config::new();
    config.set_clients_max(2);
    let mut cluster = Cluster::with_config(3, config);
    let one = cluster.add_client();
    cluster.other(one).register();
    cluster.tick();
    cluster.request(Op::Add(1));
    cluster.tick();
    cluster.idle();
    cluster.tick();
    let state_machine = cluster.replicas[1].state_machine().clone();
    let applied = cluster.replicas[1].applied();
    let disk = cluster.disks[1].clone();
    cluster.replicas[1] =
        Replica::restart(1, cluster.config.clone(), state_machine, applied, disk, 7);

    let two = cluster.add_client();
    cluster.other(two).register();
    cluster.tick();
    cluster.idle();
    cluster.tick();
    for replica in &cluster.replicas {
        let table: Vec<_> = replica
            .client_table()
            .iter()
            .map(|record| record.client_id)
            .collect();
        assert_eq!(vec![0, two], table);
    }
}

/// A client re-sends a request only once it has gone a whole idle period
/// without a reply.
#[test]
fn test_resend_waits_a_whole_idle_period() {
    let mut cluster = Cluster::new(3);
    cluster.request(Op::Add(1));
    cluster.client.drain::<Chunk>().for_each(drop);
    cluster.client.on_idle();
    assert_eq!(0, cluster.client.drain::<Chunk>().count());
    cluster.client.on_idle();
    assert_eq!(3, cluster.client.drain::<Chunk>().count());
    cluster.client.on_idle();
    assert_eq!(0, cluster.client.drain::<Chunk>().count());
}

/// A scan of the client table takes its oldest sessions as candidates for
/// eviction. A candidate that executed an entry since gives way, and every
/// replica evicts the session whose latest entry executed earliest,
/// whatever candidates it holds: here a restarted replica holds none.
#[test]
fn test_eviction_skips_candidates_active_since_the_scan() {
    let mut config = Config::new();
    config.set_clients_max(32);
    let mut cluster = Cluster::with_config(3, config);
    let ids: Vec<ClientID> = (0..31).map(|_| cluster.add_client()).collect();
    for &id in &ids {
        cluster.other(id).register();
    }
    cluster.tick();
    // The next registration scans for the two oldest sessions, client 0's
    // and ids[0]'s, and evicts client 0.
    let late = cluster.add_client();
    cluster.other(late).register();
    cluster.tick();
    // ids[0] runs a request, so the next eviction passes it over for
    // ids[1], on replica 1 too, which restarts and scans afresh.
    cluster.other(ids[0]).on_request(Op::Add(1));
    cluster.tick();
    cluster.idle();
    cluster.tick();
    let state_machine = cluster.replicas[1].state_machine().clone();
    let applied = cluster.replicas[1].applied();
    let disk = cluster.disks[1].clone();
    cluster.replicas[1] =
        Replica::restart(1, cluster.config.clone(), state_machine, applied, disk, 7);
    let later = cluster.add_client();
    cluster.other(later).register();
    cluster.tick();
    cluster.idle();
    cluster.tick();
    for replica in &cluster.replicas {
        let table: BTreeSet<ClientID> = replica
            .client_table()
            .iter()
            .map(|record| record.client_id)
            .collect();
        assert_eq!(32, table.len());
        assert!(!table.contains(&0) && !table.contains(&ids[1]), "{table:?}");
        assert!(
            table.contains(&ids[0]) && table.contains(&later),
            "{table:?}"
        );
    }
}

/// The reply to a query, as the value it read.
fn queried(reply: &Reply<i32>) -> i32 {
    match reply {
        Reply::Queried { result, .. } => *result,
        reply => panic!("not a query's reply: {reply:?}"),
    }
}

/// A query executes once a quorum has confirmed the primary's view in a
/// round started after the query arrived, from a state holding what may
/// have completed: op 3, prepared and not committed, has not, and the
/// query does not wait for it.
#[test]
fn test_query_waits_for_its_round() {
    let mut cluster = Cluster::new(3);
    cluster.request(Op::Add(10));
    cluster.tick();
    cluster.request(Op::Add(20));
    let unacknowledged =
        |_: ReplicaID, message: &Msg| !matches!(message, Message::PrepareOk { .. });
    cluster.tick_with(&unacknowledged);
    assert_eq!(3, cluster.replicas[0].op_number());
    assert_eq!(2, cluster.replicas[0].commit_number());
    cluster.take_replies();
    let query = Message::Query {
        client_id: 5,
        query_number: 1,
        query: (),
    };
    cluster.replicas[0].on_message(query);
    cluster.tick_with(&|_, message| {
        !matches!(
            message,
            Message::ConfirmViewOk { .. } | Message::PrepareOk { .. }
        )
    });
    assert!(cluster.take_replies().is_empty());
    cluster.idle();
    cluster.tick_with(&unacknowledged);
    let answers: Vec<i32> = cluster
        .take_replies()
        .iter()
        .filter(|reply| reply.client_id() == 5)
        .map(queried)
        .collect();
    assert_eq!(vec![10], answers);
}

/// The old primary committed op 3 and answered its client, and went down
/// before the backups heard it committed. The new primary starts its view
/// with op 3 and commit number 2, and a query waits for op 3 to commit:
/// it may have completed in the old view.
#[test]
fn test_query_waits_for_the_ops_its_view_started_with() {
    let mut cluster = Cluster::new(3);
    cluster.request(Op::Add(10));
    cluster.tick();
    cluster.request(Op::Add(20));
    cluster.tick_with(&|replica_id, message| {
        replica_id == 0 || !matches!(message, Message::Commit { .. })
    });
    assert_eq!(3, cluster.replicas[0].commit_number());
    assert_eq!(2, cluster.replicas[1].commit_number());
    cluster.take_replies();
    for _ in 0..10 {
        cluster.idle_without(0);
        cluster.tick_with(&|replica_id, message| {
            replica_id != 0 && !matches!(message, Message::PrepareOk { .. })
        });
    }
    assert!(cluster.replicas[1].is_primary());
    assert_eq!(Status::Normal, cluster.replicas[1].status());
    assert_eq!(2, cluster.replicas[1].commit_number());
    cluster.replicas[1].on_message(Message::Query {
        client_id: 5,
        query_number: 1,
        query: (),
    });
    for _ in 0..3 {
        cluster.idle_without(0);
        cluster.tick_without(0);
    }
    let answers: Vec<i32> = cluster
        .take_replies()
        .iter()
        .filter(|reply| reply.client_id() == 5)
        .map(queried)
        .collect();
    assert_eq!(vec![30], answers);
}

/// A primary cut off from a view that moved on answers no query: no quorum
/// confirms its view. The new primary answers.
#[test]
fn test_deposed_primary_answers_no_query() {
    let mut cluster = Cluster::new(3);
    cluster.request(Op::Add(10));
    cluster.tick();
    for _ in 0..10 {
        cluster.idle_without(0);
        cluster.tick_without(0);
    }
    assert!(cluster.replicas[1].is_primary());
    assert!(cluster.replicas[0].is_primary());
    cluster.take_replies();
    let query = |replica: &mut Replica<Accumulator>| {
        replica.on_message(Message::Query {
            client_id: 5,
            query_number: 1,
            query: (),
        });
    };
    query(&mut cluster.replicas[0]);
    for _ in 0..3 {
        cluster.idle();
        cluster.tick();
    }
    assert!(cluster.take_replies().is_empty());
    query(&mut cluster.replicas[1]);
    cluster.tick();
    let replies = cluster.take_replies();
    assert_eq!(vec![10], replies.iter().map(queried).collect::<Vec<_>>());
    assert_eq!(1, replies[0].view_number());
}

/// A round whose confirmations are lost is sent again on the primary's
/// idle period, and the query waiting on it goes.
#[test]
fn test_round_resent_on_idle() {
    let mut cluster = Cluster::new(3);
    cluster.request(Op::Add(10));
    cluster.tick();
    cluster.take_replies();
    cluster.client.on_query(());
    cluster.tick_with(&|_, message| !matches!(message, Message::ConfirmView { .. }));
    assert!(cluster.take_replies().is_empty());
    cluster.idle();
    cluster.tick();
    let replies = cluster.take_replies();
    assert_eq!(vec![10], replies.iter().map(queried).collect::<Vec<_>>());
}

/// 32 rounds are out at a time: a query that arrives once the last
/// round's `ConfirmView` has left starts another, up to 32, and later ones
/// wait for the next, which starts once one is confirmed. All of them are
/// answered.
#[test]
fn test_rounds_out_are_bounded() {
    let mut cluster = Cluster::new(3);
    cluster.request(Op::Add(10));
    cluster.tick();
    cluster.take_replies();
    let mut held = Vec::new();
    for query_number in 1..=34 {
        cluster.replicas[0].on_message(Message::Query {
            client_id: 5,
            query_number,
            query: (),
        });
        held.extend(cluster.replicas[0].drain_messages_before_persist());
    }
    let rounds: Vec<u64> = held
        .iter()
        .filter_map(|(replica_id, message)| match message {
            Message::ConfirmView { round, .. } if *replica_id == 1 => Some(*round),
            _ => None,
        })
        .collect();
    assert_eq!((1..=32).collect::<Vec<u64>>(), rounds);
    cluster.queue.extend(held);
    cluster.tick();
    let answers: Vec<i32> = cluster.take_replies().iter().map(queried).collect();
    assert_eq!(vec![10; 34], answers);
}

/// Queries that arrive before the last round's `ConfirmView` leaves join
/// that round, and a query re-sent while it waits waits once.
#[test]
fn test_queries_of_a_step_share_a_round() {
    let mut cluster = Cluster::new(3);
    cluster.request(Op::Add(10));
    cluster.tick();
    cluster.take_replies();
    for query_number in [1, 2, 3, 3] {
        cluster.replicas[0].on_message(Message::Query {
            client_id: 5,
            query_number,
            query: (),
        });
    }
    let rounds = RefCell::new(Vec::new());
    cluster.tick_with(&|replica_id, message| {
        if let Message::ConfirmView { round, .. } = message {
            if replica_id == 1 {
                rounds.borrow_mut().push(*round);
            }
        }
        true
    });
    assert_eq!(vec![1], rounds.into_inner());
    let answers: Vec<i32> = cluster.take_replies().iter().map(queried).collect();
    assert_eq!(vec![10; 3], answers);
}

/// A client sends a query only once every earlier request has its reply,
/// and a request only once every earlier query has its reply: the query
/// sees the write before it, and not the one after.
#[test]
fn test_client_keeps_program_order() {
    let mut cluster = Cluster::new(3);
    cluster.request(Op::Add(10));
    cluster.client.on_query(());
    cluster.request(Op::Add(20));
    let kinds: Vec<&str> = cluster
        .client
        .drain::<Chunk>()
        .map(|(_, message)| match message {
            Message::Request { .. } => "request",
            Message::Query { .. } => "query",
            _ => "other",
        })
        .collect();
    assert_eq!(vec!["request"], kinds);
    cluster.client.on_idle();
    cluster.client.on_idle();
    cluster.tick();
    cluster.idle();
    cluster.tick();
    let replies = cluster.take_replies();
    let queried: Vec<i32> = replies
        .iter()
        .filter(|reply| matches!(reply, Reply::Queried { .. }))
        .map(queried)
        .collect();
    assert_eq!(vec![10], queried);
    assert_eq!(30, cluster.value(0));
}

/// A request waits for the reply to a query in flight, though nothing is
/// queued ahead of it, and goes once that reply comes.
#[test]
fn test_request_waits_for_a_query_in_flight() {
    let mut cluster = Cluster::new(3);
    cluster.client.on_query(());
    cluster.request(Op::Add(10));
    let sent: Vec<_> = cluster.client.drain::<Chunk>().collect();
    assert!(matches!(sent[..], [(_, Message::Query { .. })]), "{sent:?}");
    cluster.queue.extend(sent);
    cluster.tick();
    let queried: Vec<i32> = cluster
        .take_replies()
        .iter()
        .filter(|reply| matches!(reply, Reply::Queried { .. }))
        .map(queried)
        .collect();
    assert_eq!(vec![0], queried);
    assert_eq!(10, cluster.value(0));
}

/// An eviction fails a query in flight, and the client numbers queries on:
/// a late reply to the failed query answers none of the later ones.
#[test]
fn test_late_query_reply_after_eviction() {
    let mut cluster = Cluster::new(3);
    cluster.request(Op::Add(1));
    cluster.tick();
    assert_eq!(1, cluster.client.on_query(()));
    let evicted = Reply::<i32>::Evicted {
        view_number: 0,
        client_id: 0,
        session: 1,
    };
    assert_eq!(Some(Completion::Evicted), cluster.client.on_reply(evicted));
    assert_eq!(2, cluster.client.on_query(()));
    let late = Reply::<i32>::Queried {
        view_number: 0,
        client_id: 0,
        query_number: 1,
        result: 0,
    };
    assert_eq!(None, cluster.client.on_reply(late));
}

/// A reply for a request of an earlier session, late after an eviction,
/// leaves the request with the same number in the client's current
/// session in flight, and that request's own reply completes it.
#[test]
fn test_late_reply_of_an_earlier_session() {
    let mut config = Config::new();
    config.set_clients_max(1);
    let mut cluster = Cluster::with_config(3, config);
    let one = cluster.add_client();
    cluster.other(one).register();
    cluster.tick();
    // Client 0's session is gone: its request fails, and the next one
    // opens a session whose request 1 is lost on its way.
    cluster.request(Op::Add(10));
    cluster.tick();
    assert_eq!(None, cluster.client.session());
    let request_number = cluster.request(Op::Add(20));
    cluster.tick_with(&|_, message| !matches!(message, Message::Request { .. }));
    let session = cluster.client.session().expect("a new session");
    assert!(session > 1);
    let late = Reply::<i32>::Executed {
        view_number: 0,
        client_id: 0,
        session: 1,
        request_number,
        result: 0,
    };
    assert_eq!(None, cluster.client.on_reply(late));
    cluster.resend();
    cluster.tick();
    assert_eq!(20, cluster.value(0));
    cluster.request(Op::Add(30));
    cluster.tick();
    assert_eq!(50, cluster.value(0));
}
