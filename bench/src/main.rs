//! Measures what durability costs in the kvstore's event loop.
//!
//! Three kvstore nodes run in one process, each on its own thread with the
//! kvstore's own event loop, store, journal, constants, and timer. What a
//! node sends goes through a router thread of its own to the node it is
//! for, as the kvstore's sender threads would put it on the wire, so the
//! numbers leave the network out. Closed-loop clients send their commands
//! to node 0 the way the kvstore's client connections do, each with
//! `PIPELINE` commands in flight, one unless set, and `READS` percent of
//! them GETs, none unless set, over 100,000 keys. Two configurations:
//!
//! - `journal`: the kvstore as it runs: a journal write and fsync of what
//!   the replica changed more than the commit number, one write at a time
//!   while the event loop steps on, and a store persist every second.
//! - `no-journal`: the same with the journal off. The difference is what
//!   the journal costs.
//!
//! Each configuration runs `REPEAT` times, three unless set, taking turns
//! with the other so that drift in the machine falls on both. The report
//! gives the median of each column, the range of throughput, and the
//! number of runs in which the cluster changed views, which none should.
//! Latencies are those of the requests answered in the measured seconds.
//! `fsync/s` counts, per node, the journal writes and the store persists,
//! each of which fsyncs; the syncs with which the journal starts a new
//! file are left out. A run fails if no request is answered for three
//! seconds or a node's thread ends, and so does a shutdown that takes ten;
//! either prints what each node last did and which threads still run. The
//! library's own CPU cost is for `vsr-micro` to measure.
//!
//! ```console
//! cargo run --release -p vsr-bench
//! CLIENTS=1,16,256 REPEAT=5 cargo run --release -p vsr-bench
//! CONFIG=journal cargo run --release -p vsr-bench
//! ```
//!
//! Data goes under `target/bench`, or the directory given as the
//! argument, so fsync hits whatever disk that is on. To emulate other
//! hardware, put the data on a tmpfs, where fsync costs nothing, and set
//! `FSYNC_US` to the microseconds each journal write spends after its
//! fsync, and `NET_US` to the one-way delay of every frame between nodes.
//! The clients stay on node 0, as if on its machine, and the store's
//! persists, once a second, stay free.
//!
//! ```console
//! NET_US=100 FSYNC_US=50 cargo run --release -p vsr-bench -- /dev/shm/vsr-bench
//! ```

#[allow(dead_code)]
#[path = "../../examples/kvstore/journal.rs"]
mod journal;
#[allow(dead_code)]
#[path = "../../examples/kvstore/node.rs"]
mod node;

#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

use node::{client_id, config, run_timer, Command, Event, Frame, Node, Start, Stats, TICK};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{channel, Receiver, RecvTimeoutError, Sender, TryRecvError};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use vsr_rs::ReplicaID;

const REPLICAS: usize = 3;
const KEY_SPACE: u64 = 100_000;
const WARMUP: Duration = Duration::from_secs(2);
const MEASURE: Duration = Duration::from_secs(8);
/// How long a run may go without answering a request.
const STALL: Duration = Duration::from_secs(3);
/// How long the threads of a run may take to stop.
const SHUTDOWN: Duration = Duration::from_secs(10);
const CONFIGURATIONS: [(&str, bool); 2] = [("journal", true), ("no-journal", false)];

/// The hardware the bench emulates, see the module documentation.
#[derive(Clone, Copy)]
struct Emulation {
    /// One-way delay of every frame between nodes.
    network: Duration,
    /// Time each journal write spends after its fsync.
    fsync: Duration,
}

/// What one run measured.
struct Measurement {
    ops_per_second: f64,
    p50: Duration,
    p99: Duration,
    max: Duration,
    /// Journal writes and store persists per node per second.
    fsyncs_per_second: f64,
    /// Whether any node left the first view.
    view_changed: bool,
}

/// The threads of one run, and what they share.
struct Cluster {
    stop: Arc<AtomicBool>,
    /// The nodes' event channels.
    events: Vec<Sender<Event>>,
    stats: Vec<Arc<Stats>>,
    nodes: Vec<JoinHandle<()>>,
    helpers: Vec<JoinHandle<()>>,
    /// Requests answered so far, by every client.
    answered: Arc<AtomicU64>,
    /// The incarnation of node 0, which the clients' ids carry.
    incarnation: u64,
}

/// Spawns a thread named `name`.
fn spawn<T: Send + 'static>(
    name: String,
    run: impl FnOnce() -> T + Send + 'static,
) -> JoinHandle<T> {
    thread::Builder::new()
        .name(name)
        .spawn(run)
        .expect("spawn a thread")
}

impl Cluster {
    /// Starts three new nodes in `dir`, with the journal on or off, a timer
    /// and a router for each.
    fn start(dir: &Path, journaled: bool, emulation: Emulation) -> Result<Cluster, String> {
        let config = config(REPLICAS);
        let stop = Arc::new(AtomicBool::new(false));
        let mut cluster = Cluster {
            stop: stop.clone(),
            events: Vec::new(),
            stats: Vec::new(),
            nodes: Vec::new(),
            helpers: Vec::new(),
            answered: Arc::new(AtomicU64::new(0)),
            incarnation: 0,
        };
        let mut nodes = Vec::new();
        for id in 0..REPLICAS {
            let node_dir = dir.join(format!("node{id}"));
            let mut node = match Node::open(id, config.clone(), &node_dir, Start::Init) {
                Ok(node) => node,
                Err(err) => {
                    cluster.stop()?;
                    return Err(err);
                }
            };
            node.journaled = journaled;
            node.announce_views = false;
            cluster.stats.push(node.stats.clone());
            let (events, events_rx) = channel();
            let (timer_events, stop) = (events.clone(), stop.clone());
            cluster.helpers.push(spawn(format!("timer {id}"), move || {
                run_timer(timer_events, || stop.load(Ordering::Relaxed))
            }));
            cluster.events.push(events);
            nodes.push((node, events_rx));
        }
        cluster.incarnation = nodes[0].0.incarnation;
        for (id, (mut node, events_rx)) in nodes.into_iter().enumerate() {
            let (frames, frames_rx) = channel::<(ReplicaID, Frame)>();
            let (peers, stop) = (cluster.events.clone(), stop.clone());
            cluster.helpers.push(spawn(format!("router {id}"), move || {
                route(frames_rx, peers, stop, emulation.network)
            }));
            let wake = cluster.events[id].clone();
            cluster.nodes.push(spawn(format!("node {id}"), move || {
                node.run(events_rx, wake, frames);
            }));
        }
        Ok(cluster)
    }

    /// Starts `count` closed-loop clients of node 0, each with `pipeline`
    /// commands in flight, a GET with probability `reads` and a SET
    /// otherwise. Each returns the latencies of the commands it had
    /// answered from `measure_from` to `end`.
    fn clients(
        &mut self,
        count: usize,
        pipeline: usize,
        reads: f64,
        measure_from: Instant,
        end: Instant,
    ) -> Vec<JoinHandle<Vec<Duration>>> {
        (0..count)
            .map(|i| {
                let events = self.events[0].clone();
                let stop = self.stop.clone();
                let answered = self.answered.clone();
                let connection = client_id(0, self.incarnation, i as u64);
                spawn(format!("client {i}"), move || {
                    let mut prng = ChaCha8Rng::seed_from_u64(i as u64);
                    let (respond_tx, respond_rx) = channel();
                    let mut latencies = Vec::new();
                    // The node answers a connection's commands in the order
                    // they came.
                    let mut in_flight = VecDeque::new();
                    while !stop.load(Ordering::Relaxed) {
                        while in_flight.len() < pipeline {
                            let key = format!("k{}", prng.gen_range(0..KEY_SPACE));
                            let command = if prng.gen_bool(reads) {
                                Command::Get(key)
                            } else {
                                Command::Set(key, prng.gen::<u64>().to_string())
                            };
                            let event = Event::Command {
                                connection,
                                command,
                                respond: respond_tx.clone(),
                            };
                            in_flight.push_back(Instant::now());
                            if events.send(event).is_err() {
                                return latencies;
                            }
                        }
                        if !wait(&respond_rx, &stop) {
                            break;
                        }
                        let sent = in_flight.pop_front().expect("a request in flight");
                        let now = Instant::now();
                        answered.fetch_add(1, Ordering::Relaxed);
                        if (measure_from..=end).contains(&now) {
                            latencies.push(now - sent);
                        }
                    }
                    latencies
                })
            })
            .collect()
    }

    /// What each node last did, and which threads still run.
    fn report(&self) -> String {
        let mut report = String::new();
        for (id, stats) in self.stats.iter().enumerate() {
            report.push_str(&format!(
                "\n  node {id}: {} batches, {} journal writes, {} store persists, view {}, commit {}",
                stats.batches.load(Ordering::Relaxed),
                stats.journal_writes.load(Ordering::Relaxed),
                stats.store_persists.load(Ordering::Relaxed),
                stats.view_number.load(Ordering::Relaxed),
                stats.commit_number.load(Ordering::Relaxed),
            ));
        }
        let running: Vec<&str> = self
            .nodes
            .iter()
            .chain(&self.helpers)
            .filter(|thread| !thread.is_finished())
            .filter_map(|thread| thread.thread().name())
            .collect();
        report.push_str(&format!("\n  threads running: {}", running.join(", ")));
        report
    }

    /// Journal writes and store persists so far, over every node.
    fn fsyncs(&self) -> u64 {
        self.stats
            .iter()
            .map(|stats| {
                stats.journal_writes.load(Ordering::Relaxed)
                    + stats.store_persists.load(Ordering::Relaxed)
            })
            .sum()
    }

    /// Stops every thread, and fails with a report if one does not stop in
    /// time. The nodes stop on the `Event::Stop` sent here.
    fn stop(mut self) -> Result<(), String> {
        self.stop.store(true, Ordering::Relaxed);
        for events in self.events.drain(..) {
            let _ = events.send(Event::Stop);
        }
        let deadline = Instant::now() + SHUTDOWN;
        while !self
            .nodes
            .iter()
            .chain(&self.helpers)
            .all(JoinHandle::is_finished)
        {
            if Instant::now() > deadline {
                return Err(format!("the cluster did not stop:{}", self.report()));
            }
            thread::sleep(Duration::from_millis(10));
        }
        for thread in self.nodes.into_iter().chain(self.helpers) {
            thread.join().map_err(|_| "a node panicked".to_string())?;
        }
        Ok(())
    }
}

/// Moves what a node sends to the nodes it is for, until stopped, each
/// frame `delay` after it was sent. A delay spins the router's thread,
/// since a sleep this short overshoots by more than it waits.
fn route(
    frames: Receiver<(ReplicaID, Frame)>,
    nodes: Vec<Sender<Event>>,
    stop: Arc<AtomicBool>,
    delay: Duration,
) {
    if delay.is_zero() {
        while !stop.load(Ordering::Relaxed) {
            match frames.recv_timeout(TICK) {
                Ok((dst, frame)) => {
                    let _ = nodes[dst].send(frame.into());
                }
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => return,
            }
        }
        return;
    }
    let mut due = VecDeque::new();
    while !stop.load(Ordering::Relaxed) {
        loop {
            match frames.try_recv() {
                Ok((dst, frame)) => due.push_back((Instant::now() + delay, dst, frame)),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => return,
            }
        }
        let now = Instant::now();
        while due.front().is_some_and(|(at, _, _)| *at <= now) {
            let (_, dst, frame) = due.pop_front().expect("a frame is due");
            let _ = nodes[dst].send(frame.into());
        }
        std::hint::spin_loop();
    }
}

/// Waits for a client's reply. Returns false if the run stopped first.
fn wait(replies: &Receiver<String>, stop: &AtomicBool) -> bool {
    loop {
        match replies.recv_timeout(TICK) {
            Ok(_) => return true,
            Err(RecvTimeoutError::Timeout) if !stop.load(Ordering::Relaxed) => {}
            Err(_) => return false,
        }
    }
}

/// Runs `clients` closed-loop clients, each with `pipeline` commands in
/// flight and a share `reads` of them GETs, against three new nodes in
/// `dir`.
fn run(
    dir: &Path,
    journaled: bool,
    clients: usize,
    pipeline: usize,
    reads: f64,
    emulation: Emulation,
) -> Result<Measurement, String> {
    let _ = std::fs::remove_dir_all(dir);
    let mut cluster = Cluster::start(dir, journaled, emulation)?;
    let started = Instant::now();
    let measure_from = started + WARMUP;
    let end = measure_from + MEASURE;
    let client_threads = cluster.clients(clients, pipeline, reads, measure_from, end);
    let (mut last_answered, mut last_progress) = (0, Instant::now());
    let mut fsyncs_from = None;
    while Instant::now() < end {
        thread::sleep(Duration::from_millis(50));
        if fsyncs_from.is_none() && Instant::now() >= measure_from {
            fsyncs_from = Some(cluster.fsyncs());
        }
        let answered = cluster.answered.load(Ordering::Relaxed);
        if answered > last_answered {
            (last_answered, last_progress) = (answered, Instant::now());
        }
        let failure = if cluster.nodes.iter().any(JoinHandle::is_finished) {
            Some("a node's thread ended".to_string())
        } else if last_progress.elapsed() > STALL {
            Some(format!(
                "no request answered for {:.1}s",
                last_progress.elapsed().as_secs_f64()
            ))
        } else {
            None
        };
        if let Some(failure) = failure {
            let report = cluster.report();
            let _ = cluster.stop();
            return Err(format!("{failure}:{report}"));
        }
    }
    let fsyncs = cluster.fsyncs() - fsyncs_from.unwrap_or(0);
    let view_changed = cluster
        .stats
        .iter()
        .any(|stats| stats.view_number.load(Ordering::Relaxed) > 0);
    cluster.stop()?;
    let mut latencies: Vec<Duration> = Vec::new();
    for client in client_threads {
        latencies.extend(client.join().map_err(|_| "a client panicked".to_string())?);
    }
    latencies.sort();
    let percentile = |p: f64| -> Duration {
        latencies
            .get(((latencies.len().max(1) - 1) as f64 * p).round() as usize)
            .copied()
            .unwrap_or_default()
    };
    let seconds = MEASURE.as_secs_f64();
    let _ = std::fs::remove_dir_all(dir);
    Ok(Measurement {
        ops_per_second: latencies.len() as f64 / seconds,
        p50: percentile(0.5),
        p99: percentile(0.99),
        max: percentile(1.0),
        fsyncs_per_second: fsyncs as f64 / seconds / REPLICAS as f64,
        view_changed,
    })
}

/// The median of `values`, which must not be empty.
fn median(mut values: Vec<f64>) -> f64 {
    values.sort_by(f64::total_cmp);
    let middle = values.len() / 2;
    if values.len() % 2 == 1 {
        values[middle]
    } else {
        (values[middle - 1] + values[middle]) / 2.0
    }
}

fn env_list(name: &str, default: &[usize]) -> Vec<usize> {
    match std::env::var(name) {
        Ok(list) => list
            .split(',')
            .map(|n| n.trim().parse().unwrap_or_else(|_| panic!("bad {name}")))
            .collect(),
        Err(_) => default.to_vec(),
    }
}

/// Prints one row of the report, from the runs of one configuration.
fn report(name: &str, clients: usize, runs: &[Measurement]) {
    let column = |value: &dyn Fn(&Measurement) -> f64| median(runs.iter().map(value).collect());
    let ms = |d: Duration| d.as_secs_f64() * 1e3;
    let (low, high) = runs.iter().fold((f64::MAX, 0f64), |(low, high), run| {
        (low.min(run.ops_per_second), high.max(run.ops_per_second))
    });
    println!(
        "{name:<13} {clients:>7} {:>10.0} {:>17} {:>9.2} {:>9.2} {:>9.2} {:>9.0} {:>9.1} {:>6}",
        column(&|run| run.ops_per_second),
        format!("{low:.0}-{high:.0}"),
        column(&|run| ms(run.p50)),
        column(&|run| ms(run.p99)),
        column(&|run| ms(run.max)),
        column(&|run| run.fsyncs_per_second),
        column(&|run| run.ops_per_second / run.fsyncs_per_second.max(f64::MIN_POSITIVE)),
        runs.iter().filter(|run| run.view_changed).count(),
    );
}

fn main() {
    let _ = env_logger::try_init();
    let data = PathBuf::from(
        std::env::args()
            .nth(1)
            .unwrap_or_else(|| "target/bench".to_string()),
    );
    let client_counts = env_list("CLIENTS", &[1, 16, 64]);
    let repeat = env_list("REPEAT", &[3])[0].max(1);
    let pipeline = env_list("PIPELINE", &[1])[0].max(1);
    let reads = env_list("READS", &[0])[0].min(100) as f64 / 100.0;
    let configurations: Vec<(&str, bool)> = match std::env::var("CONFIG") {
        Ok(only) => match CONFIGURATIONS.iter().find(|(name, _)| *name == only) {
            Some(configuration) => vec![*configuration],
            None => {
                eprintln!("unknown CONFIG {only:?}: journal or no-journal");
                std::process::exit(2);
            }
        },
        Err(_) => CONFIGURATIONS.to_vec(),
    };
    let micros = |name| Duration::from_micros(env_list(name, &[0])[0] as u64);
    let emulation = Emulation {
        network: micros("NET_US"),
        fsync: micros("FSYNC_US"),
    };
    journal::set_sync_delay(emulation.fsync);
    std::fs::create_dir_all(&data).expect("data directory");
    println!(
        "{REPLICAS} kvstore nodes on their own threads, channels between them, data in {}; median of {repeat} runs of {}s",
        data.display(),
        MEASURE.as_secs()
    );
    if pipeline > 1 {
        println!("each client keeps {pipeline} commands in flight");
    }
    if reads > 0.0 {
        println!("{:.0}% of commands are GETs", reads * 100.0);
    }
    if emulation.network > Duration::ZERO || emulation.fsync > Duration::ZERO {
        println!(
            "emulating {} µs between nodes each way and {} µs more per journal fsync",
            emulation.network.as_micros(),
            emulation.fsync.as_micros()
        );
    }
    println!();
    println!(
        "{:<13} {:>7} {:>10} {:>17} {:>9} {:>9} {:>9} {:>9} {:>9} {:>6}",
        "configuration",
        "clients",
        "ops/s",
        "ops/s range",
        "p50 ms",
        "p99 ms",
        "max ms",
        "fsync/s",
        "ops/fsync",
        "views"
    );
    for &clients in &client_counts {
        let mut runs: Vec<Vec<Measurement>> = configurations.iter().map(|_| Vec::new()).collect();
        for i in 0..repeat {
            for (runs, (name, journaled)) in runs.iter_mut().zip(&configurations) {
                let dir = data.join(format!("{name}-{clients}-{i}"));
                match run(&dir, *journaled, clients, pipeline, reads, emulation) {
                    Ok(measurement) => runs.push(measurement),
                    Err(err) => {
                        eprintln!("{name} with {clients} clients, run {i}: {err}");
                        std::process::exit(1);
                    }
                }
            }
        }
        for (runs, (name, _)) in runs.iter().zip(&configurations) {
            report(name, clients, runs);
        }
    }
}
