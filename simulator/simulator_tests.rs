//! Tests of the simulator itself: determinism and a few fixed
//! configurations that must pass.

use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use vsr_simulator::{parse_script, Fault, Limits, NetworkOptions, Options, Simulator};

/// Runs the swarm configuration for `seed`, with `adjust` applied to it.
fn run(seed: u64, adjust: impl FnOnce(&mut Options)) -> Simulator {
    let _ = env_logger::try_init();
    let mut prng = ChaCha8Rng::seed_from_u64(seed);
    let mut options = Options::lite(&mut prng);
    adjust(&mut options);
    let mut simulator = Simulator::init(seed, options).expect("options are valid");
    if let Err(err) = simulator.run(Limits::default()) {
        panic!("seed {seed} failed at tick {}: {err:#}", simulator.ticks);
    }
    simulator
}

fn perfect_network(options: &mut Options) {
    options.network = NetworkOptions::perfect();
}

fn replay_only(options: &mut Options) {
    options.network = NetworkOptions {
        packet_replay_probability: 0.1,
        ..NetworkOptions::perfect()
    };
}

#[test]
fn same_seed_gives_same_run() {
    let a = run(1, replay_only);
    let b = run(1, replay_only);
    assert_eq!(a.ticks, b.ticks);
    assert_eq!(
        format!("{:?}", a.message_summary()),
        format!("{:?}", b.message_summary())
    );
    assert_eq!(
        a.replicas()[0].state_machine().value,
        b.replicas()[0].state_machine().value
    );
}

#[test]
fn perfect_network_replies_to_every_request() {
    let simulator = run(2, perfect_network);
    assert_eq!(simulator.requests_sent, simulator.options.requests_max);
    assert_eq!(simulator.requests_replied, simulator.options.requests_max);
}

#[test]
fn replayed_messages() {
    for seed in [3, 4, 5] {
        run(seed, replay_only);
    }
}

#[test]
fn seven_replicas() {
    run(6, |options| {
        options.replica_count = 7;
        replay_only(options);
    });
}

/// Injected faults on a cluster with no random ones: the primary crashes
/// and comes back, a backup reboots with no memory, a replica is cut off
/// and reconnected. The run must still pass.
#[test]
fn fault_script() {
    let _ = env_logger::try_init();
    let mut prng = ChaCha8Rng::seed_from_u64(7);
    let mut options = Options::lite(&mut prng);
    options.network = NetworkOptions::perfect();
    options.replica_crash_probability = 0.0;
    options.blackout_probability = 0.0;
    options.checkpoint_power_loss_probability = 0.0;
    options.full_core = true;
    options.requests_max = 20_000;
    let script = parse_script(
        "100 crash 0\n\
         2000 restart 0\n\
         2500 crash 1\n\
         2600 reboot 1\n\
         4000 partition 2\n\
         # replica 2 is alone for a while\n\
         5000 heal-all\n",
    )
    .unwrap();
    assert_eq!(script[1], (2000, Fault::Restart(0)));
    let mut simulator = Simulator::init(7, options).expect("options are valid");
    if let Err(err) = simulator.run_script(&script, Limits::default()) {
        panic!("script failed at tick {}: {err:#}", simulator.ticks);
    }
    let snapshot = simulator.snapshot();
    assert!(
        snapshot.tick > 5000,
        "the run ended at tick {}",
        snapshot.tick
    );
    assert!(
        snapshot
            .replicas
            .iter()
            .all(|replica| replica.up && !replica.partitioned),
        "{:?}",
        snapshot.replicas
    );
    assert!(snapshot
        .replicas
        .iter()
        .any(|replica| replica.view_number > 0));
    assert_eq!(1, snapshot.reboots);
}

/// Runs `script` on a quiet cluster: perfect network, no random crashes,
/// restarts, blackouts, or flushes, every replica in the liveness core, so
/// that the script alone decides what happens.
fn run_script(seed: u64, requests_max: usize, script: &str) -> Simulator {
    let _ = env_logger::try_init();
    let mut prng = ChaCha8Rng::seed_from_u64(seed);
    let mut options = Options::lite(&mut prng);
    options.network = NetworkOptions::perfect();
    options.replica_crash_probability = 0.0;
    options.replica_restart_probability = 0.0;
    options.replica_reboot_probability = 0.0;
    options.blackout_probability = 0.0;
    options.replica_flush_probability = 0.0;
    options.checkpoint_power_loss_probability = 0.0;
    options.log_retention = 0;
    options.full_core = true;
    options.requests_max = requests_max;
    let script = parse_script(script).unwrap();
    let last_tick = script.last().map(|(tick, _)| *tick).unwrap_or(0);
    let mut simulator = Simulator::init(seed, options).expect("options are valid");
    if let Err(err) = simulator.run_script(&script, Limits::default()) {
        panic!("script failed at tick {}: {err:#}", simulator.ticks);
    }
    assert!(
        simulator.ticks > last_tick,
        "the run ended at tick {} before the script did",
        simulator.ticks
    );
    simulator
}

/// Replicas lose power one after another and restart from their disks,
/// with and without a flush of the state machine before the loss. The
/// primary is among them, so a view change happens while it is down and
/// it comes back to a view it has to catch up with.
#[test]
fn power_loss_script() {
    let simulator = run_script(
        8,
        20_000,
        "100 flush 0\n\
         300 power-loss 0\n\
         800 restart 0\n\
         1500 power-loss 1\n\
         1600 flush 2\n\
         2000 restart 1\n\
         2500 flush 0\n\
         2501 power-loss 2\n\
         3000 restart 2\n",
    );
    let snapshot = simulator.snapshot();
    assert_eq!(3, snapshot.power_losses);
    assert_eq!(3, snapshot.flushes);
    assert_eq!(0, snapshot.reboots);
}

/// Every replica loses power at once, twice. The cluster must come back
/// with everything it had committed, and finish the run.
#[test]
fn blackout_script() {
    let simulator = run_script(
        9,
        20_000,
        "500 flush 1\n\
         1000 blackout\n\
         1200 restart 0\n\
         1300 restart 1\n\
         1400 restart 2\n\
         3000 blackout\n\
         3100 restart 2\n\
         3200 restart 1\n\
         3300 restart 0\n",
    );
    let snapshot = simulator.snapshot();
    assert_eq!(6, snapshot.power_losses);
}

/// Flushes compact the log on every replica, with no retention, while a
/// replica is cut off and while one has lost power: both come back to
/// logs whose entries they need have been compacted, so they catch up
/// from checkpoints.
#[test]
fn compaction_script() {
    let simulator = run_script(
        10,
        20_000,
        "300 partition 2\n\
         600 flush 0\n\
         601 flush 1\n\
         1000 heal 2\n\
         1500 power-loss 0\n\
         1800 flush 1\n\
         1801 flush 2\n\
         2300 restart 0\n\
         3000 flush 0\n\
         3001 flush 1\n\
         3002 flush 2\n",
    );
    let snapshot = simulator.snapshot();
    assert_eq!(7, snapshot.flushes);
    assert!(
        simulator
            .replicas()
            .iter()
            .all(|replica| replica.log_start() > 0),
        "no replica compacted"
    );
}

/// Random power losses, blackouts, and flushes with no retention, on top
/// of a lossy network, for a few seeds.
#[test]
fn power_losses_and_compaction() {
    for seed in [11, 12, 13, 14] {
        run(seed, |options| {
            options.replica_crash_probability = 0.0005;
            options.replica_restart_probability = 0.01;
            options.replica_power_loss_probability = 1.0;
            options.replica_reboot_probability = 0.1;
            options.blackout_probability = 0.0001;
            options.replica_flush_probability = 0.05;
            options.log_retention = 0;
        });
    }
}

/// Every checkpoint a replica restores is followed by a power loss before
/// the step is persisted, so the replica comes back with its state
/// machine ahead of its log, for a few seeds.
#[test]
fn power_loss_after_every_checkpoint() {
    for seed in [15, 16, 17] {
        let simulator = run(seed, |options| {
            options.replica_crash_probability = 0.0005;
            options.replica_restart_probability = 0.02;
            options.replica_power_loss_probability = 1.0;
            options.replica_reboot_probability = 0.0;
            options.replica_flush_probability = 0.05;
            options.checkpoint_power_loss_probability = 1.0;
            options.log_retention = 0;
        });
        assert!(
            simulator.power_losses > simulator.crashes / 2,
            "seed {seed}"
        );
    }
}

/// The liveness core must judge a replica that lost power by what its
/// disk says, since that is what it restarts as. Seed 17238020951159820783
/// at commit cf5628a put a replica that had restored a checkpoint, then
/// lost power before persisting the step, into the core on the strength of
/// its in-memory status; it restarted recovering, with the primary of its
/// persisted view crashed for good, and the core never converged.
#[test]
fn liveness_core_judges_power_lost_replicas_by_disk() {
    let _ = env_logger::try_init();
    let seed = 17238020951159820783;
    let mut prng = ChaCha8Rng::seed_from_u64(seed);
    let options = Options::swarm(&mut prng);
    let mut simulator = Simulator::init(seed, options).expect("options are valid");
    if let Err(err) = simulator.run(Limits::default()) {
        panic!("seed {seed} failed at tick {}: {err:#}", simulator.ticks);
    }
}
