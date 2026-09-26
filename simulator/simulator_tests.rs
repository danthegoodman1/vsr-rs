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
    options.replica_restart_probability = 0.0;
    options.blackout_probability = 0.0;
    options.checkpoint_power_loss_probability = 0.0;
    options.step_power_loss_probability = 0.0;
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

/// A quiet cluster: perfect network, no random crashes, restarts,
/// blackouts, or flushes, every write landing in its step, every replica in
/// the liveness core, and requests only, so that a script alone decides
/// what happens.
fn quiet(seed: u64, requests_max: usize) -> Simulator {
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
    options.step_power_loss_probability = 0.0;
    options.log_retention = 0;
    options.full_core = true;
    options.requests_max = requests_max;
    options.write_out_probability = 0.0;
    options.query_probability = 0.0;
    Simulator::init(seed, options).expect("options are valid")
}

/// Runs `script` on a quiet cluster, see [`quiet`].
fn run_script(seed: u64, requests_max: usize, script: &str) -> Simulator {
    let script = parse_script(script).unwrap();
    let last_tick = script.last().map(|(tick, _)| *tick).unwrap_or(0);
    let mut simulator = quiet(seed, requests_max);
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
    assert!(snapshot.restores > 0, "no replica restored a checkpoint");
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
        assert!(simulator.lost_writes > 0, "seed {seed} lost no write");
    }
}

/// Runs `simulator` through `script` up to tick `until`, and returns the
/// faults left.
fn run_until<'a>(
    simulator: &mut Simulator,
    script: &'a [(u64, Fault)],
    until: u64,
) -> &'a [(u64, Fault)] {
    let mut next = 0;
    while simulator.ticks < until {
        while next < script.len() && script[next].0 <= simulator.ticks {
            simulator.apply(script[next].1);
            next += 1;
        }
        simulator.step_run(Limits::default()).unwrap();
    }
    &script[next..]
}

/// A replica that loses power in the step that completes its recovery,
/// having restored the primary's checkpoint, comes back recovering, and
/// the liveness core takes it for what it restarts as. Seed
/// 17238020951159820783 at commit cf5628a put such a replica into the
/// core as a healthy one; it restarted recovering, with the primary of
/// its persisted view crashed for good, and the core never converged.
#[test]
fn power_loss_in_the_recovery_step() {
    let script = parse_script(
        "300 flush 0\n\
         300 flush 1\n\
         300 flush 2\n\
         500 reboot 1\n",
    )
    .unwrap();
    let mut simulator = quiet(11, 20_000);
    simulator.options.checkpoint_power_loss_probability = 1.0;
    simulator.options.full_core = false;
    let rest = run_until(&mut simulator, &script, 700);
    assert!(!simulator.is_up(1));
    assert_eq!(1, simulator.lost_writes);
    assert!(simulator.replicas()[1].is_recovering());
    if let Err(err) = simulator.run_script(rest, Limits::default()) {
        panic!("script failed at tick {}: {err:#}", simulator.ticks);
    }
}

/// A backup restores a checkpoint, which its state machine makes durable
/// at once, and loses power before the step is written. It restarts from
/// a disk behind its state machine: its commit number steps back from
/// what it had in memory, its log gives way to the checkpoint, and its
/// client table is the one the state machine flushed. Seeds
/// 18270094277230390851 and 4992333150870101077 at commit 534b85c tripped
/// the monotonic commit property on that.
#[test]
fn restart_from_a_disk_that_lost_the_last_step() {
    let script = parse_script(
        "300 partition 2\n\
         600 flush 0\n\
         601 flush 1\n\
         1000 heal 2\n\
         1600 restart 2\n",
    )
    .unwrap();
    let mut simulator = quiet(10, 20_000);
    simulator.options.checkpoint_power_loss_probability = 1.0;
    let rest = run_until(&mut simulator, &script, 1600);
    assert!(!simulator.is_up(2));
    assert_eq!(1, simulator.lost_writes);
    if let Err(err) = simulator.run_script(rest, Limits::default()) {
        panic!("script failed at tick {}: {err:#}", simulator.ticks);
    }
    assert!(simulator.is_up(2));
}

/// A power loss loses whatever a replica did since its last write, a
/// compaction here, whether the replica was running, caught in a
/// blackout, or paused.
#[test]
fn power_loss_before_a_step_is_written() {
    let simulator = run_script(
        8,
        20_000,
        "300 flush 0\n\
         300 power-loss 0\n\
         600 restart 0\n\
         900 flush 1\n\
         900 blackout\n\
         1000 restart 0\n\
         1000 restart 1\n\
         1000 restart 2\n\
         1500 flush 2\n\
         1500 crash 2\n\
         1700 power-loss 2\n\
         1900 restart 2\n",
    );
    assert_eq!(3, simulator.lost_steps);
    assert_eq!(3, simulator.lost_writes);
}

/// A replica that lost its disk writes it again as recovering, in the view
/// it had, before it does anything: a power loss in its first step brings
/// it back recovering, in that view.
#[test]
fn power_loss_before_a_rebooted_replica_writes() {
    let script = parse_script(
        "100 crash 0\n\
         1500 restart 0\n\
         2000 crash 1\n\
         2100 reboot 1\n\
         2100 lose-step 1\n\
         2500 restart 1\n",
    )
    .unwrap();
    let mut simulator = quiet(20, 20_000);
    let rest = run_until(&mut simulator, &script, 2100);
    let view_number = simulator.replicas()[1].view_number();
    let rest = run_until(&mut simulator, rest, 2101);
    assert!(view_number > 0, "no view change before the reboot");
    assert!(!simulator.is_up(1));
    assert_eq!(1, simulator.lost_steps);
    let replica = &simulator.replicas()[1];
    assert!(replica.is_recovering());
    assert_eq!(view_number, replica.view_number());
    if let Err(err) = simulator.run_script(rest, Limits::default()) {
        panic!("script failed at tick {}: {err:#}", simulator.ticks);
    }
}

/// A primary rebuilt from its disk restarts into a view change, which
/// moves its view in memory: the restart is a step. A second power loss
/// before its write loses it, and brings the replica back from the same
/// disk.
#[test]
fn power_loss_before_a_restart_is_written() {
    let simulator = run_script(
        21,
        20_000,
        "100 power-loss 0\n\
         500 restart 0\n\
         500 power-loss 0\n\
         900 restart 0\n",
    );
    assert_eq!(2, simulator.power_losses);
    assert_eq!(1, simulator.lost_steps);
    assert_eq!(1, simulator.lost_writes);
}

/// Replicas lose power after sending what need not wait and before
/// persisting the step, often, for a few seeds. A primary that sent a
/// `Prepare` for an entry it never made durable must not resume as the
/// primary; a step whose commit number was never written comes back from
/// the state machine.
#[test]
fn power_loss_between_send_and_persist() {
    for seed in [18, 19, 20] {
        let simulator = run(seed, |options| {
            options.replica_crash_probability = 0.0;
            options.replica_restart_probability = 0.02;
            options.replica_reboot_probability = 0.0;
            options.replica_flush_probability = 0.05;
            options.step_power_loss_probability = 0.002;
            options.log_retention = 0;
        });
        assert!(simulator.lost_writes > 0, "seed {seed} lost no write");
    }
}

/// Writes stay out while their replicas step on, and replicas lose power
/// often, for a few seeds. Some crashes find a write out, which lands in
/// some and is lost in others; replicas execute only what landed writes
/// hold.
#[test]
fn power_loss_with_a_write_out() {
    for seed in [25, 26, 27] {
        let simulator = run(seed, |options| {
            options.write_out_probability = 0.9;
            options.replica_crash_probability = 0.0;
            options.replica_restart_probability = 0.02;
            options.replica_reboot_probability = 0.0;
            options.replica_flush_probability = 0.05;
            options.step_power_loss_probability = 0.002;
        });
        assert!(simulator.writes_landed_at_crash > 0, "seed {seed}");
        assert!(
            simulator.writes_landed_at_crash < simulator.writes_out_at_crash,
            "seed {seed}"
        );
    }
}

/// Replica 1's write of op 1 stays out, and it loses power: the write
/// lands whole or not at all, and the restart holds op 1 exactly when it
/// landed. Over a few seeds both happen, and no acknowledgement of op 1
/// leaves first.
#[test]
fn power_loss_with_a_write_out_lands_it_or_not() {
    let mut outcomes = [0; 2];
    for seed in 30..50 {
        let mut simulator = quiet(seed, 1);
        simulator.options.request_probability = 0.0;
        while simulator.ticks < 2 {
            simulator.step_run(Limits::default()).unwrap();
        }
        simulator.options.request_probability = 1.0;
        simulator.options.write_out_probability = 1.0;
        simulator.options.write_land_probability = 0.0;
        while simulator.replicas()[1].op_number() == 0 {
            simulator.step_run(Limits::default()).unwrap();
        }
        simulator.apply(Fault::PowerLoss(1));
        assert_eq!(1, simulator.writes_out_at_crash, "seed {seed}");
        let landed = simulator.writes_landed_at_crash;
        assert_eq!(landed, simulator.replicas()[1].op_number(), "seed {seed}");
        outcomes[landed] += 1;
    }
    assert!(outcomes.iter().all(|&count| count > 0), "{outcomes:?}");
}

/// A primary executes an op in the step that commits it, sends what it
/// executed, and loses power before the step's write, taking its commit
/// number with it; a quorum's disks hold the op. The simulator recorded
/// commits only at the end of a tick, and failed these seeds of the full
/// swarm, and one of the lite one, at 32fccf2 with execution at commit
/// added: in 17626432488707759623 the primary sends a lagging replica a
/// checkpoint that holds the op, and in 5371941143654615824 it replies to
/// the client. Every write lands in its step.
#[test]
fn op_executed_in_a_step_that_lost_power() {
    let seeds = [
        (17626432488707759623, false),
        (15906584926357116181, false),
        (860930819270137469, false),
        (5273492533068031821, false),
        (5371941143654615824, true),
    ];
    for (seed, lite) in seeds {
        let mut prng = ChaCha8Rng::seed_from_u64(seed);
        let mut options = if lite {
            Options::lite(&mut prng)
        } else {
            Options::swarm(&mut prng)
        };
        options.write_out_probability = 0.0;
        let mut simulator = Simulator::init(seed, options).expect("options are valid");
        if let Err(err) = simulator.run(Limits::default()) {
            panic!("seed {seed} failed at tick {}: {err:#}", simulator.ticks);
        }
    }
}

/// A replica acknowledges an op and enters a view change, and the write
/// that holds the acknowledged op stays out while the replica goes on into
/// a later view, whose log replaces the one it acknowledged. The
/// acknowledgement leaves once the write lands, backed by the log of its
/// view on disk. `durable-promise` judged it by the replica's log instead,
/// and failed seeds 2086361338446829227 and 16092517535111013271 of the
/// full swarm at 32fccf2 with writes that stay out added.
#[test]
fn acknowledgement_released_after_a_later_view_started() {
    for seed in [2086361338446829227, 16092517535111013271] {
        let mut prng = ChaCha8Rng::seed_from_u64(seed);
        let options = Options::swarm(&mut prng);
        assert!(options.write_out_probability > 0.0);
        let mut simulator = Simulator::init(seed, options).expect("options are valid");
        if let Err(err) = simulator.run(Limits::default()) {
            panic!("seed {seed} failed at tick {}: {err:#}", simulator.ticks);
        }
    }
}

/// Replicas' processes die with their state machines anywhere between
/// their last flush and what they had applied, which their owners make
/// durable before the restart. A state machine can then be ahead of the
/// log's commit number, and the restart compacts the log up to it.
#[test]
fn process_crashes() {
    let mut ahead = 0;
    for seed in [28, 30, 31] {
        let simulator = run(seed, |options| {
            options.replica_crash_probability = 0.0005;
            options.replica_restart_probability = 0.01;
            options.replica_power_loss_probability = 0.0;
            options.replica_process_crash_probability = 1.0;
            options.replica_reboot_probability = 0.0;
            options.replica_flush_probability = 0.01;
            options.log_retention = 0;
        });
        assert!(simulator.process_crashes > 0, "seed {seed}");
        ahead += simulator.process_crashes_ahead;
    }
    assert!(ahead > 0);
}

/// A backup and then the primary lose their processes, the backup after a
/// flush that leaves some of what it applied unflushed.
#[test]
fn process_crash_script() {
    let simulator = run_script(
        12,
        20_000,
        "300 flush 1\n\
         800 process-crash 1\n\
         1200 restart 1\n\
         1500 process-crash 0\n\
         1900 restart 0\n",
    );
    assert_eq!(2, simulator.process_crashes);
    assert_eq!(0, simulator.power_losses);
}

/// More clients than the client table holds: they evict each other's
/// sessions, whose requests in flight fail, and register again.
#[test]
fn clients_evict_each_other() {
    let mut evicted = 0;
    for seed in [31, 32, 33] {
        let simulator = run(seed, |options| {
            options.client_count = 4;
            options.clients_max = 2;
            options.in_flight_max = 3;
        });
        evicted += simulator.requests_evicted;
    }
    assert!(evicted > 0);
}

/// Clients keep several requests in flight on a network that loses,
/// replays, and delays them.
#[test]
fn pipelined_clients() {
    for seed in [41, 42, 43] {
        let simulator = run(seed, |options| {
            options.clients_max = options.client_count;
            options.in_flight_max = 4;
            options.request_probability = 1.0;
        });
        assert_eq!(0, simulator.requests_evicted);
        assert_eq!(simulator.requests_replied, simulator.options.requests_max);
    }
}

/// Clients mix queries with their requests on a network that loses,
/// replays, and delays both, and every query reads a state at or past
/// every write completed before it.
#[test]
fn queries_among_requests() {
    for seed in [51, 52, 53] {
        let simulator = run(seed, |options| {
            options.query_probability = 0.5;
            options.network.fault_client_messages = true;
        });
        assert_eq!(simulator.requests_replied, simulator.options.requests_max);
    }
}
