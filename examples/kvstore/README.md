# kvstore

A key-value store replicated with vsr-rs. Every node accepts clients over a
Redis-like inline protocol, so `nc` works.

## Run

Build and start three nodes, each in its own terminal:

```console
cargo build --example kvstore
./target/debug/examples/kvstore --id 0 --replicas 127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002 --listen 127.0.0.1:6379
./target/debug/examples/kvstore --id 1 --replicas 127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002 --listen 127.0.0.1:6380
./target/debug/examples/kvstore --id 2 --replicas 127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002 --listen 127.0.0.1:6381
```

Talk to any node:

```console
$ nc localhost 6379
PING
+PONG
SET foo bar
+OK
GET foo
$3
bar
GET nope
$-1
```

Stop node 0 with Ctrl-C, or kill it. The others pick node 1 as the new
primary within a second and keep serving. Start node 0 again and it comes
back from its disk and rejoins as a backup. Kill all three and start them
again, and they come back with everything they had committed.

## On disk

Each node keeps its data in `kvstore-node-N`, or the directory given with
`--data`:

- `journal/` holds the replica's log and counters, in a write-ahead log
  from the `writeahead` crate, see [`journal.rs`](journal.rs). After every
  batch of events, the node sends the messages that need not wait for the
  write, then appends what changed and fsyncs once, then sends the rest:
  the log entries from the change marker on, a truncation or compaction if
  there was one, and the counters, which close the batch. A batch that
  changed only the commit number is not written. A replay applies a batch
  only once it has seen the counters, since a torn write can leave the
  first records of a batch on disk.
- `store/` is a `fjall` database with the keys and values, the client
  table, and the number of operations applied, all written in the same
  batch as each operation. The replica executes operations only after the
  journal is written, so the store is never ahead of the journal, except
  for a checkpoint it restored, which is persisted at once. The store never
  fsyncs on its own otherwise. A timer persists it once a second, and the
  replica then compacts its log up to what the store has made durable,
  less a thousand entries kept for replicas a little behind. Journal files
  with nothing left in them are deleted.

On restart the node replays the journal, opens the store, and applies the
committed entries the store had not persisted. A node whose data is gone
starts empty; with `--recover`, which needs an empty data directory, it
recovers from the others instead, as a replacement machine should. A
store behind the journal's compaction point, as after restoring the wrong
backup, is refused.

## Notes

- Keys and values are single words. One command at a time per connection.
- `RUST_LOG=trace` shows every protocol message, `RUST_LOG=debug` the
  journal's file deletions.
- `cargo test --example kvstore` runs an in-process cluster against real
  journals and stores in the temp directory.
