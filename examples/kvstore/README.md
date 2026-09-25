# kvstore

A key-value store replicated with vsr-rs. Every node accepts clients over a
Redis-like inline protocol, so `nc` works.

## Run

Build and start the three nodes of a new cluster, each in its own
terminal:

```console
cargo build --example kvstore
./target/debug/examples/kvstore --init --id 0 --replicas 127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002 --listen 127.0.0.1:6379
./target/debug/examples/kvstore --init --id 1 --replicas 127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002 --listen 127.0.0.1:6380
./target/debug/examples/kvstore --init --id 2 --replicas 127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002 --listen 127.0.0.1:6381
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
primary within a second and keep serving. Start node 0 again, without
`--init`, and it comes back from its disk and rejoins as a backup. Kill all
three and start them again, and they come back with everything they had
committed.

## Starting

A node starts in one of three ways:

- `--init` starts a node of a new cluster. It needs an empty data
  directory.
- With no flag, the node restarts from its data directory. A directory
  with no journal is refused: a node that forgets it ran can forget what
  it acknowledged, and let a view change lose committed operations.
- `--recover --view N` starts a node that lost its data. It needs an empty
  data directory, and recovers the state from the others. `N` must be at
  least every view the lost node could have taken part in: use the highest
  view any other node reports, which each prints as it enters it. A larger
  `N` is safe; the others move to that view.

## On disk

Each node keeps its data in `kvstore-node-N`, or the directory given with
`--data`:

- `journal/` holds the replica's log and counters, in a write-ahead log
  from the `writeahead` crate, see [`journal.rs`](journal.rs). The crate's
  writer thread appends each of the replica's writes and fsyncs once, and
  wakes the event loop when it is done; the loop steps on meanwhile. After
  every batch of events the loop sends the messages that need not wait for
  a write, the `Prepare`s above all, and the replies ready so far; what a
  write held back goes out once it comes back, and the next write, with
  everything the batches since changed, goes out then. A write that stays
  out for two ticks means a stalled disk: the node sends nothing until it
  comes back, so that the others elect a new primary instead of waiting on
  this one. A write is one checksummed record: the log entries that
  changed, and the counters, which say how far the log is compacted and
  from which op number the entries replace what came before. A torn write
  fails the checksum, and recovery drops it whole. A write that changed
  only the commit number is not written. writeahead grows the journal's
  files 256 KiB at a time, with zeros, so that each write lands in blocks
  already allocated and its fsync writes only data: on ext4, an fsync
  that allocates blocks commits the filesystem's journal too, and takes
  nearly twice as long.
- `store/` is a `fjall` database with the keys and values, the client
  table, the number of operations applied, and the number of times the
  node has started, which keeps its client ids apart from those of earlier
  runs. The node keeps what the operations it executes write in memory.
  Once a second, a flush on a thread of its own writes all of it to fjall
  in one batch, with the number of operations it reaches, and fsyncs, so
  fjall always holds the state as of an operation number it records. When
  the flush lands, the replica compacts its log up to that number, less a
  thousand entries kept for replicas a little behind. The replica executes
  an operation only once a journal write holds it, so every operation in
  the store is in the journal, except those of a checkpoint it restored,
  which it persists at once. Journal files with nothing left in them are
  deleted.

On restart the node replays the journal, opens the store, and persists
it: after a process crash the store can hold operations that reached only
the page cache, and the restart may compact the log up to them. Then it
applies the committed entries the store had not persisted. A store behind
the journal's compaction point, as after restoring the wrong backup, is
refused.

## Notes

- Keys and values are single words. One command at a time per connection.
- `RUST_LOG=trace` shows every protocol message, `RUST_LOG=debug` the
  journal's file deletions.
- `cargo test --example kvstore` runs an in-process cluster against real
  journals and stores in the temp directory.
