//! A replica's persistent state in a write-ahead log from the `writeahead`
//! crate. The kvstore example and the benchmark share it.
//!
//! The log is append-only, so the state is kept as the library's writes,
//! one writeahead record each: a line per entry (`E`), then a line with the
//! counters (`H`), which says how far the log is compacted and from which
//! op number the entries replace what came before. A record is
//! checksummed, and recovery keeps the longest valid prefix of records, so
//! a write is on disk whole or not at all. Files that hold nothing a replay
//! needs are deleted.

use futures::executor::block_on;
use futures::StreamExt;
use std::collections::BTreeMap;
use std::fmt::Write;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};
use vsr_rs::{LogEntry, LogWrite, OpNumber, PersistentState};
use writeahead::{
    FileIo, RecordID, SimpleFile, TrimStats, WriteAhead, WriteAheadOptions, WriteHandle,
};

/// The counters of one write.
#[derive(Clone, Copy, Debug)]
struct Header {
    view_number: usize,
    last_normal_view: usize,
    commit_number: usize,
    log_start: OpNumber,
    entries_from: OpNumber,
    op_number: OpNumber,
    recovering: bool,
}

/// One line of a write, as read back.
enum Record<Op> {
    Entry(OpNumber, LogEntry<Op>),
    Header(Header),
}

/// Encodes an entry as one line of text, and decodes it again.
pub struct EntryCodec<Op> {
    pub encode: fn(&LogEntry<Op>) -> String,
    pub decode: fn(&str) -> Result<LogEntry<Op>, String>,
}

/// The journal of one replica. writeahead's writer thread holds the
/// directory's lock until the journal and every [`Landing`] it returned
/// are dropped; the last of them to go waits for the writer to finish what
/// it was sent.
pub struct Journal<Op> {
    writer: WriteHandle,
    codec: EntryCodec<Op>,
    /// The files holding the retained entries: each maps the first op
    /// number of a run of entries to the file that holds the run, which
    /// lasts until the next run. Later runs were written later, so their
    /// files are no older.
    runs: BTreeMap<OpNumber, u64>,
    /// The file holding the latest write.
    last_file: u64,
    trimmed_before: u64,
    /// The deletion of files a replay no longer needs, while writeahead's
    /// writer thread has yet to answer it, see [`Journal::finish`].
    trimming: Option<Trim>,
}

/// A write on its way to the journal, see [`Journal::submit`].
pub type Landing = Pin<Box<dyn Future<Output = Result<Vec<RecordID>, String>> + Send>>;

/// A deletion of journal files on its way to writeahead's writer thread.
type Trim = Pin<Box<dyn Future<Output = Result<TrimStats, String>> + Send>>;

/// Nanoseconds every journal fsync in this process takes beyond the
/// disk's, see [`set_sync_delay`].
static SYNC_DELAY_NANOS: AtomicU64 = AtomicU64::new(0);

/// Makes every journal fsync in this process take `delay` longer, as on a
/// slower disk. The benchmark sets it to emulate a disk with its data on a
/// tmpfs, where fsync costs nothing; the kvstore leaves it at zero.
#[allow(dead_code)]
pub fn set_sync_delay(delay: Duration) {
    SYNC_DELAY_NANOS.store(delay.as_nanos() as u64, Ordering::Relaxed);
}

/// How far ahead of its records writeahead fills a journal file with
/// zeros, so that each write lands in blocks already allocated. The zeros
/// go to disk with the write that crosses into a new window and slow it,
/// and every replica crosses on the same ops, since their logs match, so a
/// long fill slows a quorum at once; 256 KiB keeps that write within the
/// spread of the others.
pub(crate) const PREALLOCATION: u64 = 256 * 1024;

/// The journal's files: writeahead's own, with each fsync lengthened by
/// the delay [`set_sync_delay`] sets. The delay is spent on the thread
/// that fsyncs, and is a spin, since a sleep this short overshoots by more
/// than it waits.
#[derive(Debug)]
pub struct JournalFile(SimpleFile);

impl FileIo for JournalFile {
    fn open(path: &Path) -> anyhow::Result<Self> {
        SimpleFile::open(path).map(JournalFile)
    }

    fn open_existing(path: &Path) -> anyhow::Result<Self> {
        SimpleFile::open_existing(path).map(JournalFile)
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> anyhow::Result<()> {
        self.0.read_at(offset, buf)
    }

    fn write_at(&mut self, offset: u64, data: &[u8]) -> anyhow::Result<()> {
        self.0.write_at(offset, data)
    }

    fn sync(&mut self) -> anyhow::Result<()> {
        self.0.sync()?;
        let delay = Duration::from_nanos(SYNC_DELAY_NANOS.load(Ordering::Relaxed));
        let synced = Instant::now();
        while synced.elapsed() < delay {
            std::hint::spin_loop();
        }
        Ok(())
    }

    fn len(&self) -> anyhow::Result<u64> {
        self.0.len()
    }

    fn set_len(&mut self, len: u64) -> anyhow::Result<()> {
        self.0.set_len(len)
    }
}

/// A journal and the state it replayed, if it had run before.
pub type Opened<Op> = (Journal<Op>, Option<PersistentState<Op>>);

fn number(word: Option<&str>) -> Result<usize, String> {
    let word = word.ok_or("truncated journal record")?;
    word.parse()
        .map_err(|_| format!("bad number {word:?} in journal"))
}

impl<Op: Clone> Journal<Op> {
    /// Opens the journal in `dir` and replays it. Returns the replica's
    /// state if it has run before.
    pub fn open(
        dir: &Path,
        max_file_size: u64,
        codec: EntryCodec<Op>,
    ) -> Result<Opened<Op>, String> {
        let mut wal = WriteAhead::<JournalFile>::with_options(WriteAheadOptions {
            log_dir: dir.to_path_buf(),
            max_file_size,
            preallocation_chunk_size: Some(PREALLOCATION),
            // A write is one record, which the log must take whole. One
            // that installs a log, after a view change, a recovery, or a
            // checkpoint, can hold every retained entry: under load, more
            // than writeahead's default limit on a batch.
            max_batch_bytes: isize::MAX as usize,
            ..Default::default()
        });
        wal.start()
            .map_err(|err| format!("cannot open journal at {}: {err}", dir.display()))?;
        let writer = wal
            .writer()
            .map_err(|err| format!("cannot write journal: {err}"))?;
        let mut entries: BTreeMap<OpNumber, (LogEntry<Op>, u64)> = BTreeMap::new();
        let mut header: Option<(Header, u64)> = None;
        let mut stream = wal
            .create_stream()
            .map_err(|err| format!("cannot read journal: {err}"))?;
        block_on(async {
            while let Some(item) = stream.next().await {
                let (id, bytes) = item.map_err(|err| format!("cannot read journal: {err}"))?;
                let write = String::from_utf8(bytes)
                    .map_err(|_| "journal record is not UTF-8".to_string())?;
                let mut written = Vec::new();
                let mut counters = None;
                for line in write.lines() {
                    if counters.is_some() {
                        return Err("journal record continues after its counters".to_string());
                    }
                    match Self::decode(&codec, line)? {
                        Record::Entry(op_number, entry) => written.push((op_number, entry)),
                        Record::Header(latest) => counters = Some(latest),
                    }
                }
                let counters = counters.ok_or(
                    "journal record without counters, as an older kvstore wrote them: remove the data directory and start the node with --recover",
                )?;
                entries = entries.split_off(&(counters.log_start + 1));
                entries.split_off(&counters.entries_from);
                for (op_number, entry) in written {
                    entries.insert(op_number, (entry, id.file_id));
                }
                header = Some((counters, id.file_id));
            }
            Ok::<(), String>(())
        })?;
        // The journal only writes from here on, and the manager, which
        // keeps a cache for reads up to date on every write, goes.
        drop(stream);
        drop(wal);
        let mut journal = Journal {
            writer,
            codec,
            runs: BTreeMap::new(),
            last_file: 0,
            trimmed_before: 0,
            trimming: None,
        };
        let Some((header, last_file)) = header else {
            return Ok((journal, None));
        };
        let mut log = Vec::with_capacity(header.op_number - header.log_start);
        for op_number in header.log_start + 1..=header.op_number {
            let (entry, file) = entries
                .remove(&op_number)
                .ok_or_else(|| format!("journal lacks op {op_number}"))?;
            log.push(entry);
            if journal.runs.last_key_value().map(|(_, run)| *run) != Some(file) {
                journal.runs.insert(op_number, file);
            }
        }
        journal.last_file = last_file;
        let state = PersistentState {
            view_number: header.view_number,
            last_normal_view: header.last_normal_view,
            commit_number: header.commit_number,
            log_start: header.log_start,
            log,
            recovering: header.recovering,
        };
        Ok((journal, Some(state)))
    }

    fn decode(codec: &EntryCodec<Op>, line: &str) -> Result<Record<Op>, String> {
        let (kind, rest) = line.split_once(' ').unwrap_or((line, ""));
        let mut words = rest.split_whitespace();
        Ok(match kind {
            "E" => {
                let op_number = number(words.next())?;
                let text = rest
                    .split_once(' ')
                    .map(|(_, text)| text)
                    .ok_or("truncated journal record")?;
                Record::Entry(op_number, (codec.decode)(text)?)
            }
            "H" => Record::Header(Header {
                view_number: number(words.next())?,
                last_normal_view: number(words.next())?,
                commit_number: number(words.next())?,
                log_start: number(words.next())?,
                entries_from: number(words.next())?,
                op_number: number(words.next())?,
                recovering: number(words.next())? != 0,
            }),
            kind => return Err(format!("bad journal record {kind:?}")),
        })
    }

    /// Appends `write` as one record, with one fsync, then has writeahead
    /// delete the files that hold nothing a replay needs any more.
    pub fn append(&mut self, write: &LogWrite<Op>) -> Result<(), String> {
        let ids = block_on(self.submit(write))?;
        self.finish(write, ids)
    }

    /// Starts appending `write` as one record, with one fsync, on
    /// writeahead's writer thread, and returns the future of its landing.
    /// The first poll queues the record, since the journal keeps at most a
    /// write and a trim in writeahead's queue, far below its capacity, and
    /// the future wakes its waker from that thread once the record is
    /// durable. Hand what it yields to `finish` before the next `submit`.
    pub fn submit(&self, write: &LogWrite<Op>) -> Landing {
        let mut record = String::new();
        for (i, entry) in write.entries.iter().enumerate() {
            let op_number = write.entries_from + i;
            let _ = writeln!(record, "E {op_number} {}", (self.codec.encode)(entry));
        }
        let _ = write!(
            record,
            "H {} {} {} {} {} {} {}",
            write.view_number,
            write.last_normal_view,
            write.commit_number,
            write.log_start,
            write.entries_from,
            write.op_number(),
            u8::from(write.recovering)
        );
        let writer = self.writer.clone();
        Box::pin(async move {
            writer
                .write_batch(vec![record.into_bytes()])
                .await
                .map_err(|err| format!("cannot write journal: {err}"))
        })
    }

    /// Records where `write`, which `submit` started, landed, then has
    /// writeahead delete the files that hold nothing a replay needs any
    /// more.
    pub fn finish(&mut self, write: &LogWrite<Op>, ids: Vec<RecordID>) -> Result<(), String> {
        self.last_file = ids.last().map(|id| id.file_id).unwrap_or(self.last_file);
        // The replaced entries no longer pin their files; the new ones pin
        // this one, and the compacted ones leave the first run.
        self.runs.split_off(&write.entries_from);
        if !write.entries.is_empty() {
            self.runs.insert(write.entries_from, self.last_file);
        }
        let first_retained = write.log_start + 1;
        let covering = self
            .runs
            .range(..=first_retained)
            .next_back()
            .map(|(_, file)| *file);
        self.runs = self.runs.split_off(&first_retained);
        if let Some(file) = covering {
            if write.op_number() >= first_retained {
                self.runs.entry(first_retained).or_insert(file);
            }
        }
        // A replay needs the files from the oldest retained entry's on, and
        // the one just written, which holds the counters.
        let oldest = self
            .runs
            .first_key_value()
            .map_or(self.last_file, |(_, file)| (*file).min(self.last_file));
        let trim_out = self.trim_out()?;
        if oldest > self.trimmed_before && !trim_out {
            // Sent, not awaited: the first poll queues it for the writer,
            // which deletes the files once the writes queued ahead of it
            // are done, while the caller goes on; a later `finish` reads
            // the answer. A crash before then leaves files a later trim
            // deletes.
            let writer = self.writer.clone();
            self.trimming = Some(Box::pin(async move {
                writer
                    .trim_before(oldest)
                    .await
                    .map_err(|err| format!("cannot trim journal: {err}"))
            }));
            self.trim_out()?;
            self.trimmed_before = oldest;
        }
        Ok(())
    }

    /// Reads writeahead's answer to the trim that is out, if it has come.
    /// Returns whether the trim is still out.
    fn trim_out(&mut self) -> Result<bool, String> {
        let Some(trim) = &mut self.trimming else {
            return Ok(false);
        };
        let Poll::Ready(stats) = trim.as_mut().poll(&mut Context::from_waker(Waker::noop())) else {
            return Ok(true);
        };
        self.trimming = None;
        let stats = stats?;
        if stats.files_deleted > 0 {
            log::debug!(
                "journal: deleted {} files, {} bytes",
                stats.files_deleted,
                stats.bytes_reclaimed
            );
        }
        Ok(false)
    }
}
