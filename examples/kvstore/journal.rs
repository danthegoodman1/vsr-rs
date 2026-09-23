//! A replica's persistent state in a write-ahead log from the `writeahead`
//! crate. The kvstore example and the benchmark share it.
//!
//! The log is append-only, so the state is kept as batches, one
//! writeahead record each: what the last steps changed, as one line per
//! change. A line is an entry (`E`), a truncation of the entries from an op
//! number on (`T`), a compaction of the entries up to an op number (`C`),
//! or the counters (`H`), which end every batch. A record is checksummed,
//! and recovery keeps the longest valid prefix of records, so a batch is on
//! disk whole or not at all. A step that changed nothing but the commit
//! number is not written: the next batch's counters carry it, and a restart
//! takes the commit number from the state machine when the log is behind
//! it. Files that hold nothing a replay needs are deleted.

use futures::executor::block_on;
use futures::StreamExt;
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use vsr_rs::{LogEntry, OpNumber, PersistentState, Replica, StateMachine};
use writeahead::{SimpleFile, WriteAhead, WriteAheadOptions, WriteHandle};

/// The replica's counters, as last written to the journal.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Header {
    view_number: usize,
    last_normal_view: usize,
    commit_number: usize,
    log_start: OpNumber,
    op_number: OpNumber,
    recovering: bool,
}

/// One line of a batch, as read back.
enum Record<Op> {
    Entry(OpNumber, LogEntry<Op>),
    Truncate(OpNumber),
    Compact(OpNumber),
    Header(Header),
}

/// Encodes an entry as one line of text, and decodes it again.
pub struct EntryCodec<Op> {
    pub encode: fn(&LogEntry<Op>) -> String,
    pub decode: fn(&str) -> Result<LogEntry<Op>, String>,
}

pub struct Journal<Op> {
    dir: PathBuf,
    writer: WriteHandle,
    codec: EntryCodec<Op>,
    /// The counters as last written, or `None` before the first batch.
    header: Option<Header>,
    /// The files holding the retained entries: each maps the first op
    /// number of a run of entries to the file that holds the run, which
    /// lasts until the next run. Later runs were written later, so their
    /// files are no older.
    runs: BTreeMap<OpNumber, u64>,
    /// The file holding the latest batch.
    last_file: u64,
    trimmed_before: u64,
    _wal: WriteAhead<SimpleFile>,
}

/// A journal and the state it replayed, if it had run before.
pub type Opened<Op, Output> = (Journal<Op>, Option<PersistentState<Op, Output>>);

fn number(word: Option<&str>) -> Result<usize, String> {
    let word = word.ok_or("truncated journal record")?;
    word.parse()
        .map_err(|_| format!("bad number {word:?} in journal"))
}

impl<Op: Clone> Journal<Op> {
    /// Opens the journal in `dir` and replays it. Returns the replica's
    /// state if it has run before, without the client table, which the
    /// state machine keeps.
    pub fn open<Output: Clone>(
        dir: &Path,
        max_file_size: u64,
        codec: EntryCodec<Op>,
    ) -> Result<Opened<Op, Output>, String> {
        let mut wal = WriteAhead::<SimpleFile>::with_options(WriteAheadOptions {
            log_dir: dir.to_path_buf(),
            max_file_size,
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
                let batch = String::from_utf8(bytes)
                    .map_err(|_| "journal batch is not UTF-8".to_string())?;
                let mut latest = None;
                for line in batch.lines() {
                    if latest.is_some() {
                        return Err("journal batch continues after its counters".to_string());
                    }
                    match Self::decode(&codec, line)? {
                        Record::Entry(op_number, entry) => {
                            entries.insert(op_number, (entry, id.file_id));
                        }
                        Record::Truncate(from) => {
                            entries.split_off(&from);
                        }
                        Record::Compact(log_start) => {
                            entries = entries.split_off(&(log_start + 1));
                        }
                        Record::Header(counters) => latest = Some(counters),
                    }
                }
                let latest = latest.ok_or(
                    "journal batch without counters, as an older kvstore wrote them: remove the data directory and start the node with --recover",
                )?;
                header = Some((latest, id.file_id));
            }
            Ok::<(), String>(())
        })?;
        drop(stream);
        let mut journal = Journal {
            dir: dir.to_path_buf(),
            writer,
            codec,
            header: None,
            runs: BTreeMap::new(),
            last_file: 0,
            trimmed_before: 0,
            _wal: wal,
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
        journal.header = Some(header);
        journal.last_file = last_file;
        let state = PersistentState {
            view_number: header.view_number,
            last_normal_view: header.last_normal_view,
            commit_number: header.commit_number,
            log_start: header.log_start,
            log,
            client_table: Vec::new(),
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
            "T" => Record::Truncate(number(words.next())?),
            "C" => Record::Compact(number(words.next())?),
            "H" => Record::Header(Header {
                view_number: number(words.next())?,
                last_normal_view: number(words.next())?,
                commit_number: number(words.next())?,
                log_start: number(words.next())?,
                op_number: number(words.next())?,
                recovering: number(words.next())? != 0,
            }),
            kind => return Err(format!("bad journal record {kind:?}")),
        })
    }

    /// Writes what the last steps changed as one batch, with one fsync,
    /// then deletes the files that hold nothing a replay needs any more.
    /// The first call always writes, so that a journal that exists holds
    /// the replica's counters. Returns whether anything had to be written.
    pub fn persist<SM: StateMachine<Input = Op>>(
        &mut self,
        replica: &mut Replica<SM>,
    ) -> Result<bool, String> {
        let written = self.header.unwrap_or_default();
        let mut batch = String::new();
        let log_start = replica.log_start();
        if log_start > written.log_start {
            let _ = writeln!(batch, "C {log_start}");
        }
        let mut first_written = None;
        if let Some(from) = replica.take_log_changes() {
            if from <= written.op_number {
                let _ = writeln!(batch, "T {from}");
            }
            let first = from.max(log_start + 1);
            for (i, entry) in replica.log_from(first).iter().enumerate() {
                let op_number = first + i;
                let _ = writeln!(batch, "E {op_number} {}", (self.codec.encode)(entry));
                first_written.get_or_insert(op_number);
            }
        }
        let header = Header {
            view_number: replica.view_number(),
            last_normal_view: replica.last_normal_view(),
            commit_number: replica.commit_number(),
            log_start,
            op_number: replica.op_number(),
            recovering: replica.is_recovering(),
        };
        let commit_only = Header {
            commit_number: written.commit_number,
            ..header
        } == written;
        if self.header.is_some() && batch.is_empty() && commit_only {
            // Only the commit number moved, which needs no write before
            // anything the step produced is delivered.
            self.header = Some(header);
            return Ok(false);
        }
        let _ = write!(
            batch,
            "H {} {} {} {} {} {}",
            header.view_number,
            header.last_normal_view,
            header.commit_number,
            header.log_start,
            header.op_number,
            u8::from(header.recovering)
        );
        let ids = block_on(self.writer.write_batch(vec![batch.into_bytes()]))
            .map_err(|err| format!("cannot write journal: {err}"))?;
        self.last_file = ids.last().map(|id| id.file_id).unwrap_or(self.last_file);
        // Entries truncated away no longer pin their files, and neither do
        // those the new ones replace; compacted ones leave the first run.
        self.runs
            .split_off(&(first_written.unwrap_or(header.op_number + 1)));
        if let Some(first) = first_written {
            self.runs.insert(first, self.last_file);
        }
        // The run that holds the first retained entry starts there now.
        let first_retained = log_start + 1;
        let covering = self
            .runs
            .range(..=first_retained)
            .next_back()
            .map(|(_, file)| *file);
        self.runs = self.runs.split_off(&first_retained);
        if let Some(file) = covering {
            if header.op_number >= first_retained {
                self.runs.entry(first_retained).or_insert(file);
            }
        }
        self.header = Some(header);
        // A replay needs the files from the oldest retained entry's on, and
        // the one just written, which holds the counters.
        let oldest = self
            .runs
            .first_key_value()
            .map_or(self.last_file, |(_, file)| (*file).min(self.last_file));
        if oldest > self.trimmed_before {
            let stats = block_on(self.writer.trim_before(oldest))
                .map_err(|err| format!("cannot trim journal: {err}"))?;
            if stats.files_deleted > 0 {
                log::debug!(
                    "journal {}: deleted {} files, {} bytes",
                    self.dir.display(),
                    stats.files_deleted,
                    stats.bytes_reclaimed
                );
            }
            self.trimmed_before = oldest;
        }
        Ok(true)
    }
}
