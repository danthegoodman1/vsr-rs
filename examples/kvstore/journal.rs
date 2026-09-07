//! A replica's persistent state in a write-ahead log from the `writeahead`
//! crate. The kvstore example and the benchmark share it.
//!
//! The log is append-only, so the state is kept as records: an entry
//! (`E`), a truncation of the entries from an op number on (`T`), a
//! compaction of the entries up to an op number (`C`), and the counters
//! (`H`). Every batch the owner writes after a step ends with an `H`
//! record, and a replay applies a batch only once it has seen that record:
//! a crash in the middle of a write can leave the first records of a
//! batch on disk, and those describe a state the replica never
//! acknowledged. Files that hold nothing a replay needs are deleted.

use futures::executor::block_on;
use futures::StreamExt;
use std::collections::BTreeMap;
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

/// One record, as read back.
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
    header: Header,
    /// The file holding the latest copy of each retained entry.
    files: BTreeMap<OpNumber, u64>,
    trimmed_before: u64,
    _wal: WriteAhead<SimpleFile>,
}

/// A journal and the state it replayed, if it had run before.
pub type Opened<Op, Output> = (Journal<Op>, Option<PersistentState<Op, Output>>);

fn utf8(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

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
        let mut header: Option<Header> = None;
        let mut pending: Vec<(Record<Op>, u64)> = Vec::new();
        let mut stream = wal
            .create_stream()
            .map_err(|err| format!("cannot read journal: {err}"))?;
        block_on(async {
            while let Some(item) = stream.next().await {
                let (id, bytes) = item.map_err(|err| format!("cannot read journal: {err}"))?;
                let line = utf8(&bytes);
                let record = Self::decode(&codec, &line)?;
                let Record::Header(latest) = record else {
                    pending.push((record, id.file_id));
                    continue;
                };
                // The batch is whole: apply it.
                for (record, file_id) in pending.drain(..) {
                    match record {
                        Record::Entry(op_number, entry) => {
                            entries.insert(op_number, (entry, file_id));
                        }
                        Record::Truncate(from) => {
                            entries.split_off(&from);
                        }
                        Record::Compact(log_start) => {
                            entries = entries.split_off(&(log_start + 1));
                        }
                        Record::Header(_) => unreachable!(),
                    }
                }
                header = Some(latest);
            }
            Ok::<(), String>(())
        })?;
        drop(stream);
        let mut journal = Journal {
            dir: dir.to_path_buf(),
            writer,
            codec,
            header: Header::default(),
            files: BTreeMap::new(),
            trimmed_before: 0,
            _wal: wal,
        };
        let Some(header) = header else {
            return Ok((journal, None));
        };
        let mut log = Vec::with_capacity(header.op_number - header.log_start);
        for op_number in header.log_start + 1..=header.op_number {
            let (entry, file) = entries
                .remove(&op_number)
                .ok_or_else(|| format!("journal lacks op {op_number}"))?;
            log.push(entry);
            journal.files.insert(op_number, file);
        }
        journal.header = header;
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

    /// Writes what the last steps changed, in one batch with one fsync,
    /// then deletes the files that hold nothing a replay needs any more.
    /// Returns whether anything had to be written.
    pub fn persist<SM: StateMachine<Input = Op>>(
        &mut self,
        replica: &mut Replica<SM>,
    ) -> Result<bool, String> {
        let mut records: Vec<Vec<u8>> = Vec::new();
        let mut written: Vec<OpNumber> = Vec::new();
        let log_start = replica.log_start();
        if log_start > self.header.log_start {
            records.push(format!("C {log_start}").into_bytes());
        }
        if let Some(from) = replica.take_log_changes() {
            if from <= self.header.op_number {
                records.push(format!("T {from}").into_bytes());
            }
            let first = from.max(log_start + 1);
            for (i, entry) in replica.log_from(first).iter().enumerate() {
                let op_number = first + i;
                records.push(format!("E {op_number} {}", (self.codec.encode)(entry)).into_bytes());
                written.push(op_number);
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
        if records.is_empty() && header == self.header {
            return Ok(false);
        }
        records.push(
            format!(
                "H {} {} {} {} {} {}",
                header.view_number,
                header.last_normal_view,
                header.commit_number,
                header.log_start,
                header.op_number,
                u8::from(header.recovering)
            )
            .into_bytes(),
        );
        let ids = block_on(self.writer.write_batch(records))
            .map_err(|err| format!("cannot write journal: {err}"))?;
        let file = ids.last().map(|id| id.file_id).unwrap_or(0);
        for op_number in written {
            self.files.insert(op_number, file);
        }
        // Entries compacted or truncated away no longer pin their files.
        self.files = self.files.split_off(&(log_start + 1));
        self.files.split_off(&(header.op_number + 1));
        self.header = header;
        // A replay needs the files from the oldest retained entry's on, and
        // the one just written, which holds the counters.
        let oldest = self.files.values().min().copied().unwrap_or(file).min(file);
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
