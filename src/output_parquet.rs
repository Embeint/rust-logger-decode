use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Cursor, Read};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use itertools::Itertools;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::arrow_writer::{ArrowColumnWriter, ArrowRowGroupWriterFactory, compute_leaves};
use parquet::column::writer::ColumnCloseResult;
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader};
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::types::ColumnPath;
use tdf::TdfOutput;
use tdf::decoders_parquet::{TdfParquetBatchBuilder, TdfParquetRowMeta};

use crate::output_common::{
    OutputKey, merged_output_path, rename_first_file_if_splitting, worker_output_path,
};
use crate::{ProgressReporter, RunArgs, TdfDecoderOutputs};

const DEFAULT_BATCH_ROWS: usize = 65536;

/// Maximum number of rows in a row group encoded while merging
/// (matches the `ArrowWriter` default)
const MERGE_ROW_GROUP_ROWS: usize = 1024 * 1024;

/// Row groups at least this large are copied without re-encoding while merging.
/// Smaller row groups are re-encoded so they can be combined.
const MERGE_COPY_MIN_ROWS: usize = DEFAULT_BATCH_ROWS;

/// Properties for all Parquet files written
fn writer_properties() -> WriterProperties {
    WriterProperties::builder()
        // Timestamps are unique per row, so dictionary encoding only adds
        // overhead before falling back to plain encoding.
        .set_column_dictionary_enabled(ColumnPath::from("timestamp"), false)
        .build()
}

struct TdfParquetOutputFile {
    path: PathBuf,
    tdf_id: u16,
    builder: TdfParquetBatchBuilder,
    writer: ArrowWriter<File>,
    finished: bool,
}

impl TdfParquetOutputFile {
    fn new(path: PathBuf, tdf_id: u16, batch_rows: usize) -> io::Result<Self> {
        let builder = tdf::decoders_parquet::tdf_parquet_builder(tdf_id, batch_rows)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "Unknown TDF ID"))?;
        let file = File::create(path.clone())?;
        let writer = ArrowWriter::try_new(file, builder.schema(), Some(writer_properties()))
            .map_err(to_io_error)?;

        Ok(Self {
            path,
            tdf_id,
            builder,
            writer,
            finished: false,
        })
    }

    fn append(
        &mut self,
        meta: TdfParquetRowMeta,
        size: u8,
        cursor: &mut Cursor<&[u8]>,
    ) -> io::Result<()> {
        self.builder.append(meta, size, cursor)?;
        Ok(())
    }

    fn flush_batch(&mut self, batch_rows: usize) -> io::Result<()> {
        if self.builder.rows() == 0 {
            return Ok(());
        }

        let batch = self.builder.finish_batch().map_err(to_io_error)?;
        self.writer.write(&batch).map_err(to_io_error)?;
        self.builder = tdf::decoders_parquet::tdf_parquet_builder(self.tdf_id, batch_rows)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "Unknown TDF ID"))?;
        Ok(())
    }

    fn finish(&mut self, batch_rows: usize) -> io::Result<()> {
        if self.finished {
            return Ok(());
        }

        self.flush_batch(batch_rows)?;
        self.writer.finish().map_err(to_io_error)?;
        self.finished = true;
        Ok(())
    }
}

impl Drop for TdfParquetOutputFile {
    fn drop(&mut self) {
        let _ = self.finish(DEFAULT_BATCH_ROWS);
    }
}

pub struct TdfParquetWriter {
    decoder_idx: usize,
    output_folder: PathBuf,
    output_prefix: String,
    batch_rows: usize,
    /// Output files, in the order they were created
    outputs: Vec<ParquetWorkerOutput>,
    /// Index into `outputs` for each key
    output_idx: HashMap<OutputKey, usize>,
    /// Most recently written output, consecutive samples usually share it
    last_output: Option<(OutputKey, usize)>,
}

/// A single worker output file
struct ParquetWorkerOutput {
    key: OutputKey,
    file: TdfParquetOutputFile,
    /// Number of readings written
    count: usize,
}

impl TdfParquetWriter {
    pub fn new(decoder_idx: usize, output_folder: PathBuf, output_prefix: String) -> Self {
        Self::new_with_batch_rows(
            decoder_idx,
            output_folder,
            output_prefix,
            DEFAULT_BATCH_ROWS,
        )
    }

    pub fn new_with_batch_rows(
        decoder_idx: usize,
        output_folder: PathBuf,
        output_prefix: String,
        batch_rows: usize,
    ) -> Self {
        Self {
            decoder_idx,
            output_folder,
            output_prefix,
            batch_rows: batch_rows.max(1),
            outputs: Vec::new(),
            output_idx: HashMap::new(),
            last_output: None,
        }
    }

    pub fn finish(&mut self) -> io::Result<()> {
        for output in self.outputs.iter_mut() {
            output.file.finish(self.batch_rows)?;
        }
        Ok(())
    }

    fn create_output(
        decoder_idx: usize,
        output_folder: &std::path::Path,
        output_prefix: &str,
        batch_rows: usize,
        remote_id: Option<u64>,
        tdf_id: u16,
    ) -> io::Result<TdfParquetOutputFile> {
        let path = worker_output_path(
            output_folder,
            output_prefix,
            remote_id,
            tdf_id,
            decoder_idx,
            "parquet",
        );

        TdfParquetOutputFile::new(path, tdf_id, batch_rows)
    }
}

impl TdfParquetWriter {
    /// Index of the output for `key`, creating the output if it doesn't exist
    fn output_index(&mut self, key: OutputKey) -> io::Result<usize> {
        // Fast path, avoids hashing for consecutive samples of the same TDF
        if let Some((last_key, idx)) = self.last_output {
            if last_key == key {
                return Ok(idx);
            }
        }

        let idx = match self.output_idx.get(&key) {
            Some(&idx) => idx,
            None => {
                let (remote_id, tdf_id) = key;
                let file = Self::create_output(
                    self.decoder_idx,
                    &self.output_folder,
                    &self.output_prefix,
                    self.batch_rows,
                    remote_id,
                    tdf_id,
                )?;

                // Output is tracked (with a zero count) even if decoding fails
                self.outputs.push(ParquetWorkerOutput {
                    key,
                    file,
                    count: 0,
                });
                self.output_idx.insert(key, self.outputs.len() - 1);
                self.outputs.len() - 1
            }
        };
        self.last_output = Some((key, idx));
        Ok(idx)
    }
}

impl Drop for TdfParquetWriter {
    fn drop(&mut self) {
        let _ = self.finish();
    }
}

impl TdfOutput for TdfParquetWriter {
    fn output_path(&self, remote_id: Option<u64>, tdf_id: u16) -> Option<PathBuf> {
        self.output_idx
            .get(&(remote_id, tdf_id))
            .map(|&idx| self.outputs[idx].file.path.clone())
    }

    fn write(
        &mut self,
        remote_id: Option<u64>,
        tdf_id: u16,
        tdf_time: i64,
        tdf_idx: Option<u16>,
        size: u8,
        cursor: &mut Cursor<&[u8]>,
    ) -> io::Result<()> {
        if !tdf::decoders_parquet::tdf_parquet_has_schema(tdf_id) {
            let mut buf = vec![0; size as usize];
            cursor.read_exact(&mut buf)?;
            return Ok(());
        }

        let output_idx = self.output_index((remote_id, tdf_id))?;

        let meta = match tdf_idx {
            Some(idx) => TdfParquetRowMeta {
                time_unix_micros: None,
                sample_idx: Some(idx),
            },
            None => TdfParquetRowMeta {
                time_unix_micros: Some(tdf::time::tdf_time_to_unix_micros(tdf_time)),
                sample_idx: None,
            },
        };

        let output = &mut self.outputs[output_idx];
        output.file.append(meta, size, cursor)?;

        if output.file.builder.rows() >= self.batch_rows {
            output.file.flush_batch(self.batch_rows)?;
        }

        output.count += 1;

        Ok(())
    }

    fn iter_written(&self) -> impl Iterator<Item = (&(Option<u64>, u16), &usize)> {
        self.outputs
            .iter()
            .map(|output| (&output.key, &output.count))
    }

    fn written(&self, remote_id: Option<u64>, tdf_id: u16) -> usize {
        self.output_idx
            .get(&(remote_id, tdf_id))
            .map_or(0, |&idx| self.outputs[idx].count)
    }
}

/// A merged Parquet output file, built from a mix of row groups copied
/// unchanged from worker files and row groups encoded from record batches.
struct MergedParquetFile {
    writer: SerializedFileWriter<File>,
    factory: ArrowRowGroupWriterFactory,
    schema: SchemaRef,
    /// Column writers for the row group currently being encoded
    pending: Option<Vec<ArrowColumnWriter>>,
    pending_rows: usize,
    num_row_groups: usize,
}

impl MergedParquetFile {
    fn new(path: &PathBuf, schema: SchemaRef) -> io::Result<Self> {
        let file = File::create(path)?;
        // Construct through `ArrowWriter` so the file has the same schema,
        // properties and embedded Arrow schema as the worker files.
        let (writer, factory) =
            ArrowWriter::try_new(file, schema.clone(), Some(writer_properties()))
                .and_then(|w| w.into_serialized_writer())
                .map_err(to_io_error)?;

        Ok(Self {
            writer,
            factory,
            schema,
            pending: None,
            pending_rows: 0,
            num_row_groups: 0,
        })
    }

    /// Encode a record batch, starting a new row group as required
    fn write_batch(&mut self, batch: &RecordBatch) -> io::Result<()> {
        let mut offset = 0;
        while offset < batch.num_rows() {
            let rows = (MERGE_ROW_GROUP_ROWS - self.pending_rows).min(batch.num_rows() - offset);
            let slice = batch.slice(offset, rows);

            if self.pending.is_none() {
                self.pending = Some(
                    self.factory
                        .create_column_writers(self.num_row_groups)
                        .map_err(to_io_error)?,
                );
            }
            let writers = self.pending.as_mut().unwrap();
            let mut writer_iter = writers.iter_mut();
            for (field, column) in self.schema.fields().iter().zip(slice.columns()) {
                for leaf in compute_leaves(field, column).map_err(to_io_error)? {
                    writer_iter
                        .next()
                        .expect("Column writer per leaf")
                        .write(&leaf)
                        .map_err(to_io_error)?;
                }
            }

            self.pending_rows += rows;
            offset += rows;
            if self.pending_rows >= MERGE_ROW_GROUP_ROWS {
                self.flush_pending()?;
            }
        }
        Ok(())
    }

    /// Write out the row group currently being encoded
    fn flush_pending(&mut self) -> io::Result<()> {
        if let Some(writers) = self.pending.take() {
            let mut row_group = self.writer.next_row_group().map_err(to_io_error)?;
            for writer in writers {
                writer
                    .close()
                    .and_then(|chunk| chunk.append_to_row_group(&mut row_group))
                    .map_err(to_io_error)?;
            }
            row_group.close().map_err(to_io_error)?;
            self.num_row_groups += 1;
            self.pending_rows = 0;
        }
        Ok(())
    }

    /// Copy a row group from another file without decoding it
    fn copy_row_group(
        &mut self,
        input: &File,
        metadata: &ParquetMetaData,
        rg_idx: usize,
    ) -> io::Result<()> {
        // Preserve row order with respect to any buffered rows
        self.flush_pending()?;

        let rg = metadata.row_group(rg_idx);
        let column_indexes = metadata.column_index().and_then(|ci| ci.get(rg_idx));
        let offset_indexes = metadata.offset_index().and_then(|oi| oi.get(rg_idx));

        let mut row_group = self.writer.next_row_group().map_err(to_io_error)?;
        for (col_idx, column) in rg.columns().iter().enumerate() {
            let close = ColumnCloseResult {
                bytes_written: column.compressed_size() as u64,
                rows_written: rg.num_rows() as u64,
                metadata: column.clone(),
                bloom_filter: None,
                column_index: column_indexes.and_then(|row| row.get(col_idx)).cloned(),
                offset_index: offset_indexes.and_then(|row| row.get(col_idx)).cloned(),
            };
            row_group.append_column(input, close).map_err(to_io_error)?;
        }
        row_group.close().map_err(to_io_error)?;
        self.num_row_groups += 1;
        Ok(())
    }

    fn close(mut self) -> io::Result<()> {
        self.flush_pending()?;
        self.writer.close().map_err(to_io_error)?;
        Ok(())
    }
}

struct TdfParquetMergedOutput {
    output_folder: PathBuf,
    output_prefix: String,
    remote_id: Option<u64>,
    tdf_id: u16,
    schema: SchemaRef,
    threshold_rows: Option<usize>,
    output_files: Vec<PathBuf>,
    writer: Option<MergedParquetFile>,
    rows_in_file: usize,
    part_idx: usize,
}

impl TdfParquetMergedOutput {
    fn new(
        output_folder: PathBuf,
        output_prefix: String,
        remote_id: Option<u64>,
        tdf_id: u16,
        threshold_rows: usize,
    ) -> io::Result<Self> {
        let schema = tdf::decoders_parquet::tdf_parquet_schema(tdf_id)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "Unknown TDF ID"))?;

        Ok(Self {
            output_folder,
            output_prefix,
            remote_id,
            tdf_id,
            schema,
            threshold_rows: match threshold_rows {
                0 => None,
                value => Some(value),
            },
            output_files: Vec::new(),
            writer: None,
            rows_in_file: 0,
            part_idx: 0,
        })
    }

    /// Start a new output file if none is open or the current one is full
    fn ensure_writer(&mut self) -> io::Result<()> {
        if self.writer.is_none()
            || self
                .threshold_rows
                .is_some_and(|threshold| self.rows_in_file >= threshold)
        {
            self.start_next_file()?;
        }
        Ok(())
    }

    /// Rows that can still be written to the current output file
    fn rows_remaining(&self) -> usize {
        self.threshold_rows
            .map_or(usize::MAX, |threshold| threshold - self.rows_in_file)
    }

    /// Append all rows of a worker output file
    fn append_file(&mut self, path: &PathBuf) -> io::Result<()> {
        let file = File::open(path)?;
        let metadata = ParquetMetaDataReader::new()
            .with_page_index_policy(PageIndexPolicy::Optional)
            .parse_and_finish(&file)
            .map_err(to_io_error)?;

        for rg_idx in 0..metadata.num_row_groups() {
            let rows = metadata.row_group(rg_idx).num_rows() as usize;
            if rows == 0 {
                continue;
            }

            self.ensure_writer()?;
            if rows >= MERGE_COPY_MIN_ROWS && rows <= self.rows_remaining() {
                // Large row group that fits in the current file, copy it unchanged
                self.writer
                    .as_mut()
                    .expect("Parquet writer should be open")
                    .copy_row_group(&file, &metadata, rg_idx)?;
                self.rows_in_file += rows;
            } else {
                // Small, or needs splitting across files, re-encode it
                let reader = ParquetRecordBatchReaderBuilder::try_new(file.try_clone()?)
                    .map_err(to_io_error)?
                    .with_row_groups(vec![rg_idx])
                    .with_batch_size(DEFAULT_BATCH_ROWS)
                    .build()
                    .map_err(to_io_error)?;
                for batch in reader {
                    self.append_batch(&batch.map_err(to_io_error)?)?;
                }
            }
        }
        Ok(())
    }

    fn append_batch(&mut self, batch: &RecordBatch) -> io::Result<()> {
        let mut offset = 0;

        while offset < batch.num_rows() {
            self.ensure_writer()?;

            let rows_to_write = self.rows_remaining().min(batch.num_rows() - offset);
            let batch = batch.slice(offset, rows_to_write);

            self.writer
                .as_mut()
                .expect("Parquet writer should be open")
                .write_batch(&batch)?;

            self.rows_in_file += rows_to_write;
            offset += rows_to_write;
        }

        Ok(())
    }

    fn finish(&mut self) -> io::Result<Vec<PathBuf>> {
        self.finish_current_file()?;
        Ok(std::mem::take(&mut self.output_files))
    }

    fn start_next_file(&mut self) -> io::Result<()> {
        self.finish_current_file()?;
        self.rename_first_file_if_splitting()?;

        let path = self.output_path();
        let writer = MergedParquetFile::new(&path, self.schema.clone())?;

        self.output_files.push(path);
        self.writer = Some(writer);
        self.rows_in_file = 0;
        self.part_idx += 1;

        Ok(())
    }

    fn rename_first_file_if_splitting(&mut self) -> io::Result<()> {
        let plain_path = self.plain_output_path();
        let numbered_path = self.numbered_output_path(0);

        rename_first_file_if_splitting(
            self.part_idx,
            &mut self.output_files,
            plain_path,
            numbered_path,
        )
    }

    fn finish_current_file(&mut self) -> io::Result<()> {
        if let Some(writer) = self.writer.take() {
            writer.close()?;
        }
        Ok(())
    }

    fn output_path(&self) -> PathBuf {
        if self.part_idx == 0 {
            self.plain_output_path()
        } else {
            self.numbered_output_path(self.part_idx)
        }
    }

    fn plain_output_path(&self) -> PathBuf {
        merged_output_path(
            &self.output_folder,
            &self.output_prefix,
            self.remote_id,
            self.tdf_id,
            None,
            "parquet",
        )
    }

    fn numbered_output_path(&self, part_idx: usize) -> PathBuf {
        merged_output_path(
            &self.output_folder,
            &self.output_prefix,
            self.remote_id,
            self.tdf_id,
            Some(part_idx),
            "parquet",
        )
    }
}

/// Merge the per-worker files of a single output, in worker order
fn merge_output<T: ProgressReporter>(
    mut output: TdfParquetMergedOutput,
    worker_outputs: &HashMap<usize, TdfDecoderOutputs>,
    reporter: &mut T,
) -> io::Result<Vec<PathBuf>> {
    for worker in worker_outputs.keys().sorted() {
        let input_path = worker_outputs[worker].output.clone();
        output.append_file(&input_path)?;
        std::fs::remove_file(input_path)?;
        reporter.increment(1);
    }

    output.finish()
}

pub fn merge_with_threshold<T: ProgressReporter + Clone + Send>(
    args: &mut RunArgs<T>,
    output_files: &mut Vec<PathBuf>,
    stats_tdf: &Arc<Mutex<HashMap<(Option<u64>, u16), HashMap<usize, TdfDecoderOutputs>>>>,
    threshold_rows: usize,
) -> io::Result<()> {
    let results = stats_tdf.lock().unwrap();
    let num_files: usize = results.values().map(|inner| inner.len()).sum();
    let jobs: Vec<_> = results.iter().collect();

    // Merge the largest outputs first so they don't end up running alone at the end
    let mut order: Vec<usize> = (0..jobs.len()).collect();
    order.sort_by_key(|&idx| {
        std::cmp::Reverse(jobs[idx].1.values().map(|o| o.num_output).sum::<usize>())
    });

    args.merge_reporter.start("Merging output files", num_files);

    // Each output is independent, merge them in parallel
    let next_job = AtomicUsize::new(0);
    let num_threads = jobs.len().min(num_cpus::get()).max(1);
    let merged: Vec<io::Result<Vec<(usize, Vec<PathBuf>)>>> = thread::scope(|s| {
        let handles: Vec<_> = (0..num_threads)
            .map(|_| {
                let mut reporter = args.merge_reporter.clone();
                let (jobs, order, next_job) = (&jobs, &order, &next_job);
                let (output_folder, output_prefix) = (&args.output_folder, &args.output_prefix);
                s.spawn(move || {
                    let mut done = Vec::new();
                    loop {
                        let job_idx = match order.get(next_job.fetch_add(1, Ordering::Relaxed)) {
                            Some(&idx) => idx,
                            None => break,
                        };
                        let ((remote_id, tdf_id), worker_outputs) = jobs[job_idx];
                        let output = TdfParquetMergedOutput::new(
                            output_folder.clone(),
                            output_prefix.clone(),
                            *remote_id,
                            *tdf_id,
                            threshold_rows,
                        )?;
                        let files = merge_output(output, worker_outputs, &mut reporter)?;
                        done.push((job_idx, files));
                    }
                    Ok(done)
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });

    // Report output files in a deterministic order, independent of thread scheduling
    let mut merged: Vec<(usize, Vec<PathBuf>)> = merged
        .into_iter()
        .collect::<io::Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect();
    merged.sort_by_key(|(job_idx, _)| *job_idx);
    output_files.extend(merged.into_iter().flat_map(|(_, files)| files));

    args.merge_reporter.stop();

    Ok(())
}

fn to_io_error<E: std::error::Error + Send + Sync + 'static>(err: E) -> io::Error {
    io::Error::new(io::ErrorKind::Other, err)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_select::concat::concat_batches;

    struct NoProgress;
    impl ProgressReporter for NoProgress {
        fn start(&mut self, _msg: &'static str, _total: usize) {}
        fn increment(&mut self, _value: usize) {}
        fn stop(&mut self) {}
    }

    const TDF_ID: u16 = 10;

    fn unique_temp_dir(name: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("infuse_decoder_{name}_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// Create one worker output file per entry in `rows`, each a single row group
    fn create_worker_files(dir: &PathBuf, rows: &[usize]) -> HashMap<usize, TdfDecoderOutputs> {
        let mut sample = 0u32;
        let mut outputs = HashMap::new();
        for (worker, &num) in rows.iter().enumerate() {
            let mut writer = TdfParquetWriter::new(worker, dir.clone(), "test".to_string());
            for _ in 0..num {
                let mut data = Vec::new();
                for axis in 0..3u32 {
                    data.extend_from_slice(&((sample * 3 + axis) as i16).to_le_bytes());
                }
                let time = 1_400_000_000i64 << 16 | (sample & 0xFFFF) as i64;
                writer
                    .write(None, TDF_ID, time, None, 6, &mut Cursor::new(&data[..]))
                    .unwrap();
                sample += 1;
            }
            writer.finish().unwrap();
            outputs.insert(
                worker,
                TdfDecoderOutputs {
                    output: writer.output_path(None, TDF_ID).unwrap(),
                    num_output: num,
                },
            );
        }
        outputs
    }

    fn read_file(path: &PathBuf) -> (RecordBatch, Vec<usize>) {
        let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap()).unwrap();
        let row_groups = builder
            .metadata()
            .row_groups()
            .iter()
            .map(|rg| rg.num_rows() as usize)
            .collect();
        let schema = builder.schema().clone();
        let batches: Vec<_> = builder.build().unwrap().map(|b| b.unwrap()).collect();
        (concat_batches(&schema, &batches).unwrap(), row_groups)
    }

    /// Merge worker files with the given threshold, returning the decoded
    /// worker data and each merged file's data and row group sizes
    fn merge(
        name: &str,
        rows: &[usize],
        threshold: usize,
    ) -> (RecordBatch, Vec<(RecordBatch, Vec<usize>)>) {
        let dir = unique_temp_dir(name);
        let workers = create_worker_files(&dir, rows);
        let input: Vec<_> = workers
            .keys()
            .sorted()
            .map(|w| read_file(&workers[w].output).0)
            .collect();
        let input = concat_batches(&input[0].schema(), &input).unwrap();

        let output =
            TdfParquetMergedOutput::new(dir.clone(), "out".into(), None, TDF_ID, threshold)
                .unwrap();
        let files = merge_output(output, &workers, &mut NoProgress).unwrap();
        let merged = files.iter().map(read_file).collect();

        std::fs::remove_dir_all(dir).unwrap();
        (input, merged)
    }

    #[test]
    fn merge_copies_large_row_groups_and_combines_small_ones() {
        let (input, merged) = merge("pq_merge_copy", &[100_000, 1_000, 2_000, 100_000], 0);

        assert_eq!(merged.len(), 1);
        let (data, row_groups) = &merged[0];
        assert_eq!(data, &input);
        // Large row groups are copied as is, small ones combined
        assert_eq!(row_groups, &vec![100_000, 3_000, 100_000]);
    }

    #[test]
    fn merge_splits_row_groups_at_reading_limit() {
        let (input, merged) = merge("pq_merge_split", &[100_000, 1_000, 100_000], 150_000);

        let rows: Vec<usize> = merged.iter().map(|(data, _)| data.num_rows()).collect();
        assert_eq!(rows, vec![150_000, 51_000]);
        // First row group fits and is copied, the last is split across files
        assert_eq!(merged[0].1, vec![100_000, 50_000]);
        assert_eq!(merged[1].1, vec![51_000]);

        let parts: Vec<RecordBatch> = merged.into_iter().map(|(data, _)| data).collect();
        assert_eq!(concat_batches(&input.schema(), &parts).unwrap(), input);
    }
}
