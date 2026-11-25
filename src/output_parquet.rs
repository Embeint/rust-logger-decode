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
use parquet::file::properties::WriterProperties;
use parquet::schema::types::ColumnPath;
use tdf::TdfOutput;
use tdf::decoders_parquet::{TdfParquetBatchBuilder, TdfParquetRowMeta};

use crate::output_common::{
    OutputKey, merged_output_path, rename_first_file_if_splitting, worker_output_path,
};
use crate::{ProgressReporter, RunArgs, TdfDecoderOutputs};

const DEFAULT_BATCH_ROWS: usize = 65536;

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

struct TdfParquetMergedOutput {
    output_folder: PathBuf,
    output_prefix: String,
    remote_id: Option<u64>,
    tdf_id: u16,
    threshold_rows: Option<usize>,
    output_files: Vec<PathBuf>,
    writer: Option<ArrowWriter<File>>,
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
    ) -> Self {
        Self {
            output_folder,
            output_prefix,
            remote_id,
            tdf_id,
            threshold_rows: match threshold_rows {
                0 => None,
                value => Some(value),
            },
            output_files: Vec::new(),
            writer: None,
            rows_in_file: 0,
            part_idx: 0,
        }
    }

    fn append_batch(&mut self, batch: &RecordBatch) -> io::Result<()> {
        let mut offset = 0;

        while offset < batch.num_rows() {
            if self.writer.is_none()
                || self
                    .threshold_rows
                    .is_some_and(|threshold| self.rows_in_file >= threshold)
            {
                self.start_next_file(batch.schema())?;
            }

            let rows_to_write = match self.threshold_rows {
                Some(threshold) => {
                    let rows_remaining = threshold - self.rows_in_file;
                    rows_remaining.min(batch.num_rows() - offset)
                }
                None => batch.num_rows() - offset,
            };
            let batch = batch.slice(offset, rows_to_write);

            self.writer
                .as_mut()
                .expect("Parquet writer should be open")
                .write(&batch)
                .map_err(to_io_error)?;

            self.rows_in_file += rows_to_write;
            offset += rows_to_write;
        }

        Ok(())
    }

    fn finish(&mut self) -> io::Result<Vec<PathBuf>> {
        self.finish_current_file()?;
        Ok(std::mem::take(&mut self.output_files))
    }

    fn start_next_file(&mut self, schema: SchemaRef) -> io::Result<()> {
        self.finish_current_file()?;
        self.rename_first_file_if_splitting()?;

        let path = self.output_path();
        let file = File::create(path.clone())?;
        let writer =
            ArrowWriter::try_new(file, schema, Some(writer_properties())).map_err(to_io_error)?;

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
        if let Some(mut writer) = self.writer.take() {
            writer.finish().map_err(to_io_error)?;
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
        let file = File::open(&input_path)?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(to_io_error)?
            .with_batch_size(DEFAULT_BATCH_ROWS)
            .build()
            .map_err(to_io_error)?;

        for batch in reader {
            output.append_batch(&batch.map_err(to_io_error)?)?;
        }

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
                        );
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
