use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fs::File;
use std::io::{self, Cursor, Read};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use itertools::Itertools;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tdf::TdfOutput;
use tdf::decoders_parquet::{TdfParquetBatchBuilder, TdfParquetRowMeta};

use crate::output_common::{
    OutputKey, increment_output_count, merged_output_path, rename_first_file_if_splitting,
    touch_output_count, worker_output_path, written,
};
use crate::{ProgressReporter, RunArgs, TdfDecoderOutputs};

const DEFAULT_BATCH_ROWS: usize = 65536;

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
        let writer = ArrowWriter::try_new(file, builder.schema(), None).map_err(to_io_error)?;

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
    outputs: HashMap<(Option<u64>, u16), TdfParquetOutputFile>,
    output_cnt: HashMap<OutputKey, usize>,
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
            outputs: HashMap::new(),
            output_cnt: HashMap::new(),
        }
    }

    pub fn finish(&mut self) -> io::Result<()> {
        for output in self.outputs.values_mut() {
            output.finish(self.batch_rows)?;
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

impl Drop for TdfParquetWriter {
    fn drop(&mut self) {
        let _ = self.finish();
    }
}

impl TdfOutput for TdfParquetWriter {
    fn output_path(&self, remote_id: Option<u64>, tdf_id: u16) -> Option<PathBuf> {
        self.outputs
            .get(&(remote_id, tdf_id))
            .map(|output| output.path.clone())
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

        let output = match self.outputs.entry((remote_id, tdf_id)) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                let output = Self::create_output(
                    self.decoder_idx,
                    &self.output_folder,
                    &self.output_prefix,
                    self.batch_rows,
                    remote_id,
                    tdf_id,
                )?;

                touch_output_count(&mut self.output_cnt, (remote_id, tdf_id));
                entry.insert(output)
            }
        };

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

        output.append(meta, size, cursor)?;

        if output.builder.rows() >= self.batch_rows {
            output.flush_batch(self.batch_rows)?;
        }

        increment_output_count(&mut self.output_cnt, (remote_id, tdf_id));

        Ok(())
    }

    fn iter_written(&self) -> impl Iterator<Item = (&(Option<u64>, u16), &usize)> {
        self.output_cnt.iter()
    }

    fn written(&self, remote_id: Option<u64>, tdf_id: u16) -> usize {
        written(&self.output_cnt, (remote_id, tdf_id))
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
        let writer = ArrowWriter::try_new(file, schema, None).map_err(to_io_error)?;

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

pub fn merge_with_threshold<T: ProgressReporter>(
    args: &mut RunArgs<T>,
    output_files: &mut Vec<PathBuf>,
    stats_tdf: &Arc<Mutex<HashMap<(Option<u64>, u16), HashMap<usize, TdfDecoderOutputs>>>>,
    threshold_rows: usize,
) -> io::Result<()> {
    let results = stats_tdf.lock().unwrap();
    let num_files: usize = results.values().map(|inner| inner.len()).sum();

    args.merge_reporter.start("Merging output files", num_files);

    for ((remote_id, tdf_id), worker_outputs) in results.iter() {
        let mut output = TdfParquetMergedOutput::new(
            args.output_folder.clone(),
            args.output_prefix.clone(),
            *remote_id,
            *tdf_id,
            threshold_rows,
        );

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
            args.merge_reporter.increment(1);
        }

        output_files.extend(output.finish()?);
    }

    args.merge_reporter.stop();

    Ok(())
}

fn to_io_error<E: std::error::Error + Send + Sync + 'static>(err: E) -> io::Error {
    io::Error::new(io::ErrorKind::Other, err)
}
