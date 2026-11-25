use chrono::SecondsFormat;
use itertools::Itertools;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Cursor;
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use tdf::TdfOutput;

use crate::output_common::{
    OutputKey, merged_output_path, rename_first_file_if_splitting, worker_output_path,
};
use crate::{ProgressReporter, RunArgs, TdfDecoderOutputs};

/// Read buffer size used when merging worker output files
const MERGE_BUFFER_SIZE: usize = 1 << 20;

pub struct TdfCsvWriter {
    decoder_idx: usize,
    output_folder: std::path::PathBuf,
    output_prefix: String,
    output_unix: bool,
    /// Output files, in the order they were created
    outputs: Vec<CsvWorkerOutput>,
    /// Index into `outputs` for each key
    output_idx: HashMap<OutputKey, usize>,
    /// Most recently written output, consecutive samples usually share it
    last_output: Option<(OutputKey, usize)>,
    /// Reused buffers for constructing CSV lines
    time: Vec<u8>,
    reading: Vec<u8>,
    rfc3339: Rfc3339Formatter,
}

/// A single worker output file
struct CsvWorkerOutput {
    key: OutputKey,
    path: PathBuf,
    writer: BufWriter<File>,
    /// Number of readings written
    count: usize,
}

fn push_int<I: itoa::Integer>(out: &mut Vec<u8>, val: I) {
    let mut buf = itoa::Buffer::new();
    out.extend_from_slice(buf.format(val).as_bytes());
}

/// Append the microseconds of `unix_nano` as exactly 6 zero-padded digits
fn push_micros(out: &mut Vec<u8>, unix_nano: u32) {
    let mut buf = itoa::Buffer::new();
    let micros = buf.format(unix_nano / 1000).as_bytes();
    out.extend_from_slice(&b"000000"[micros.len().min(6)..]);
    out.extend_from_slice(micros);
}

/// RFC 3339 timestamp formatter that caches the date and time up to the
/// whole second, which is shared by consecutive samples.
///
/// Output is identical to `to_rfc3339_opts(SecondsFormat::Micros, true)`.
#[derive(Default)]
struct Rfc3339Formatter {
    /// Unix seconds that `prefix` was generated for
    seconds: Option<i64>,
    /// Formatted timestamp up to and including the whole seconds
    prefix: Vec<u8>,
}

impl Rfc3339Formatter {
    /// Trailing part of a chrono timestamp with zero sub-second component
    const ZERO_SUFFIX: &'static [u8] = b".000000Z";

    fn push(&mut self, out: &mut Vec<u8>, tdf_time: i64) {
        let (unix_seconds, unix_nano) = tdf::time::tdf_time_to_unix(tdf_time);

        if self.seconds != Some(unix_seconds) {
            // Let chrono format the whole second, then strip the fraction
            let datetime =
                tdf::time::tdf_time_to_datetime(tdf_time & !0xFFFF).expect("Invalid time");
            let formatted = datetime.to_rfc3339_opts(SecondsFormat::Micros, true);
            let prefix = formatted
                .as_bytes()
                .strip_suffix(Self::ZERO_SUFFIX)
                .expect("Unexpected RFC 3339 format");

            self.prefix.clear();
            self.prefix.extend_from_slice(prefix);
            self.seconds = Some(unix_seconds);
        }

        out.extend_from_slice(&self.prefix);
        out.push(b'.');
        push_micros(out, unix_nano);
        out.push(b'Z');
    }
}

impl TdfCsvWriter {
    pub fn new(
        decoder_idx: usize,
        output_folder: std::path::PathBuf,
        output_prefix: String,
        output_unix_time: bool,
    ) -> Self {
        Self {
            decoder_idx: decoder_idx,
            output_folder: output_folder,
            output_prefix,
            output_unix: output_unix_time,
            outputs: Vec::new(),
            output_idx: HashMap::new(),
            last_output: None,
            time: Vec::with_capacity(64),
            reading: Vec::with_capacity(256),
            rfc3339: Rfc3339Formatter::default(),
        }
    }

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
                let path = worker_output_path(
                    &self.output_folder,
                    &self.output_prefix,
                    remote_id,
                    tdf_id,
                    self.decoder_idx,
                    "csv",
                );
                let mut writer = BufWriter::new(File::create(path.clone())?);

                // Write header into file
                let heading = tdf::decoders_csv::tdf_fields(&tdf_id).join(",");
                writer.write_all(format!("time,{}\n", heading).as_bytes())?;

                // Output is tracked (with a zero count) even if decoding fails
                self.outputs.push(CsvWorkerOutput {
                    key,
                    path,
                    writer,
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

impl TdfOutput for TdfCsvWriter {
    fn output_path(self: &Self, remote_id: Option<u64>, tdf_id: u16) -> Option<PathBuf> {
        self.output_idx
            .get(&(remote_id, tdf_id))
            .map(|&idx| self.outputs[idx].path.clone())
    }

    fn write(
        &mut self,
        remote_id: Option<u64>,
        tdf_id: u16,
        tdf_time: i64,
        tdf_idx: Option<u16>,
        size: u8,
        cursor: &mut Cursor<&[u8]>,
    ) -> std::io::Result<()> {
        // Create writer if it doesn't exist
        let output_idx = self.output_index((remote_id, tdf_id))?;

        // Decode the reading into the reused row buffer
        let reading = &mut self.reading;
        reading.clear();
        tdf::decoders_csv::tdf_write_csv(&tdf_id, size, cursor, reading)?;
        reading.push(b'\n');

        // Format the time column
        let time = &mut self.time;
        time.clear();
        match tdf_idx {
            // Use the index directly if provided
            Some(idx) => push_int(time, idx),
            None => match self.output_unix {
                // Otherwise, format the time to a string
                true => {
                    let (unix_seconds, unix_nano) = tdf::time::tdf_time_to_unix(tdf_time);
                    push_int(time, unix_seconds);
                    time.push(b'.');
                    push_micros(time, unix_nano);
                }
                false => self.rfc3339.push(time, tdf_time),
            },
        };
        time.push(b',');

        // Write line to output
        let output = &mut self.outputs[output_idx];
        output.writer.write_all(time)?;
        output.writer.write_all(reading)?;

        // Increment output counter
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

struct TdfCsvMergedOutput {
    output_folder: PathBuf,
    output_prefix: String,
    remote_id: Option<u64>,
    tdf_id: u16,
    max_readings_per_file: Option<usize>,
    output_files: Vec<PathBuf>,
    writer: Option<BufWriter<File>>,
    header: Option<String>,
    readings_in_file: usize,
    part_idx: usize,
    /// Last byte written was not a newline
    line_open: bool,
}

impl TdfCsvMergedOutput {
    fn new(
        output_folder: PathBuf,
        output_prefix: String,
        remote_id: Option<u64>,
        tdf_id: u16,
        max_readings_per_file: usize,
    ) -> Self {
        Self {
            output_folder,
            output_prefix,
            remote_id,
            tdf_id,
            max_readings_per_file: match max_readings_per_file {
                0 => None,
                value => Some(value),
            },
            output_files: Vec::new(),
            writer: None,
            header: None,
            readings_in_file: 0,
            part_idx: 0,
            line_open: false,
        }
    }

    fn set_header(&mut self, header: String) -> io::Result<()> {
        if self.header.is_none() {
            self.header = Some(header);
            self.start_next_file()?;
        }
        Ok(())
    }

    /// Append raw CSV data (complete or partial lines) to the output, starting
    /// a new file whenever the per-file reading limit is reached.
    fn append_data(&mut self, mut data: &[u8]) -> io::Result<()> {
        while !data.is_empty() {
            if self.writer.is_none()
                || self
                    .max_readings_per_file
                    .is_some_and(|max_readings| self.readings_in_file >= max_readings)
            {
                self.start_next_file()?;
            }

            // Determine how much of the data can go into the current file.
            // Readings are only counted when files are being split.
            let len = match self.max_readings_per_file {
                None => data.len(),
                Some(max_readings) => {
                    let remaining = max_readings - self.readings_in_file;
                    let mut end = data.len();
                    let mut lines = 0;
                    for pos in memchr::memchr_iter(b'\n', data) {
                        lines += 1;
                        if lines == remaining {
                            end = pos + 1;
                            break;
                        }
                    }
                    self.readings_in_file += lines;
                    end
                }
            };

            self.writer
                .as_mut()
                .expect("CSV writer should be open")
                .write_all(&data[..len])?;
            self.line_open = data[len - 1] != b'\n';
            data = &data[len..];
        }
        Ok(())
    }

    /// Terminate a trailing partial line at the end of an input file
    fn end_input(&mut self) -> io::Result<()> {
        if self.line_open {
            self.append_data(b"\n")?;
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
        let err_path = path.clone();
        let file = File::create(path.clone()).map_err(|e| {
            io::Error::new(
                e.kind(),
                format!(
                    "Failed to create output file '{}': {}",
                    err_path.display(),
                    e
                ),
            )
        })?;
        let mut writer = BufWriter::new(file);

        if let Some(header) = &self.header {
            writer.write_all(header.as_bytes())?;
            writer.write_all(b"\n")?;
            self.readings_in_file = 0;
        } else {
            self.readings_in_file = 0;
        }

        self.output_files.push(path);
        self.writer = Some(writer);
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
            writer.flush()?;
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
            "csv",
        )
    }

    fn numbered_output_path(&self, part_idx: usize) -> PathBuf {
        merged_output_path(
            &self.output_folder,
            &self.output_prefix,
            self.remote_id,
            self.tdf_id,
            Some(part_idx),
            "csv",
        )
    }
}

/// Merge the per-worker files of a single output, in worker order
fn merge_output<T: ProgressReporter>(
    mut output: TdfCsvMergedOutput,
    worker_outputs: &HashMap<usize, TdfDecoderOutputs>,
    reporter: &mut T,
) -> io::Result<Vec<PathBuf>> {
    for worker in worker_outputs.keys().sorted() {
        let input_path = worker_outputs[worker].output.clone();
        let mut input = BufReader::with_capacity(MERGE_BUFFER_SIZE, File::open(&input_path)?);

        // First line is the CSV header
        let mut header = Vec::new();
        if input.read_until(b'\n', &mut header)? > 0 {
            if header.last() == Some(&b'\n') {
                header.pop();
                if header.last() == Some(&b'\r') {
                    header.pop();
                }
            }
            let header = String::from_utf8(header)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            output.set_header(header)?;
        }

        // Remaining data is copied without parsing individual lines
        loop {
            let chunk = input.fill_buf()?;
            if chunk.is_empty() {
                break;
            }
            let len = chunk.len();
            output.append_data(chunk)?;
            input.consume(len);
        }
        output.end_input()?;
        drop(input);

        fs::remove_file(input_path)?;

        reporter.increment(1);
    }
    output.finish()
}

pub fn merge<T: ProgressReporter + Clone + Send>(
    args: &mut RunArgs<T>,
    output_files: &mut Vec<PathBuf>,
    stats_tdf: &Arc<Mutex<HashMap<(Option<u64>, u16), HashMap<usize, TdfDecoderOutputs>>>>,
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
                let (output_folder, output_prefix, max_readings) = (
                    &args.output_folder,
                    &args.output_prefix,
                    args.max_readings_per_output_file,
                );
                s.spawn(move || {
                    let mut done = Vec::new();
                    loop {
                        let job_idx = match order.get(next_job.fetch_add(1, Ordering::Relaxed)) {
                            Some(&idx) => idx,
                            None => break,
                        };
                        let ((remote_id, tdf_id), worker_outputs) = jobs[job_idx];
                        let output = TdfCsvMergedOutput::new(
                            output_folder.clone(),
                            output_prefix.clone(),
                            *remote_id,
                            *tdf_id,
                            max_readings,
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    fn unique_temp_dir(name: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("infuse_decoder_{name}_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn zero_max_readings_keeps_csv_output_in_one_file() {
        let output_dir = unique_temp_dir("zero_max_readings_csv");
        let mut output = TdfCsvMergedOutput::new(output_dir.clone(), "out".to_string(), None, 1, 0);

        output.set_header("time,value".to_string()).unwrap();
        output.append_data(b"1,10\n2,20\n3,30\n").unwrap();
        output.end_input().unwrap();

        let files = output.finish().unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(
            files[0],
            output_dir.join(format!("out_{}.csv", tdf::decoders::tdf_name(&1)))
        );

        let mut contents = String::new();
        File::open(&files[0])
            .unwrap()
            .read_to_string(&mut contents)
            .unwrap();
        assert_eq!(contents, "time,value\n1,10\n2,20\n3,30\n");

        fs::remove_dir_all(output_dir).unwrap();
    }

    #[test]
    fn rfc3339_formatter_matches_chrono() {
        let reference = |tdf_time: i64| {
            tdf::time::tdf_time_to_datetime(tdf_time)
                .unwrap()
                .to_rfc3339_opts(SecondsFormat::Micros, true)
        };

        let mut times = Vec::new();
        // Samples at 100 Hz crossing second boundaries, then jumping backwards
        let base: i64 = 1_400_000_000 << 16;
        times.extend((0..500).map(|i| base + i * 655));
        times.extend((0..10).map(|i| base - i * 30_000));
        // Every sub-second edge value within a single second
        times.extend([0, 1, 0x0041, 0x0042, 0x7FFF, 0x8000, 0xFFFE, 0xFFFF].map(|f| base + f));
        // Pseudo-random times over +-17000 years, covering negative and 5 digit years
        let mut state: u64 = 0x9E3779B97F4A7C15;
        for _ in 0..100_000 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            times.push((state as i64) >> 8);
        }

        let mut formatter = Rfc3339Formatter::default();
        let mut out = Vec::new();
        for tdf_time in times {
            out.clear();
            formatter.push(&mut out, tdf_time);
            assert_eq!(
                std::str::from_utf8(&out).unwrap(),
                reference(tdf_time),
                "tdf_time {tdf_time:#x}"
            );
        }
    }

    fn read_files(files: &[PathBuf]) -> Vec<String> {
        files
            .iter()
            .map(|f| {
                let mut contents = String::new();
                File::open(f)
                    .unwrap()
                    .read_to_string(&mut contents)
                    .unwrap();
                contents
            })
            .collect()
    }

    #[test]
    fn max_readings_splits_on_line_boundaries_across_chunks() {
        let output_dir = unique_temp_dir("split_chunks_csv");
        let mut output = TdfCsvMergedOutput::new(output_dir.clone(), "out".to_string(), None, 1, 2);

        output.set_header("time,value".to_string()).unwrap();
        // Chunk boundaries fall in the middle of lines
        output.append_data(b"1,10\n2,").unwrap();
        output.append_data(b"20\n3,30\n4").unwrap();
        output.append_data(b",40\n5,50").unwrap();
        output.end_input().unwrap();
        // Second input file
        output.append_data(b"6,60\n").unwrap();
        output.end_input().unwrap();

        let files = output.finish().unwrap();
        assert_eq!(
            read_files(&files),
            vec![
                "time,value\n1,10\n2,20\n",
                "time,value\n3,30\n4,40\n",
                "time,value\n5,50\n6,60\n",
            ]
        );
        assert_eq!(
            files[0],
            output_dir.join(format!("out_{}_00000.csv", tdf::decoders::tdf_name(&1)))
        );

        fs::remove_dir_all(output_dir).unwrap();
    }

    #[test]
    fn max_readings_does_not_create_empty_trailing_file() {
        let output_dir = unique_temp_dir("split_exact_csv");
        let mut output = TdfCsvMergedOutput::new(output_dir.clone(), "out".to_string(), None, 1, 2);

        output.set_header("time,value".to_string()).unwrap();
        output.append_data(b"1,10\n2,20\n3,30\n4,40\n").unwrap();
        output.end_input().unwrap();

        let files = output.finish().unwrap();
        assert_eq!(
            read_files(&files),
            vec!["time,value\n1,10\n2,20\n", "time,value\n3,30\n4,40\n"]
        );

        fs::remove_dir_all(output_dir).unwrap();
    }
}
