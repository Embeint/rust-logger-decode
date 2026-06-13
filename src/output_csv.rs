use chrono::SecondsFormat;
use itertools::Itertools;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fs::File;
use std::io::Cursor;
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tdf::TdfOutput;

use crate::{ProgressReporter, RunArgs, TdfDecoderOutputs};

pub struct TdfCsvWriter {
    decoder_idx: usize,
    output_folder: std::path::PathBuf,
    output_unix: bool,
    pub outputs:
        HashMap<(Option<u64>, u16), (std::path::PathBuf, std::io::BufWriter<std::fs::File>)>,
    output_cnt: HashMap<(Option<u64>, u16), usize>,
}
impl TdfCsvWriter {
    pub fn new(
        decoder_idx: usize,
        output_folder: std::path::PathBuf,
        output_unix_time: bool,
    ) -> Self {
        Self {
            decoder_idx: decoder_idx,
            output_folder: output_folder,
            output_unix: output_unix_time,
            outputs: HashMap::new(),
            output_cnt: HashMap::new(),
        }
    }

    pub fn output_path(self: &Self, remote_id: Option<u64>, tdf_id: u16) -> Option<PathBuf> {
        self.outputs
            .get(&(remote_id, tdf_id))
            .map(|(pathbuf, _)| pathbuf.clone())
    }
}

impl TdfOutput for TdfCsvWriter {
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
        let (_, writer) = match self.outputs.entry((remote_id, tdf_id)) {
            Entry::Occupied(o) => o.into_mut(),
            Entry::Vacant(v) => {
                let id_prefix = match remote_id {
                    Some(id) => format!("_{:016x}", id),
                    None => "".to_string(),
                };
                let fname = format!(
                    "{}_{}_{:05}.csv",
                    id_prefix,
                    tdf::decoders::tdf_name(&tdf_id),
                    self.decoder_idx
                );
                let path = self.output_folder.join(fname);
                let mut writer = std::io::BufWriter::new(std::fs::File::create(path.clone())?);

                // Write header into file
                let heading = tdf::decoders_csv::tdf_fields(&tdf_id).join(",");
                writer.write_all(format!("time,{}\n", heading).as_bytes())?;

                // Touch the count variable in case the decoding fails
                *self
                    .output_cnt
                    .entry((remote_id, tdf_id.to_owned()))
                    .or_default() += 0;

                // Insert into hashmap and return
                v.insert((path, writer))
            }
        };

        // Construct CSV line
        let reading = tdf::decoders_csv::tdf_read_into_str(&tdf_id, size, cursor)?;
        let time = match tdf_idx {
            Some(idx) => {
                // Use the index directly if provided
                format!("{idx}")
            }
            None => match self.output_unix {
                // Otherwise, format the time to a string
                true => {
                    let (unix_seconds, unix_nano) = tdf::time::tdf_time_to_unix(tdf_time);
                    format!("{}.{:06}", unix_seconds, unix_nano / 1000)
                }
                false => {
                    let datetime = tdf::time::tdf_time_to_datetime(tdf_time).expect("Invalid time");
                    datetime.to_rfc3339_opts(SecondsFormat::Micros, true)
                }
            },
        };

        let line: String = format!("{},{}\n", time, reading);

        // Write line to output
        writer.write_all(line.as_bytes())?;

        // Increment output counter
        *self
            .output_cnt
            .entry((remote_id, tdf_id.to_owned()))
            .or_default() += 1;
        Ok(())
    }

    fn iter_written(&self) -> impl Iterator<Item = (&(Option<u64>, u16), &usize)> {
        self.output_cnt.iter()
    }

    fn written(&self, remote_id: Option<u64>, tdf_id: u16) -> usize {
        match self.output_cnt.get(&(remote_id, tdf_id)) {
            Some(val) => *val,
            None => 0,
        }
    }
}

pub fn merge<T: ProgressReporter>(
    args: &mut RunArgs<T>,
    output_files: &mut Vec<PathBuf>,
    stats_tdf: &Arc<Mutex<HashMap<(Option<u64>, u16), HashMap<usize, TdfDecoderOutputs>>>>,
) -> io::Result<()> {
    let results = stats_tdf.lock().unwrap();
    let num_files: usize = results.values().map(|inner| inner.len()).sum();

    args.merge_reporter.start("Merging output files", num_files);

    for ((remote_id, tdf_id), worker_outputs) in results.iter() {
        let id_prefix = match remote_id {
            Some(id) => format!("_{:016x}", id),
            None => "".to_string(),
        };
        let output_path = args.output_folder.join(format!(
            "{}{}_{}.csv",
            args.output_prefix,
            id_prefix,
            tdf::decoders::tdf_name(tdf_id)
        ));
        output_files.push(output_path.clone());
        let err_path = output_path.clone();
        let output_file = File::create(output_path).map_err(|e| {
            io::Error::new(
                e.kind(),
                format!(
                    "Failed to create output file '{}': {}",
                    err_path.display(),
                    e
                ),
            )
        })?;
        let mut output = BufWriter::new(output_file);
        let mut write_headings = true;

        for worker in worker_outputs.keys().sorted() {
            let input_path = worker_outputs[worker].output.clone();
            let input = BufReader::new(File::open(&input_path)?);

            // Copy from input to output
            for (idx, line) in input.lines().flatten().enumerate() {
                if idx == 0 && !write_headings {
                    continue;
                }
                output.write_all(line.as_bytes())?;
                output.write_all(b"\n")?;
            }
            // Remove input file
            std::fs::remove_file(input_path)?;
            write_headings = false;

            args.merge_reporter.increment(1);
        }
        output.flush()?;
    }
    args.merge_reporter.stop();

    Ok(())
}
