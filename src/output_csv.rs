use chrono::SecondsFormat;
use itertools::Itertools;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fs::{self, File};
use std::io::Cursor;
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tdf::TdfOutput;

use crate::output_common::{
    OutputKey, increment_output_count, merged_output_path, rename_first_file_if_splitting,
    touch_output_count, worker_output_path, written,
};
use crate::{ProgressReporter, RunArgs, TdfDecoderOutputs};

pub struct TdfCsvWriter {
    decoder_idx: usize,
    output_folder: std::path::PathBuf,
    output_prefix: String,
    output_unix: bool,
    pub outputs:
        HashMap<(Option<u64>, u16), (std::path::PathBuf, std::io::BufWriter<std::fs::File>)>,
    output_cnt: HashMap<OutputKey, usize>,
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
            outputs: HashMap::new(),
            output_cnt: HashMap::new(),
        }
    }
}

impl TdfOutput for TdfCsvWriter {
    fn output_path(self: &Self, remote_id: Option<u64>, tdf_id: u16) -> Option<PathBuf> {
        self.outputs
            .get(&(remote_id, tdf_id))
            .map(|(pathbuf, _)| pathbuf.clone())
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
        let (_, writer) = match self.outputs.entry((remote_id, tdf_id)) {
            Entry::Occupied(o) => o.into_mut(),
            Entry::Vacant(v) => {
                let path = worker_output_path(
                    &self.output_folder,
                    &self.output_prefix,
                    remote_id,
                    tdf_id,
                    self.decoder_idx,
                    "csv",
                );
                let mut writer = std::io::BufWriter::new(std::fs::File::create(path.clone())?);

                // Write header into file
                let heading = tdf::decoders_csv::tdf_fields(&tdf_id).join(",");
                writer.write_all(format!("time,{}\n", heading).as_bytes())?;

                // Touch the count variable in case the decoding fails
                touch_output_count(&mut self.output_cnt, (remote_id, tdf_id));

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
        }
    }

    fn set_header(&mut self, header: String) -> io::Result<()> {
        if self.header.is_none() {
            self.header = Some(header);
            self.start_next_file()?;
        }
        Ok(())
    }

    fn append_line(&mut self, line: &str) -> io::Result<()> {
        if self.writer.is_none() {
            self.start_next_file()?;
        }

        if self
            .max_readings_per_file
            .is_some_and(|max_readings| self.readings_in_file >= max_readings)
        {
            self.start_next_file()?;
        }

        self.writer
            .as_mut()
            .expect("CSV writer should be open")
            .write_all(line.as_bytes())?;
        self.writer
            .as_mut()
            .expect("CSV writer should be open")
            .write_all(b"\n")?;
        self.readings_in_file += 1;
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

pub fn merge<T: ProgressReporter>(
    args: &mut RunArgs<T>,
    output_files: &mut Vec<PathBuf>,
    stats_tdf: &Arc<Mutex<HashMap<(Option<u64>, u16), HashMap<usize, TdfDecoderOutputs>>>>,
) -> io::Result<()> {
    let results = stats_tdf.lock().unwrap();
    let num_files: usize = results.values().map(|inner| inner.len()).sum();

    args.merge_reporter.start("Merging output files", num_files);

    for ((remote_id, tdf_id), worker_outputs) in results.iter() {
        let mut output = TdfCsvMergedOutput::new(
            args.output_folder.clone(),
            args.output_prefix.clone(),
            *remote_id,
            *tdf_id,
            args.max_readings_per_output_file,
        );

        for worker in worker_outputs.keys().sorted() {
            let input_path = worker_outputs[worker].output.clone();
            let input = BufReader::new(File::open(&input_path)?);

            for (idx, line) in input.lines().enumerate() {
                let line = line?;
                if idx == 0 {
                    output.set_header(line)?;
                    continue;
                }
                output.append_line(&line)?;
            }

            fs::remove_file(input_path)?;

            args.merge_reporter.increment(1);
        }
        output_files.extend(output.finish()?);
    }
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
        output.append_line("1,10").unwrap();
        output.append_line("2,20").unwrap();
        output.append_line("3,30").unwrap();

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
}
