use chrono::SecondsFormat;
use itertools::Itertools;
use memmap::Mmap;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fs::File;
use std::io::{self, BufRead, BufReader, BufWriter, Cursor, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread;

use tdf::TdfOutput;

pub mod fs_util;

pub trait ProgressReporter {
    /// Called when progress starts. Could be used to initialize the state or display a start message.
    fn start(&mut self, msg: &'static str, total: usize);

    /// Called to increment the progress. Takes the increment value as an argument.
    fn increment(&mut self, value: usize);

    /// Called when progress stops. Could be used to finalize the state or display a completion message.
    fn stop(&mut self);
}

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
                let heading = tdf::decoders::tdf_fields(&tdf_id).join(",");
                writer.write_all(format!("time,{}\n", heading).as_bytes())?;

                // Insert into hashmap and return
                v.insert((path, writer))
            }
        };

        // Construct CSV line
        let reading = tdf::decoders::tdf_read_into_str(&tdf_id, size, cursor)?;
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

pub fn merge_input_files<T: ProgressReporter>(
    output_prefix: &String,
    input_files: &Vec<PathBuf>,
    output_folder: &PathBuf,
    reporter: &mut T,
) -> io::Result<(PathBuf, usize)> {
    let output_filename = format!("{output_prefix}.bin");
    let output_filepath = output_folder.join(output_filename);
    let mut merged_output = File::create(&output_filepath)?;

    reporter.start("Merging input files", input_files.len());
    for i in input_files {
        let mut input = File::open(i)?;
        io::copy(&mut input, &mut merged_output)?;
        reporter.increment(1);
    }
    reporter.stop();
    Ok((output_filepath, merged_output.metadata()?.len() as usize))
}

#[derive(Debug)]
pub struct DecodeWorkerArgs {
    pub decoder_idx: usize,
    pub input_file: std::path::PathBuf,
    pub output_folder: std::path::PathBuf,
    pub output_unix_time: bool,
    pub start_block: usize,
    pub num_blocks: usize,
}

#[derive(Clone)]
pub struct TdfDecoderOutputs {
    pub output: PathBuf,
    pub num_output: usize,
}

pub struct DecodeWorkerArgsReporter<T: ProgressReporter> {
    pub decode_args: DecodeWorkerArgs,
    pub block_stats: Arc<Mutex<HashMap<blocks::BlockTypes, usize>>>,
    pub tdf_stats: Arc<Mutex<HashMap<(Option<u64>, u16), HashMap<usize, TdfDecoderOutputs>>>>,
    pub reporter: T,
}

pub fn worker_run_decode<T: ProgressReporter>(mut args: DecodeWorkerArgsReporter<T>) {
    let mut block_counter: HashMap<blocks::BlockTypes, usize> = HashMap::new();
    let mut csv_writer = TdfCsvWriter::new(
        args.decode_args.decoder_idx,
        args.decode_args.output_folder,
        args.decode_args.output_unix_time,
    );

    // Open file
    let file = File::open(args.decode_args.input_file.clone()).unwrap();
    let mmap = unsafe { Mmap::map(&file).unwrap() };
    let mmap_start = blocks::BLOCK_SIZE * args.decode_args.start_block;
    let mmap_end =
        blocks::BLOCK_SIZE * (args.decode_args.start_block + args.decode_args.num_blocks);

    // Range of the file for this worker
    let mmap_slice = &mmap[mmap_start..mmap_end];

    // Iterate over the blocks
    for (index, block) in mmap_slice.chunks_exact(blocks::BLOCK_SIZE).enumerate() {
        match blocks::decode_block(&mut csv_writer, block) {
            Ok(block_type) => *block_counter.entry(block_type).or_default() += 1,
            Err(_) => *block_counter.entry(blocks::BlockTypes::ERROR).or_default() += 1,
        }

        // Report every 10 blocks finished
        if index % 10 == 9 {
            args.reporter.increment(10);
        }
    }

    // Push TDF stats into the output hashmap
    let mut tdf_stats = args.tdf_stats.lock().unwrap();

    for ((remote_id, tdf_id), tdf_cnt) in csv_writer.iter_written() {
        let res = tdf_stats
            .entry((*remote_id, *tdf_id))
            .or_insert_with(|| HashMap::new());
        let path = csv_writer.output_path(*remote_id, *tdf_id).unwrap();

        res.insert(
            args.decode_args.decoder_idx,
            TdfDecoderOutputs {
                output: path.clone(),
                num_output: *tdf_cnt,
            },
        );
    }
    drop(tdf_stats);
    // Update block stats
    let mut global_block_stats = args.block_stats.lock().unwrap();

    for (block_type, block_cnt) in block_counter.iter() {
        *global_block_stats.entry(*block_type).or_default() += block_cnt;
    }
}

pub struct RunArgs<T: ProgressReporter> {
    pub device_id: u64,
    pub input_files: Vec<PathBuf>,
    pub output_folder: PathBuf,
    pub output_prefix: String,
    pub output_unix_time: bool,
    pub copy_reporter: T,
    pub decode_reporter: T,
    pub merge_reporter: T,
}

pub fn run<T: ProgressReporter + Clone + Send + 'static>(
    args: &mut RunArgs<T>,
) -> io::Result<(
    HashMap<blocks::BlockTypes, usize>,
    HashMap<Option<u64>, HashMap<u16, usize>>,
    Vec<PathBuf>,
)> {
    let stats_block = Arc::new(Mutex::new(HashMap::new()));
    let stats_tdf = Arc::new(Mutex::new(HashMap::new()));
    let mut output_files: Vec<PathBuf> = Vec::new();

    // Ensure output folder exists
    std::fs::create_dir_all(args.output_folder.clone())?;

    let (merged_file, size) = if args.input_files.len() == 1 {
        let f: PathBuf = args.input_files.get(0).unwrap().clone();
        let s = f.metadata().unwrap().len() as usize;
        (f, s)
    } else {
        let (f, s) = merge_input_files(
            &args.output_prefix,
            &args.input_files,
            &args.output_folder,
            &mut args.copy_reporter,
        )?;
        output_files.push(f.clone());
        (f, s)
    };

    let num_blocks = size / blocks::BLOCK_SIZE;
    let max_workers = (num_blocks / 100) + 1;
    let num_workers = std::cmp::min(max_workers, num_cpus::get());
    let blocks_per_worker = num_blocks / num_workers;
    let trailing = num_blocks - (blocks_per_worker * num_workers);

    args.decode_reporter.start("Decoding blocks", num_blocks);

    // Construct arguments for decode workers
    let mut worker_args = vec![];
    for idx in 0..num_workers {
        let mut num = blocks_per_worker;
        if idx == num_workers - 1 {
            num += trailing;
        }

        worker_args.push(DecodeWorkerArgsReporter {
            decode_args: DecodeWorkerArgs {
                decoder_idx: idx,
                input_file: merged_file.clone(),
                output_folder: args.output_folder.clone(),
                output_unix_time: args.output_unix_time,
                start_block: idx * blocks_per_worker,
                num_blocks: num,
            },
            block_stats: stats_block.clone(),
            tdf_stats: stats_tdf.clone(),
            reporter: args.decode_reporter.clone(),
        });
    }

    // Spin up decoder workers
    let mut workers = vec![];
    for worker_arg in worker_args.into_iter() {
        workers.push(thread::spawn(move || {
            worker_run_decode(worker_arg);
        }));
    }

    // Wait for workers to terminate
    for worker in workers.into_iter() {
        worker.join().unwrap();
    }
    args.decode_reporter.stop();

    // Merge output files
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
        let mut output = BufWriter::new(File::create(output_path)?);
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

    let block = stats_block.lock().unwrap().clone();
    let mut tdf = HashMap::new();

    for ((remote_id, tdf_id), worker_outputs) in results.iter() {
        let values = tdf.entry(*remote_id).or_insert_with(|| HashMap::new());

        let mut sum = 0;
        for (_, output) in worker_outputs.iter() {
            sum += output.num_output;
        }
        values.insert(*tdf_id, sum);
    }

    Ok((block, tdf, output_files))
}
