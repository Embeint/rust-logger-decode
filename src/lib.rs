use memmap::Mmap;
use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread;

use tdf::TdfOutput;

pub mod args;
pub mod fs_util;
mod output_common;
mod output_csv;
mod output_parquet;

pub const DEFAULT_MAX_READINGS_PER_OUTPUT_FILE: usize = 0;

pub trait ProgressReporter {
    /// Called when progress starts. Could be used to initialize the state or display a start message.
    fn start(&mut self, msg: &'static str, total: usize);

    /// Called to increment the progress. Takes the increment value as an argument.
    fn increment(&mut self, value: usize);

    /// Called when progress stops. Could be used to finalize the state or display a completion message.
    fn stop(&mut self);
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
    pub output_prefix: String,
    pub output_unix_time: bool,
    pub start_block: usize,
    pub num_blocks: usize,
    pub block_size: usize,
    pub output_format: args::OutputFormat,
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

pub fn worker_run_decode<T: ProgressReporter, U: TdfOutput>(
    mut args: DecodeWorkerArgsReporter<T>,
    mut writer: U,
) {
    let mut block_counter: HashMap<blocks::BlockTypes, usize> = HashMap::new();
    // Open file
    let file = File::open(args.decode_args.input_file.clone()).unwrap();
    let mmap = unsafe { Mmap::map(&file).unwrap() };
    let mmap_start = args.decode_args.block_size * args.decode_args.start_block;
    let mmap_end =
        args.decode_args.block_size * (args.decode_args.start_block + args.decode_args.num_blocks);

    // Range of the file for this worker
    let mmap_slice = &mmap[mmap_start..mmap_end];

    // Iterate over the blocks
    for (index, block) in mmap_slice
        .chunks_exact(args.decode_args.block_size)
        .enumerate()
    {
        match blocks::decode_block(&mut writer, block) {
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

    for ((remote_id, tdf_id), tdf_cnt) in writer.iter_written() {
        let res = tdf_stats
            .entry((*remote_id, *tdf_id))
            .or_insert_with(|| HashMap::new());
        let path = writer.output_path(*remote_id, *tdf_id).unwrap();

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
    pub block_size: usize,
    pub input_files: Vec<PathBuf>,
    pub output_folder: PathBuf,
    pub output_prefix: String,
    pub output_unix_time: bool,
    pub output_format: args::OutputFormat,
    pub merge_output_files: bool,
    pub max_readings_per_output_file: usize,
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
        if !f.exists() {
            return io::Result::Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Input file does not exist",
            ));
        }
        let m = f.metadata()?;
        let s = m.len() as usize;
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

    let num_blocks = size / args.block_size;
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
                output_prefix: args.output_prefix.clone(),
                output_unix_time: args.output_unix_time,
                start_block: idx * blocks_per_worker,
                num_blocks: num,
                block_size: args.block_size,
                output_format: args.output_format,
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
            match worker_arg.decode_args.output_format {
                args::OutputFormat::CSV => {
                    let writer = output_csv::TdfCsvWriter::new(
                        worker_arg.decode_args.decoder_idx,
                        worker_arg.decode_args.output_folder.clone(),
                        worker_arg.decode_args.output_prefix.clone(),
                        worker_arg.decode_args.output_unix_time,
                    );
                    worker_run_decode(worker_arg, writer);
                }
                args::OutputFormat::PARQUET => {
                    let writer = output_parquet::TdfParquetWriter::new(
                        worker_arg.decode_args.decoder_idx,
                        worker_arg.decode_args.output_folder.clone(),
                        worker_arg.decode_args.output_prefix.clone(),
                    );
                    worker_run_decode(worker_arg, writer);
                }
            };
        }));
    }

    // Wait for workers to terminate
    for worker in workers.into_iter() {
        worker.join().unwrap();
    }
    args.decode_reporter.stop();

    if args.merge_output_files {
        match args.output_format {
            args::OutputFormat::CSV => {
                output_csv::merge(args, &mut output_files, &stats_tdf)?;
            }
            args::OutputFormat::PARQUET => {
                output_parquet::merge_with_threshold(
                    args,
                    &mut output_files,
                    &stats_tdf,
                    args.max_readings_per_output_file,
                )?;
            }
        }
    } else {
        let results = stats_tdf.lock().unwrap();
        let mut worker_output_files: Vec<PathBuf> = results
            .values()
            .flat_map(|worker_outputs| worker_outputs.values().map(|output| output.output.clone()))
            .collect();
        worker_output_files.sort();
        output_files.extend(worker_output_files);
    }

    let block = stats_block.lock().unwrap().clone();
    let mut tdf = HashMap::new();
    let results = stats_tdf.lock().unwrap();

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
