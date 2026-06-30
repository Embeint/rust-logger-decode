use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use infuse_decoder::args;
use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use std::process::ExitCode;

#[macro_use]
extern crate prettytable;
use prettytable::{Table, format};

#[derive(Clone)]
pub struct IndicatifProgress {
    bar: Option<ProgressBar>,
}

impl IndicatifProgress {
    pub fn new() -> Self {
        Self { bar: None }
    }
}

impl infuse_decoder::ProgressReporter for IndicatifProgress {
    fn start(&mut self, msg: &'static str, total: usize) {
        let bar = ProgressBar::new(total as u64);

        bar.set_message(msg);
        bar.set_style(ProgressStyle::with_template("[{elapsed_precise}/{duration_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}...")
        .unwrap()
        .progress_chars("##-"));

        self.bar = Some(bar);
    }

    fn increment(&mut self, value: usize) {
        self.bar.as_ref().unwrap().inc(value as u64);
    }

    fn stop(&mut self) {
        self.bar.as_ref().unwrap().finish();
    }
}

/// Decode Infuse-IoT binary files to CSV
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Cli {
    /// The path to the file/folder containing Infuse-IoT binary files
    #[arg(short, long, required = true)]
    path: std::path::PathBuf,
    /// Output path for decoded files
    #[arg(short, long, required = true)]
    output: std::path::PathBuf,
    /// Prefix for output filenames
    #[arg(short, long)]
    name: Option<String>,
    /// Write Unix timestamps instead of UTC strings
    #[arg(short, long)]
    unix: bool,
    #[arg(long, default_value_t = args::OutputFormat::CSV)]
    format: args::OutputFormat,
    /// Verbose CLI output
    #[arg(short, long)]
    verbose: bool,
    #[arg(long, default_value_t = infuse_decoder::args::BlockSizeOptions::B512)]
    block_size: infuse_decoder::args::BlockSizeOptions,
    /// Maximum readings per output file (0 is no limit)
    #[arg(long, default_value_t = infuse_decoder::DEFAULT_MAX_READINGS_PER_OUTPUT_FILE)]
    max_readings_per_output_file: usize,
    /// Keep decoder worker output files instead of merging them into linearized outputs
    #[arg(long = "no-linearize-output", alias = "no-merge-output-files")]
    no_linearize_output: bool,
}

fn print_run_error(err: &io::Error, device_id: u64, files: &[PathBuf], output_folder: &PathBuf) {
    eprintln!();
    eprintln!("Decode failed");
    eprintln!("=============");
    eprintln!("Device ID     : {device_id:016x}");
    eprintln!("Input files   : {}", files.len());
    for file in files {
        eprintln!("  - {}", file.display());
    }
    eprintln!("Output folder : {}", output_folder.display());
    eprintln!("Error kind    : {:?}", err.kind());
    eprintln!("Cause         : {err}");
}

fn main() -> ExitCode {
    let args = Cli::parse();

    // Handle single file supplied
    let iot_bin_files: HashMap<u64, Vec<PathBuf>> = if args.path.is_dir() {
        match infuse_decoder::fs_util::find_infuse_iot_files(&args.path) {
            Ok(files) => files,
            Err(err) => {
                eprintln!("Failed to scan input path '{}': {err}", args.path.display());
                return ExitCode::FAILURE;
            }
        }
    } else {
        let mut f: HashMap<u64, Vec<PathBuf>> = HashMap::new();
        f.insert(0, vec![args.path.clone()]);
        f
    };
    let num_devices = iot_bin_files.len();

    for (device_id, files) in iot_bin_files.iter() {
        if args.path.is_dir() {
            println!("Handling Infuse-IoT device ID: {:016x}...", device_id);
        } else {
            println!("Handling Infuse-IoT file: {:?}...", args.path);
        }

        let output_prefix = match args.name.as_ref() {
            Some(name) => {
                if num_devices > 1 {
                    format!("{name}_{device_id:016x}")
                } else {
                    format!("{name}")
                }
            }
            None => {
                if args.path.is_file() {
                    match args.path.file_stem().and_then(|stem| stem.to_str()) {
                        Some(stem) => stem.to_string(),
                        None => {
                            eprintln!(
                                "Failed to derive output name from input path '{}'",
                                args.path.display()
                            );
                            return ExitCode::FAILURE;
                        }
                    }
                } else {
                    format!("{device_id:016x}")
                }
            }
        };

        let mut run_args = infuse_decoder::RunArgs {
            device_id: *device_id,
            block_size: args.block_size as usize,
            input_files: files.clone(),
            output_folder: args.output.clone(),
            output_prefix: output_prefix,
            output_unix_time: args.unix,
            output_format: args.format,
            merge_output_files: !args.no_linearize_output,
            max_readings_per_output_file: args.max_readings_per_output_file,
            copy_reporter: IndicatifProgress::new(),
            decode_reporter: IndicatifProgress::new(),
            merge_reporter: IndicatifProgress::new(),
        };

        let (block_stats, tdf_stats, _output_files) = match infuse_decoder::run(&mut run_args) {
            Ok(result) => result,
            Err(err) => {
                print_run_error(&err, *device_id, files, &args.output);
                return ExitCode::FAILURE;
            }
        };

        if args.verbose {
            for (remote_id, tdfs) in tdf_stats.iter() {
                let mut table = Table::new();

                for (tdf_id, count) in tdfs.iter() {
                    table.add_row(row![tdf::decoders::tdf_name(tdf_id), count]);
                }
                table.set_titles(row!["TDF", "Count"]);
                table.set_format(*format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);

                if let Some(x) = remote_id {
                    println!("Remote ID: {:016x}", x);
                }
                table.printstd();
                println!("");
            }

            // Output Block statistics
            let mut sorted: Vec<(&blocks::BlockTypes, &usize)> = block_stats.iter().collect();
            sorted.sort_by(|a, b| b.1.cmp(a.1));

            let mut table = Table::new();
            table.set_titles(row!["Block Type", "Count"]);
            for (block_type, block_cnt) in sorted {
                table.add_row(row![format!("{}", block_type), block_cnt]);
            }
            table.set_format(*format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);
            table.printstd();
        }
    }
    ExitCode::SUCCESS
}
