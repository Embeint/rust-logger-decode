use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use std::collections::HashMap;
use std::io;
use std::path::PathBuf;

#[macro_use]
extern crate prettytable;
use prettytable::{format, Table};

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
    /// The path to the folder containing Infuse-IoT binary files
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
    /// Verbose CLI output
    #[arg(short, long)]
    verbose: bool,
}

fn main() -> io::Result<()> {
    let args = Cli::parse();

    if args.path.is_file() && !args.name.is_some() {
        println!("Expected `--name` to be provided when `--path` is a file");
        return Ok(());
    }

    // Handle single file supplied
    let iot_bin_files: HashMap<u64, Vec<PathBuf>> = if args.path.is_dir() {
        infuse_decoder::fs_util::find_infuse_iot_files(&args.path).unwrap()
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
                format!("{device_id:016x}")
            }
        };

        let mut run_args = infuse_decoder::RunArgs {
            device_id: *device_id,
            input_files: files.clone(),
            output_folder: args.output.clone(),
            output_prefix: output_prefix,
            output_unix_time: args.unix,
            copy_reporter: IndicatifProgress::new(),
            decode_reporter: IndicatifProgress::new(),
            merge_reporter: IndicatifProgress::new(),
        };

        let (block_stats, tdf_stats, _output_files) = infuse_decoder::run(&mut run_args)?;

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
    Ok(())
}
