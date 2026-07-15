#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use std::env;
use std::io;
use std::sync::{Arc, Mutex};
use std::thread;
use std::{collections::HashMap, path::PathBuf};

use chrono::{Datelike, Utc};
use eframe::egui::{self, IconData};
use egui_extras::{Column, TableBuilder};
use image::GenericImageView;
use infuse_decoder::args::OutputFormat;
use rfd::FileDialog;

use infuse_decoder::args::BlockSizeOptions;

#[derive(PartialEq)]
enum TimeOutput {
    UNIX,
    UTC,
}

struct SliderInternalState {
    total: usize,
    current: usize,
    enabled: bool,
}

#[derive(Clone)]
struct SliderState {
    label: &'static str,
    state: Arc<Mutex<SliderInternalState>>,
}
impl SliderState {
    pub fn new(label: &'static str) -> Self {
        Self {
            label: label,
            state: Arc::new(Mutex::new(SliderInternalState {
                total: 1,
                current: 0,
                enabled: false,
            })),
        }
    }
    pub fn reset(self: &mut Self) {
        let mut s = self.state.lock().unwrap();
        s.current = 0;
        s.enabled = false;
    }

    pub fn draw(self: &mut Self, ui: &mut egui::Ui) {
        ui.label(self.label);

        let s = self.state.lock().unwrap();
        let progress = s.current as f32 / s.total as f32;
        let progress_bar = egui::ProgressBar::new(progress).show_percentage();
        ui.add_enabled(s.enabled, progress_bar);
    }

    pub fn draw_count(self: &mut Self, ui: &mut egui::Ui) {
        ui.label(self.label);

        let s = self.state.lock().unwrap();
        let progress = s.current as f32 / s.total as f32;
        let progress_bar =
            egui::ProgressBar::new(progress).text(format!("{} / {}", s.current, s.total));
        ui.add_enabled(s.enabled, progress_bar);
    }
}

impl infuse_decoder::ProgressReporter for SliderState {
    fn start(&mut self, msg: &'static str, total: usize) {
        self.label = msg;
        let mut s = self.state.lock().unwrap();
        s.total = total;
        s.current = 0;
        s.enabled = false;
    }

    fn increment(&mut self, value: usize) {
        let mut s = self.state.lock().unwrap();
        s.current += value;
    }

    fn stop(&mut self) {
        let mut s = self.state.lock().unwrap();
        s.current = s.total;
    }
}

struct MyApp {
    doc_capture: Option<DocCapture>,
    time_mode: TimeOutput,
    output_format: OutputFormat,
    linearize_output_files: bool,
    decode_all_devices: bool,
    device_id: u64,
    block_size: BlockSizeOptions,
    max_readings_per_output_file: usize,
    error_msg: Option<String>,
    input_path: Option<PathBuf>,
    input_files: Option<HashMap<u64, Vec<PathBuf>>>,
    output_folder: PathBuf,
    output_prefix: String,
    progress_copy: SliderState,
    progress_devices: SliderState,
    progress_decode: SliderState,
    progress_merge: SliderState,
    block_stats: Option<Vec<(blocks::BlockTypes, usize)>>,
    tdf_stats: Option<HashMap<Option<u64>, HashMap<u16, usize>>>,
    output_files: Option<Vec<PathBuf>>,
    runner_thread: Option<
        std::thread::JoinHandle<
            Result<
                (
                    HashMap<blocks::BlockTypes, usize>,
                    HashMap<Option<u64>, HashMap<u16, usize>>,
                    Vec<PathBuf>,
                ),
                std::io::Error,
            >,
        >,
    >,
}

struct DocCapture {
    markers: Vec<DocMarker>,
    screenshot_requested: bool,
    output_path: PathBuf,
}

struct DocMarker {
    label: &'static str,
    rect: egui::Rect,
}

impl DocCapture {
    fn new() -> Self {
        Self {
            markers: Vec::new(),
            screenshot_requested: false,
            output_path: PathBuf::from("assets/configuration_options.png"),
        }
    }

    fn reset_markers(&mut self) {
        self.markers.clear();
    }
}
use directories::UserDirs;

const DOC_MARKER_GUTTER: f32 = 20.0;

impl Default for MyApp {
    fn default() -> Self {
        let mut default_out = if let Some(user_dirs) = UserDirs::new() {
            user_dirs
                .document_dir()
                .map(|x| x.to_owned().join("infuse_iot"))
        } else {
            match env::current_dir() {
                Ok(dir) => Some(dir),
                _ => None,
            }
        };
        if default_out.is_none() {
            default_out = Some(PathBuf::from("."));
        }
        let doc_capture = doc_capture_enabled().then(DocCapture::new);

        let input_path = doc_capture.as_ref().map(|_| PathBuf::from("E:\\INFUSE"));
        let mut input_files = None;
        let mut device_id = 0;
        let mut output_prefix = String::from("");

        if doc_capture.is_some() {
            device_id = 0x0000_0000_5aa5_f00d;
            output_prefix = format!("{device_id:016x}");
            let mut files = HashMap::new();
            files.insert(
                device_id,
                vec![PathBuf::from("E:\\INFUSE\\infuse_cc0000000000000a.bin")],
            );
            input_files = Some(files);
        }

        Self {
            doc_capture,
            time_mode: TimeOutput::UTC,
            output_format: OutputFormat::CSV,
            linearize_output_files: true,
            decode_all_devices: false,
            device_id,
            block_size: BlockSizeOptions::B512,
            max_readings_per_output_file: infuse_decoder::DEFAULT_MAX_READINGS_PER_OUTPUT_FILE,
            error_msg: None,
            input_path,
            input_files,
            output_folder: default_out.unwrap(),
            output_prefix,
            progress_copy: SliderState::new("Copying files"),
            progress_devices: SliderState::new("Devices decoded"),
            progress_decode: SliderState::new("Decoding files"),
            progress_merge: SliderState::new("Merging output"),
            block_stats: None,
            tdf_stats: None,
            output_files: None,
            runner_thread: None,
        }
    }
}

fn doc_capture_enabled() -> bool {
    env::var_os("INFUSE_DECODER_DOC_SCREENSHOT").is_some()
        || env::args().any(|arg| arg == "--docs-screenshot")
}

impl MyApp {
    fn is_doc_capture(&self) -> bool {
        self.doc_capture.is_some()
    }

    fn mark_doc(&mut self, label: &'static str, rect: egui::Rect) {
        if let Some(doc_capture) = &mut self.doc_capture {
            doc_capture.markers.push(DocMarker { label, rect });
        }
    }

    fn handle_doc_capture(&mut self, ctx: &egui::Context) {
        let Some(doc_capture) = &mut self.doc_capture else {
            return;
        };

        let mut screenshot = None;
        ctx.input(|input| {
            for event in &input.events {
                if let egui::Event::Screenshot { image, .. } = event {
                    screenshot = Some(Arc::clone(image));
                }
            }
        });

        if let Some(image) = screenshot {
            if let Err(err) = save_color_image(&doc_capture.output_path, &image) {
                eprintln!(
                    "Failed to save documentation screenshot to {}: {err}",
                    doc_capture.output_path.display()
                );
            }
            ctx.send_viewport_cmd(egui::ViewportCommand::Close);
            return;
        }

        let painter = ctx.layer_painter(egui::LayerId::new(
            egui::Order::Foreground,
            egui::Id::new("doc_markers"),
        ));

        for marker in &doc_capture.markers {
            draw_doc_marker(&painter, marker);
        }

        if !doc_capture.screenshot_requested {
            doc_capture.screenshot_requested = true;
            ctx.send_viewport_cmd(egui::ViewportCommand::Screenshot(egui::UserData::default()));
            ctx.request_repaint();
        }
    }
}

fn draw_doc_marker(painter: &egui::Painter, marker: &DocMarker) {
    let radius = 12.0;
    let position = marker.rect.left_center() + egui::vec2(-14.0, 0.0);
    painter.circle_filled(position, radius, egui::Color32::from_rgb(0, 0x89, 0x47));
    painter.circle_stroke(
        position,
        radius,
        egui::Stroke::new(1.5_f32, egui::Color32::WHITE),
    );
    painter.text(
        position,
        egui::Align2::CENTER_CENTER,
        marker.label,
        egui::FontId::proportional(13.0),
        egui::Color32::WHITE,
    );
}

fn save_color_image(path: &std::path::Path, image: &egui::ColorImage) -> image::ImageResult<()> {
    let mut rgba = Vec::with_capacity(image.pixels.len() * 4);
    for pixel in &image.pixels {
        rgba.extend_from_slice(&[pixel.r(), pixel.g(), pixel.b(), pixel.a()]);
    }

    image::save_buffer(
        path,
        &rgba,
        image.size[0] as u32,
        image.size[1] as u32,
        image::ColorType::Rgba8,
    )
}

fn trimmed_label(label: &String, max_len: usize) -> String {
    if label.len() > max_len {
        let idx = label.len() - (max_len - 3);
        format!("...{}", &label[idx..])
    } else {
        label.clone()
    }
}

pub fn open_in_native_browser(path: &std::path::Path) -> Result<(), String> {
    if !path.exists() {
        return Err(format!("Path does not exist: {}", path.display()));
    }
    if !path.is_dir() {
        return Err(format!("Path is not a directory: {}", path.display()));
    }

    #[cfg(target_os = "windows")]
    {
        std::process::Command::new("explorer")
            .arg(path)
            .spawn()
            .map_err(|e| e.to_string())?;
    }

    #[cfg(target_os = "macos")]
    {
        std::process::Command::new("open")
            .arg(path)
            .spawn()
            .map_err(|e| e.to_string())?;
    }

    #[cfg(target_os = "linux")]
    {
        std::process::Command::new("xdg-open")
            .arg(path)
            .spawn()
            .map_err(|e| e.to_string())?;
    }

    Ok(())
}

fn core_options(app: &mut MyApp, ui: &mut egui::Ui) {
    egui::Grid::new("folder_selection")
        .num_columns(2)
        .show(ui, |ui| {
            let folder_str = app.output_folder.display().to_string();
            let output_folder_label = ui.label("Output folder");
            app.mark_doc("1", output_folder_label.rect);
            ui.label(egui::RichText::new(trimmed_label(&folder_str, 48)).code());
            ui.horizontal(|ui| {
                let folder_button = ui.button("Folder");
                if folder_button.clicked() {
                    if let Some(folder) = FileDialog::new()
                        .set_directory(app.output_folder.as_path())
                        .pick_folder()
                    {
                        app.output_folder = folder;
                    }
                }
                let open_button = ui.button("Open");
                if open_button.clicked() {
                    let _ = open_in_native_browser(app.output_folder.as_path());
                };
            });
            ui.end_row();

            let folder_str = match app.input_path.as_ref() {
                Some(folder) => folder.display().to_string(),
                None => String::from("N/A"),
            };

            let input_label = ui.label("Input folder/file");
            app.mark_doc("2", input_label.rect);
            ui.label(egui::RichText::new(trimmed_label(&folder_str, 48)).code());

            ui.horizontal(|ui| {
                let folder_button = ui.button("Folder");
                if folder_button.clicked() {
                    if let Some(folder) = FileDialog::new().pick_folder() {
                        match infuse_decoder::fs_util::find_infuse_iot_files(&folder) {
                            Ok(files) => {
                                app.device_id = *files.keys().next().unwrap_or(&0);
                                app.output_prefix = format!("{:016x}", app.device_id);
                                app.input_path = Some(folder);
                                app.input_files = Some(files);
                                // Reset the 'decode all' option when the folder changes
                                app.decode_all_devices = false;
                            }
                            _ => {}
                        }
                    }
                }
                let file_button = ui.button("File");
                if file_button.clicked() {
                    if let Some(file) = FileDialog::new().pick_file() {
                        let mut h = HashMap::new();
                        h.insert(0, vec![file.clone()]);
                        let prefix = match file.file_stem() {
                            Some(name) => name.to_str().unwrap_or("0"),
                            None => "0",
                        };

                        app.device_id = 0;
                        app.decode_all_devices = false;
                        app.output_prefix = prefix.to_string();
                        app.input_path = Some(file);
                        app.input_files = Some(h);
                    }
                }
            });
            ui.end_row();

            // Clear the selected paths if they no longer exist (SD card removed)
            if let Some(input) = &app.input_path {
                if !app.is_doc_capture() && !input.exists() {
                    app.input_path = None;
                    app.input_files = None;
                }
            }

            let device_label = ui.label("Device ID");
            app.mark_doc("3", device_label.rect);
            ui.horizontal(|ui| {
                if let Some(file_list) = &app.input_files {
                    ui.add_enabled_ui(file_list.len() > 1 && !app.decode_all_devices, |ui| {
                        egui::ComboBox::from_label("")
                            .selected_text(format!("{:016x}", app.device_id))
                            .show_ui(ui, |ui| {
                                for id in file_list.keys() {
                                    ui.selectable_value(
                                        &mut app.device_id,
                                        *id,
                                        format!("{:016x}", id),
                                    );
                                }
                            });
                    });
                    ui.add_enabled_ui(file_list.len() > 1, |ui| {
                        ui.checkbox(&mut app.decode_all_devices, "All");
                    });
                } else {
                    ui.add_enabled(
                        false,
                        egui::Checkbox::new(&mut app.decode_all_devices, "All"),
                    );
                }
            });
            ui.end_row();

            let prefix_label = ui.label("Output Prefix");
            app.mark_doc("4", prefix_label.rect);
            ui.text_edit_singleline(&mut app.output_prefix);
            let extension = match app.output_format {
                OutputFormat::CSV => "csv",
                OutputFormat::PARQUET => "parquet",
            };
            let num_devices = app.input_files.as_ref().map_or(1, HashMap::len);
            let example_prefix = output_prefix_for_device(
                &app.output_prefix,
                app.device_id,
                app.device_id,
                num_devices,
                app.decode_all_devices,
            );
            ui.label(format!(
                "(e.g. {}_BATTERY_STATE.{extension})",
                example_prefix
            ));
            ui.end_row();
        });
}

fn decode_options(app: &mut MyApp, ui: &mut egui::Ui) {
    ui.horizontal(|ui| {
        if app.is_doc_capture() {
            ui.add_space(DOC_MARKER_GUTTER);
        }
        ui.vertical(|ui| {
            let output_format_label = ui.label("Output Format");
            app.mark_doc("5", output_format_label.rect);
            ui.radio_value(&mut app.output_format, OutputFormat::CSV, "CSV");
            ui.radio_value(&mut app.output_format, OutputFormat::PARQUET, "Parquet");
        });
        ui.separator();
        ui.vertical(|ui| {
            let file_output_control = ui.label("File Output Control");
            app.mark_doc("6", file_output_control.rect);
            ui.checkbox(&mut app.linearize_output_files, "Linearize Output");
            ui.label("Max Readings Per File");
            ui.add_enabled_ui(app.linearize_output_files, |ui| {
                ui.add(
                    egui::DragValue::new(&mut app.max_readings_per_output_file)
                        .range(0..=usize::MAX)
                        .speed(10_000),
                );
            });
        });
        ui.separator();
        ui.vertical(|ui| {
            let time_format_label = ui.label("Time Output Format");
            app.mark_doc("7", time_format_label.rect);
            ui.add_enabled_ui(app.output_format == OutputFormat::CSV, |ui| {
                ui.radio_value(
                    &mut app.time_mode,
                    TimeOutput::UTC,
                    "UTC  (2020-01-01T00:00:00.000000Z)",
                );
                ui.radio_value(
                    &mut app.time_mode,
                    TimeOutput::UNIX,
                    "UNIX (1577800800.000000)",
                );
            });
        });
        ui.separator();
        ui.vertical(|ui| {
            let block_size_label = ui.label("Input Block Size");
            app.mark_doc("8", block_size_label.rect);
            egui::ComboBox::from_id_salt("Block Size")
                .selected_text(format!("{:}", app.block_size))
                .show_ui(ui, |ui| {
                    ui.selectable_value(&mut app.block_size, BlockSizeOptions::B512, "512");
                    ui.selectable_value(&mut app.block_size, BlockSizeOptions::B4096, "4096");
                });
        });
        ui.separator();
    });
}

fn output_prefix_for_device(
    base_prefix: &str,
    selected_device_id: u64,
    device_id: u64,
    num_devices: usize,
    decode_all_devices: bool,
) -> String {
    if decode_all_devices && num_devices > 1 {
        let selected_device_prefix = format!("{:016x}", selected_device_id);
        if base_prefix.is_empty() || base_prefix == selected_device_prefix {
            format!("{:016x}", device_id)
        } else {
            format!("{base_prefix}_{device_id:016x}")
        }
    } else {
        base_prefix.to_string()
    }
}

fn merge_block_stats(
    combined: &mut HashMap<blocks::BlockTypes, usize>,
    stats: HashMap<blocks::BlockTypes, usize>,
) {
    for (block_type, count) in stats {
        *combined.entry(block_type).or_default() += count;
    }
}

fn merge_tdf_stats(
    combined: &mut HashMap<Option<u64>, HashMap<u16, usize>>,
    stats: HashMap<Option<u64>, HashMap<u16, usize>>,
) {
    for (remote_id, tdfs) in stats {
        let combined_tdfs = combined.entry(remote_id).or_default();
        for (tdf_id, count) in tdfs {
            *combined_tdfs.entry(tdf_id).or_default() += count;
        }
    }
}

fn start_button(app: &mut MyApp, ui: &mut egui::Ui) {
    let start_button = egui::Button::new("DECODE")
        .fill(egui::Color32::from_rgb(0, 0x89, 0x47))
        .min_size((100.0, ui.available_height()).into());
    ui.add_space(ui.available_width() - 100.0);
    let response = ui
        .add_enabled(
            app.runner_thread.is_none() && app.input_path.is_some(),
            start_button,
        )
        .on_hover_text("Decode");
    app.mark_doc("9", response.rect);
    if response.clicked() {
        // Reset progress bars
        app.progress_copy.reset();
        app.progress_devices.reset();
        app.progress_decode.reset();
        app.progress_merge.reset();
        app.block_stats = None;
        app.tdf_stats = None;
        app.output_files = None;

        let input_path = app.input_path.as_ref().unwrap();
        let device_jobs = if input_path.is_dir() {
            let iot_bin_files: HashMap<u64, Vec<PathBuf>> =
                infuse_decoder::fs_util::find_infuse_iot_files(input_path).unwrap();

            if iot_bin_files.is_empty() {
                let input_folder = input_path.display().to_string();
                app.runner_thread = Some(thread::spawn(move || {
                    return std::result::Result::Err(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        format!("No valid files found in '{}'", input_folder),
                    ));
                }));
                return;
            }

            let mut jobs: Vec<(u64, Vec<PathBuf>)> = if app.decode_all_devices {
                iot_bin_files.into_iter().collect()
            } else {
                match iot_bin_files.get(&app.device_id) {
                    Some(files) => vec![(app.device_id, files.clone())],
                    None => {
                        let device_id = app.device_id;
                        app.runner_thread = Some(thread::spawn(move || {
                            return std::result::Result::Err(std::io::Error::new(
                                std::io::ErrorKind::NotFound,
                                format!("No files found for device ID {device_id:016x}"),
                            ));
                        }));
                        return;
                    }
                }
            };
            jobs.sort_by_key(|(device_id, _)| *device_id);
            jobs
        } else {
            vec![(app.device_id, vec![input_path.clone()])]
        };
        let num_devices = device_jobs.len();
        infuse_decoder::ProgressReporter::start(
            &mut app.progress_devices,
            "Devices decoded",
            num_devices,
        );
        let run_args = device_jobs
            .into_iter()
            .map(|(device_id, input_files)| infuse_decoder::RunArgs {
                device_id,
                block_size: app.block_size as usize,
                input_files,
                output_folder: app.output_folder.clone(),
                output_prefix: output_prefix_for_device(
                    &app.output_prefix,
                    app.device_id,
                    device_id,
                    num_devices,
                    app.decode_all_devices,
                ),
                output_unix_time: app.time_mode == TimeOutput::UNIX,
                output_format: app.output_format,
                merge_output_files: app.linearize_output_files,
                max_readings_per_output_file: app.max_readings_per_output_file,
                copy_reporter: app.progress_copy.clone(),
                decode_reporter: app.progress_decode.clone(),
                merge_reporter: app.progress_merge.clone(),
            })
            .collect::<Vec<_>>();
        let mut device_reporter = app.progress_devices.clone();

        app.runner_thread = Some(thread::spawn(move || {
            let mut combined_block_stats = HashMap::new();
            let mut combined_tdf_stats = HashMap::new();
            let mut combined_output_files = Vec::new();

            for mut run_args in run_args {
                let (block_stats, tdf_stats, mut output_files) =
                    infuse_decoder::run(&mut run_args)?;
                merge_block_stats(&mut combined_block_stats, block_stats);
                merge_tdf_stats(&mut combined_tdf_stats, tdf_stats);
                combined_output_files.append(&mut output_files);
                infuse_decoder::ProgressReporter::increment(&mut device_reporter, 1);
            }

            Ok::<_, io::Error>((
                combined_block_stats,
                combined_tdf_stats,
                combined_output_files,
            ))
        }));
    };
}

fn copyright_bar(ui: &mut egui::Ui) {
    egui::Grid::new("copyright_bar")
        .num_columns(2)
        .show(ui, |ui| {
            ui.with_layout(egui::Layout::right_to_left(egui::Align::LEFT), |ui| {
                ui.label(format!(
                    "v{} © Embeint Inc 2024-{}",
                    env!("CARGO_PKG_VERSION"),
                    Utc::now().year()
                ));
            });

            ui.with_layout(egui::Layout::right_to_left(egui::Align::RIGHT), |ui| {
                egui::widgets::global_theme_preference_buttons(ui);
            });
        });
}

fn hashmap_sort<T>(hashmap: HashMap<T, usize>) -> Vec<(T, usize)> {
    let mut sorted: Vec<(T, usize)> = hashmap.into_iter().collect();
    sorted.sort_by(|a, b| b.1.cmp(&a.1));
    sorted
}

fn draw_right_edge(ui: &mut egui::Ui, width: f32, color: egui::Color32) {
    let available_rect = ui.available_rect_before_wrap();
    let painter = ui.painter();
    let right_edge = [
        egui::Pos2::new(available_rect.right() + 5.0, available_rect.top()),
        egui::Pos2::new(available_rect.right() + 5.0, available_rect.bottom()),
    ];

    painter.line_segment(right_edge, egui::Stroke::new(width, color));
}

fn draw_tdf_table(ui: &mut egui::Ui, id: Option<u64>, tdfs: &HashMap<u16, usize>) {
    if let Some(id_val) = id {
        ui.heading(format!("{:016x}", id_val));
    }
    TableBuilder::new(ui)
        .id_salt(id)
        .column(Column::auto())
        .column(Column::auto())
        .header(5.0, |mut header| {
            header.col(|ui| {
                ui.heading("TDF");
            });
            header.col(|ui| {
                ui.with_layout(egui::Layout::top_down_justified(egui::Align::RIGHT), |ui| {
                    ui.heading("Count");
                });
            });
        })
        .body(|mut body| {
            for (id, count) in hashmap_sort(tdfs.clone()).iter() {
                body.row(5.0, |mut row| {
                    row.col(|ui| {
                        ui.add(
                            egui::Label::new(tdf::decoders::tdf_name(id))
                                .wrap_mode(egui::TextWrapMode::Truncate),
                        );
                    });
                    row.col(|ui| {
                        ui.with_layout(
                            egui::Layout::top_down_justified(egui::Align::RIGHT),
                            |ui| {
                                ui.label(format!("{count}"));
                            },
                        );
                    });
                });
            }
        });
}

fn gui_stats(app: &mut MyApp, ui: &mut egui::Ui) {
    egui::CentralPanel::default().show_inside(ui, |ui| {
        ui.columns_const(|[col_blocks, col_tdfs, col_files]| {
            col_blocks.push_id(0, |ui| {
                draw_right_edge(ui, 1.0, egui::Color32::GRAY);

                TableBuilder::new(ui)
                    .striped(true)
                    .column(Column::auto())
                    .column(Column::auto())
                    .header(5.0, |mut header| {
                        header.col(|ui| {
                            ui.heading("Block Type");
                        });
                        header.col(|ui| {
                            ui.with_layout(
                                egui::Layout::top_down_justified(egui::Align::RIGHT),
                                |ui| {
                                    ui.heading("Count");
                                },
                            );
                        });
                    })
                    .body(|mut body| {
                        if let Some(block) = app.block_stats.as_ref() {
                            for (block, count) in block.iter() {
                                body.row(5.0, |mut row| {
                                    row.col(|ui| {
                                        ui.label(format!("{block}"));
                                    });
                                    row.col(|ui| {
                                        ui.with_layout(
                                            egui::Layout::top_down_justified(egui::Align::RIGHT),
                                            |ui| {
                                                ui.label(format!("{count}"));
                                            },
                                        );
                                    });
                                });
                            }
                        }
                    });
            });

            col_tdfs.push_id(1, |ui| {
                draw_right_edge(ui, 1.0, egui::Color32::GRAY);

                if let Some(tdf_per_id) = app.tdf_stats.as_ref() {
                    // Show the loval TDFs first
                    if let Some(tdfs) = tdf_per_id.get(&None) {
                        draw_tdf_table(ui, None, tdfs);
                    }
                    for (id, tdfs) in tdf_per_id.iter() {
                        if id.is_some() {
                            ui.separator();
                            draw_tdf_table(ui, *id, tdfs);
                        }
                    }
                }
            });

            col_files.push_id(2, |ui| {
                ui.heading("Output Files");

                let scroll_height = (ui.clip_rect().bottom() - ui.cursor().top()).max(0.0);
                egui::ScrollArea::vertical()
                    .id_salt("OutputFiles")
                    .auto_shrink([false, false])
                    .max_height(scroll_height)
                    .show(ui, |ui| {
                        ui.set_width(ui.available_width());

                        if let Some(files) = app.output_files.as_ref() {
                            for file in files {
                                let name = file.file_name().unwrap().to_str().unwrap();
                                ui.add(
                                    egui::Label::new(name).wrap_mode(egui::TextWrapMode::Truncate),
                                );
                            }
                        }
                    });
            });
        });
    });
}

impl eframe::App for MyApp {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        if let Some(doc_capture) = &mut self.doc_capture {
            doc_capture.reset_markers();
        }

        // Check if executing work has completed
        if let Some(handle) = self.runner_thread.as_ref() {
            if handle.is_finished() {
                let res = self.runner_thread.take().unwrap().join().unwrap();
                match res {
                    Ok((block_stats, tdf_stats, output_files)) => {
                        let mut files = output_files.clone();
                        files.sort();
                        self.block_stats = Some(hashmap_sort(block_stats));
                        self.tdf_stats = Some(tdf_stats);
                        self.output_files = Some(files);
                    }
                    Err(e) => {
                        self.error_msg = Some(if e.kind() == std::io::ErrorKind::NotFound {
                            e.to_string()
                        } else {
                            format!("{e:?}")
                        });
                    }
                }
            }
            // Decoding is running, request periodic repaints
            ui.request_repaint_after(core::time::Duration::from_millis(100));
        }

        if let Some(msg) = &self.error_msg.clone() {
            egui::Window::new("Decoding Error")
                .collapsible(false)
                .resizable(false)
                .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
                .show(ui.ctx(), |ui| {
                    ui.label(msg);
                    ui.add_space(8.0);
                    if ui.button("OK").clicked() {
                        self.error_msg = None;
                    }
                });
            println!();
        }

        egui::Panel::top("top_panel").show_inside(ui, |ui| {
            ui.horizontal(|ui| {
                if self.is_doc_capture() {
                    ui.add_space(DOC_MARKER_GUTTER);
                }
                core_options(self, ui);
                start_button(self, ui);
            });
            ui.separator();
            decode_options(self, ui);
        });

        egui::Panel::bottom("bottom_panel").show_inside(ui, |ui| {
            copyright_bar(ui);
        });

        egui::Panel::top("progress_panel").show_inside(ui, |ui| {
            ui.add_space(5.0);
            egui::Grid::new("progress_bars")
                .num_columns(2)
                .show(ui, |ui| {
                    self.progress_copy.draw(ui);
                    ui.end_row();
                    if self.decode_all_devices {
                        self.progress_devices.draw_count(ui);
                        ui.end_row();
                    }
                    self.progress_decode.draw(ui);
                    ui.end_row();
                    self.progress_merge.draw(ui);
                    ui.end_row();
                });
            ui.add_space(5.0);
        });
        gui_stats(self, ui);
        self.handle_doc_capture(ui.ctx());
    }
}

fn load_icon() -> IconData {
    let image_data = include_bytes!("../assets/icon.png");
    let img = image::load_from_memory_with_format(image_data, image::ImageFormat::Png)
        .expect("Failed to load embedded icon image");
    let (width, height) = img.dimensions();
    let rgba = img.to_rgba8().into_raw();
    IconData {
        rgba,
        width,
        height,
    }
}

fn main() -> eframe::Result {
    let icon = load_icon();
    let mut viewport = egui::viewport::ViewportBuilder::default().with_icon(icon);
    if doc_capture_enabled() {
        viewport = viewport.with_inner_size([1160.0, 680.0]);
    }

    let options = eframe::NativeOptions {
        viewport,
        ..Default::default()
    };

    eframe::run_native(
        "Infuse-IoT Data Decoder",
        options,
        Box::new(|_cc| Ok(Box::new(MyApp::default()))),
    )?;
    Ok(())
}
