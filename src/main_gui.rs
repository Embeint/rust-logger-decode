#![windows_subsystem = "windows"]

use std::env;
use std::sync::{Arc, Mutex};
use std::thread;
use std::{collections::HashMap, path::PathBuf};

use eframe::egui::{self, IconData};
use egui_extras::{Column, TableBuilder};
use image::GenericImageView;
use rfd::FileDialog;

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
    time_mode: TimeOutput,
    device_id: u64,
    input_path: Option<PathBuf>,
    input_files: Option<HashMap<u64, Vec<PathBuf>>>,
    output_folder: PathBuf,
    output_prefix: String,
    progress_copy: SliderState,
    progress_decode: SliderState,
    progress_merge: SliderState,
    block_stats: Option<Vec<(blocks::BlockTypes, usize)>>,
    tdf_stats: Option<Vec<(u16, usize)>>,
    output_files: Option<Vec<PathBuf>>,
    runner_thread: Option<
        std::thread::JoinHandle<
            Result<
                (
                    HashMap<blocks::BlockTypes, usize>,
                    HashMap<u16, usize>,
                    Vec<PathBuf>,
                ),
                std::io::Error,
            >,
        >,
    >,
}
use directories::UserDirs;

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

        Self {
            time_mode: TimeOutput::UTC,
            device_id: 0,
            input_path: None,
            input_files: None,
            output_folder: default_out.unwrap(),
            output_prefix: String::from(""),
            progress_copy: SliderState::new("Copying files"),
            progress_decode: SliderState::new("Decoding files"),
            progress_merge: SliderState::new("Merging output"),
            block_stats: None,
            tdf_stats: None,
            output_files: None,
            runner_thread: None,
        }
    }
}

fn trimmed_label(label: &String, max_len: usize) -> String {
    if label.len() > max_len {
        let idx = label.len() - (max_len - 3);
        format!("...{}", &label[idx..])
    } else {
        label.clone()
    }
}

fn core_options(app: &mut MyApp, _ctx: &egui::Context, ui: &mut egui::Ui) {
    egui::Grid::new("folder_selection")
        .num_columns(2)
        .show(ui, |ui| {
            let folder_str = app.output_folder.display().to_string();
            ui.label("Output folder");
            ui.label(egui::RichText::new(trimmed_label(&folder_str, 48)).code());
            if ui.button("Folder").clicked() {
                if let Some(folder) = FileDialog::new()
                    .set_directory(app.output_folder.as_path())
                    .pick_folder()
                {
                    app.output_folder = folder;
                }
            }
            ui.end_row();

            let folder_str = match app.input_path.as_ref() {
                Some(folder) => folder.display().to_string(),
                None => String::from("N/A"),
            };

            ui.label("Input folder/file");
            ui.label(egui::RichText::new(trimmed_label(&folder_str, 48)).code());

            ui.horizontal(|ui| {
                if ui.button("Folder").clicked() {
                    if let Some(folder) = FileDialog::new().pick_folder() {
                        match infuse_decoder::fs_util::find_infuse_iot_files(&folder) {
                            Ok(files) => {
                                app.device_id = *files.keys().next().unwrap_or(&0);
                                app.output_prefix = format!("{:016x}", app.device_id);
                                app.input_path = Some(folder);
                                app.input_files = Some(files);
                            }
                            _ => {}
                        }
                    }
                }
                if ui.button("File").clicked() {
                    if let Some(file) = FileDialog::new().pick_file() {
                        let mut h = HashMap::new();
                        h.insert(0, vec![file.clone()]);

                        app.device_id = 0;
                        app.output_prefix = format!("{}", 0);
                        app.input_path = Some(file);
                        app.input_files = Some(h);
                    }
                }
            });
            ui.end_row();

            ui.label("Device ID");
            if let Some(file_list) = &app.input_files {
                ui.add_enabled_ui(file_list.len() > 1, |ui| {
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
            }
            ui.end_row();

            ui.label("Output Prefix");
            ui.text_edit_singleline(&mut app.output_prefix);
            ui.label(format!("(e.g. {}_BATTERY_STATE.csv)", app.output_prefix));
            ui.end_row();
        });
}

fn decode_options(app: &mut MyApp, _ctx: &egui::Context, ui: &mut egui::Ui) {
    ui.vertical(|ui| {
        ui.label("Time Output Format");
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
}

fn start_button(app: &mut MyApp, _ctx: &egui::Context, ui: &mut egui::Ui) {
    let start_button = egui::Button::new("DECODE")
        .fill(egui::Color32::from_rgb(0, 0x89, 0x47))
        .min_size((100.0, ui.available_height()).into());
    ui.add_space(ui.available_width() - 100.0);
    if ui
        .add_enabled(
            app.runner_thread.is_none() && app.input_path.is_some(),
            start_button,
        )
        .clicked()
    {
        // Reset progress bars
        app.progress_copy.reset();
        app.progress_decode.reset();
        app.progress_merge.reset();
        app.block_stats = None;
        app.tdf_stats = None;
        app.output_files = None;

        let p = app.input_path.as_ref().unwrap();
        let files = if p.is_dir() {
            let iot_bin_files: HashMap<u64, Vec<PathBuf>> =
                infuse_decoder::fs_util::find_infuse_iot_files(&app.input_path.as_ref().unwrap())
                    .unwrap();
            iot_bin_files.get(&app.device_id).unwrap().clone()
        } else {
            vec![p.clone()]
        };

        let mut run_args = infuse_decoder::RunArgs {
            device_id: app.device_id,
            input_files: files,
            output_folder: app.output_folder.clone(),
            output_prefix: app.output_prefix.clone(),
            output_unix_time: app.time_mode == TimeOutput::UNIX,
            copy_reporter: app.progress_copy.clone(),
            decode_reporter: app.progress_decode.clone(),
            merge_reporter: app.progress_merge.clone(),
        };

        // Spawn the thread to run the decode process
        app.runner_thread = Some(thread::spawn(move || infuse_decoder::run(&mut run_args)));
    };
}

fn copyright_bar(_ctx: &egui::Context, ui: &mut egui::Ui) {
    egui::Grid::new("copyright_bar")
        .num_columns(2)
        .show(ui, |ui| {
            ui.with_layout(egui::Layout::right_to_left(egui::Align::LEFT), |ui| {
                ui.label("© Embeint Inc 2024");
            });

            ui.with_layout(egui::Layout::right_to_left(egui::Align::RIGHT), |ui| {
                egui::widgets::global_dark_light_mode_buttons(ui);
            });
        });
}

fn gui_stats(app: &mut MyApp, ctx: &egui::Context) {
    egui::SidePanel::left("TDF Stats")
        .resizable(false)
        .show(ctx, |ui| {
            TableBuilder::new(ui)
                .striped(true)
                .column(Column::remainder())
                .column(Column::remainder())
                .header(5.0, |mut header| {
                    header.col(|ui| {
                        ui.heading("TDF");
                    });
                    header.col(|ui| {
                        ui.heading("Count");
                    });
                })
                .body(|mut body| {
                    if let Some(tdf) = app.tdf_stats.as_ref() {
                        for (id, count) in tdf.iter() {
                            body.row(5.0, |mut row| {
                                row.col(|ui| {
                                    ui.label(tdf::decoders::tdf_name(id));
                                });
                                row.col(|ui| {
                                    ui.label(format!("{count}"));
                                });
                            });
                        }
                    }
                });
        });

    egui::SidePanel::left("Block Stats")
        .resizable(false)
        .show(ctx, |ui| {
            TableBuilder::new(ui)
                .striped(true)
                .column(Column::remainder())
                .column(Column::remainder())
                .header(5.0, |mut header| {
                    header.col(|ui| {
                        ui.heading("Block Type");
                    });
                    header.col(|ui| {
                        ui.heading("Count");
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
                                    ui.label(format!("{count}"));
                                });
                            });
                        }
                    }
                });
        });

    egui::CentralPanel::default().show(ctx, |ui| {
        TableBuilder::new(ui)
            .striped(true)
            .column(Column::remainder())
            .header(5.0, |mut header| {
                header.col(|ui| {
                    ui.heading("Output Files");
                });
            })
            .body(|mut body| {
                if let Some(files) = app.output_files.as_ref() {
                    for file in files.iter() {
                        body.row(5.0, |mut row| {
                            row.col(|ui| {
                                let name =
                                    format!("{}", file.file_name().unwrap().to_str().unwrap());
                                ui.label(egui::RichText::new(name));
                            });
                        });
                    }
                }
            });
    });
}

fn hashmap_sort<T>(hashmap: HashMap<T, usize>) -> Vec<(T, usize)> {
    let mut sorted: Vec<(T, usize)> = hashmap.into_iter().collect();
    sorted.sort_by(|a, b| b.1.cmp(&a.1));
    sorted
}

impl eframe::App for MyApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        // Check if executing work has completed
        if let Some(handle) = self.runner_thread.as_ref() {
            if handle.is_finished() {
                let res = self.runner_thread.take().unwrap().join().unwrap();
                match res {
                    Ok((block_stats, tdf_stats, output_files)) => {
                        self.block_stats = Some(hashmap_sort(block_stats));
                        self.tdf_stats = Some(hashmap_sort(tdf_stats));
                        self.output_files = Some(output_files);
                    }
                    _ => {}
                }
            }
        }

        egui::TopBottomPanel::top("top_panel").show(ctx, |ui| {
            ui.horizontal(|ui| {
                core_options(self, ctx, ui);
                start_button(self, ctx, ui);
            });
            ui.separator();
            decode_options(self, ctx, ui);
        });

        egui::TopBottomPanel::bottom("bottom_panel").show(ctx, |ui| {
            copyright_bar(ctx, ui);
        });

        egui::TopBottomPanel::top("progress_panel").show(ctx, |ui| {
            ui.add_space(5.0);
            egui::Grid::new("progress_bars")
                .num_columns(2)
                .show(ui, |ui| {
                    self.progress_copy.draw(ui);
                    ui.end_row();
                    self.progress_decode.draw(ui);
                    ui.end_row();
                    self.progress_merge.draw(ui);
                    ui.end_row();
                });
            ui.add_space(5.0);
        });
        gui_stats(self, ctx);
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

    let options = eframe::NativeOptions {
        viewport: egui::viewport::ViewportBuilder::default().with_icon(icon),
        ..Default::default()
    };

    eframe::run_native(
        "Infuse-IoT Data Decoder",
        options,
        Box::new(|_cc| Ok(Box::new(MyApp::default()))),
    )?;
    Ok(())
}
