use regex::Regex;
use std::{collections::HashMap, fs, io, path::PathBuf};

pub fn find_infuse_iot_files(dir: &PathBuf) -> io::Result<HashMap<u64, Vec<PathBuf>>> {
    // Regular expression to match the pattern "infuse_%016x_%d.bin"
    let pattern = Regex::new(r"^infuse_([0-9a-fA-F]{16})_[0-9]+\.bin$").unwrap();
    let mut matching_files: HashMap<u64, Vec<PathBuf>> = HashMap::new();

    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() {
            if let Some(file_name) = path.file_name().and_then(|n| n.to_str()) {
                if let Some(captures) = pattern.captures(file_name) {
                    let device_id = u64::from_str_radix(&captures[1], 16).unwrap();

                    match matching_files.get_mut(&device_id) {
                        Some(path_list) => {
                            path_list.push(path);
                        }
                        None => {
                            matching_files.insert(device_id, vec![path]);
                        }
                    }
                }
            }
        }
    }

    Ok(matching_files)
}

#[cfg(target_os = "windows")]
pub fn path_on_removable_drive(path: &std::path::PathBuf) -> bool {
    use std::os::windows::ffi::OsStrExt;
    use winapi::um::fileapi::GetDriveTypeW;
    use winapi::um::winbase::{DRIVE_CDROM, DRIVE_REMOVABLE};

    let drive = path.components().next().unwrap().as_os_str();
    let drive_wide: Vec<u16> = drive.encode_wide().chain(Some(0)).collect();

    let drive_type = unsafe { GetDriveTypeW(drive_wide.as_ptr()) };
    drive_type == DRIVE_REMOVABLE || drive_type == DRIVE_CDROM
}

#[cfg(target_os = "linux")]
fn path_on_removable_drive(path: &PathBuf) -> bool {
    use std::fs;

    if let Ok(metadata) = fs::metadata(path) {
        if let Ok(dev) = metadata.dev() {
            if let Ok(partitions) = fs::read_to_string("/proc/partitions") {
                for line in partitions.lines() {
                    if line.contains(&format!("{:02x}", dev)) {
                        return line.contains("sd");
                    }
                }
            }
        }
    }
    false
}

#[cfg(target_os = "macos")]
fn path_on_removable_drive(path: &PathBuf) -> bool {
    use std::process::Command;

    if let Ok(output) = Command::new("diskutil").arg("info").arg(path).output() {
        if let Ok(info) = String::from_utf8(output.stdout) {
            return info.contains("Removable Media: Yes");
        }
    }
    false
}
