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
