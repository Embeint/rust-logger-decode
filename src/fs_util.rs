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

    if !matching_files.is_empty() {
        return Ok(matching_files);
    }

    // Fallback for files that include a standalone 16-character hex ID and end in ".bin".
    let fallback_pattern =
        Regex::new(r"(?:^|[^0-9a-fA-F])([0-9a-fA-F]{16})(?:[^0-9a-fA-F].*)?\.bin$").unwrap();

    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() {
            if let Some(file_name) = path.file_name().and_then(|n| n.to_str()) {
                if let Some(captures) = fallback_pattern.captures(file_name) {
                    let device_id = u64::from_str_radix(&captures[1], 16).unwrap();

                    if let Some(existing_paths) = matching_files.get(&device_id) {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!(
                                "Multiple fallback files found for Infuse-IoT device ID {device_id:016x}: {:?} and {:?}",
                                existing_paths[0], path
                            ),
                        ));
                    }

                    matching_files.insert(device_id, vec![path]);
                }
            }
        }
    }

    Ok(matching_files)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs::File,
        time::{SystemTime, UNIX_EPOCH},
    };

    fn temp_dir(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("infuse_decoder_{name}_{nanos}"));
        fs::create_dir(&dir).unwrap();
        dir
    }

    fn touch(dir: &PathBuf, file_name: &str) {
        File::create(dir.join(file_name)).unwrap();
    }

    #[test]
    fn finds_current_infuse_iot_pattern() {
        let dir = temp_dir("current_pattern");
        touch(&dir, "infuse_0123456789abcdef_0.bin");
        touch(&dir, "infuse_0123456789abcdef_1.bin");
        touch(&dir, "capture_fedcba9876543210.bin");

        let files = find_infuse_iot_files(&dir).unwrap();

        assert_eq!(files.len(), 1);
        assert_eq!(files.get(&0x0123_4567_89ab_cdef).unwrap().len(), 2);

        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn falls_back_to_standalone_hex_id_bin_files() {
        let dir = temp_dir("fallback");
        touch(&dir, "capture_0123456789abcdef.bin");
        touch(&dir, "fedcba9876543210.bin");
        touch(&dir, "ignored_00123456789abcdef.bin");

        let files = find_infuse_iot_files(&dir).unwrap();

        assert_eq!(files.len(), 2);
        assert!(files.contains_key(&0x0123_4567_89ab_cdef));
        assert!(files.contains_key(&0xfedc_ba98_7654_3210));

        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn fallback_errors_on_duplicate_device_ids() {
        let dir = temp_dir("fallback_duplicate");
        touch(&dir, "capture_0123456789abcdef.bin");
        touch(&dir, "backup_0123456789abcdef.bin");

        let err = find_infuse_iot_files(&dir).unwrap_err();

        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("0123456789abcdef"));

        fs::remove_dir_all(dir).unwrap();
    }
}
