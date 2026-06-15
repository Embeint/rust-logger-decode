use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

pub(crate) type OutputKey = (Option<u64>, u16);

pub(crate) fn worker_output_path(
    output_folder: &Path,
    output_prefix: &str,
    remote_id: Option<u64>,
    tdf_id: u16,
    decoder_idx: usize,
    extension: &str,
) -> PathBuf {
    let mut fname_parts = Vec::new();
    if !output_prefix.is_empty() {
        fname_parts.push(output_prefix.to_string());
    }
    if let Some(id) = remote_id {
        fname_parts.push(format!("{id:016x}"));
    }
    fname_parts.push(tdf::decoders::tdf_name(&tdf_id).to_string());

    output_folder.join(format!(
        "{}_{:05}.{}",
        fname_parts.join("_"),
        decoder_idx,
        extension
    ))
}

pub(crate) fn merged_output_path(
    output_folder: &Path,
    output_prefix: &str,
    remote_id: Option<u64>,
    tdf_id: u16,
    part_idx: Option<usize>,
    extension: &str,
) -> PathBuf {
    let id_prefix = match remote_id {
        Some(id) => format!("_{id:016x}"),
        None => String::new(),
    };
    let name = tdf::decoders::tdf_name(&tdf_id);

    match part_idx {
        Some(part_idx) => output_folder.join(format!(
            "{}{}_{}_{:05}.{}",
            output_prefix, id_prefix, name, part_idx, extension
        )),
        None => output_folder.join(format!(
            "{}{}_{}.{}",
            output_prefix, id_prefix, name, extension
        )),
    }
}

pub(crate) fn rename_first_file_if_splitting(
    part_idx: usize,
    output_files: &mut [PathBuf],
    plain_path: PathBuf,
    numbered_path: PathBuf,
) -> io::Result<()> {
    if part_idx != 1 || plain_path == numbered_path || !plain_path.exists() {
        return Ok(());
    }

    match fs::rename(&plain_path, &numbered_path) {
        Ok(()) => {}
        Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
            fs::remove_file(&numbered_path)?;
            fs::rename(&plain_path, &numbered_path)?;
        }
        Err(err) => return Err(err),
    }

    if let Some(path) = output_files.first_mut() {
        *path = numbered_path;
    }

    Ok(())
}

pub(crate) fn touch_output_count(output_cnt: &mut HashMap<OutputKey, usize>, key: OutputKey) {
    output_cnt.entry(key).or_default();
}

pub(crate) fn increment_output_count(output_cnt: &mut HashMap<OutputKey, usize>, key: OutputKey) {
    *output_cnt.entry(key).or_default() += 1;
}

pub(crate) fn written(output_cnt: &HashMap<OutputKey, usize>, key: OutputKey) -> usize {
    output_cnt.get(&key).copied().unwrap_or_default()
}
