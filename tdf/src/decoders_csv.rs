use std::io::{Cursor, Error, ErrorKind, Read, Result, Write};

use byteorder::{BigEndian, LittleEndian, ReadBytesExt};

pub fn tdf_fields(tdf_id: &u16) -> Vec<&'static str> {
    match tdf_id {
        1 => vec![
            "application",
            "version.major",
            "version.minor",
            "version.revision",
            "version.build_num",
            "kv_crc",
            "blocks",
            "uptime",
            "reboots",
            "flags",
        ],
        2 => vec!["voltage_mv", "current_ua", "soc"],
        3 => vec!["temperature", "pressure", "humidity"],
        4 => vec!["temperature"],
        5 => vec!["source", "shift"],
        6 => vec![
            "reason",
            "hardware_flags",
            "count",
            "uptime",
            "param_1",
            "param_2",
            "thread",
        ],
        7 => vec![
            "application",
            "version.major",
            "version.minor",
            "version.revision",
            "version.build_num",
            "board_crc",
            "kv_crc",
            "blocks",
            "uptime",
            "reboots",
            "flags",
        ],
        8 => vec!["temperature"],
        10 => vec!["sample.x", "sample.y", "sample.z"],
        11 => vec!["sample.x", "sample.y", "sample.z"],
        12 => vec!["sample.x", "sample.y", "sample.z"],
        13 => vec!["sample.x", "sample.y", "sample.z"],
        14 => vec!["sample.x", "sample.y", "sample.z"],
        15 => vec!["sample.x", "sample.y", "sample.z"],
        16 => vec!["sample.x", "sample.y", "sample.z"],
        17 => vec!["sample.x", "sample.y", "sample.z"],
        18 => vec!["sample.x", "sample.y", "sample.z"],
        19 => vec![
            "location.latitude",
            "location.longitude",
            "location.height",
            "h_acc",
            "v_acc",
        ],
        20 => vec![
            "itow",
            "year",
            "month",
            "day",
            "hour",
            "min",
            "sec",
            "valid",
            "t_acc",
            "nano",
            "fix_type",
            "flags",
            "flags2",
            "num_sv",
            "lon",
            "lat",
            "height",
            "h_msl",
            "h_acc",
            "v_acc",
            "vel_n",
            "vel_e",
            "vel_d",
            "g_speed",
            "head_mot",
            "s_acc",
            "head_acc",
            "p_dop",
            "flags3",
            "reserved0[0]",
            "reserved0[1]",
            "reserved0[2]",
            "reserved0[3]",
            "head_veh",
            "mag_dec",
            "mag_acc",
        ],
        21 => vec![
            "cell.mcc", "cell.mnc", "cell.eci", "cell.tac", "earfcn", "status", "tech", "rsrp",
            "rsrq",
        ],
        22 => vec![
            "payload[0]",
            "payload[1]",
            "payload[2]",
            "payload[3]",
            "payload[4]",
            "payload[5]",
            "payload[6]",
            "payload[7]",
            "payload[8]",
        ],
        23 => vec!["count", "std_dev"],
        24 => vec!["value"],
        25 => vec!["algorithm_id", "algorithm_version", "output"],
        26 => vec!["error_id", "error_ctx"],
        27 => vec!["enabled"],
        28 => vec!["time_fix", "location_fix", "num_sv"],
        29 => vec!["address.type", "address.val", "connected"],
        30 => vec!["address.type", "address.val", "rssi"],
        31 => vec!["address.type", "address.val", "throughput"],
        32 => vec!["algorithm_id", "algorithm_version", "classes"],
        33 => vec!["algorithm_id", "algorithm_version", "values"],
        34 => vec![
            "cell.mcc",
            "cell.mnc",
            "cell.eci",
            "cell.tac",
            "earfcn",
            "rsrp",
            "rsrq",
            "neighbours.earfcn",
            "neighbours.pci",
            "neighbours.time_diff",
            "neighbours.rsrp",
            "neighbours.rsrq",
        ],
        35 => vec!["bssid.val", "channel", "rsrp"],
        36 => vec!["cosine"],
        37 => vec![
            "lat",
            "lon",
            "height",
            "h_acc",
            "v_acc",
            "h_speed",
            "h_speed_acc",
            "v_speed",
            "v_speed_acc",
            "head_mot",
            "head_acc",
            "year",
            "month",
            "day",
            "hour",
            "min",
            "sec",
            "ms",
            "p_dop",
            "h_dop",
            "v_dop",
            "t_dop",
            "flags",
            "num_sv",
        ],
        38 => vec!["charge"],
        39 => vec!["infuse_id", "rssi"],
        40 => vec!["val"],
        41 => vec!["val"],
        42 => vec!["val"],
        43 => vec!["timestamp", "event"],
        44 => vec!["snr", "rssi", "payload"],
        45 => vec!["payload"],
        46 => vec!["tdf_id", "frequency"],
        47 => vec!["tdf_id", "period"],
        48 => vec![
            "network.bssid",
            "network.band",
            "network.channel",
            "network.iface_mode",
            "network.link_mode",
            "network.security",
            "network.rssi",
            "network.beacon_interval",
            "network.twt_capable",
        ],
        49 => vec!["reason"],
        50 => vec!["reason"],
        51 => vec!["num_wifi", "num_lte"],
        52 => vec!["frame"],
        53 => vec!["voltage"],
        54 => vec!["soc"],
        55 => vec!["state"],
        56 => vec!["state"],
        57 => vec!["state", "duration"],
        58 => vec!["val"],
        59 => vec!["val"],
        60 => vec!["left", "right"],
        61 => vec!["key", "value"],
        62 => vec!["pressure"],
        _ => vec!["unknown"],
    }
}

fn tdf_field_read_string_to_str(
    cursor: &mut Cursor<&[u8]>,
    cursor_start: u64,
    num: u8,
    size: u8,
) -> Result<String> {
    let buf = crate::decoders::tdf_field_read_string(cursor, cursor_start, num, size)?;

    match String::from_utf8(buf) {
        Ok(val) => Ok(format!("\"{}\"", val.trim_matches(char::from(0)))),
        Err(..) => Ok(String::from("\"\"")),
    }
}

fn tdf_field_read_vla_to_str(
    cursor: &mut Cursor<&[u8]>,
    cursor_start: u64,
    size: u8,
) -> Result<String> {
    let buf = crate::decoders::tdf_field_read_vla(cursor, cursor_start, size)?;

    Ok(format!("{}", hex::encode(buf)))
}

#[allow(dead_code)]
fn tdf_field_read_fixed_bytes_to_hex(cursor: &mut Cursor<&[u8]>, num: usize) -> Result<String> {
    let mut buf = vec![0u8; num];
    cursor.read_exact(&mut buf)?;

    Ok(format!("{}", hex::encode(buf)))
}

fn tdf_variable_item_count(size: u8, base_size: usize, item_size: usize) -> Result<usize> {
    if (size as usize) < base_size {
        return Result::Err(Error::new(
            ErrorKind::InvalidData,
            "Read underflow, corrupt data/metadata",
        ));
    }

    let bytes_remaining = size as usize - base_size;
    if bytes_remaining % item_size != 0 {
        return Result::Err(Error::new(
            ErrorKind::InvalidData,
            "Variable-length array does not align to element size",
        ));
    }

    Ok(bytes_remaining / item_size)
}

#[inline(always)]
fn write_int<I: itoa::Integer>(out: &mut Vec<u8>, val: I) {
    let mut buf = itoa::Buffer::new();
    out.extend_from_slice(buf.format(val).as_bytes());
}

/// Decode a TDF and append its CSV representation to `out`.
///
/// On error, `out` may contain a partially written row.
pub fn tdf_write_csv(
    tdf_id: &u16,
    size: u8,
    cursor: &mut Cursor<&[u8]>,
    out: &mut Vec<u8>,
) -> Result<()> {
    let cursor_start = cursor.position();

    match tdf_id {
        1 => {
            write!(out, "0x{:08x}", cursor.read_u32::<LittleEndian>()?)?;
            out.push(b',');
            write_int(out, cursor.read_u8()?);
            out.push(b',');
            write_int(out, cursor.read_u8()?);
            out.push(b',');
            write_int(out, cursor.read_u16::<LittleEndian>()?);
            out.push(b',');
            write!(out, "0x{:08x}", cursor.read_u32::<LittleEndian>()?)?;
            out.push(b',');
            write!(out, "0x{:08x}", cursor.read_u32::<LittleEndian>()?)?;
            out.push(b',');
            write_int(out, cursor.read_u32::<LittleEndian>()?);
            out.push(b',');
            write_int(out, cursor.read_u32::<LittleEndian>()?);
            out.push(b',');
            write_int(out, cursor.read_u16::<LittleEndian>()?);
            out.push(b',');
            write!(out, "0x{:02x}", cursor.read_u8()?)?;
        }
        2 => {
            write_int(out, cursor.read_u32::<LittleEndian>()?);
            out.push(b',');
            write_int(out, cursor.read_i32::<LittleEndian>()?);
            out.push(b',');
            write_int(out, cursor.read_u8()?);
        }
        3 => {
            write!(
                out,
                "{}",
                cursor.read_i32::<LittleEndian>()? as f64 / 1000.0
            )?;
            out.push(b',');
            write!(
                out,
                "{}",
                cursor.read_u32::<LittleEndian>()? as f64 / 1000.0
            )?;
            out.push(b',');
            write!(out, "{}", cursor.read_u16::<LittleEndian>()? as f64 / 100.0)?;
        }
        4 => {
            write!(
                out,
                "{}",
                cursor.read_i32::<LittleEndian>()? as f64 / 1000.0
            )?;
        }
        5 => {
            write_int(out, cursor.read_u8()?);
            out.push(b',');
            write!(
                out,
                "{}",
                cursor.read_i32::<LittleEndian>()? as f64 / 1000000.0
            )?;
        }
        6 => {
            write_int(out, cursor.read_u8()?);
            out.push(b',');
            write!(out, "0x{:08x}", cursor.read_u32::<LittleEndian>()?)?;
            out.push(b',');
            write_int(out, cursor.read_u32::<LittleEndian>()?);
            out.push(b',');
            write_int(out, cursor.read_u32::<LittleEndian>()?);
            out.push(b',');
            write!(out, "0x{:08x}", cursor.read_u32::<LittleEndian>()?)?;
            out.push(b',');
            write!(out, "0x{:08x}", cursor.read_u32::<LittleEndian>()?)?;
            out.push(b',');
            out.extend_from_slice(
                tdf_field_read_string_to_str(cursor, cursor_start, 8, size)?.as_bytes(),
            );
        }
        7 => {
            write!(out, "0x{:08x}", cursor.read_u32::<LittleEndian>()?)?;
            out.push(b',');
            write_int(out, cursor.read_u8()?);
            out.push(b',');
            write_int(out, cursor.read_u8()?);
            out.push(b',');
            write_int(out, cursor.read_u16::<LittleEndian>()?);
            out.push(b',');
            write!(out, "0x{:08x}", cursor.read_u32::<LittleEndian>()?)?;
            out.push(b',');
            write!(out, "0x{:04x}", cursor.read_u16::<LittleEndian>()?)?;
            out.push(b',');
            write!(out, "0x{:08x}", cursor.read_u32::<LittleEndian>()?)?;
            out.push(b',');
            write_int(out, cursor.read_u32::<LittleEndian>()?);
            out.push(b',');
            write_int(out, cursor.read_u32::<LittleEndian>()?);
            out.push(b',');
            write_int(out, cursor.read_u16::<LittleEndian>()?);
            out.push(b',');
            write!(out, "0x{:02x}", cursor.read_u8()?)?;
        }
        8 => {
            write!(out, "{}", cursor.read_i16::<LittleEndian>()? as f64 / 100.0)?;
        }
        10 => {
            write_int(out, cursor.read_i16::<LittleEndian>()?);
            out.push(b',');
            write_int(out, cursor.read_i16::<LittleEndian>()?);
            out.push(b',');
            write_int(out, cursor.read_i16::<LittleEndian>()?);
        }
        11 => {
            write_int(out, cursor.read_i16::<LittleEndian>()?);
            out.push(b',');
            write_int(out, cursor.read_i16::<LittleEndian>()?);
            out.push(b',');
            write_int(out, cursor.read_i16::<LittleEndian>()?);
        }
        12 => {
            write_int(out, cursor.read_i16::<LittleEndian>()?);
            out.push(b',');
            write_int(out, cursor.read_i16::<LittleEndian>()?);
            out.push(b',');
            write_int(out, cursor.read_i16::<LittleEndian>()?);
        }
        13 => {
            write_int(out, cursor.read_i16::<LittleEndian>()?);
            out.push(b',');
            write_int(out, cursor.read_i16::<LittleEndian>()?);
            out.push(b',');
            write_int(out, cursor.read_i16::<LittleEndian>()?);
        }
        14 => {
            write_int(out, cursor.read_i16::<LittleEndian>()?);
            out.push(b',');
            write_int(out, cursor.read_i16::<LittleEndian>()?);
            out.push(b',');
            write_int(out, cursor.read_i16::<LittleEndian>()?);
        }
        15 => {
            write_int(out, cursor.read_i16::<LittleEndian>()?);
            out.push(b',');
            write_int(out, cursor.read_i16::<LittleEndian>()?);
            out.push(b',');
            write_int(out, cursor.read_i16::<LittleEndian>()?);
        }
        16 => {
            write_int(out, cursor.read_i16::<LittleEndian>()?);
            out.push(b',');
            write_int(out, cursor.read_i16::<LittleEndian>()?);
            out.push(b',');
            write_int(out, cursor.read_i16::<LittleEndian>()?);
        }
        17 => {
            write_int(out, cursor.read_i16::<LittleEndian>()?);
            out.push(b',');
            write_int(out, cursor.read_i16::<LittleEndian>()?);
            out.push(b',');
            write_int(out, cursor.read_i16::<LittleEndian>()?);
        }
        18 => {
            write_int(out, cursor.read_i16::<LittleEndian>()?);
            out.push(b',');
            write_int(out, cursor.read_i16::<LittleEndian>()?);
            out.push(b',');
            write_int(out, cursor.read_i16::<LittleEndian>()?);
        }
        19 => {
            write!(
                out,
                "{}",
                cursor.read_i32::<LittleEndian>()? as f64 / 10000000.0
            )?;
            out.push(b',');
            write!(
                out,
                "{}",
                cursor.read_i32::<LittleEndian>()? as f64 / 10000000.0
            )?;
            out.push(b',');
            write!(
                out,
                "{}",
                cursor.read_i32::<LittleEndian>()? as f64 / 1000.0
            )?;
            out.push(b',');
            write!(
                out,
                "{}",
                cursor.read_i32::<LittleEndian>()? as f64 / 1000.0
            )?;
            out.push(b',');
            write!(
                out,
                "{}",
                cursor.read_i32::<LittleEndian>()? as f64 / 1000.0
            )?;
        }
        20 => {
            write_int(out, cursor.read_u32::<LittleEndian>()?);
            out.push(b',');
            write_int(out, cursor.read_u16::<LittleEndian>()?);
            out.push(b',');
            write_int(out, cursor.read_u8()?);
            out.push(b',');
            write_int(out, cursor.read_u8()?);
            out.push(b',');
            write_int(out, cursor.read_u8()?);
            out.push(b',');
            write_int(out, cursor.read_u8()?);
            out.push(b',');
            write_int(out, cursor.read_u8()?);
            out.push(b',');
            write!(out, "0x{:02x}", cursor.read_u8()?)?;
            out.push(b',');
            write_int(out, cursor.read_u32::<LittleEndian>()?);
            out.push(b',');
            write_int(out, cursor.read_i32::<LittleEndian>()?);
            out.push(b',');
            write_int(out, cursor.read_u8()?);
            out.push(b',');
            write!(out, "0x{:02x}", cursor.read_u8()?)?;
            out.push(b',');
            write!(out, "0x{:02x}", cursor.read_u8()?)?;
            out.push(b',');
            write_int(out, cursor.read_u8()?);
            out.push(b',');
            write!(
                out,
                "{}",
                cursor.read_i32::<LittleEndian>()? as f64 / 10000000.0
            )?;
            out.push(b',');
            write!(
                out,
                "{}",
                cursor.read_i32::<LittleEndian>()? as f64 / 10000000.0
            )?;
            out.push(b',');
            write!(
                out,
                "{}",
                cursor.read_i32::<LittleEndian>()? as f64 / 1000.0
            )?;
            out.push(b',');
            write!(
                out,
                "{}",
                cursor.read_i32::<LittleEndian>()? as f64 / 1000.0
            )?;
            out.push(b',');
            write!(
                out,
                "{}",
                cursor.read_u32::<LittleEndian>()? as f64 / 1000.0
            )?;
            out.push(b',');
            write!(
                out,
                "{}",
                cursor.read_u32::<LittleEndian>()? as f64 / 1000.0
            )?;
            out.push(b',');
            write!(
                out,
                "{}",
                cursor.read_i32::<LittleEndian>()? as f64 / 1000.0
            )?;
            out.push(b',');
            write!(
                out,
                "{}",
                cursor.read_i32::<LittleEndian>()? as f64 / 1000.0
            )?;
            out.push(b',');
            write!(
                out,
                "{}",
                cursor.read_i32::<LittleEndian>()? as f64 / 1000.0
            )?;
            out.push(b',');
            write!(
                out,
                "{}",
                cursor.read_i32::<LittleEndian>()? as f64 / 1000.0
            )?;
            out.push(b',');
            write!(
                out,
                "{}",
                cursor.read_i32::<LittleEndian>()? as f64 / 100000.0
            )?;
            out.push(b',');
            write!(
                out,
                "{}",
                cursor.read_u32::<LittleEndian>()? as f64 / 1000.0
            )?;
            out.push(b',');
            write!(
                out,
                "{}",
                cursor.read_u32::<LittleEndian>()? as f64 / 100000.0
            )?;
            out.push(b',');
            write!(out, "{}", cursor.read_u16::<LittleEndian>()? as f64 / 100.0)?;
            out.push(b',');
            write!(out, "0x{:04x}", cursor.read_u16::<LittleEndian>()?)?;
            out.push(b',');
            write_int(out, cursor.read_u8()?);
            out.push(b',');
            write_int(out, cursor.read_u8()?);
            out.push(b',');
            write_int(out, cursor.read_u8()?);
            out.push(b',');
            write_int(out, cursor.read_u8()?);
            out.push(b',');
            write!(
                out,
                "{}",
                cursor.read_i32::<LittleEndian>()? as f64 / 100000.0
            )?;
            out.push(b',');
            write!(out, "{}", cursor.read_i16::<LittleEndian>()? as f64 / 100.0)?;
            out.push(b',');
            write!(out, "{}", cursor.read_u16::<LittleEndian>()? as f64 / 100.0)?;
        }
        21 => {
            write_int(out, cursor.read_u16::<LittleEndian>()?);
            out.push(b',');
            write_int(out, cursor.read_u16::<LittleEndian>()?);
            out.push(b',');
            write_int(out, cursor.read_u32::<LittleEndian>()?);
            out.push(b',');
            write_int(out, cursor.read_u16::<LittleEndian>()?);
            out.push(b',');
            write_int(out, cursor.read_u32::<LittleEndian>()?);
            out.push(b',');
            write_int(out, cursor.read_u8()?);
            out.push(b',');
            write_int(out, cursor.read_u8()?);
            out.push(b',');
            write!(out, "{}", cursor.read_u8()? as f64 / -1.0)?;
            out.push(b',');
            write_int(out, cursor.read_i8()?);
        }
        22 => {
            write_int(out, cursor.read_u8()?);
            out.push(b',');
            write_int(out, cursor.read_u8()?);
            out.push(b',');
            write_int(out, cursor.read_u8()?);
            out.push(b',');
            write_int(out, cursor.read_u8()?);
            out.push(b',');
            write_int(out, cursor.read_u8()?);
            out.push(b',');
            write_int(out, cursor.read_u8()?);
            out.push(b',');
            write_int(out, cursor.read_u8()?);
            out.push(b',');
            write_int(out, cursor.read_u8()?);
            out.push(b',');
            write_int(out, cursor.read_u8()?);
        }
        23 => {
            write_int(out, cursor.read_u32::<LittleEndian>()?);
            out.push(b',');
            write_int(out, cursor.read_u32::<LittleEndian>()?);
        }
        24 => {
            write_int(out, cursor.read_u32::<LittleEndian>()?);
        }
        25 => {
            write!(out, "0x{:08x}", cursor.read_u32::<LittleEndian>()?)?;
            out.push(b',');
            write_int(out, cursor.read_u16::<LittleEndian>()?);
            out.push(b',');
            out.extend_from_slice(
                tdf_field_read_vla_to_str(cursor, cursor_start, size)?.as_bytes(),
            );
        }
        26 => {
            write_int(out, cursor.read_u32::<LittleEndian>()?);
            out.push(b',');
            write_int(out, cursor.read_u32::<LittleEndian>()?);
        }
        27 => {
            write_int(out, cursor.read_u8()?);
        }
        28 => {
            write_int(out, cursor.read_u16::<LittleEndian>()?);
            out.push(b',');
            write_int(out, cursor.read_u16::<LittleEndian>()?);
            out.push(b',');
            write_int(out, cursor.read_u8()?);
        }
        29 => {
            write_int(out, cursor.read_u8()?);
            out.push(b',');
            write!(out, "0x{:012x}", cursor.read_u48::<LittleEndian>()?)?;
            out.push(b',');
            write_int(out, cursor.read_u8()?);
        }
        30 => {
            write_int(out, cursor.read_u8()?);
            out.push(b',');
            write!(out, "0x{:012x}", cursor.read_u48::<LittleEndian>()?)?;
            out.push(b',');
            write_int(out, cursor.read_i8()?);
        }
        31 => {
            write_int(out, cursor.read_u8()?);
            out.push(b',');
            write!(out, "0x{:012x}", cursor.read_u48::<LittleEndian>()?)?;
            out.push(b',');
            write_int(out, cursor.read_i32::<LittleEndian>()?);
        }
        32 => {
            write!(out, "0x{:08x}", cursor.read_u32::<LittleEndian>()?)?;
            out.push(b',');
            write_int(out, cursor.read_u16::<LittleEndian>()?);
            out.push(b',');
            out.extend_from_slice(
                tdf_field_read_vla_to_str(cursor, cursor_start, size)?.as_bytes(),
            );
        }
        33 => {
            write!(out, "0x{:08x}", cursor.read_u32::<LittleEndian>()?)?;
            out.push(b',');
            write_int(out, cursor.read_u16::<LittleEndian>()?);
            out.push(b',');
            out.extend_from_slice(
                tdf_field_read_vla_to_str(cursor, cursor_start, size)?.as_bytes(),
            );
        }
        34 => {
            let item_count = tdf_variable_item_count(size, 16, 10)?;
            if item_count == 0 {
                write_int(out, cursor.read_u16::<LittleEndian>()?);
                out.push(b',');
                write_int(out, cursor.read_u16::<LittleEndian>()?);
                out.push(b',');
                write_int(out, cursor.read_u32::<LittleEndian>()?);
                out.push(b',');
                write_int(out, cursor.read_u16::<LittleEndian>()?);
                out.push(b',');
                write_int(out, cursor.read_u32::<LittleEndian>()?);
                out.push(b',');
                write!(out, "{}", cursor.read_u8()? as f64 / -1.0)?;
                out.push(b',');
                write_int(out, cursor.read_i8()?);
                out.push(b',');
                out.push(b',');
                out.push(b',');
                out.push(b',');
                out.push(b',');
            } else {
                write_int(out, cursor.read_u16::<LittleEndian>()?);
                out.push(b',');
                write_int(out, cursor.read_u16::<LittleEndian>()?);
                out.push(b',');
                write_int(out, cursor.read_u32::<LittleEndian>()?);
                out.push(b',');
                write_int(out, cursor.read_u16::<LittleEndian>()?);
                out.push(b',');
                write_int(out, cursor.read_u32::<LittleEndian>()?);
                out.push(b',');
                write!(out, "{}", cursor.read_u8()? as f64 / -1.0)?;
                out.push(b',');
                write_int(out, cursor.read_i8()?);
                out.push(b',');
                write_int(out, cursor.read_u32::<LittleEndian>()?);
                out.push(b',');
                write_int(out, cursor.read_u16::<LittleEndian>()?);
                out.push(b',');
                write!(
                    out,
                    "{}",
                    cursor.read_u16::<LittleEndian>()? as f64 / 1000.0
                )?;
                out.push(b',');
                write!(out, "{}", cursor.read_u8()? as f64 / -1.0)?;
                out.push(b',');
                write_int(out, cursor.read_i8()?);
                for _ in 1..item_count {
                    out.extend_from_slice(b"\n,,,,,,,,");
                    write_int(out, cursor.read_u32::<LittleEndian>()?);
                    out.push(b',');
                    write_int(out, cursor.read_u16::<LittleEndian>()?);
                    out.push(b',');
                    write!(
                        out,
                        "{}",
                        cursor.read_u16::<LittleEndian>()? as f64 / 1000.0
                    )?;
                    out.push(b',');
                    write!(out, "{}", cursor.read_u8()? as f64 / -1.0)?;
                    out.push(b',');
                    write_int(out, cursor.read_i8()?);
                }
            }
        }
        35 => {
            write!(out, "0x{:012x}", cursor.read_u48::<BigEndian>()?)?;
            out.push(b',');
            write_int(out, cursor.read_u8()?);
            out.push(b',');
            write_int(out, cursor.read_i8()?);
        }
        36 => {
            write!(out, "{}", cursor.read_f32::<LittleEndian>()?)?;
        }
        37 => {
            write!(
                out,
                "{}",
                cursor.read_i32::<LittleEndian>()? as f64 / 10000000.0
            )?;
            out.push(b',');
            write!(
                out,
                "{}",
                cursor.read_i32::<LittleEndian>()? as f64 / 10000000.0
            )?;
            out.push(b',');
            write!(
                out,
                "{}",
                cursor.read_i32::<LittleEndian>()? as f64 / 1000.0
            )?;
            out.push(b',');
            write!(
                out,
                "{}",
                cursor.read_u32::<LittleEndian>()? as f64 / 1000.0
            )?;
            out.push(b',');
            write!(
                out,
                "{}",
                cursor.read_u32::<LittleEndian>()? as f64 / 1000.0
            )?;
            out.push(b',');
            write!(
                out,
                "{}",
                cursor.read_i32::<LittleEndian>()? as f64 / 1000.0
            )?;
            out.push(b',');
            write!(
                out,
                "{}",
                cursor.read_u32::<LittleEndian>()? as f64 / 1000.0
            )?;
            out.push(b',');
            write!(
                out,
                "{}",
                cursor.read_i32::<LittleEndian>()? as f64 / 1000.0
            )?;
            out.push(b',');
            write!(
                out,
                "{}",
                cursor.read_u32::<LittleEndian>()? as f64 / 1000.0
            )?;
            out.push(b',');
            write!(
                out,
                "{}",
                cursor.read_i32::<LittleEndian>()? as f64 / 100000.0
            )?;
            out.push(b',');
            write!(
                out,
                "{}",
                cursor.read_u32::<LittleEndian>()? as f64 / 100000.0
            )?;
            out.push(b',');
            write_int(out, cursor.read_u16::<LittleEndian>()?);
            out.push(b',');
            write_int(out, cursor.read_u8()?);
            out.push(b',');
            write_int(out, cursor.read_u8()?);
            out.push(b',');
            write_int(out, cursor.read_u8()?);
            out.push(b',');
            write_int(out, cursor.read_u8()?);
            out.push(b',');
            write_int(out, cursor.read_u8()?);
            out.push(b',');
            write_int(out, cursor.read_u16::<LittleEndian>()?);
            out.push(b',');
            write!(out, "{}", cursor.read_u16::<LittleEndian>()? as f64 / 100.0)?;
            out.push(b',');
            write!(out, "{}", cursor.read_u16::<LittleEndian>()? as f64 / 100.0)?;
            out.push(b',');
            write!(out, "{}", cursor.read_u16::<LittleEndian>()? as f64 / 100.0)?;
            out.push(b',');
            write!(out, "{}", cursor.read_u16::<LittleEndian>()? as f64 / 100.0)?;
            out.push(b',');
            write!(out, "0x{:02x}", cursor.read_u8()?)?;
            out.push(b',');
            write_int(out, cursor.read_u8()?);
        }
        38 => {
            write_int(out, cursor.read_i32::<LittleEndian>()?);
        }
        39 => {
            write!(out, "0x{:016x}", cursor.read_u64::<LittleEndian>()?)?;
            out.push(b',');
            write_int(out, cursor.read_i8()?);
        }
        40 => {
            write_int(out, cursor.read_i8()?);
        }
        41 => {
            write_int(out, cursor.read_i16::<LittleEndian>()?);
        }
        42 => {
            write_int(out, cursor.read_i32::<LittleEndian>()?);
        }
        43 => {
            write_int(out, cursor.read_u32::<LittleEndian>()?);
            out.push(b',');
            out.extend_from_slice(
                tdf_field_read_string_to_str(cursor, cursor_start, 0, size)?.as_bytes(),
            );
        }
        44 => {
            write_int(out, cursor.read_i8()?);
            out.push(b',');
            write_int(out, cursor.read_i16::<LittleEndian>()?);
            out.push(b',');
            out.extend_from_slice(
                tdf_field_read_vla_to_str(cursor, cursor_start, size)?.as_bytes(),
            );
        }
        45 => {
            out.extend_from_slice(
                tdf_field_read_vla_to_str(cursor, cursor_start, size)?.as_bytes(),
            );
        }
        46 => {
            write_int(out, cursor.read_u16::<LittleEndian>()?);
            out.push(b',');
            write_int(out, cursor.read_u32::<LittleEndian>()?);
        }
        47 => {
            write_int(out, cursor.read_u16::<LittleEndian>()?);
            out.push(b',');
            write_int(out, cursor.read_u32::<LittleEndian>()?);
        }
        48 => {
            write!(out, "0x{:012x}", cursor.read_u48::<BigEndian>()?)?;
            out.push(b',');
            write_int(out, cursor.read_u8()?);
            out.push(b',');
            write_int(out, cursor.read_u8()?);
            out.push(b',');
            write_int(out, cursor.read_u8()?);
            out.push(b',');
            write_int(out, cursor.read_u8()?);
            out.push(b',');
            write_int(out, cursor.read_u8()?);
            out.push(b',');
            write_int(out, cursor.read_i8()?);
            out.push(b',');
            write_int(out, cursor.read_u16::<LittleEndian>()?);
            out.push(b',');
            write_int(out, cursor.read_u8()?);
        }
        49 => {
            write_int(out, cursor.read_u8()?);
        }
        50 => {
            write_int(out, cursor.read_u8()?);
        }
        51 => {
            write_int(out, cursor.read_u8()?);
            out.push(b',');
            write_int(out, cursor.read_u8()?);
        }
        52 => {
            let item_count = tdf_variable_item_count(size, 0, 4)?;
            if item_count == 0 {
            } else {
                write!(out, "0x{:08x}", cursor.read_u32::<LittleEndian>()?)?;
                for _ in 1..item_count {
                    out.extend_from_slice(b"\n,");
                    write!(out, "0x{:08x}", cursor.read_u32::<LittleEndian>()?)?;
                }
            }
        }
        53 => {
            write_int(out, cursor.read_u16::<LittleEndian>()?);
        }
        54 => {
            write_int(out, cursor.read_u8()?);
        }
        55 => {
            write_int(out, cursor.read_u8()?);
        }
        56 => {
            write_int(out, cursor.read_u8()?);
        }
        57 => {
            write_int(out, cursor.read_u8()?);
            out.push(b',');
            write_int(out, cursor.read_u32::<LittleEndian>()?);
        }
        58 => {
            write_int(out, cursor.read_i16::<LittleEndian>()?);
        }
        59 => {
            write_int(out, cursor.read_i16::<LittleEndian>()?);
        }
        60 => {
            write_int(out, cursor.read_i16::<LittleEndian>()?);
            out.push(b',');
            write_int(out, cursor.read_i16::<LittleEndian>()?);
        }
        61 => {
            write_int(out, cursor.read_u16::<LittleEndian>()?);
            out.push(b',');
            out.extend_from_slice(
                tdf_field_read_vla_to_str(cursor, cursor_start, size)?.as_bytes(),
            );
        }
        62 => {
            write!(
                out,
                "{}",
                cursor.read_u32::<LittleEndian>()? as f64 / 1000.0
            )?;
        }
        _ => {
            let mut buf = vec![0; size as usize];
            cursor.read_exact(&mut buf)?;
            out.extend_from_slice(hex::encode(buf).as_bytes());
        }
    };
    let cursor_end = cursor.position();
    let cursor_read = cursor_end - cursor_start;

    if (size as u64) < cursor_read {
        // Hande read overflow (more data read than specified)
        return Result::Err(Error::new(
            ErrorKind::InvalidData,
            "Read overflow, corrupt data/metadata",
        ));
    }

    let underflow = size as u64 - cursor_read;

    // Handle read underflow (more data specified than expected)
    if underflow > 0 {
        crate::decoders::tdf_field_read_string(cursor, cursor_start, 0, underflow as u8)?;
    }
    Ok(())
}

/// Decode a TDF into a CSV formatted string
pub fn tdf_read_into_str(tdf_id: &u16, size: u8, cursor: &mut Cursor<&[u8]>) -> Result<String> {
    let mut out = Vec::new();
    tdf_write_csv(tdf_id, size, cursor, &mut out)?;
    String::from_utf8(out).map_err(|e| Error::new(ErrorKind::InvalidData, e))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tdf34_base_bytes() -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&1u16.to_le_bytes());
        bytes.extend_from_slice(&2u16.to_le_bytes());
        bytes.extend_from_slice(&3u32.to_le_bytes());
        bytes.extend_from_slice(&4u16.to_le_bytes());
        bytes.extend_from_slice(&5u32.to_le_bytes());
        bytes.push(6);
        bytes.push((-7i8) as u8);
        bytes
    }

    fn push_tdf34_neighbour(
        bytes: &mut Vec<u8>,
        earfcn: u32,
        pci: u16,
        time_diff: u16,
        rsrp: u8,
        rsrq: i8,
    ) {
        bytes.extend_from_slice(&earfcn.to_le_bytes());
        bytes.extend_from_slice(&pci.to_le_bytes());
        bytes.extend_from_slice(&time_diff.to_le_bytes());
        bytes.push(rsrp);
        bytes.push(rsrq as u8);
    }

    #[test]
    fn trailing_variable_array_zero_items_blanks_last_columns() {
        let bytes = tdf34_base_bytes();
        let mut cursor = Cursor::new(bytes.as_slice());

        let row = tdf_read_into_str(&34, bytes.len() as u8, &mut cursor).unwrap();

        assert_eq!(row, "1,2,3,4,5,-6,-7,,,,,");
    }

    #[test]
    fn trailing_variable_array_one_item_stays_on_first_row() {
        let mut bytes = tdf34_base_bytes();
        push_tdf34_neighbour(&mut bytes, 100, 11, 2500, 8, -9);
        let mut cursor = Cursor::new(bytes.as_slice());

        let row = tdf_read_into_str(&34, bytes.len() as u8, &mut cursor).unwrap();

        assert_eq!(row, "1,2,3,4,5,-6,-7,100,11,2.5,-8,-9");
    }

    #[test]
    fn trailing_variable_array_extra_items_add_blank_prefix_rows() {
        let mut bytes = tdf34_base_bytes();
        push_tdf34_neighbour(&mut bytes, 100, 11, 2500, 8, -9);
        push_tdf34_neighbour(&mut bytes, 200, 12, 3000, 10, -11);
        let mut cursor = Cursor::new(bytes.as_slice());

        let row = tdf_read_into_str(&34, bytes.len() as u8, &mut cursor).unwrap();

        assert_eq!(
            row,
            "1,2,3,4,5,-6,-7,100,11,2.5,-8,-9\n,,,,,,,,200,12,3,-10,-11"
        );
    }

    #[test]
    fn trailing_variable_array_items_use_single_field_formatting() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&0x12345678u32.to_le_bytes());
        bytes.extend_from_slice(&0x90abcdefu32.to_le_bytes());
        let mut cursor = Cursor::new(bytes.as_slice());

        let row = tdf_read_into_str(&52, bytes.len() as u8, &mut cursor).unwrap();

        assert_eq!(row, "0x12345678\n,0x90abcdef");
    }

    #[test]
    fn trailing_uint8_variable_array_stays_as_hex_payload() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&0x12345678u32.to_le_bytes());
        bytes.extend_from_slice(&9u16.to_le_bytes());
        bytes.extend_from_slice(&[0xab, 0xcd, 0xef]);
        let mut cursor = Cursor::new(bytes.as_slice());

        let row = tdf_read_into_str(&25, bytes.len() as u8, &mut cursor).unwrap();

        assert_eq!(row, "0x12345678,9,abcdef");
    }

    #[test]
    fn conversion_hex_byte_array_uses_single_field_formatting() {
        let bytes = [0xab, 0xcd, 0xef];
        let mut cursor = Cursor::new(bytes.as_slice());

        let val = tdf_field_read_fixed_bytes_to_hex(&mut cursor, bytes.len()).unwrap();

        assert_eq!(val, "abcdef");
        assert_eq!(cursor.position(), bytes.len() as u64);
    }
}
