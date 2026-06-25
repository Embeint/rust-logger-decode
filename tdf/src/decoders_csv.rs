use std::io::{Cursor, Error, ErrorKind, Read, Result};

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

pub fn tdf_read_into_str(tdf_id: &u16, size: u8, cursor: &mut Cursor<&[u8]>) -> Result<String> {
    let cursor_start = cursor.position();

    let res = match tdf_id {
        1 =>
            Ok(format!(
                "0x{:08x},{},{},{},0x{:08x},0x{:08x},{},{},{},0x{:02x}",
                cursor.read_u32::<LittleEndian>()?,
                cursor.read_u8()?,
                cursor.read_u8()?,
                cursor.read_u16::<LittleEndian>()?,
                cursor.read_u32::<LittleEndian>()?,
                cursor.read_u32::<LittleEndian>()?,
                cursor.read_u32::<LittleEndian>()?,
                cursor.read_u32::<LittleEndian>()?,
                cursor.read_u16::<LittleEndian>()?,
                cursor.read_u8()?,
            )),
        2 =>
            Ok(format!(
                "{},{},{}",
                cursor.read_u32::<LittleEndian>()?,
                cursor.read_i32::<LittleEndian>()?,
                cursor.read_u8()?,
            )),
        3 =>
            Ok(format!(
                "{},{},{}",
                cursor.read_i32::<LittleEndian>()? as f64 / 1000.0,
                cursor.read_u32::<LittleEndian>()? as f64 / 1000.0,
                cursor.read_u16::<LittleEndian>()? as f64 / 100.0,
            )),
        4 =>
            Ok(format!(
                "{}",
                cursor.read_i32::<LittleEndian>()? as f64 / 1000.0,
            )),
        5 =>
            Ok(format!(
                "{},{}",
                cursor.read_u8()?,
                cursor.read_i32::<LittleEndian>()? as f64 / 1000000.0,
            )),
        6 =>
            Ok(format!(
                "{},0x{:08x},{},{},0x{:08x},0x{:08x},{}",
                cursor.read_u8()?,
                cursor.read_u32::<LittleEndian>()?,
                cursor.read_u32::<LittleEndian>()?,
                cursor.read_u32::<LittleEndian>()?,
                cursor.read_u32::<LittleEndian>()?,
                cursor.read_u32::<LittleEndian>()?,
                tdf_field_read_string_to_str(cursor, cursor_start, 8, size)?,
            )),
        7 =>
            Ok(format!(
                "0x{:08x},{},{},{},0x{:08x},0x{:04x},0x{:08x},{},{},{},0x{:02x}",
                cursor.read_u32::<LittleEndian>()?,
                cursor.read_u8()?,
                cursor.read_u8()?,
                cursor.read_u16::<LittleEndian>()?,
                cursor.read_u32::<LittleEndian>()?,
                cursor.read_u16::<LittleEndian>()?,
                cursor.read_u32::<LittleEndian>()?,
                cursor.read_u32::<LittleEndian>()?,
                cursor.read_u32::<LittleEndian>()?,
                cursor.read_u16::<LittleEndian>()?,
                cursor.read_u8()?,
            )),
        8 =>
            Ok(format!(
                "{}",
                cursor.read_i16::<LittleEndian>()? as f64 / 100.0,
            )),
        10 =>
            Ok(format!(
                "{},{},{}",
                cursor.read_i16::<LittleEndian>()?,
                cursor.read_i16::<LittleEndian>()?,
                cursor.read_i16::<LittleEndian>()?,
            )),
        11 =>
            Ok(format!(
                "{},{},{}",
                cursor.read_i16::<LittleEndian>()?,
                cursor.read_i16::<LittleEndian>()?,
                cursor.read_i16::<LittleEndian>()?,
            )),
        12 =>
            Ok(format!(
                "{},{},{}",
                cursor.read_i16::<LittleEndian>()?,
                cursor.read_i16::<LittleEndian>()?,
                cursor.read_i16::<LittleEndian>()?,
            )),
        13 =>
            Ok(format!(
                "{},{},{}",
                cursor.read_i16::<LittleEndian>()?,
                cursor.read_i16::<LittleEndian>()?,
                cursor.read_i16::<LittleEndian>()?,
            )),
        14 =>
            Ok(format!(
                "{},{},{}",
                cursor.read_i16::<LittleEndian>()?,
                cursor.read_i16::<LittleEndian>()?,
                cursor.read_i16::<LittleEndian>()?,
            )),
        15 =>
            Ok(format!(
                "{},{},{}",
                cursor.read_i16::<LittleEndian>()?,
                cursor.read_i16::<LittleEndian>()?,
                cursor.read_i16::<LittleEndian>()?,
            )),
        16 =>
            Ok(format!(
                "{},{},{}",
                cursor.read_i16::<LittleEndian>()?,
                cursor.read_i16::<LittleEndian>()?,
                cursor.read_i16::<LittleEndian>()?,
            )),
        17 =>
            Ok(format!(
                "{},{},{}",
                cursor.read_i16::<LittleEndian>()?,
                cursor.read_i16::<LittleEndian>()?,
                cursor.read_i16::<LittleEndian>()?,
            )),
        18 =>
            Ok(format!(
                "{},{},{}",
                cursor.read_i16::<LittleEndian>()?,
                cursor.read_i16::<LittleEndian>()?,
                cursor.read_i16::<LittleEndian>()?,
            )),
        19 =>
            Ok(format!(
                "{},{},{},{},{}",
                cursor.read_i32::<LittleEndian>()? as f64 / 10000000.0,
                cursor.read_i32::<LittleEndian>()? as f64 / 10000000.0,
                cursor.read_i32::<LittleEndian>()? as f64 / 1000.0,
                cursor.read_i32::<LittleEndian>()? as f64 / 1000.0,
                cursor.read_i32::<LittleEndian>()? as f64 / 1000.0,
            )),
        20 =>
            Ok(format!(
                "{},{},{},{},{},{},{},0x{:02x},{},{},{},0x{:02x},0x{:02x},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},0x{:04x},{},{},{},{},{},{},{}",
                cursor.read_u32::<LittleEndian>()?,
                cursor.read_u16::<LittleEndian>()?,
                cursor.read_u8()?,
                cursor.read_u8()?,
                cursor.read_u8()?,
                cursor.read_u8()?,
                cursor.read_u8()?,
                cursor.read_u8()?,
                cursor.read_u32::<LittleEndian>()?,
                cursor.read_i32::<LittleEndian>()?,
                cursor.read_u8()?,
                cursor.read_u8()?,
                cursor.read_u8()?,
                cursor.read_u8()?,
                cursor.read_i32::<LittleEndian>()? as f64 / 10000000.0,
                cursor.read_i32::<LittleEndian>()? as f64 / 10000000.0,
                cursor.read_i32::<LittleEndian>()? as f64 / 1000.0,
                cursor.read_i32::<LittleEndian>()? as f64 / 1000.0,
                cursor.read_u32::<LittleEndian>()? as f64 / 1000.0,
                cursor.read_u32::<LittleEndian>()? as f64 / 1000.0,
                cursor.read_i32::<LittleEndian>()? as f64 / 1000.0,
                cursor.read_i32::<LittleEndian>()? as f64 / 1000.0,
                cursor.read_i32::<LittleEndian>()? as f64 / 1000.0,
                cursor.read_i32::<LittleEndian>()? as f64 / 1000.0,
                cursor.read_i32::<LittleEndian>()? as f64 / 100000.0,
                cursor.read_u32::<LittleEndian>()? as f64 / 1000.0,
                cursor.read_u32::<LittleEndian>()? as f64 / 100000.0,
                cursor.read_u16::<LittleEndian>()? as f64 / 100.0,
                cursor.read_u16::<LittleEndian>()?,
                cursor.read_u8()?,
                cursor.read_u8()?,
                cursor.read_u8()?,
                cursor.read_u8()?,
                cursor.read_i32::<LittleEndian>()? as f64 / 100000.0,
                cursor.read_i16::<LittleEndian>()? as f64 / 100.0,
                cursor.read_u16::<LittleEndian>()? as f64 / 100.0,
            )),
        21 =>
            Ok(format!(
                "{},{},{},{},{},{},{},{},{}",
                cursor.read_u16::<LittleEndian>()?,
                cursor.read_u16::<LittleEndian>()?,
                cursor.read_u32::<LittleEndian>()?,
                cursor.read_u16::<LittleEndian>()?,
                cursor.read_u32::<LittleEndian>()?,
                cursor.read_u8()?,
                cursor.read_u8()?,
                cursor.read_u8()? as f64 / -1.0,
                cursor.read_i8()?,
            )),
        22 =>
            Ok(format!(
                "{},{},{},{},{},{},{},{},{}",
                cursor.read_u8()?,
                cursor.read_u8()?,
                cursor.read_u8()?,
                cursor.read_u8()?,
                cursor.read_u8()?,
                cursor.read_u8()?,
                cursor.read_u8()?,
                cursor.read_u8()?,
                cursor.read_u8()?,
            )),
        23 =>
            Ok(format!(
                "{},{}",
                cursor.read_u32::<LittleEndian>()?,
                cursor.read_u32::<LittleEndian>()?,
            )),
        24 =>
            Ok(format!(
                "{}",
                cursor.read_u32::<LittleEndian>()?,
            )),
        25 =>
            Ok(format!(
                "0x{:08x},{},{}",
                cursor.read_u32::<LittleEndian>()?,
                cursor.read_u16::<LittleEndian>()?,
                tdf_field_read_vla_to_str(cursor, cursor_start, size)?,
            )),
        26 =>
            Ok(format!(
                "{},{}",
                cursor.read_u32::<LittleEndian>()?,
                cursor.read_u32::<LittleEndian>()?,
            )),
        27 =>
            Ok(format!(
                "{}",
                cursor.read_u8()?,
            )),
        28 =>
            Ok(format!(
                "{},{},{}",
                cursor.read_u16::<LittleEndian>()?,
                cursor.read_u16::<LittleEndian>()?,
                cursor.read_u8()?,
            )),
        29 =>
            Ok(format!(
                "{},0x{:012x},{}",
                cursor.read_u8()?,
                cursor.read_u48::<LittleEndian>()?,
                cursor.read_u8()?,
            )),
        30 =>
            Ok(format!(
                "{},0x{:012x},{}",
                cursor.read_u8()?,
                cursor.read_u48::<LittleEndian>()?,
                cursor.read_i8()?,
            )),
        31 =>
            Ok(format!(
                "{},0x{:012x},{}",
                cursor.read_u8()?,
                cursor.read_u48::<LittleEndian>()?,
                cursor.read_i32::<LittleEndian>()?,
            )),
        32 =>
            Ok(format!(
                "0x{:08x},{},{}",
                cursor.read_u32::<LittleEndian>()?,
                cursor.read_u16::<LittleEndian>()?,
                tdf_field_read_vla_to_str(cursor, cursor_start, size)?,
            )),
        33 =>
            Ok(format!(
                "0x{:08x},{},{}",
                cursor.read_u32::<LittleEndian>()?,
                cursor.read_u16::<LittleEndian>()?,
                tdf_field_read_vla_to_str(cursor, cursor_start, size)?,
            )),
        34 =>
            Ok(format!(
                "{},{},{},{},{},{},{},{},{},{},{},{}",
                cursor.read_u16::<LittleEndian>()?,
                cursor.read_u16::<LittleEndian>()?,
                cursor.read_u32::<LittleEndian>()?,
                cursor.read_u16::<LittleEndian>()?,
                cursor.read_u32::<LittleEndian>()?,
                cursor.read_u8()? as f64 / -1.0,
                cursor.read_i8()?,
                cursor.read_u32::<LittleEndian>()?,
                cursor.read_u16::<LittleEndian>()?,
                cursor.read_u16::<LittleEndian>()? as f64 / 1000.0,
                cursor.read_u8()? as f64 / -1.0,
                cursor.read_i8()?,
            )),
        35 =>
            Ok(format!(
                "0x{:012x},{},{}",
                cursor.read_u48::<BigEndian>()?,
                cursor.read_u8()?,
                cursor.read_i8()?,
            )),
        36 =>
            Ok(format!(
                "{}",
                cursor.read_f32::<LittleEndian>()?,
            )),
        37 =>
            Ok(format!(
                "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},0x{:02x},{}",
                cursor.read_i32::<LittleEndian>()? as f64 / 10000000.0,
                cursor.read_i32::<LittleEndian>()? as f64 / 10000000.0,
                cursor.read_i32::<LittleEndian>()? as f64 / 1000.0,
                cursor.read_u32::<LittleEndian>()? as f64 / 1000.0,
                cursor.read_u32::<LittleEndian>()? as f64 / 1000.0,
                cursor.read_i32::<LittleEndian>()? as f64 / 1000.0,
                cursor.read_u32::<LittleEndian>()? as f64 / 1000.0,
                cursor.read_i32::<LittleEndian>()? as f64 / 1000.0,
                cursor.read_u32::<LittleEndian>()? as f64 / 1000.0,
                cursor.read_i32::<LittleEndian>()? as f64 / 100000.0,
                cursor.read_u32::<LittleEndian>()? as f64 / 100000.0,
                cursor.read_u16::<LittleEndian>()?,
                cursor.read_u8()?,
                cursor.read_u8()?,
                cursor.read_u8()?,
                cursor.read_u8()?,
                cursor.read_u8()?,
                cursor.read_u16::<LittleEndian>()?,
                cursor.read_u16::<LittleEndian>()? as f64 / 100.0,
                cursor.read_u16::<LittleEndian>()? as f64 / 100.0,
                cursor.read_u16::<LittleEndian>()? as f64 / 100.0,
                cursor.read_u16::<LittleEndian>()? as f64 / 100.0,
                cursor.read_u8()?,
                cursor.read_u8()?,
            )),
        38 =>
            Ok(format!(
                "{}",
                cursor.read_i32::<LittleEndian>()?,
            )),
        39 =>
            Ok(format!(
                "0x{:016x},{}",
                cursor.read_u64::<LittleEndian>()?,
                cursor.read_i8()?,
            )),
        40 =>
            Ok(format!(
                "{}",
                cursor.read_i8()?,
            )),
        41 =>
            Ok(format!(
                "{}",
                cursor.read_i16::<LittleEndian>()?,
            )),
        42 =>
            Ok(format!(
                "{}",
                cursor.read_i32::<LittleEndian>()?,
            )),
        43 =>
            Ok(format!(
                "{},{}",
                cursor.read_u32::<LittleEndian>()?,
                tdf_field_read_string_to_str(cursor, cursor_start, 0, size)?,
            )),
        44 =>
            Ok(format!(
                "{},{},{}",
                cursor.read_i8()?,
                cursor.read_i16::<LittleEndian>()?,
                tdf_field_read_vla_to_str(cursor, cursor_start, size)?,
            )),
        45 =>
            Ok(format!(
                "{}",
                tdf_field_read_vla_to_str(cursor, cursor_start, size)?,
            )),
        46 =>
            Ok(format!(
                "{},{}",
                cursor.read_u16::<LittleEndian>()?,
                cursor.read_u32::<LittleEndian>()?,
            )),
        47 =>
            Ok(format!(
                "{},{}",
                cursor.read_u16::<LittleEndian>()?,
                cursor.read_u32::<LittleEndian>()?,
            )),
        48 =>
            Ok(format!(
                "0x{:012x},{},{},{},{},{},{},{},{}",
                cursor.read_u48::<BigEndian>()?,
                cursor.read_u8()?,
                cursor.read_u8()?,
                cursor.read_u8()?,
                cursor.read_u8()?,
                cursor.read_u8()?,
                cursor.read_i8()?,
                cursor.read_u16::<LittleEndian>()?,
                cursor.read_u8()?,
            )),
        49 =>
            Ok(format!(
                "{}",
                cursor.read_u8()?,
            )),
        50 =>
            Ok(format!(
                "{}",
                cursor.read_u8()?,
            )),
        51 =>
            Ok(format!(
                "{},{}",
                cursor.read_u8()?,
                cursor.read_u8()?,
            )),
        52 =>
            Ok(format!(
                "{}",
                tdf_field_read_vla_to_str(cursor, cursor_start, size)?,
            )),
        53 =>
            Ok(format!(
                "{}",
                cursor.read_u16::<LittleEndian>()?,
            )),
        54 =>
            Ok(format!(
                "{}",
                cursor.read_u8()?,
            )),
        55 =>
            Ok(format!(
                "{}",
                cursor.read_u8()?,
            )),
        56 =>
            Ok(format!(
                "{}",
                cursor.read_u8()?,
            )),
        57 =>
            Ok(format!(
                "{},{}",
                cursor.read_u8()?,
                cursor.read_u32::<LittleEndian>()?,
            )),
        58 =>
            Ok(format!(
                "{}",
                cursor.read_i16::<LittleEndian>()?,
            )),
        59 =>
            Ok(format!(
                "{}",
                cursor.read_i16::<LittleEndian>()?,
            )),
        60 =>
            Ok(format!(
                "{},{}",
                cursor.read_i16::<LittleEndian>()?,
                cursor.read_i16::<LittleEndian>()?,
            )),
        61 =>
            Ok(format!(
                "{},{}",
                cursor.read_u16::<LittleEndian>()?,
                tdf_field_read_vla_to_str(cursor, cursor_start, size)?,
            )),
        _ => {
            let mut buf = vec![0; size as usize];
            cursor.read_exact(&mut buf)?;
            Ok(format!("{}", hex::encode(buf)))
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
    res
}
