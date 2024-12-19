use std::io::{Cursor, Read};

use byteorder::{LittleEndian, ReadBytesExt};

pub fn tdf_name(tdf_id: &u16) -> String
{
    match tdf_id {
        1 => String::from("ANNOUNCE"),
        2 => String::from("BATTERY_STATE"),
        3 => String::from("AMBIENT_TEMP_PRES_HUM"),
        4 => String::from("AMBIENT_TEMPERATURE"),
        5 => String::from("TIME_SYNC"),
        6 => String::from("REBOOT_INFO"),
        10 => String::from("ACC_2G"),
        11 => String::from("ACC_4G"),
        12 => String::from("ACC_8G"),
        13 => String::from("ACC_16G"),
        14 => String::from("GYR_125DPS"),
        15 => String::from("GYR_250DPS"),
        16 => String::from("GYR_500DPS"),
        17 => String::from("GYR_1000DPS"),
        18 => String::from("GYR_2000DPS"),
        19 => String::from("GCS_WGS84_LLHA"),
        20 => String::from("UBX_NAV_PVT"),
        21 => String::from("LTE_CONN_STATUS"),
        22 => String::from("GLOBALSTAR_PKT"),
        23 => String::from("ACC_MAGNITUDE_STD_DEV"),
        24 => String::from("ACTIVITY_METRIC"),
        25 => String::from("ALGORITHM_OUTPUT"),
        26 => String::from("RUNTIME_ERROR"),
        27 => String::from("CHARGER_EN_CONTROL"),
        28 => String::from("GNSS_FIX_INFO"),
        29 => String::from("BLUETOOTH_CONNECTION"),
        30 => String::from("BLUETOOTH_RSSI"),
        31 => String::from("BLUETOOTH_DATA_THROUGHPUT"),
        32 => String::from("ALGORITHM_CLASS_HISTOGRAM"),
        33 => String::from("ALGORITHM_CLASS_TIME_SERIES"),
        100 => String::from("ARRAY_TYPE"),
        _ => format!("{}", tdf_id),
    }
}

pub fn tdf_fields(tdf_id: &u16) -> Vec<&'static str>
{
    match tdf_id {
        1 => vec!["application","major","minor","revision","build_num","kv_crc","blocks","uptime","reboots","flags"],
        2 => vec!["voltage_mv","current_ua","soc"],
        3 => vec!["temperature","pressure","humidity"],
        4 => vec!["temperature"],
        5 => vec!["source","shift"],
        6 => vec!["reason","hardware_flags","count","uptime","param_1","param_2","thread"],
        10 => vec!["x","y","z"],
        11 => vec!["x","y","z"],
        12 => vec!["x","y","z"],
        13 => vec!["x","y","z"],
        14 => vec!["x","y","z"],
        15 => vec!["x","y","z"],
        16 => vec!["x","y","z"],
        17 => vec!["x","y","z"],
        18 => vec!["x","y","z"],
        19 => vec!["latitude","longitude","height","h_acc","v_acc"],
        20 => vec!["itow","year","month","day","hour","min","sec","valid","t_acc","nano","fix_type","flags","flags2","num_sv","lon","lat","height","h_msl","h_acc","v_acc","vel_n","vel_e","vel_d","g_speed","head_mot","s_acc","head_acc","p_dop","flags3","reserved0[0]","reserved0[1]","reserved0[2]","reserved0[3]","head_veh","mag_dec","mag_acc"],
        21 => vec!["mcc","mnc","eci","tac","earfcn","status","tech","rsrp","rsrq"],
        22 => vec!["payload[0]","payload[1]","payload[2]","payload[3]","payload[4]","payload[5]","payload[6]","payload[7]","payload[8]"],
        23 => vec!["count","std_dev"],
        24 => vec!["value"],
        25 => vec!["algorithm_id","algorithm_version","output"],
        26 => vec!["error_id","error_ctx"],
        27 => vec!["enabled"],
        28 => vec!["time_fix","location_fix","num_sv"],
        29 => vec!["type","val","connected"],
        30 => vec!["type","val","rssi"],
        31 => vec!["type","val","throughput"],
        32 => vec!["algorithm_id","algorithm_version","classes"],
        33 => vec!["algorithm_id","algorithm_version","values"],
        100 => vec!["array[0]","array[1]","array[2]","array[3]"],
        _ => vec!["unknown"],
    }
}

fn tdf_field_read_string(cursor: &mut Cursor<&[u8]>, size: u8) ->  std::io::Result<String>
{
    let mut buf = vec![0u8; size as usize];
    cursor.read_exact(&mut buf)?;

    match String::from_utf8(buf) {
        Ok(val) => Ok(format!("\"{}\"", val.trim_matches(char::from(0)))),
        Err(..) => Ok(String::from("\"\""))
    }
}

fn tdf_field_read_vla(cursor: &mut Cursor<&[u8]>, cursor_start: u64, size: u8) ->  std::io::Result<String>
{
    let cursor_current = cursor.position();
    let cursor_read = cursor_current - cursor_start;
    let bytes_remaining = size as u64 - cursor_read;
    let mut buf = vec![0u8; bytes_remaining as usize];

    cursor.read_exact(&mut buf)?;
    Ok(format!("{}", hex::encode(buf)))
}

pub fn tdf_read_into_str(tdf_id: &u16, size: u8, cursor: &mut Cursor<&[u8]>) -> std::io::Result<String>
{
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
                tdf_field_read_string(cursor, 8)?,
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
                tdf_field_read_vla(cursor, cursor_start, size)?,
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
                tdf_field_read_vla(cursor, cursor_start, size)?,
            )),
        33 => 
            Ok(format!(
                "0x{:08x},{},{}",
                cursor.read_u32::<LittleEndian>()?,
                cursor.read_u16::<LittleEndian>()?,
                tdf_field_read_vla(cursor, cursor_start, size)?,
            )),
        100 => 
            Ok(format!(
                "{},{},{},{}",
                cursor.read_u8()?,
                cursor.read_u8()?,
                cursor.read_u8()?,
                cursor.read_u8()?,
            )),
        _ => {
            let mut buf = vec![0; size as usize];
            cursor.read_exact(&mut buf)?;
            Ok(format!("{}", hex::encode(buf)))
        }
    };
    let cursor_end = cursor.position();
    let cursor_read = cursor_end - cursor_start;
    let underflow = size as u64 - cursor_read;

    // Handle read underflow (more data specified than expected)
    if underflow > 0 {
        tdf_field_read_string(cursor, underflow as u8)?;
    }
    res
}
