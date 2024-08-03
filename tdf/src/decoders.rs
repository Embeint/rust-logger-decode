use std::io::{Cursor, Read};

use byteorder::{LittleEndian, ReadBytesExt};

pub fn tdf_name(tdf_id: &u16) -> String
{
    match tdf_id {
        1 => String::from("ANNOUNCE"),
        2 => String::from("BATTERY_STATE"),
        3 => String::from("AMBIENT_TEMP_PRES_HUM"),
        4 => String::from("AMBIENT_TEMPERATURE"),
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
        100 => String::from("ARRAY_TYPE"),
        _ => format!("{}", tdf_id),
    }
}

pub fn tdf_fields(tdf_id: &u16) -> Vec<&'static str>
{
    match tdf_id {
        1 => vec!["time","application","major","minor","revision","build_num","kv_crc","uptime","reboots"],
        2 => vec!["time","voltage_mv","charge_ua","soc"],
        3 => vec!["time","temperature","pressure","humidity"],
        4 => vec!["time","temperature"],
        10 => vec!["time","x","y","z"],
        11 => vec!["time","x","y","z"],
        12 => vec!["time","x","y","z"],
        13 => vec!["time","x","y","z"],
        14 => vec!["time","x","y","z"],
        15 => vec!["time","x","y","z"],
        16 => vec!["time","x","y","z"],
        17 => vec!["time","x","y","z"],
        18 => vec!["time","x","y","z"],
        19 => vec!["time","latitude","longitude","height","h_acc","v_acc"],
        20 => vec!["time","itow","year","month","day","hour","min","sec","valid","t_acc","nano","fix_type","flags","flags2","num_sv","lon","lat","height","h_msl","h_acc","v_acc","vel_n","vel_e","vel_d","g_speed","head_mot","s_acc","head_acc","p_dop","flags3","reserved0[0]","reserved0[1]","reserved0[2]","reserved0[3]","head_veh","mag_dec","mag_acc"],
        100 => vec!["time","array[0]","array[1]","array[2]","array[3]"],
        _ => vec!["unknown"],
    }
}

pub fn tdf_read_into_str(tdf_id: &u16, size: u8, cursor: &mut Cursor<&[u8]>) -> std::io::Result<String>
{
    match tdf_id {
        1 => 
            Ok(format!(
                "{},{},{},{},{},{},{},{}",
                cursor.read_u32::<LittleEndian>()?,
                cursor.read_u8()?,
                cursor.read_u8()?,
                cursor.read_u16::<LittleEndian>()?,
                cursor.read_u32::<LittleEndian>()?,
                cursor.read_u32::<LittleEndian>()?,
                cursor.read_u32::<LittleEndian>()?,
                cursor.read_u16::<LittleEndian>()?,
            )),
        2 => 
            Ok(format!(
                "{},{},{}",
                cursor.read_u32::<LittleEndian>()?,
                cursor.read_u16::<LittleEndian>()?,
                cursor.read_u16::<LittleEndian>()? as f64 / 100.0,
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
                "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
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
    }
}
