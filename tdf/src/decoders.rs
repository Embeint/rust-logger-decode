use std::io::{Cursor, Error, ErrorKind, Read, Result};

pub fn tdf_name(tdf_id: &u16) -> String {
    match tdf_id {
        1 => String::from("ANNOUNCE"),
        2 => String::from("BATTERY_STATE"),
        3 => String::from("AMBIENT_TEMP_PRES_HUM"),
        4 => String::from("AMBIENT_TEMPERATURE"),
        5 => String::from("TIME_SYNC"),
        6 => String::from("REBOOT_INFO"),
        7 => String::from("ANNOUNCE_V2"),
        8 => String::from("SOC_TEMPERATURE"),
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
        34 => String::from("LTE_TAC_CELLS"),
        35 => String::from("WIFI_AP_INFO"),
        36 => String::from("DEVICE_TILT"),
        37 => String::from("NRF9X_GNSS_PVT"),
        38 => String::from("BATTERY_CHARGE_ACCUMULATED"),
        39 => String::from("INFUSE_BLUETOOTH_RSSI"),
        40 => String::from("ADC_RAW_8"),
        41 => String::from("ADC_RAW_16"),
        42 => String::from("ADC_RAW_32"),
        43 => String::from("ANNOTATION"),
        44 => String::from("LORA_RX"),
        45 => String::from("LORA_TX"),
        46 => String::from("IDX_ARRAY_FREQ"),
        47 => String::from("IDX_ARRAY_PERIOD"),
        48 => String::from("WIFI_CONNECTED"),
        49 => String::from("WIFI_CONNECTION_FAILED"),
        50 => String::from("WIFI_DISCONNECTED"),
        51 => String::from("NETWORK_SCAN_COUNT"),
        52 => String::from("EXCEPTION_STACK_FRAME"),
        53 => String::from("BATTERY_VOLTAGE"),
        54 => String::from("BATTERY_SOC"),
        55 => String::from("STATE_EVENT_SET"),
        56 => String::from("STATE_EVENT_CLEARED"),
        57 => String::from("STATE_DURATION"),
        58 => String::from("PCM_16BIT_CHAN_LEFT"),
        59 => String::from("PCM_16BIT_CHAN_RIGHT"),
        60 => String::from("PCM_16BIT_CHAN_DUAL"),
        61 => String::from("KVS_VALUE_CHANGED"),
        _ => format!("{}", tdf_id),
    }
}

pub fn vla_bytes_remaining(
    cursor: &mut Cursor<&[u8]>,
    cursor_start: u64,
    size: u8,
) -> Result<usize> {
    let cursor_current = cursor.position();
    let cursor_read = cursor_current - cursor_start;
    if cursor_read > size as u64 {
        return Result::Err(Error::new(
            ErrorKind::InvalidData,
            "Insufficient data remaining",
        ));
    }
    let bytes_remaining = size as u64 - cursor_read;

    Ok(bytes_remaining as usize)
}

pub fn tdf_field_read_string(
    cursor: &mut Cursor<&[u8]>,
    cursor_start: u64,
    num: u8,
    size: u8,
) -> Result<Vec<u8>> {
    let string_length = match num {
        0 => vla_bytes_remaining(cursor, cursor_start, size)?,
        _ => num as usize,
    };

    let mut buf = vec![0u8; string_length];
    cursor.read_exact(&mut buf)?;

    Ok(buf)
}

pub fn tdf_field_read_vla(
    cursor: &mut Cursor<&[u8]>,
    cursor_start: u64,
    size: u8,
) -> Result<Vec<u8>> {
    let bytes_remaining = vla_bytes_remaining(cursor, cursor_start, size)?;
    let mut buf = vec![0u8; bytes_remaining];

    cursor.read_exact(&mut buf)?;
    Ok(buf)
}
