use std::io::{Cursor, Error, ErrorKind, Read, Result};
use std::sync::Arc;

use arrow_array::{
    ArrayRef, BinaryArray, FixedSizeListArray, Float32Array, Float64Array, Int16Array, Int32Array,
    Int8Array, ListArray, RecordBatch, StringArray, StructArray, TimestampMicrosecondArray,
    UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_schema::{ArrowError, DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
use byteorder::{BigEndian, LittleEndian, ReadBytesExt};

fn timestamp_field() -> Field {
    Field::new(
        "timestamp",
        DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
        true,
    )
}

fn sample_idx_field() -> Field {
    Field::new("sample_idx", DataType::UInt16, true)
}

fn tdf_field_read_string_to_string(
    cursor: &mut Cursor<&[u8]>,
    cursor_start: u64,
    num: u8,
    size: u8,
) -> Result<String> {
    let buf = crate::decoders::tdf_field_read_string(cursor, cursor_start, num, size)?;

    match String::from_utf8(buf) {
        Ok(val) => Ok(val.trim_matches(char::from(0)).to_string()),
        Err(..) => Ok(String::new()),
    }
}

fn finish_tdf_read(cursor: &mut Cursor<&[u8]>, cursor_start: u64, size: u8) -> Result<()> {
    let cursor_end = cursor.position();
    let cursor_read = cursor_end - cursor_start;

    if (size as u64) < cursor_read {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "Read overflow, corrupt data/metadata",
        ));
    }

    let underflow = size as u64 - cursor_read;
    if underflow > 0 {
        let mut buf = vec![0; underflow as usize];
        cursor.read_exact(&mut buf)?;
    }

    Ok(())
}

pub fn tdf_parquet_schemas() -> Vec<(u16, &'static str, SchemaRef)> {
    vec![
        (1, "ANNOUNCE", tdf_parquet_schema(1).unwrap()),
        (2, "BATTERY_STATE", tdf_parquet_schema(2).unwrap()),
        (3, "AMBIENT_TEMP_PRES_HUM", tdf_parquet_schema(3).unwrap()),
        (4, "AMBIENT_TEMPERATURE", tdf_parquet_schema(4).unwrap()),
        (5, "TIME_SYNC", tdf_parquet_schema(5).unwrap()),
        (6, "REBOOT_INFO", tdf_parquet_schema(6).unwrap()),
        (7, "ANNOUNCE_V2", tdf_parquet_schema(7).unwrap()),
        (8, "SOC_TEMPERATURE", tdf_parquet_schema(8).unwrap()),
        (10, "ACC_2G", tdf_parquet_schema(10).unwrap()),
        (11, "ACC_4G", tdf_parquet_schema(11).unwrap()),
        (12, "ACC_8G", tdf_parquet_schema(12).unwrap()),
        (13, "ACC_16G", tdf_parquet_schema(13).unwrap()),
        (14, "GYR_125DPS", tdf_parquet_schema(14).unwrap()),
        (15, "GYR_250DPS", tdf_parquet_schema(15).unwrap()),
        (16, "GYR_500DPS", tdf_parquet_schema(16).unwrap()),
        (17, "GYR_1000DPS", tdf_parquet_schema(17).unwrap()),
        (18, "GYR_2000DPS", tdf_parquet_schema(18).unwrap()),
        (19, "GCS_WGS84_LLHA", tdf_parquet_schema(19).unwrap()),
        (20, "UBX_NAV_PVT", tdf_parquet_schema(20).unwrap()),
        (21, "LTE_CONN_STATUS", tdf_parquet_schema(21).unwrap()),
        (22, "GLOBALSTAR_PKT", tdf_parquet_schema(22).unwrap()),
        (23, "ACC_MAGNITUDE_STD_DEV", tdf_parquet_schema(23).unwrap()),
        (24, "ACTIVITY_METRIC", tdf_parquet_schema(24).unwrap()),
        (25, "ALGORITHM_OUTPUT", tdf_parquet_schema(25).unwrap()),
        (26, "RUNTIME_ERROR", tdf_parquet_schema(26).unwrap()),
        (27, "CHARGER_EN_CONTROL", tdf_parquet_schema(27).unwrap()),
        (28, "GNSS_FIX_INFO", tdf_parquet_schema(28).unwrap()),
        (29, "BLUETOOTH_CONNECTION", tdf_parquet_schema(29).unwrap()),
        (30, "BLUETOOTH_RSSI", tdf_parquet_schema(30).unwrap()),
        (
            31,
            "BLUETOOTH_DATA_THROUGHPUT",
            tdf_parquet_schema(31).unwrap(),
        ),
        (
            32,
            "ALGORITHM_CLASS_HISTOGRAM",
            tdf_parquet_schema(32).unwrap(),
        ),
        (
            33,
            "ALGORITHM_CLASS_TIME_SERIES",
            tdf_parquet_schema(33).unwrap(),
        ),
        (34, "LTE_TAC_CELLS", tdf_parquet_schema(34).unwrap()),
        (35, "WIFI_AP_INFO", tdf_parquet_schema(35).unwrap()),
        (36, "DEVICE_TILT", tdf_parquet_schema(36).unwrap()),
        (37, "NRF9X_GNSS_PVT", tdf_parquet_schema(37).unwrap()),
        (
            38,
            "BATTERY_CHARGE_ACCUMULATED",
            tdf_parquet_schema(38).unwrap(),
        ),
        (39, "INFUSE_BLUETOOTH_RSSI", tdf_parquet_schema(39).unwrap()),
        (40, "ADC_RAW_8", tdf_parquet_schema(40).unwrap()),
        (41, "ADC_RAW_16", tdf_parquet_schema(41).unwrap()),
        (42, "ADC_RAW_32", tdf_parquet_schema(42).unwrap()),
        (43, "ANNOTATION", tdf_parquet_schema(43).unwrap()),
        (44, "LORA_RX", tdf_parquet_schema(44).unwrap()),
        (45, "LORA_TX", tdf_parquet_schema(45).unwrap()),
        (46, "IDX_ARRAY_FREQ", tdf_parquet_schema(46).unwrap()),
        (47, "IDX_ARRAY_PERIOD", tdf_parquet_schema(47).unwrap()),
        (48, "WIFI_CONNECTED", tdf_parquet_schema(48).unwrap()),
        (
            49,
            "WIFI_CONNECTION_FAILED",
            tdf_parquet_schema(49).unwrap(),
        ),
        (50, "WIFI_DISCONNECTED", tdf_parquet_schema(50).unwrap()),
        (51, "NETWORK_SCAN_COUNT", tdf_parquet_schema(51).unwrap()),
        (52, "EXCEPTION_STACK_FRAME", tdf_parquet_schema(52).unwrap()),
        (53, "BATTERY_VOLTAGE", tdf_parquet_schema(53).unwrap()),
        (54, "BATTERY_SOC", tdf_parquet_schema(54).unwrap()),
        (55, "STATE_EVENT_SET", tdf_parquet_schema(55).unwrap()),
        (56, "STATE_EVENT_CLEARED", tdf_parquet_schema(56).unwrap()),
        (57, "STATE_DURATION", tdf_parquet_schema(57).unwrap()),
        (58, "PCM_16BIT_CHAN_LEFT", tdf_parquet_schema(58).unwrap()),
        (59, "PCM_16BIT_CHAN_RIGHT", tdf_parquet_schema(59).unwrap()),
        (60, "PCM_16BIT_CHAN_DUAL", tdf_parquet_schema(60).unwrap()),
        (61, "KVS_VALUE_CHANGED", tdf_parquet_schema(61).unwrap()),
    ]
}

pub fn tdf_parquet_has_schema(tdf_id: u16) -> bool {
    match tdf_id {
        1 => true,
        2 => true,
        3 => true,
        4 => true,
        5 => true,
        6 => true,
        7 => true,
        8 => true,
        10 => true,
        11 => true,
        12 => true,
        13 => true,
        14 => true,
        15 => true,
        16 => true,
        17 => true,
        18 => true,
        19 => true,
        20 => true,
        21 => true,
        22 => true,
        23 => true,
        24 => true,
        25 => true,
        26 => true,
        27 => true,
        28 => true,
        29 => true,
        30 => true,
        31 => true,
        32 => true,
        33 => true,
        34 => true,
        35 => true,
        36 => true,
        37 => true,
        38 => true,
        39 => true,
        40 => true,
        41 => true,
        42 => true,
        43 => true,
        44 => true,
        45 => true,
        46 => true,
        47 => true,
        48 => true,
        49 => true,
        50 => true,
        51 => true,
        52 => true,
        53 => true,
        54 => true,
        55 => true,
        56 => true,
        57 => true,
        58 => true,
        59 => true,
        60 => true,
        61 => true,
        _ => false,
    }
}

pub fn tdf_parquet_schema(tdf_id: u16) -> Option<SchemaRef> {
    match tdf_id {
        1 => Some(Arc::new(Schema::new(vec![
            timestamp_field(),
            sample_idx_field(),
            Field::new("application", DataType::UInt32, false),
            Field::new(
                "version",
                DataType::Struct(Fields::from(vec![
                    Field::new("major", DataType::UInt8, false),
                    Field::new("minor", DataType::UInt8, false),
                    Field::new("revision", DataType::UInt16, false),
                    Field::new("build_num", DataType::UInt32, false),
                ])),
                false,
            ),
            Field::new("kv_crc", DataType::UInt32, false),
            Field::new("blocks", DataType::UInt32, false),
            Field::new("uptime", DataType::UInt32, false),
            Field::new("reboots", DataType::UInt16, false),
            Field::new("flags", DataType::UInt8, false),
        ]))),
        2 => Some(Arc::new(Schema::new(vec![
            timestamp_field(),
            sample_idx_field(),
            Field::new("voltage_mv", DataType::UInt32, false),
            Field::new("current_ua", DataType::Int32, false),
            Field::new("soc", DataType::UInt8, false),
        ]))),
        3 => Some(Arc::new(Schema::new(vec![
            timestamp_field(),
            sample_idx_field(),
            Field::new("temperature", DataType::Float64, false),
            Field::new("pressure", DataType::Float64, false),
            Field::new("humidity", DataType::Float64, false),
        ]))),
        4 => Some(Arc::new(Schema::new(vec![
            timestamp_field(),
            sample_idx_field(),
            Field::new("temperature", DataType::Float64, false),
        ]))),
        5 => Some(Arc::new(Schema::new(vec![
            timestamp_field(),
            sample_idx_field(),
            Field::new("source", DataType::UInt8, false),
            Field::new("shift", DataType::Float64, false),
        ]))),
        6 => Some(Arc::new(Schema::new(vec![
            timestamp_field(),
            sample_idx_field(),
            Field::new("reason", DataType::UInt8, false),
            Field::new("hardware_flags", DataType::UInt32, false),
            Field::new("count", DataType::UInt32, false),
            Field::new("uptime", DataType::UInt32, false),
            Field::new("param_1", DataType::UInt32, false),
            Field::new("param_2", DataType::UInt32, false),
            Field::new("thread", DataType::Utf8, false),
        ]))),
        7 => Some(Arc::new(Schema::new(vec![
            timestamp_field(),
            sample_idx_field(),
            Field::new("application", DataType::UInt32, false),
            Field::new(
                "version",
                DataType::Struct(Fields::from(vec![
                    Field::new("major", DataType::UInt8, false),
                    Field::new("minor", DataType::UInt8, false),
                    Field::new("revision", DataType::UInt16, false),
                    Field::new("build_num", DataType::UInt32, false),
                ])),
                false,
            ),
            Field::new("board_crc", DataType::UInt16, false),
            Field::new("kv_crc", DataType::UInt32, false),
            Field::new("blocks", DataType::UInt32, false),
            Field::new("uptime", DataType::UInt32, false),
            Field::new("reboots", DataType::UInt16, false),
            Field::new("flags", DataType::UInt8, false),
        ]))),
        8 => Some(Arc::new(Schema::new(vec![
            timestamp_field(),
            sample_idx_field(),
            Field::new("temperature", DataType::Float64, false),
        ]))),
        10 => Some(Arc::new(Schema::new(vec![
            timestamp_field(),
            sample_idx_field(),
            Field::new(
                "sample",
                DataType::Struct(Fields::from(vec![
                    Field::new("x", DataType::Int16, false),
                    Field::new("y", DataType::Int16, false),
                    Field::new("z", DataType::Int16, false),
                ])),
                false,
            ),
        ]))),
        11 => Some(Arc::new(Schema::new(vec![
            timestamp_field(),
            sample_idx_field(),
            Field::new(
                "sample",
                DataType::Struct(Fields::from(vec![
                    Field::new("x", DataType::Int16, false),
                    Field::new("y", DataType::Int16, false),
                    Field::new("z", DataType::Int16, false),
                ])),
                false,
            ),
        ]))),
        12 => Some(Arc::new(Schema::new(vec![
            timestamp_field(),
            sample_idx_field(),
            Field::new(
                "sample",
                DataType::Struct(Fields::from(vec![
                    Field::new("x", DataType::Int16, false),
                    Field::new("y", DataType::Int16, false),
                    Field::new("z", DataType::Int16, false),
                ])),
                false,
            ),
        ]))),
        13 => Some(Arc::new(Schema::new(vec![
            timestamp_field(),
            sample_idx_field(),
            Field::new(
                "sample",
                DataType::Struct(Fields::from(vec![
                    Field::new("x", DataType::Int16, false),
                    Field::new("y", DataType::Int16, false),
                    Field::new("z", DataType::Int16, false),
                ])),
                false,
            ),
        ]))),
        14 => Some(Arc::new(Schema::new(vec![
            timestamp_field(),
            sample_idx_field(),
            Field::new(
                "sample",
                DataType::Struct(Fields::from(vec![
                    Field::new("x", DataType::Int16, false),
                    Field::new("y", DataType::Int16, false),
                    Field::new("z", DataType::Int16, false),
                ])),
                false,
            ),
        ]))),
        15 => Some(Arc::new(Schema::new(vec![
            timestamp_field(),
            sample_idx_field(),
            Field::new(
                "sample",
                DataType::Struct(Fields::from(vec![
                    Field::new("x", DataType::Int16, false),
                    Field::new("y", DataType::Int16, false),
                    Field::new("z", DataType::Int16, false),
                ])),
                false,
            ),
        ]))),
        16 => Some(Arc::new(Schema::new(vec![
            timestamp_field(),
            sample_idx_field(),
            Field::new(
                "sample",
                DataType::Struct(Fields::from(vec![
                    Field::new("x", DataType::Int16, false),
                    Field::new("y", DataType::Int16, false),
                    Field::new("z", DataType::Int16, false),
                ])),
                false,
            ),
        ]))),
        17 => Some(Arc::new(Schema::new(vec![
            timestamp_field(),
            sample_idx_field(),
            Field::new(
                "sample",
                DataType::Struct(Fields::from(vec![
                    Field::new("x", DataType::Int16, false),
                    Field::new("y", DataType::Int16, false),
                    Field::new("z", DataType::Int16, false),
                ])),
                false,
            ),
        ]))),
        18 => Some(Arc::new(Schema::new(vec![
            timestamp_field(),
            sample_idx_field(),
            Field::new(
                "sample",
                DataType::Struct(Fields::from(vec![
                    Field::new("x", DataType::Int16, false),
                    Field::new("y", DataType::Int16, false),
                    Field::new("z", DataType::Int16, false),
                ])),
                false,
            ),
        ]))),
        19 => Some(Arc::new(Schema::new(vec![
            timestamp_field(),
            sample_idx_field(),
            Field::new(
                "location",
                DataType::Struct(Fields::from(vec![
                    Field::new("latitude", DataType::Float64, false),
                    Field::new("longitude", DataType::Float64, false),
                    Field::new("height", DataType::Float64, false),
                ])),
                false,
            ),
            Field::new("h_acc", DataType::Float64, false),
            Field::new("v_acc", DataType::Float64, false),
        ]))),
        20 => Some(Arc::new(Schema::new(vec![
            timestamp_field(),
            sample_idx_field(),
            Field::new("itow", DataType::UInt32, false),
            Field::new("year", DataType::UInt16, false),
            Field::new("month", DataType::UInt8, false),
            Field::new("day", DataType::UInt8, false),
            Field::new("hour", DataType::UInt8, false),
            Field::new("min", DataType::UInt8, false),
            Field::new("sec", DataType::UInt8, false),
            Field::new("valid", DataType::UInt8, false),
            Field::new("t_acc", DataType::UInt32, false),
            Field::new("nano", DataType::Int32, false),
            Field::new("fix_type", DataType::UInt8, false),
            Field::new("flags", DataType::UInt8, false),
            Field::new("flags2", DataType::UInt8, false),
            Field::new("num_sv", DataType::UInt8, false),
            Field::new("lon", DataType::Float64, false),
            Field::new("lat", DataType::Float64, false),
            Field::new("height", DataType::Float64, false),
            Field::new("h_msl", DataType::Float64, false),
            Field::new("h_acc", DataType::Float64, false),
            Field::new("v_acc", DataType::Float64, false),
            Field::new("vel_n", DataType::Float64, false),
            Field::new("vel_e", DataType::Float64, false),
            Field::new("vel_d", DataType::Float64, false),
            Field::new("g_speed", DataType::Float64, false),
            Field::new("head_mot", DataType::Float64, false),
            Field::new("s_acc", DataType::Float64, false),
            Field::new("head_acc", DataType::Float64, false),
            Field::new("p_dop", DataType::Float64, false),
            Field::new("flags3", DataType::UInt16, false),
            Field::new(
                "reserved0",
                DataType::FixedSizeList(Arc::new(Field::new_list_field(DataType::UInt8, false)), 4),
                false,
            ),
            Field::new("head_veh", DataType::Float64, false),
            Field::new("mag_dec", DataType::Float64, false),
            Field::new("mag_acc", DataType::Float64, false),
        ]))),
        21 => Some(Arc::new(Schema::new(vec![
            timestamp_field(),
            sample_idx_field(),
            Field::new(
                "cell",
                DataType::Struct(Fields::from(vec![
                    Field::new("mcc", DataType::UInt16, false),
                    Field::new("mnc", DataType::UInt16, false),
                    Field::new("eci", DataType::UInt32, false),
                    Field::new("tac", DataType::UInt16, false),
                ])),
                false,
            ),
            Field::new("earfcn", DataType::UInt32, false),
            Field::new("status", DataType::UInt8, false),
            Field::new("tech", DataType::UInt8, false),
            Field::new("rsrp", DataType::Float64, false),
            Field::new("rsrq", DataType::Int8, false),
        ]))),
        22 => Some(Arc::new(Schema::new(vec![
            timestamp_field(),
            sample_idx_field(),
            Field::new(
                "payload",
                DataType::FixedSizeList(Arc::new(Field::new_list_field(DataType::UInt8, false)), 9),
                false,
            ),
        ]))),
        23 => Some(Arc::new(Schema::new(vec![
            timestamp_field(),
            sample_idx_field(),
            Field::new("count", DataType::UInt32, false),
            Field::new("std_dev", DataType::UInt32, false),
        ]))),
        24 => Some(Arc::new(Schema::new(vec![
            timestamp_field(),
            sample_idx_field(),
            Field::new("value", DataType::UInt32, false),
        ]))),
        25 => Some(Arc::new(Schema::new(vec![
            timestamp_field(),
            sample_idx_field(),
            Field::new("algorithm_id", DataType::UInt32, false),
            Field::new("algorithm_version", DataType::UInt16, false),
            Field::new("output", DataType::Binary, false),
        ]))),
        26 => Some(Arc::new(Schema::new(vec![
            timestamp_field(),
            sample_idx_field(),
            Field::new("error_id", DataType::UInt32, false),
            Field::new("error_ctx", DataType::UInt32, false),
        ]))),
        27 => Some(Arc::new(Schema::new(vec![
            timestamp_field(),
            sample_idx_field(),
            Field::new("enabled", DataType::UInt8, false),
        ]))),
        28 => Some(Arc::new(Schema::new(vec![
            timestamp_field(),
            sample_idx_field(),
            Field::new("time_fix", DataType::UInt16, false),
            Field::new("location_fix", DataType::UInt16, false),
            Field::new("num_sv", DataType::UInt8, false),
        ]))),
        29 => Some(Arc::new(Schema::new(vec![
            timestamp_field(),
            sample_idx_field(),
            Field::new(
                "address",
                DataType::Struct(Fields::from(vec![
                    Field::new("type", DataType::UInt8, false),
                    Field::new("val", DataType::UInt64, false),
                ])),
                false,
            ),
            Field::new("connected", DataType::UInt8, false),
        ]))),
        30 => Some(Arc::new(Schema::new(vec![
            timestamp_field(),
            sample_idx_field(),
            Field::new(
                "address",
                DataType::Struct(Fields::from(vec![
                    Field::new("type", DataType::UInt8, false),
                    Field::new("val", DataType::UInt64, false),
                ])),
                false,
            ),
            Field::new("rssi", DataType::Int8, false),
        ]))),
        31 => Some(Arc::new(Schema::new(vec![
            timestamp_field(),
            sample_idx_field(),
            Field::new(
                "address",
                DataType::Struct(Fields::from(vec![
                    Field::new("type", DataType::UInt8, false),
                    Field::new("val", DataType::UInt64, false),
                ])),
                false,
            ),
            Field::new("throughput", DataType::Int32, false),
        ]))),
        32 => Some(Arc::new(Schema::new(vec![
            timestamp_field(),
            sample_idx_field(),
            Field::new("algorithm_id", DataType::UInt32, false),
            Field::new("algorithm_version", DataType::UInt16, false),
            Field::new("classes", DataType::Binary, false),
        ]))),
        33 => Some(Arc::new(Schema::new(vec![
            timestamp_field(),
            sample_idx_field(),
            Field::new("algorithm_id", DataType::UInt32, false),
            Field::new("algorithm_version", DataType::UInt16, false),
            Field::new("values", DataType::Binary, false),
        ]))),
        34 => Some(Arc::new(Schema::new(vec![
            timestamp_field(),
            sample_idx_field(),
            Field::new(
                "cell",
                DataType::Struct(Fields::from(vec![
                    Field::new("mcc", DataType::UInt16, false),
                    Field::new("mnc", DataType::UInt16, false),
                    Field::new("eci", DataType::UInt32, false),
                    Field::new("tac", DataType::UInt16, false),
                ])),
                false,
            ),
            Field::new("earfcn", DataType::UInt32, false),
            Field::new("rsrp", DataType::Float64, false),
            Field::new("rsrq", DataType::Int8, false),
            Field::new(
                "neighbours",
                DataType::List(Arc::new(Field::new_list_field(
                    DataType::Struct(Fields::from(vec![
                        Field::new("earfcn", DataType::UInt32, false),
                        Field::new("pci", DataType::UInt16, false),
                        Field::new("time_diff", DataType::Float64, false),
                        Field::new("rsrp", DataType::Float64, false),
                        Field::new("rsrq", DataType::Int8, false),
                    ])),
                    false,
                ))),
                false,
            ),
        ]))),
        35 => Some(Arc::new(Schema::new(vec![
            timestamp_field(),
            sample_idx_field(),
            Field::new(
                "bssid",
                DataType::Struct(Fields::from(vec![Field::new(
                    "val",
                    DataType::UInt64,
                    false,
                )])),
                false,
            ),
            Field::new("channel", DataType::UInt8, false),
            Field::new("rsrp", DataType::Int8, false),
        ]))),
        36 => Some(Arc::new(Schema::new(vec![
            timestamp_field(),
            sample_idx_field(),
            Field::new("cosine", DataType::Float32, false),
        ]))),
        37 => Some(Arc::new(Schema::new(vec![
            timestamp_field(),
            sample_idx_field(),
            Field::new("lat", DataType::Float64, false),
            Field::new("lon", DataType::Float64, false),
            Field::new("height", DataType::Float64, false),
            Field::new("h_acc", DataType::Float64, false),
            Field::new("v_acc", DataType::Float64, false),
            Field::new("h_speed", DataType::Float64, false),
            Field::new("h_speed_acc", DataType::Float64, false),
            Field::new("v_speed", DataType::Float64, false),
            Field::new("v_speed_acc", DataType::Float64, false),
            Field::new("head_mot", DataType::Float64, false),
            Field::new("head_acc", DataType::Float64, false),
            Field::new("year", DataType::UInt16, false),
            Field::new("month", DataType::UInt8, false),
            Field::new("day", DataType::UInt8, false),
            Field::new("hour", DataType::UInt8, false),
            Field::new("min", DataType::UInt8, false),
            Field::new("sec", DataType::UInt8, false),
            Field::new("ms", DataType::UInt16, false),
            Field::new("p_dop", DataType::Float64, false),
            Field::new("h_dop", DataType::Float64, false),
            Field::new("v_dop", DataType::Float64, false),
            Field::new("t_dop", DataType::Float64, false),
            Field::new("flags", DataType::UInt8, false),
            Field::new("num_sv", DataType::UInt8, false),
        ]))),
        38 => Some(Arc::new(Schema::new(vec![
            timestamp_field(),
            sample_idx_field(),
            Field::new("charge", DataType::Int32, false),
        ]))),
        39 => Some(Arc::new(Schema::new(vec![
            timestamp_field(),
            sample_idx_field(),
            Field::new("infuse_id", DataType::UInt64, false),
            Field::new("rssi", DataType::Int8, false),
        ]))),
        40 => Some(Arc::new(Schema::new(vec![
            timestamp_field(),
            sample_idx_field(),
            Field::new("val", DataType::Int8, false),
        ]))),
        41 => Some(Arc::new(Schema::new(vec![
            timestamp_field(),
            sample_idx_field(),
            Field::new("val", DataType::Int16, false),
        ]))),
        42 => Some(Arc::new(Schema::new(vec![
            timestamp_field(),
            sample_idx_field(),
            Field::new("val", DataType::Int32, false),
        ]))),
        43 => Some(Arc::new(Schema::new(vec![
            timestamp_field(),
            sample_idx_field(),
            Field::new("timestamp", DataType::UInt32, false),
            Field::new("event", DataType::Utf8, false),
        ]))),
        44 => Some(Arc::new(Schema::new(vec![
            timestamp_field(),
            sample_idx_field(),
            Field::new("snr", DataType::Int8, false),
            Field::new("rssi", DataType::Int16, false),
            Field::new("payload", DataType::Binary, false),
        ]))),
        45 => Some(Arc::new(Schema::new(vec![
            timestamp_field(),
            sample_idx_field(),
            Field::new("payload", DataType::Binary, false),
        ]))),
        46 => Some(Arc::new(Schema::new(vec![
            timestamp_field(),
            sample_idx_field(),
            Field::new("tdf_id", DataType::UInt16, false),
            Field::new("frequency", DataType::UInt32, false),
        ]))),
        47 => Some(Arc::new(Schema::new(vec![
            timestamp_field(),
            sample_idx_field(),
            Field::new("tdf_id", DataType::UInt16, false),
            Field::new("period", DataType::UInt32, false),
        ]))),
        48 => Some(Arc::new(Schema::new(vec![
            timestamp_field(),
            sample_idx_field(),
            Field::new(
                "network",
                DataType::Struct(Fields::from(vec![
                    Field::new("bssid", DataType::UInt64, false),
                    Field::new("band", DataType::UInt8, false),
                    Field::new("channel", DataType::UInt8, false),
                    Field::new("iface_mode", DataType::UInt8, false),
                    Field::new("link_mode", DataType::UInt8, false),
                    Field::new("security", DataType::UInt8, false),
                    Field::new("rssi", DataType::Int8, false),
                    Field::new("beacon_interval", DataType::UInt16, false),
                    Field::new("twt_capable", DataType::UInt8, false),
                ])),
                false,
            ),
        ]))),
        49 => Some(Arc::new(Schema::new(vec![
            timestamp_field(),
            sample_idx_field(),
            Field::new("reason", DataType::UInt8, false),
        ]))),
        50 => Some(Arc::new(Schema::new(vec![
            timestamp_field(),
            sample_idx_field(),
            Field::new("reason", DataType::UInt8, false),
        ]))),
        51 => Some(Arc::new(Schema::new(vec![
            timestamp_field(),
            sample_idx_field(),
            Field::new("num_wifi", DataType::UInt8, false),
            Field::new("num_lte", DataType::UInt8, false),
        ]))),
        52 => Some(Arc::new(Schema::new(vec![
            timestamp_field(),
            sample_idx_field(),
            Field::new(
                "frame",
                DataType::List(Arc::new(Field::new_list_field(DataType::UInt32, false))),
                false,
            ),
        ]))),
        53 => Some(Arc::new(Schema::new(vec![
            timestamp_field(),
            sample_idx_field(),
            Field::new("voltage", DataType::UInt16, false),
        ]))),
        54 => Some(Arc::new(Schema::new(vec![
            timestamp_field(),
            sample_idx_field(),
            Field::new("soc", DataType::UInt8, false),
        ]))),
        55 => Some(Arc::new(Schema::new(vec![
            timestamp_field(),
            sample_idx_field(),
            Field::new("state", DataType::UInt8, false),
        ]))),
        56 => Some(Arc::new(Schema::new(vec![
            timestamp_field(),
            sample_idx_field(),
            Field::new("state", DataType::UInt8, false),
        ]))),
        57 => Some(Arc::new(Schema::new(vec![
            timestamp_field(),
            sample_idx_field(),
            Field::new("state", DataType::UInt8, false),
            Field::new("duration", DataType::UInt32, false),
        ]))),
        58 => Some(Arc::new(Schema::new(vec![
            timestamp_field(),
            sample_idx_field(),
            Field::new("val", DataType::Int16, false),
        ]))),
        59 => Some(Arc::new(Schema::new(vec![
            timestamp_field(),
            sample_idx_field(),
            Field::new("val", DataType::Int16, false),
        ]))),
        60 => Some(Arc::new(Schema::new(vec![
            timestamp_field(),
            sample_idx_field(),
            Field::new("left", DataType::Int16, false),
            Field::new("right", DataType::Int16, false),
        ]))),
        61 => Some(Arc::new(Schema::new(vec![
            timestamp_field(),
            sample_idx_field(),
            Field::new("key", DataType::UInt16, false),
            Field::new("value", DataType::Binary, false),
        ]))),
        _ => None,
    }
}

pub fn tdf_parquet_builder(tdf_id: u16, capacity: usize) -> Option<TdfParquetBatchBuilder> {
    match tdf_id {
        1 => Some(TdfParquetBatchBuilder::Tdf1Announce(
            Tdf1AnnounceBuilder::new(capacity),
        )),
        2 => Some(TdfParquetBatchBuilder::Tdf2BatteryState(
            Tdf2BatteryStateBuilder::new(capacity),
        )),
        3 => Some(TdfParquetBatchBuilder::Tdf3AmbientTempPresHum(
            Tdf3AmbientTempPresHumBuilder::new(capacity),
        )),
        4 => Some(TdfParquetBatchBuilder::Tdf4AmbientTemperature(
            Tdf4AmbientTemperatureBuilder::new(capacity),
        )),
        5 => Some(TdfParquetBatchBuilder::Tdf5TimeSync(
            Tdf5TimeSyncBuilder::new(capacity),
        )),
        6 => Some(TdfParquetBatchBuilder::Tdf6RebootInfo(
            Tdf6RebootInfoBuilder::new(capacity),
        )),
        7 => Some(TdfParquetBatchBuilder::Tdf7AnnounceV2(
            Tdf7AnnounceV2Builder::new(capacity),
        )),
        8 => Some(TdfParquetBatchBuilder::Tdf8SocTemperature(
            Tdf8SocTemperatureBuilder::new(capacity),
        )),
        10 => Some(TdfParquetBatchBuilder::Tdf10Acc2g(Tdf10Acc2gBuilder::new(
            capacity,
        ))),
        11 => Some(TdfParquetBatchBuilder::Tdf11Acc4g(Tdf11Acc4gBuilder::new(
            capacity,
        ))),
        12 => Some(TdfParquetBatchBuilder::Tdf12Acc8g(Tdf12Acc8gBuilder::new(
            capacity,
        ))),
        13 => Some(TdfParquetBatchBuilder::Tdf13Acc16g(
            Tdf13Acc16gBuilder::new(capacity),
        )),
        14 => Some(TdfParquetBatchBuilder::Tdf14Gyr125dps(
            Tdf14Gyr125dpsBuilder::new(capacity),
        )),
        15 => Some(TdfParquetBatchBuilder::Tdf15Gyr250dps(
            Tdf15Gyr250dpsBuilder::new(capacity),
        )),
        16 => Some(TdfParquetBatchBuilder::Tdf16Gyr500dps(
            Tdf16Gyr500dpsBuilder::new(capacity),
        )),
        17 => Some(TdfParquetBatchBuilder::Tdf17Gyr1000dps(
            Tdf17Gyr1000dpsBuilder::new(capacity),
        )),
        18 => Some(TdfParquetBatchBuilder::Tdf18Gyr2000dps(
            Tdf18Gyr2000dpsBuilder::new(capacity),
        )),
        19 => Some(TdfParquetBatchBuilder::Tdf19GcsWgs84Llha(
            Tdf19GcsWgs84LlhaBuilder::new(capacity),
        )),
        20 => Some(TdfParquetBatchBuilder::Tdf20UbxNavPvt(
            Tdf20UbxNavPvtBuilder::new(capacity),
        )),
        21 => Some(TdfParquetBatchBuilder::Tdf21LteConnStatus(
            Tdf21LteConnStatusBuilder::new(capacity),
        )),
        22 => Some(TdfParquetBatchBuilder::Tdf22GlobalstarPkt(
            Tdf22GlobalstarPktBuilder::new(capacity),
        )),
        23 => Some(TdfParquetBatchBuilder::Tdf23AccMagnitudeStdDev(
            Tdf23AccMagnitudeStdDevBuilder::new(capacity),
        )),
        24 => Some(TdfParquetBatchBuilder::Tdf24ActivityMetric(
            Tdf24ActivityMetricBuilder::new(capacity),
        )),
        25 => Some(TdfParquetBatchBuilder::Tdf25AlgorithmOutput(
            Tdf25AlgorithmOutputBuilder::new(capacity),
        )),
        26 => Some(TdfParquetBatchBuilder::Tdf26RuntimeError(
            Tdf26RuntimeErrorBuilder::new(capacity),
        )),
        27 => Some(TdfParquetBatchBuilder::Tdf27ChargerEnControl(
            Tdf27ChargerEnControlBuilder::new(capacity),
        )),
        28 => Some(TdfParquetBatchBuilder::Tdf28GnssFixInfo(
            Tdf28GnssFixInfoBuilder::new(capacity),
        )),
        29 => Some(TdfParquetBatchBuilder::Tdf29BluetoothConnection(
            Tdf29BluetoothConnectionBuilder::new(capacity),
        )),
        30 => Some(TdfParquetBatchBuilder::Tdf30BluetoothRssi(
            Tdf30BluetoothRssiBuilder::new(capacity),
        )),
        31 => Some(TdfParquetBatchBuilder::Tdf31BluetoothDataThroughput(
            Tdf31BluetoothDataThroughputBuilder::new(capacity),
        )),
        32 => Some(TdfParquetBatchBuilder::Tdf32AlgorithmClassHistogram(
            Tdf32AlgorithmClassHistogramBuilder::new(capacity),
        )),
        33 => Some(TdfParquetBatchBuilder::Tdf33AlgorithmClassTimeSeries(
            Tdf33AlgorithmClassTimeSeriesBuilder::new(capacity),
        )),
        34 => Some(TdfParquetBatchBuilder::Tdf34LteTacCells(
            Tdf34LteTacCellsBuilder::new(capacity),
        )),
        35 => Some(TdfParquetBatchBuilder::Tdf35WifiApInfo(
            Tdf35WifiApInfoBuilder::new(capacity),
        )),
        36 => Some(TdfParquetBatchBuilder::Tdf36DeviceTilt(
            Tdf36DeviceTiltBuilder::new(capacity),
        )),
        37 => Some(TdfParquetBatchBuilder::Tdf37Nrf9xGnssPvt(
            Tdf37Nrf9xGnssPvtBuilder::new(capacity),
        )),
        38 => Some(TdfParquetBatchBuilder::Tdf38BatteryChargeAccumulated(
            Tdf38BatteryChargeAccumulatedBuilder::new(capacity),
        )),
        39 => Some(TdfParquetBatchBuilder::Tdf39InfuseBluetoothRssi(
            Tdf39InfuseBluetoothRssiBuilder::new(capacity),
        )),
        40 => Some(TdfParquetBatchBuilder::Tdf40AdcRaw8(
            Tdf40AdcRaw8Builder::new(capacity),
        )),
        41 => Some(TdfParquetBatchBuilder::Tdf41AdcRaw16(
            Tdf41AdcRaw16Builder::new(capacity),
        )),
        42 => Some(TdfParquetBatchBuilder::Tdf42AdcRaw32(
            Tdf42AdcRaw32Builder::new(capacity),
        )),
        43 => Some(TdfParquetBatchBuilder::Tdf43Annotation(
            Tdf43AnnotationBuilder::new(capacity),
        )),
        44 => Some(TdfParquetBatchBuilder::Tdf44LoraRx(
            Tdf44LoraRxBuilder::new(capacity),
        )),
        45 => Some(TdfParquetBatchBuilder::Tdf45LoraTx(
            Tdf45LoraTxBuilder::new(capacity),
        )),
        46 => Some(TdfParquetBatchBuilder::Tdf46IdxArrayFreq(
            Tdf46IdxArrayFreqBuilder::new(capacity),
        )),
        47 => Some(TdfParquetBatchBuilder::Tdf47IdxArrayPeriod(
            Tdf47IdxArrayPeriodBuilder::new(capacity),
        )),
        48 => Some(TdfParquetBatchBuilder::Tdf48WifiConnected(
            Tdf48WifiConnectedBuilder::new(capacity),
        )),
        49 => Some(TdfParquetBatchBuilder::Tdf49WifiConnectionFailed(
            Tdf49WifiConnectionFailedBuilder::new(capacity),
        )),
        50 => Some(TdfParquetBatchBuilder::Tdf50WifiDisconnected(
            Tdf50WifiDisconnectedBuilder::new(capacity),
        )),
        51 => Some(TdfParquetBatchBuilder::Tdf51NetworkScanCount(
            Tdf51NetworkScanCountBuilder::new(capacity),
        )),
        52 => Some(TdfParquetBatchBuilder::Tdf52ExceptionStackFrame(
            Tdf52ExceptionStackFrameBuilder::new(capacity),
        )),
        53 => Some(TdfParquetBatchBuilder::Tdf53BatteryVoltage(
            Tdf53BatteryVoltageBuilder::new(capacity),
        )),
        54 => Some(TdfParquetBatchBuilder::Tdf54BatterySoc(
            Tdf54BatterySocBuilder::new(capacity),
        )),
        55 => Some(TdfParquetBatchBuilder::Tdf55StateEventSet(
            Tdf55StateEventSetBuilder::new(capacity),
        )),
        56 => Some(TdfParquetBatchBuilder::Tdf56StateEventCleared(
            Tdf56StateEventClearedBuilder::new(capacity),
        )),
        57 => Some(TdfParquetBatchBuilder::Tdf57StateDuration(
            Tdf57StateDurationBuilder::new(capacity),
        )),
        58 => Some(TdfParquetBatchBuilder::Tdf58Pcm16bitChanLeft(
            Tdf58Pcm16bitChanLeftBuilder::new(capacity),
        )),
        59 => Some(TdfParquetBatchBuilder::Tdf59Pcm16bitChanRight(
            Tdf59Pcm16bitChanRightBuilder::new(capacity),
        )),
        60 => Some(TdfParquetBatchBuilder::Tdf60Pcm16bitChanDual(
            Tdf60Pcm16bitChanDualBuilder::new(capacity),
        )),
        61 => Some(TdfParquetBatchBuilder::Tdf61KvsValueChanged(
            Tdf61KvsValueChangedBuilder::new(capacity),
        )),
        _ => None,
    }
}

#[derive(Clone, Copy, Debug)]
pub struct TdfParquetRowMeta {
    pub time_unix_micros: Option<i64>,
    pub sample_idx: Option<u16>,
}

pub enum TdfParquetBatchBuilder {
    Tdf1Announce(Tdf1AnnounceBuilder),
    Tdf2BatteryState(Tdf2BatteryStateBuilder),
    Tdf3AmbientTempPresHum(Tdf3AmbientTempPresHumBuilder),
    Tdf4AmbientTemperature(Tdf4AmbientTemperatureBuilder),
    Tdf5TimeSync(Tdf5TimeSyncBuilder),
    Tdf6RebootInfo(Tdf6RebootInfoBuilder),
    Tdf7AnnounceV2(Tdf7AnnounceV2Builder),
    Tdf8SocTemperature(Tdf8SocTemperatureBuilder),
    Tdf10Acc2g(Tdf10Acc2gBuilder),
    Tdf11Acc4g(Tdf11Acc4gBuilder),
    Tdf12Acc8g(Tdf12Acc8gBuilder),
    Tdf13Acc16g(Tdf13Acc16gBuilder),
    Tdf14Gyr125dps(Tdf14Gyr125dpsBuilder),
    Tdf15Gyr250dps(Tdf15Gyr250dpsBuilder),
    Tdf16Gyr500dps(Tdf16Gyr500dpsBuilder),
    Tdf17Gyr1000dps(Tdf17Gyr1000dpsBuilder),
    Tdf18Gyr2000dps(Tdf18Gyr2000dpsBuilder),
    Tdf19GcsWgs84Llha(Tdf19GcsWgs84LlhaBuilder),
    Tdf20UbxNavPvt(Tdf20UbxNavPvtBuilder),
    Tdf21LteConnStatus(Tdf21LteConnStatusBuilder),
    Tdf22GlobalstarPkt(Tdf22GlobalstarPktBuilder),
    Tdf23AccMagnitudeStdDev(Tdf23AccMagnitudeStdDevBuilder),
    Tdf24ActivityMetric(Tdf24ActivityMetricBuilder),
    Tdf25AlgorithmOutput(Tdf25AlgorithmOutputBuilder),
    Tdf26RuntimeError(Tdf26RuntimeErrorBuilder),
    Tdf27ChargerEnControl(Tdf27ChargerEnControlBuilder),
    Tdf28GnssFixInfo(Tdf28GnssFixInfoBuilder),
    Tdf29BluetoothConnection(Tdf29BluetoothConnectionBuilder),
    Tdf30BluetoothRssi(Tdf30BluetoothRssiBuilder),
    Tdf31BluetoothDataThroughput(Tdf31BluetoothDataThroughputBuilder),
    Tdf32AlgorithmClassHistogram(Tdf32AlgorithmClassHistogramBuilder),
    Tdf33AlgorithmClassTimeSeries(Tdf33AlgorithmClassTimeSeriesBuilder),
    Tdf34LteTacCells(Tdf34LteTacCellsBuilder),
    Tdf35WifiApInfo(Tdf35WifiApInfoBuilder),
    Tdf36DeviceTilt(Tdf36DeviceTiltBuilder),
    Tdf37Nrf9xGnssPvt(Tdf37Nrf9xGnssPvtBuilder),
    Tdf38BatteryChargeAccumulated(Tdf38BatteryChargeAccumulatedBuilder),
    Tdf39InfuseBluetoothRssi(Tdf39InfuseBluetoothRssiBuilder),
    Tdf40AdcRaw8(Tdf40AdcRaw8Builder),
    Tdf41AdcRaw16(Tdf41AdcRaw16Builder),
    Tdf42AdcRaw32(Tdf42AdcRaw32Builder),
    Tdf43Annotation(Tdf43AnnotationBuilder),
    Tdf44LoraRx(Tdf44LoraRxBuilder),
    Tdf45LoraTx(Tdf45LoraTxBuilder),
    Tdf46IdxArrayFreq(Tdf46IdxArrayFreqBuilder),
    Tdf47IdxArrayPeriod(Tdf47IdxArrayPeriodBuilder),
    Tdf48WifiConnected(Tdf48WifiConnectedBuilder),
    Tdf49WifiConnectionFailed(Tdf49WifiConnectionFailedBuilder),
    Tdf50WifiDisconnected(Tdf50WifiDisconnectedBuilder),
    Tdf51NetworkScanCount(Tdf51NetworkScanCountBuilder),
    Tdf52ExceptionStackFrame(Tdf52ExceptionStackFrameBuilder),
    Tdf53BatteryVoltage(Tdf53BatteryVoltageBuilder),
    Tdf54BatterySoc(Tdf54BatterySocBuilder),
    Tdf55StateEventSet(Tdf55StateEventSetBuilder),
    Tdf56StateEventCleared(Tdf56StateEventClearedBuilder),
    Tdf57StateDuration(Tdf57StateDurationBuilder),
    Tdf58Pcm16bitChanLeft(Tdf58Pcm16bitChanLeftBuilder),
    Tdf59Pcm16bitChanRight(Tdf59Pcm16bitChanRightBuilder),
    Tdf60Pcm16bitChanDual(Tdf60Pcm16bitChanDualBuilder),
    Tdf61KvsValueChanged(Tdf61KvsValueChangedBuilder),
}

impl TdfParquetBatchBuilder {
    pub fn schema(&self) -> SchemaRef {
        match self {
            Self::Tdf1Announce(builder) => builder.schema(),
            Self::Tdf2BatteryState(builder) => builder.schema(),
            Self::Tdf3AmbientTempPresHum(builder) => builder.schema(),
            Self::Tdf4AmbientTemperature(builder) => builder.schema(),
            Self::Tdf5TimeSync(builder) => builder.schema(),
            Self::Tdf6RebootInfo(builder) => builder.schema(),
            Self::Tdf7AnnounceV2(builder) => builder.schema(),
            Self::Tdf8SocTemperature(builder) => builder.schema(),
            Self::Tdf10Acc2g(builder) => builder.schema(),
            Self::Tdf11Acc4g(builder) => builder.schema(),
            Self::Tdf12Acc8g(builder) => builder.schema(),
            Self::Tdf13Acc16g(builder) => builder.schema(),
            Self::Tdf14Gyr125dps(builder) => builder.schema(),
            Self::Tdf15Gyr250dps(builder) => builder.schema(),
            Self::Tdf16Gyr500dps(builder) => builder.schema(),
            Self::Tdf17Gyr1000dps(builder) => builder.schema(),
            Self::Tdf18Gyr2000dps(builder) => builder.schema(),
            Self::Tdf19GcsWgs84Llha(builder) => builder.schema(),
            Self::Tdf20UbxNavPvt(builder) => builder.schema(),
            Self::Tdf21LteConnStatus(builder) => builder.schema(),
            Self::Tdf22GlobalstarPkt(builder) => builder.schema(),
            Self::Tdf23AccMagnitudeStdDev(builder) => builder.schema(),
            Self::Tdf24ActivityMetric(builder) => builder.schema(),
            Self::Tdf25AlgorithmOutput(builder) => builder.schema(),
            Self::Tdf26RuntimeError(builder) => builder.schema(),
            Self::Tdf27ChargerEnControl(builder) => builder.schema(),
            Self::Tdf28GnssFixInfo(builder) => builder.schema(),
            Self::Tdf29BluetoothConnection(builder) => builder.schema(),
            Self::Tdf30BluetoothRssi(builder) => builder.schema(),
            Self::Tdf31BluetoothDataThroughput(builder) => builder.schema(),
            Self::Tdf32AlgorithmClassHistogram(builder) => builder.schema(),
            Self::Tdf33AlgorithmClassTimeSeries(builder) => builder.schema(),
            Self::Tdf34LteTacCells(builder) => builder.schema(),
            Self::Tdf35WifiApInfo(builder) => builder.schema(),
            Self::Tdf36DeviceTilt(builder) => builder.schema(),
            Self::Tdf37Nrf9xGnssPvt(builder) => builder.schema(),
            Self::Tdf38BatteryChargeAccumulated(builder) => builder.schema(),
            Self::Tdf39InfuseBluetoothRssi(builder) => builder.schema(),
            Self::Tdf40AdcRaw8(builder) => builder.schema(),
            Self::Tdf41AdcRaw16(builder) => builder.schema(),
            Self::Tdf42AdcRaw32(builder) => builder.schema(),
            Self::Tdf43Annotation(builder) => builder.schema(),
            Self::Tdf44LoraRx(builder) => builder.schema(),
            Self::Tdf45LoraTx(builder) => builder.schema(),
            Self::Tdf46IdxArrayFreq(builder) => builder.schema(),
            Self::Tdf47IdxArrayPeriod(builder) => builder.schema(),
            Self::Tdf48WifiConnected(builder) => builder.schema(),
            Self::Tdf49WifiConnectionFailed(builder) => builder.schema(),
            Self::Tdf50WifiDisconnected(builder) => builder.schema(),
            Self::Tdf51NetworkScanCount(builder) => builder.schema(),
            Self::Tdf52ExceptionStackFrame(builder) => builder.schema(),
            Self::Tdf53BatteryVoltage(builder) => builder.schema(),
            Self::Tdf54BatterySoc(builder) => builder.schema(),
            Self::Tdf55StateEventSet(builder) => builder.schema(),
            Self::Tdf56StateEventCleared(builder) => builder.schema(),
            Self::Tdf57StateDuration(builder) => builder.schema(),
            Self::Tdf58Pcm16bitChanLeft(builder) => builder.schema(),
            Self::Tdf59Pcm16bitChanRight(builder) => builder.schema(),
            Self::Tdf60Pcm16bitChanDual(builder) => builder.schema(),
            Self::Tdf61KvsValueChanged(builder) => builder.schema(),
        }
    }

    pub fn rows(&self) -> usize {
        match self {
            Self::Tdf1Announce(builder) => builder.rows(),
            Self::Tdf2BatteryState(builder) => builder.rows(),
            Self::Tdf3AmbientTempPresHum(builder) => builder.rows(),
            Self::Tdf4AmbientTemperature(builder) => builder.rows(),
            Self::Tdf5TimeSync(builder) => builder.rows(),
            Self::Tdf6RebootInfo(builder) => builder.rows(),
            Self::Tdf7AnnounceV2(builder) => builder.rows(),
            Self::Tdf8SocTemperature(builder) => builder.rows(),
            Self::Tdf10Acc2g(builder) => builder.rows(),
            Self::Tdf11Acc4g(builder) => builder.rows(),
            Self::Tdf12Acc8g(builder) => builder.rows(),
            Self::Tdf13Acc16g(builder) => builder.rows(),
            Self::Tdf14Gyr125dps(builder) => builder.rows(),
            Self::Tdf15Gyr250dps(builder) => builder.rows(),
            Self::Tdf16Gyr500dps(builder) => builder.rows(),
            Self::Tdf17Gyr1000dps(builder) => builder.rows(),
            Self::Tdf18Gyr2000dps(builder) => builder.rows(),
            Self::Tdf19GcsWgs84Llha(builder) => builder.rows(),
            Self::Tdf20UbxNavPvt(builder) => builder.rows(),
            Self::Tdf21LteConnStatus(builder) => builder.rows(),
            Self::Tdf22GlobalstarPkt(builder) => builder.rows(),
            Self::Tdf23AccMagnitudeStdDev(builder) => builder.rows(),
            Self::Tdf24ActivityMetric(builder) => builder.rows(),
            Self::Tdf25AlgorithmOutput(builder) => builder.rows(),
            Self::Tdf26RuntimeError(builder) => builder.rows(),
            Self::Tdf27ChargerEnControl(builder) => builder.rows(),
            Self::Tdf28GnssFixInfo(builder) => builder.rows(),
            Self::Tdf29BluetoothConnection(builder) => builder.rows(),
            Self::Tdf30BluetoothRssi(builder) => builder.rows(),
            Self::Tdf31BluetoothDataThroughput(builder) => builder.rows(),
            Self::Tdf32AlgorithmClassHistogram(builder) => builder.rows(),
            Self::Tdf33AlgorithmClassTimeSeries(builder) => builder.rows(),
            Self::Tdf34LteTacCells(builder) => builder.rows(),
            Self::Tdf35WifiApInfo(builder) => builder.rows(),
            Self::Tdf36DeviceTilt(builder) => builder.rows(),
            Self::Tdf37Nrf9xGnssPvt(builder) => builder.rows(),
            Self::Tdf38BatteryChargeAccumulated(builder) => builder.rows(),
            Self::Tdf39InfuseBluetoothRssi(builder) => builder.rows(),
            Self::Tdf40AdcRaw8(builder) => builder.rows(),
            Self::Tdf41AdcRaw16(builder) => builder.rows(),
            Self::Tdf42AdcRaw32(builder) => builder.rows(),
            Self::Tdf43Annotation(builder) => builder.rows(),
            Self::Tdf44LoraRx(builder) => builder.rows(),
            Self::Tdf45LoraTx(builder) => builder.rows(),
            Self::Tdf46IdxArrayFreq(builder) => builder.rows(),
            Self::Tdf47IdxArrayPeriod(builder) => builder.rows(),
            Self::Tdf48WifiConnected(builder) => builder.rows(),
            Self::Tdf49WifiConnectionFailed(builder) => builder.rows(),
            Self::Tdf50WifiDisconnected(builder) => builder.rows(),
            Self::Tdf51NetworkScanCount(builder) => builder.rows(),
            Self::Tdf52ExceptionStackFrame(builder) => builder.rows(),
            Self::Tdf53BatteryVoltage(builder) => builder.rows(),
            Self::Tdf54BatterySoc(builder) => builder.rows(),
            Self::Tdf55StateEventSet(builder) => builder.rows(),
            Self::Tdf56StateEventCleared(builder) => builder.rows(),
            Self::Tdf57StateDuration(builder) => builder.rows(),
            Self::Tdf58Pcm16bitChanLeft(builder) => builder.rows(),
            Self::Tdf59Pcm16bitChanRight(builder) => builder.rows(),
            Self::Tdf60Pcm16bitChanDual(builder) => builder.rows(),
            Self::Tdf61KvsValueChanged(builder) => builder.rows(),
        }
    }

    pub fn append(
        &mut self,
        meta: TdfParquetRowMeta,
        size: u8,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<()> {
        match self {
            Self::Tdf1Announce(builder) => builder.append(meta, size, cursor),
            Self::Tdf2BatteryState(builder) => builder.append(meta, size, cursor),
            Self::Tdf3AmbientTempPresHum(builder) => builder.append(meta, size, cursor),
            Self::Tdf4AmbientTemperature(builder) => builder.append(meta, size, cursor),
            Self::Tdf5TimeSync(builder) => builder.append(meta, size, cursor),
            Self::Tdf6RebootInfo(builder) => builder.append(meta, size, cursor),
            Self::Tdf7AnnounceV2(builder) => builder.append(meta, size, cursor),
            Self::Tdf8SocTemperature(builder) => builder.append(meta, size, cursor),
            Self::Tdf10Acc2g(builder) => builder.append(meta, size, cursor),
            Self::Tdf11Acc4g(builder) => builder.append(meta, size, cursor),
            Self::Tdf12Acc8g(builder) => builder.append(meta, size, cursor),
            Self::Tdf13Acc16g(builder) => builder.append(meta, size, cursor),
            Self::Tdf14Gyr125dps(builder) => builder.append(meta, size, cursor),
            Self::Tdf15Gyr250dps(builder) => builder.append(meta, size, cursor),
            Self::Tdf16Gyr500dps(builder) => builder.append(meta, size, cursor),
            Self::Tdf17Gyr1000dps(builder) => builder.append(meta, size, cursor),
            Self::Tdf18Gyr2000dps(builder) => builder.append(meta, size, cursor),
            Self::Tdf19GcsWgs84Llha(builder) => builder.append(meta, size, cursor),
            Self::Tdf20UbxNavPvt(builder) => builder.append(meta, size, cursor),
            Self::Tdf21LteConnStatus(builder) => builder.append(meta, size, cursor),
            Self::Tdf22GlobalstarPkt(builder) => builder.append(meta, size, cursor),
            Self::Tdf23AccMagnitudeStdDev(builder) => builder.append(meta, size, cursor),
            Self::Tdf24ActivityMetric(builder) => builder.append(meta, size, cursor),
            Self::Tdf25AlgorithmOutput(builder) => builder.append(meta, size, cursor),
            Self::Tdf26RuntimeError(builder) => builder.append(meta, size, cursor),
            Self::Tdf27ChargerEnControl(builder) => builder.append(meta, size, cursor),
            Self::Tdf28GnssFixInfo(builder) => builder.append(meta, size, cursor),
            Self::Tdf29BluetoothConnection(builder) => builder.append(meta, size, cursor),
            Self::Tdf30BluetoothRssi(builder) => builder.append(meta, size, cursor),
            Self::Tdf31BluetoothDataThroughput(builder) => builder.append(meta, size, cursor),
            Self::Tdf32AlgorithmClassHistogram(builder) => builder.append(meta, size, cursor),
            Self::Tdf33AlgorithmClassTimeSeries(builder) => builder.append(meta, size, cursor),
            Self::Tdf34LteTacCells(builder) => builder.append(meta, size, cursor),
            Self::Tdf35WifiApInfo(builder) => builder.append(meta, size, cursor),
            Self::Tdf36DeviceTilt(builder) => builder.append(meta, size, cursor),
            Self::Tdf37Nrf9xGnssPvt(builder) => builder.append(meta, size, cursor),
            Self::Tdf38BatteryChargeAccumulated(builder) => builder.append(meta, size, cursor),
            Self::Tdf39InfuseBluetoothRssi(builder) => builder.append(meta, size, cursor),
            Self::Tdf40AdcRaw8(builder) => builder.append(meta, size, cursor),
            Self::Tdf41AdcRaw16(builder) => builder.append(meta, size, cursor),
            Self::Tdf42AdcRaw32(builder) => builder.append(meta, size, cursor),
            Self::Tdf43Annotation(builder) => builder.append(meta, size, cursor),
            Self::Tdf44LoraRx(builder) => builder.append(meta, size, cursor),
            Self::Tdf45LoraTx(builder) => builder.append(meta, size, cursor),
            Self::Tdf46IdxArrayFreq(builder) => builder.append(meta, size, cursor),
            Self::Tdf47IdxArrayPeriod(builder) => builder.append(meta, size, cursor),
            Self::Tdf48WifiConnected(builder) => builder.append(meta, size, cursor),
            Self::Tdf49WifiConnectionFailed(builder) => builder.append(meta, size, cursor),
            Self::Tdf50WifiDisconnected(builder) => builder.append(meta, size, cursor),
            Self::Tdf51NetworkScanCount(builder) => builder.append(meta, size, cursor),
            Self::Tdf52ExceptionStackFrame(builder) => builder.append(meta, size, cursor),
            Self::Tdf53BatteryVoltage(builder) => builder.append(meta, size, cursor),
            Self::Tdf54BatterySoc(builder) => builder.append(meta, size, cursor),
            Self::Tdf55StateEventSet(builder) => builder.append(meta, size, cursor),
            Self::Tdf56StateEventCleared(builder) => builder.append(meta, size, cursor),
            Self::Tdf57StateDuration(builder) => builder.append(meta, size, cursor),
            Self::Tdf58Pcm16bitChanLeft(builder) => builder.append(meta, size, cursor),
            Self::Tdf59Pcm16bitChanRight(builder) => builder.append(meta, size, cursor),
            Self::Tdf60Pcm16bitChanDual(builder) => builder.append(meta, size, cursor),
            Self::Tdf61KvsValueChanged(builder) => builder.append(meta, size, cursor),
        }
    }

    pub fn finish_batch(&mut self) -> std::result::Result<RecordBatch, ArrowError> {
        match self {
            Self::Tdf1Announce(builder) => builder.finish_batch(),
            Self::Tdf2BatteryState(builder) => builder.finish_batch(),
            Self::Tdf3AmbientTempPresHum(builder) => builder.finish_batch(),
            Self::Tdf4AmbientTemperature(builder) => builder.finish_batch(),
            Self::Tdf5TimeSync(builder) => builder.finish_batch(),
            Self::Tdf6RebootInfo(builder) => builder.finish_batch(),
            Self::Tdf7AnnounceV2(builder) => builder.finish_batch(),
            Self::Tdf8SocTemperature(builder) => builder.finish_batch(),
            Self::Tdf10Acc2g(builder) => builder.finish_batch(),
            Self::Tdf11Acc4g(builder) => builder.finish_batch(),
            Self::Tdf12Acc8g(builder) => builder.finish_batch(),
            Self::Tdf13Acc16g(builder) => builder.finish_batch(),
            Self::Tdf14Gyr125dps(builder) => builder.finish_batch(),
            Self::Tdf15Gyr250dps(builder) => builder.finish_batch(),
            Self::Tdf16Gyr500dps(builder) => builder.finish_batch(),
            Self::Tdf17Gyr1000dps(builder) => builder.finish_batch(),
            Self::Tdf18Gyr2000dps(builder) => builder.finish_batch(),
            Self::Tdf19GcsWgs84Llha(builder) => builder.finish_batch(),
            Self::Tdf20UbxNavPvt(builder) => builder.finish_batch(),
            Self::Tdf21LteConnStatus(builder) => builder.finish_batch(),
            Self::Tdf22GlobalstarPkt(builder) => builder.finish_batch(),
            Self::Tdf23AccMagnitudeStdDev(builder) => builder.finish_batch(),
            Self::Tdf24ActivityMetric(builder) => builder.finish_batch(),
            Self::Tdf25AlgorithmOutput(builder) => builder.finish_batch(),
            Self::Tdf26RuntimeError(builder) => builder.finish_batch(),
            Self::Tdf27ChargerEnControl(builder) => builder.finish_batch(),
            Self::Tdf28GnssFixInfo(builder) => builder.finish_batch(),
            Self::Tdf29BluetoothConnection(builder) => builder.finish_batch(),
            Self::Tdf30BluetoothRssi(builder) => builder.finish_batch(),
            Self::Tdf31BluetoothDataThroughput(builder) => builder.finish_batch(),
            Self::Tdf32AlgorithmClassHistogram(builder) => builder.finish_batch(),
            Self::Tdf33AlgorithmClassTimeSeries(builder) => builder.finish_batch(),
            Self::Tdf34LteTacCells(builder) => builder.finish_batch(),
            Self::Tdf35WifiApInfo(builder) => builder.finish_batch(),
            Self::Tdf36DeviceTilt(builder) => builder.finish_batch(),
            Self::Tdf37Nrf9xGnssPvt(builder) => builder.finish_batch(),
            Self::Tdf38BatteryChargeAccumulated(builder) => builder.finish_batch(),
            Self::Tdf39InfuseBluetoothRssi(builder) => builder.finish_batch(),
            Self::Tdf40AdcRaw8(builder) => builder.finish_batch(),
            Self::Tdf41AdcRaw16(builder) => builder.finish_batch(),
            Self::Tdf42AdcRaw32(builder) => builder.finish_batch(),
            Self::Tdf43Annotation(builder) => builder.finish_batch(),
            Self::Tdf44LoraRx(builder) => builder.finish_batch(),
            Self::Tdf45LoraTx(builder) => builder.finish_batch(),
            Self::Tdf46IdxArrayFreq(builder) => builder.finish_batch(),
            Self::Tdf47IdxArrayPeriod(builder) => builder.finish_batch(),
            Self::Tdf48WifiConnected(builder) => builder.finish_batch(),
            Self::Tdf49WifiConnectionFailed(builder) => builder.finish_batch(),
            Self::Tdf50WifiDisconnected(builder) => builder.finish_batch(),
            Self::Tdf51NetworkScanCount(builder) => builder.finish_batch(),
            Self::Tdf52ExceptionStackFrame(builder) => builder.finish_batch(),
            Self::Tdf53BatteryVoltage(builder) => builder.finish_batch(),
            Self::Tdf54BatterySoc(builder) => builder.finish_batch(),
            Self::Tdf55StateEventSet(builder) => builder.finish_batch(),
            Self::Tdf56StateEventCleared(builder) => builder.finish_batch(),
            Self::Tdf57StateDuration(builder) => builder.finish_batch(),
            Self::Tdf58Pcm16bitChanLeft(builder) => builder.finish_batch(),
            Self::Tdf59Pcm16bitChanRight(builder) => builder.finish_batch(),
            Self::Tdf60Pcm16bitChanDual(builder) => builder.finish_batch(),
            Self::Tdf61KvsValueChanged(builder) => builder.finish_batch(),
        }
    }
}

pub struct Tdf1AnnounceBuilder {
    row_timestamp: Vec<Option<i64>>,
    row_sample_idx: Vec<Option<u16>>,
    application: Vec<u32>,
    version_major: Vec<u8>,
    version_minor: Vec<u8>,
    version_revision: Vec<u16>,
    version_build_num: Vec<u32>,
    kv_crc: Vec<u32>,
    blocks: Vec<u32>,
    uptime: Vec<u32>,
    reboots: Vec<u16>,
    flags: Vec<u8>,
}

impl Tdf1AnnounceBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            row_timestamp: Vec::with_capacity(capacity),
            row_sample_idx: Vec::with_capacity(capacity),
            application: Vec::with_capacity(capacity),
            version_major: Vec::with_capacity(capacity),
            version_minor: Vec::with_capacity(capacity),
            version_revision: Vec::with_capacity(capacity),
            version_build_num: Vec::with_capacity(capacity),
            kv_crc: Vec::with_capacity(capacity),
            blocks: Vec::with_capacity(capacity),
            uptime: Vec::with_capacity(capacity),
            reboots: Vec::with_capacity(capacity),
            flags: Vec::with_capacity(capacity),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        tdf_parquet_schema(1).unwrap()
    }

    pub fn rows(&self) -> usize {
        self.row_timestamp.len()
    }

    pub fn append(
        &mut self,
        meta: TdfParquetRowMeta,
        size: u8,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<()> {
        let cursor_start = cursor.position();

        self.row_timestamp.push(meta.time_unix_micros);
        self.row_sample_idx.push(meta.sample_idx);
        self.application.push(cursor.read_u32::<LittleEndian>()?);
        self.version_major.push(cursor.read_u8()?);
        self.version_minor.push(cursor.read_u8()?);
        self.version_revision
            .push(cursor.read_u16::<LittleEndian>()?);
        self.version_build_num
            .push(cursor.read_u32::<LittleEndian>()?);
        self.kv_crc.push(cursor.read_u32::<LittleEndian>()?);
        self.blocks.push(cursor.read_u32::<LittleEndian>()?);
        self.uptime.push(cursor.read_u32::<LittleEndian>()?);
        self.reboots.push(cursor.read_u16::<LittleEndian>()?);
        self.flags.push(cursor.read_u8()?);

        finish_tdf_read(cursor, cursor_start, size)
    }

    pub fn finish_batch(&mut self) -> std::result::Result<RecordBatch, ArrowError> {
        let schema = self.schema();
        let columns = vec![
            Arc::new(
                TimestampMicrosecondArray::from(std::mem::take(&mut self.row_timestamp))
                    .with_timezone("+00:00"),
            ) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.row_sample_idx))) as ArrayRef,
            Arc::new(UInt32Array::from(std::mem::take(&mut self.application))) as ArrayRef,
            Arc::new(StructArray::try_new(
                Fields::from(vec![
                    Field::new("major", DataType::UInt8, false),
                    Field::new("minor", DataType::UInt8, false),
                    Field::new("revision", DataType::UInt16, false),
                    Field::new("build_num", DataType::UInt32, false),
                ]),
                vec![
                    Arc::new(UInt8Array::from(std::mem::take(&mut self.version_major))) as ArrayRef,
                    Arc::new(UInt8Array::from(std::mem::take(&mut self.version_minor))) as ArrayRef,
                    Arc::new(UInt16Array::from(std::mem::take(
                        &mut self.version_revision,
                    ))) as ArrayRef,
                    Arc::new(UInt32Array::from(std::mem::take(
                        &mut self.version_build_num,
                    ))) as ArrayRef,
                ],
                None,
            )?) as ArrayRef,
            Arc::new(UInt32Array::from(std::mem::take(&mut self.kv_crc))) as ArrayRef,
            Arc::new(UInt32Array::from(std::mem::take(&mut self.blocks))) as ArrayRef,
            Arc::new(UInt32Array::from(std::mem::take(&mut self.uptime))) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.reboots))) as ArrayRef,
            Arc::new(UInt8Array::from(std::mem::take(&mut self.flags))) as ArrayRef,
        ];

        RecordBatch::try_new(schema, columns)
    }
}

pub struct Tdf2BatteryStateBuilder {
    row_timestamp: Vec<Option<i64>>,
    row_sample_idx: Vec<Option<u16>>,
    voltage_mv: Vec<u32>,
    current_ua: Vec<i32>,
    soc: Vec<u8>,
}

impl Tdf2BatteryStateBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            row_timestamp: Vec::with_capacity(capacity),
            row_sample_idx: Vec::with_capacity(capacity),
            voltage_mv: Vec::with_capacity(capacity),
            current_ua: Vec::with_capacity(capacity),
            soc: Vec::with_capacity(capacity),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        tdf_parquet_schema(2).unwrap()
    }

    pub fn rows(&self) -> usize {
        self.row_timestamp.len()
    }

    pub fn append(
        &mut self,
        meta: TdfParquetRowMeta,
        size: u8,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<()> {
        let cursor_start = cursor.position();

        self.row_timestamp.push(meta.time_unix_micros);
        self.row_sample_idx.push(meta.sample_idx);
        self.voltage_mv.push(cursor.read_u32::<LittleEndian>()?);
        self.current_ua.push(cursor.read_i32::<LittleEndian>()?);
        self.soc.push(cursor.read_u8()?);

        finish_tdf_read(cursor, cursor_start, size)
    }

    pub fn finish_batch(&mut self) -> std::result::Result<RecordBatch, ArrowError> {
        let schema = self.schema();
        let columns = vec![
            Arc::new(
                TimestampMicrosecondArray::from(std::mem::take(&mut self.row_timestamp))
                    .with_timezone("+00:00"),
            ) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.row_sample_idx))) as ArrayRef,
            Arc::new(UInt32Array::from(std::mem::take(&mut self.voltage_mv))) as ArrayRef,
            Arc::new(Int32Array::from(std::mem::take(&mut self.current_ua))) as ArrayRef,
            Arc::new(UInt8Array::from(std::mem::take(&mut self.soc))) as ArrayRef,
        ];

        RecordBatch::try_new(schema, columns)
    }
}

pub struct Tdf3AmbientTempPresHumBuilder {
    row_timestamp: Vec<Option<i64>>,
    row_sample_idx: Vec<Option<u16>>,
    temperature: Vec<f64>,
    pressure: Vec<f64>,
    humidity: Vec<f64>,
}

impl Tdf3AmbientTempPresHumBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            row_timestamp: Vec::with_capacity(capacity),
            row_sample_idx: Vec::with_capacity(capacity),
            temperature: Vec::with_capacity(capacity),
            pressure: Vec::with_capacity(capacity),
            humidity: Vec::with_capacity(capacity),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        tdf_parquet_schema(3).unwrap()
    }

    pub fn rows(&self) -> usize {
        self.row_timestamp.len()
    }

    pub fn append(
        &mut self,
        meta: TdfParquetRowMeta,
        size: u8,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<()> {
        let cursor_start = cursor.position();

        self.row_timestamp.push(meta.time_unix_micros);
        self.row_sample_idx.push(meta.sample_idx);
        self.temperature
            .push(cursor.read_i32::<LittleEndian>()? as f64 / 1000.0);
        self.pressure
            .push(cursor.read_u32::<LittleEndian>()? as f64 / 1000.0);
        self.humidity
            .push(cursor.read_u16::<LittleEndian>()? as f64 / 100.0);

        finish_tdf_read(cursor, cursor_start, size)
    }

    pub fn finish_batch(&mut self) -> std::result::Result<RecordBatch, ArrowError> {
        let schema = self.schema();
        let columns = vec![
            Arc::new(
                TimestampMicrosecondArray::from(std::mem::take(&mut self.row_timestamp))
                    .with_timezone("+00:00"),
            ) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.row_sample_idx))) as ArrayRef,
            Arc::new(Float64Array::from(std::mem::take(&mut self.temperature))) as ArrayRef,
            Arc::new(Float64Array::from(std::mem::take(&mut self.pressure))) as ArrayRef,
            Arc::new(Float64Array::from(std::mem::take(&mut self.humidity))) as ArrayRef,
        ];

        RecordBatch::try_new(schema, columns)
    }
}

pub struct Tdf4AmbientTemperatureBuilder {
    row_timestamp: Vec<Option<i64>>,
    row_sample_idx: Vec<Option<u16>>,
    temperature: Vec<f64>,
}

impl Tdf4AmbientTemperatureBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            row_timestamp: Vec::with_capacity(capacity),
            row_sample_idx: Vec::with_capacity(capacity),
            temperature: Vec::with_capacity(capacity),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        tdf_parquet_schema(4).unwrap()
    }

    pub fn rows(&self) -> usize {
        self.row_timestamp.len()
    }

    pub fn append(
        &mut self,
        meta: TdfParquetRowMeta,
        size: u8,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<()> {
        let cursor_start = cursor.position();

        self.row_timestamp.push(meta.time_unix_micros);
        self.row_sample_idx.push(meta.sample_idx);
        self.temperature
            .push(cursor.read_i32::<LittleEndian>()? as f64 / 1000.0);

        finish_tdf_read(cursor, cursor_start, size)
    }

    pub fn finish_batch(&mut self) -> std::result::Result<RecordBatch, ArrowError> {
        let schema = self.schema();
        let columns = vec![
            Arc::new(
                TimestampMicrosecondArray::from(std::mem::take(&mut self.row_timestamp))
                    .with_timezone("+00:00"),
            ) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.row_sample_idx))) as ArrayRef,
            Arc::new(Float64Array::from(std::mem::take(&mut self.temperature))) as ArrayRef,
        ];

        RecordBatch::try_new(schema, columns)
    }
}

pub struct Tdf5TimeSyncBuilder {
    row_timestamp: Vec<Option<i64>>,
    row_sample_idx: Vec<Option<u16>>,
    source: Vec<u8>,
    shift: Vec<f64>,
}

impl Tdf5TimeSyncBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            row_timestamp: Vec::with_capacity(capacity),
            row_sample_idx: Vec::with_capacity(capacity),
            source: Vec::with_capacity(capacity),
            shift: Vec::with_capacity(capacity),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        tdf_parquet_schema(5).unwrap()
    }

    pub fn rows(&self) -> usize {
        self.row_timestamp.len()
    }

    pub fn append(
        &mut self,
        meta: TdfParquetRowMeta,
        size: u8,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<()> {
        let cursor_start = cursor.position();

        self.row_timestamp.push(meta.time_unix_micros);
        self.row_sample_idx.push(meta.sample_idx);
        self.source.push(cursor.read_u8()?);
        self.shift
            .push(cursor.read_i32::<LittleEndian>()? as f64 / 1000000.0);

        finish_tdf_read(cursor, cursor_start, size)
    }

    pub fn finish_batch(&mut self) -> std::result::Result<RecordBatch, ArrowError> {
        let schema = self.schema();
        let columns = vec![
            Arc::new(
                TimestampMicrosecondArray::from(std::mem::take(&mut self.row_timestamp))
                    .with_timezone("+00:00"),
            ) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.row_sample_idx))) as ArrayRef,
            Arc::new(UInt8Array::from(std::mem::take(&mut self.source))) as ArrayRef,
            Arc::new(Float64Array::from(std::mem::take(&mut self.shift))) as ArrayRef,
        ];

        RecordBatch::try_new(schema, columns)
    }
}

pub struct Tdf6RebootInfoBuilder {
    row_timestamp: Vec<Option<i64>>,
    row_sample_idx: Vec<Option<u16>>,
    reason: Vec<u8>,
    hardware_flags: Vec<u32>,
    count: Vec<u32>,
    uptime: Vec<u32>,
    param_1: Vec<u32>,
    param_2: Vec<u32>,
    thread: Vec<String>,
}

impl Tdf6RebootInfoBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            row_timestamp: Vec::with_capacity(capacity),
            row_sample_idx: Vec::with_capacity(capacity),
            reason: Vec::with_capacity(capacity),
            hardware_flags: Vec::with_capacity(capacity),
            count: Vec::with_capacity(capacity),
            uptime: Vec::with_capacity(capacity),
            param_1: Vec::with_capacity(capacity),
            param_2: Vec::with_capacity(capacity),
            thread: Vec::with_capacity(capacity),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        tdf_parquet_schema(6).unwrap()
    }

    pub fn rows(&self) -> usize {
        self.row_timestamp.len()
    }

    pub fn append(
        &mut self,
        meta: TdfParquetRowMeta,
        size: u8,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<()> {
        let cursor_start = cursor.position();

        self.row_timestamp.push(meta.time_unix_micros);
        self.row_sample_idx.push(meta.sample_idx);
        self.reason.push(cursor.read_u8()?);
        self.hardware_flags.push(cursor.read_u32::<LittleEndian>()?);
        self.count.push(cursor.read_u32::<LittleEndian>()?);
        self.uptime.push(cursor.read_u32::<LittleEndian>()?);
        self.param_1.push(cursor.read_u32::<LittleEndian>()?);
        self.param_2.push(cursor.read_u32::<LittleEndian>()?);
        self.thread.push(tdf_field_read_string_to_string(
            cursor,
            cursor_start,
            8,
            size,
        )?);

        finish_tdf_read(cursor, cursor_start, size)
    }

    pub fn finish_batch(&mut self) -> std::result::Result<RecordBatch, ArrowError> {
        let schema = self.schema();
        let columns = vec![
            Arc::new(
                TimestampMicrosecondArray::from(std::mem::take(&mut self.row_timestamp))
                    .with_timezone("+00:00"),
            ) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.row_sample_idx))) as ArrayRef,
            Arc::new(UInt8Array::from(std::mem::take(&mut self.reason))) as ArrayRef,
            Arc::new(UInt32Array::from(std::mem::take(&mut self.hardware_flags))) as ArrayRef,
            Arc::new(UInt32Array::from(std::mem::take(&mut self.count))) as ArrayRef,
            Arc::new(UInt32Array::from(std::mem::take(&mut self.uptime))) as ArrayRef,
            Arc::new(UInt32Array::from(std::mem::take(&mut self.param_1))) as ArrayRef,
            Arc::new(UInt32Array::from(std::mem::take(&mut self.param_2))) as ArrayRef,
            Arc::new(StringArray::from_iter_values(std::mem::take(
                &mut self.thread,
            ))) as ArrayRef,
        ];

        RecordBatch::try_new(schema, columns)
    }
}

pub struct Tdf7AnnounceV2Builder {
    row_timestamp: Vec<Option<i64>>,
    row_sample_idx: Vec<Option<u16>>,
    application: Vec<u32>,
    version_major: Vec<u8>,
    version_minor: Vec<u8>,
    version_revision: Vec<u16>,
    version_build_num: Vec<u32>,
    board_crc: Vec<u16>,
    kv_crc: Vec<u32>,
    blocks: Vec<u32>,
    uptime: Vec<u32>,
    reboots: Vec<u16>,
    flags: Vec<u8>,
}

impl Tdf7AnnounceV2Builder {
    pub fn new(capacity: usize) -> Self {
        Self {
            row_timestamp: Vec::with_capacity(capacity),
            row_sample_idx: Vec::with_capacity(capacity),
            application: Vec::with_capacity(capacity),
            version_major: Vec::with_capacity(capacity),
            version_minor: Vec::with_capacity(capacity),
            version_revision: Vec::with_capacity(capacity),
            version_build_num: Vec::with_capacity(capacity),
            board_crc: Vec::with_capacity(capacity),
            kv_crc: Vec::with_capacity(capacity),
            blocks: Vec::with_capacity(capacity),
            uptime: Vec::with_capacity(capacity),
            reboots: Vec::with_capacity(capacity),
            flags: Vec::with_capacity(capacity),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        tdf_parquet_schema(7).unwrap()
    }

    pub fn rows(&self) -> usize {
        self.row_timestamp.len()
    }

    pub fn append(
        &mut self,
        meta: TdfParquetRowMeta,
        size: u8,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<()> {
        let cursor_start = cursor.position();

        self.row_timestamp.push(meta.time_unix_micros);
        self.row_sample_idx.push(meta.sample_idx);
        self.application.push(cursor.read_u32::<LittleEndian>()?);
        self.version_major.push(cursor.read_u8()?);
        self.version_minor.push(cursor.read_u8()?);
        self.version_revision
            .push(cursor.read_u16::<LittleEndian>()?);
        self.version_build_num
            .push(cursor.read_u32::<LittleEndian>()?);
        self.board_crc.push(cursor.read_u16::<LittleEndian>()?);
        self.kv_crc.push(cursor.read_u32::<LittleEndian>()?);
        self.blocks.push(cursor.read_u32::<LittleEndian>()?);
        self.uptime.push(cursor.read_u32::<LittleEndian>()?);
        self.reboots.push(cursor.read_u16::<LittleEndian>()?);
        self.flags.push(cursor.read_u8()?);

        finish_tdf_read(cursor, cursor_start, size)
    }

    pub fn finish_batch(&mut self) -> std::result::Result<RecordBatch, ArrowError> {
        let schema = self.schema();
        let columns = vec![
            Arc::new(
                TimestampMicrosecondArray::from(std::mem::take(&mut self.row_timestamp))
                    .with_timezone("+00:00"),
            ) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.row_sample_idx))) as ArrayRef,
            Arc::new(UInt32Array::from(std::mem::take(&mut self.application))) as ArrayRef,
            Arc::new(StructArray::try_new(
                Fields::from(vec![
                    Field::new("major", DataType::UInt8, false),
                    Field::new("minor", DataType::UInt8, false),
                    Field::new("revision", DataType::UInt16, false),
                    Field::new("build_num", DataType::UInt32, false),
                ]),
                vec![
                    Arc::new(UInt8Array::from(std::mem::take(&mut self.version_major))) as ArrayRef,
                    Arc::new(UInt8Array::from(std::mem::take(&mut self.version_minor))) as ArrayRef,
                    Arc::new(UInt16Array::from(std::mem::take(
                        &mut self.version_revision,
                    ))) as ArrayRef,
                    Arc::new(UInt32Array::from(std::mem::take(
                        &mut self.version_build_num,
                    ))) as ArrayRef,
                ],
                None,
            )?) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.board_crc))) as ArrayRef,
            Arc::new(UInt32Array::from(std::mem::take(&mut self.kv_crc))) as ArrayRef,
            Arc::new(UInt32Array::from(std::mem::take(&mut self.blocks))) as ArrayRef,
            Arc::new(UInt32Array::from(std::mem::take(&mut self.uptime))) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.reboots))) as ArrayRef,
            Arc::new(UInt8Array::from(std::mem::take(&mut self.flags))) as ArrayRef,
        ];

        RecordBatch::try_new(schema, columns)
    }
}

pub struct Tdf8SocTemperatureBuilder {
    row_timestamp: Vec<Option<i64>>,
    row_sample_idx: Vec<Option<u16>>,
    temperature: Vec<f64>,
}

impl Tdf8SocTemperatureBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            row_timestamp: Vec::with_capacity(capacity),
            row_sample_idx: Vec::with_capacity(capacity),
            temperature: Vec::with_capacity(capacity),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        tdf_parquet_schema(8).unwrap()
    }

    pub fn rows(&self) -> usize {
        self.row_timestamp.len()
    }

    pub fn append(
        &mut self,
        meta: TdfParquetRowMeta,
        size: u8,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<()> {
        let cursor_start = cursor.position();

        self.row_timestamp.push(meta.time_unix_micros);
        self.row_sample_idx.push(meta.sample_idx);
        self.temperature
            .push(cursor.read_i16::<LittleEndian>()? as f64 / 100.0);

        finish_tdf_read(cursor, cursor_start, size)
    }

    pub fn finish_batch(&mut self) -> std::result::Result<RecordBatch, ArrowError> {
        let schema = self.schema();
        let columns = vec![
            Arc::new(
                TimestampMicrosecondArray::from(std::mem::take(&mut self.row_timestamp))
                    .with_timezone("+00:00"),
            ) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.row_sample_idx))) as ArrayRef,
            Arc::new(Float64Array::from(std::mem::take(&mut self.temperature))) as ArrayRef,
        ];

        RecordBatch::try_new(schema, columns)
    }
}

pub struct Tdf10Acc2gBuilder {
    row_timestamp: Vec<Option<i64>>,
    row_sample_idx: Vec<Option<u16>>,
    sample_x: Vec<i16>,
    sample_y: Vec<i16>,
    sample_z: Vec<i16>,
}

impl Tdf10Acc2gBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            row_timestamp: Vec::with_capacity(capacity),
            row_sample_idx: Vec::with_capacity(capacity),
            sample_x: Vec::with_capacity(capacity),
            sample_y: Vec::with_capacity(capacity),
            sample_z: Vec::with_capacity(capacity),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        tdf_parquet_schema(10).unwrap()
    }

    pub fn rows(&self) -> usize {
        self.row_timestamp.len()
    }

    pub fn append(
        &mut self,
        meta: TdfParquetRowMeta,
        size: u8,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<()> {
        let cursor_start = cursor.position();

        self.row_timestamp.push(meta.time_unix_micros);
        self.row_sample_idx.push(meta.sample_idx);
        self.sample_x.push(cursor.read_i16::<LittleEndian>()?);
        self.sample_y.push(cursor.read_i16::<LittleEndian>()?);
        self.sample_z.push(cursor.read_i16::<LittleEndian>()?);

        finish_tdf_read(cursor, cursor_start, size)
    }

    pub fn finish_batch(&mut self) -> std::result::Result<RecordBatch, ArrowError> {
        let schema = self.schema();
        let columns = vec![
            Arc::new(
                TimestampMicrosecondArray::from(std::mem::take(&mut self.row_timestamp))
                    .with_timezone("+00:00"),
            ) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.row_sample_idx))) as ArrayRef,
            Arc::new(StructArray::try_new(
                Fields::from(vec![
                    Field::new("x", DataType::Int16, false),
                    Field::new("y", DataType::Int16, false),
                    Field::new("z", DataType::Int16, false),
                ]),
                vec![
                    Arc::new(Int16Array::from(std::mem::take(&mut self.sample_x))) as ArrayRef,
                    Arc::new(Int16Array::from(std::mem::take(&mut self.sample_y))) as ArrayRef,
                    Arc::new(Int16Array::from(std::mem::take(&mut self.sample_z))) as ArrayRef,
                ],
                None,
            )?) as ArrayRef,
        ];

        RecordBatch::try_new(schema, columns)
    }
}

pub struct Tdf11Acc4gBuilder {
    row_timestamp: Vec<Option<i64>>,
    row_sample_idx: Vec<Option<u16>>,
    sample_x: Vec<i16>,
    sample_y: Vec<i16>,
    sample_z: Vec<i16>,
}

impl Tdf11Acc4gBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            row_timestamp: Vec::with_capacity(capacity),
            row_sample_idx: Vec::with_capacity(capacity),
            sample_x: Vec::with_capacity(capacity),
            sample_y: Vec::with_capacity(capacity),
            sample_z: Vec::with_capacity(capacity),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        tdf_parquet_schema(11).unwrap()
    }

    pub fn rows(&self) -> usize {
        self.row_timestamp.len()
    }

    pub fn append(
        &mut self,
        meta: TdfParquetRowMeta,
        size: u8,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<()> {
        let cursor_start = cursor.position();

        self.row_timestamp.push(meta.time_unix_micros);
        self.row_sample_idx.push(meta.sample_idx);
        self.sample_x.push(cursor.read_i16::<LittleEndian>()?);
        self.sample_y.push(cursor.read_i16::<LittleEndian>()?);
        self.sample_z.push(cursor.read_i16::<LittleEndian>()?);

        finish_tdf_read(cursor, cursor_start, size)
    }

    pub fn finish_batch(&mut self) -> std::result::Result<RecordBatch, ArrowError> {
        let schema = self.schema();
        let columns = vec![
            Arc::new(
                TimestampMicrosecondArray::from(std::mem::take(&mut self.row_timestamp))
                    .with_timezone("+00:00"),
            ) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.row_sample_idx))) as ArrayRef,
            Arc::new(StructArray::try_new(
                Fields::from(vec![
                    Field::new("x", DataType::Int16, false),
                    Field::new("y", DataType::Int16, false),
                    Field::new("z", DataType::Int16, false),
                ]),
                vec![
                    Arc::new(Int16Array::from(std::mem::take(&mut self.sample_x))) as ArrayRef,
                    Arc::new(Int16Array::from(std::mem::take(&mut self.sample_y))) as ArrayRef,
                    Arc::new(Int16Array::from(std::mem::take(&mut self.sample_z))) as ArrayRef,
                ],
                None,
            )?) as ArrayRef,
        ];

        RecordBatch::try_new(schema, columns)
    }
}

pub struct Tdf12Acc8gBuilder {
    row_timestamp: Vec<Option<i64>>,
    row_sample_idx: Vec<Option<u16>>,
    sample_x: Vec<i16>,
    sample_y: Vec<i16>,
    sample_z: Vec<i16>,
}

impl Tdf12Acc8gBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            row_timestamp: Vec::with_capacity(capacity),
            row_sample_idx: Vec::with_capacity(capacity),
            sample_x: Vec::with_capacity(capacity),
            sample_y: Vec::with_capacity(capacity),
            sample_z: Vec::with_capacity(capacity),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        tdf_parquet_schema(12).unwrap()
    }

    pub fn rows(&self) -> usize {
        self.row_timestamp.len()
    }

    pub fn append(
        &mut self,
        meta: TdfParquetRowMeta,
        size: u8,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<()> {
        let cursor_start = cursor.position();

        self.row_timestamp.push(meta.time_unix_micros);
        self.row_sample_idx.push(meta.sample_idx);
        self.sample_x.push(cursor.read_i16::<LittleEndian>()?);
        self.sample_y.push(cursor.read_i16::<LittleEndian>()?);
        self.sample_z.push(cursor.read_i16::<LittleEndian>()?);

        finish_tdf_read(cursor, cursor_start, size)
    }

    pub fn finish_batch(&mut self) -> std::result::Result<RecordBatch, ArrowError> {
        let schema = self.schema();
        let columns = vec![
            Arc::new(
                TimestampMicrosecondArray::from(std::mem::take(&mut self.row_timestamp))
                    .with_timezone("+00:00"),
            ) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.row_sample_idx))) as ArrayRef,
            Arc::new(StructArray::try_new(
                Fields::from(vec![
                    Field::new("x", DataType::Int16, false),
                    Field::new("y", DataType::Int16, false),
                    Field::new("z", DataType::Int16, false),
                ]),
                vec![
                    Arc::new(Int16Array::from(std::mem::take(&mut self.sample_x))) as ArrayRef,
                    Arc::new(Int16Array::from(std::mem::take(&mut self.sample_y))) as ArrayRef,
                    Arc::new(Int16Array::from(std::mem::take(&mut self.sample_z))) as ArrayRef,
                ],
                None,
            )?) as ArrayRef,
        ];

        RecordBatch::try_new(schema, columns)
    }
}

pub struct Tdf13Acc16gBuilder {
    row_timestamp: Vec<Option<i64>>,
    row_sample_idx: Vec<Option<u16>>,
    sample_x: Vec<i16>,
    sample_y: Vec<i16>,
    sample_z: Vec<i16>,
}

impl Tdf13Acc16gBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            row_timestamp: Vec::with_capacity(capacity),
            row_sample_idx: Vec::with_capacity(capacity),
            sample_x: Vec::with_capacity(capacity),
            sample_y: Vec::with_capacity(capacity),
            sample_z: Vec::with_capacity(capacity),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        tdf_parquet_schema(13).unwrap()
    }

    pub fn rows(&self) -> usize {
        self.row_timestamp.len()
    }

    pub fn append(
        &mut self,
        meta: TdfParquetRowMeta,
        size: u8,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<()> {
        let cursor_start = cursor.position();

        self.row_timestamp.push(meta.time_unix_micros);
        self.row_sample_idx.push(meta.sample_idx);
        self.sample_x.push(cursor.read_i16::<LittleEndian>()?);
        self.sample_y.push(cursor.read_i16::<LittleEndian>()?);
        self.sample_z.push(cursor.read_i16::<LittleEndian>()?);

        finish_tdf_read(cursor, cursor_start, size)
    }

    pub fn finish_batch(&mut self) -> std::result::Result<RecordBatch, ArrowError> {
        let schema = self.schema();
        let columns = vec![
            Arc::new(
                TimestampMicrosecondArray::from(std::mem::take(&mut self.row_timestamp))
                    .with_timezone("+00:00"),
            ) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.row_sample_idx))) as ArrayRef,
            Arc::new(StructArray::try_new(
                Fields::from(vec![
                    Field::new("x", DataType::Int16, false),
                    Field::new("y", DataType::Int16, false),
                    Field::new("z", DataType::Int16, false),
                ]),
                vec![
                    Arc::new(Int16Array::from(std::mem::take(&mut self.sample_x))) as ArrayRef,
                    Arc::new(Int16Array::from(std::mem::take(&mut self.sample_y))) as ArrayRef,
                    Arc::new(Int16Array::from(std::mem::take(&mut self.sample_z))) as ArrayRef,
                ],
                None,
            )?) as ArrayRef,
        ];

        RecordBatch::try_new(schema, columns)
    }
}

pub struct Tdf14Gyr125dpsBuilder {
    row_timestamp: Vec<Option<i64>>,
    row_sample_idx: Vec<Option<u16>>,
    sample_x: Vec<i16>,
    sample_y: Vec<i16>,
    sample_z: Vec<i16>,
}

impl Tdf14Gyr125dpsBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            row_timestamp: Vec::with_capacity(capacity),
            row_sample_idx: Vec::with_capacity(capacity),
            sample_x: Vec::with_capacity(capacity),
            sample_y: Vec::with_capacity(capacity),
            sample_z: Vec::with_capacity(capacity),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        tdf_parquet_schema(14).unwrap()
    }

    pub fn rows(&self) -> usize {
        self.row_timestamp.len()
    }

    pub fn append(
        &mut self,
        meta: TdfParquetRowMeta,
        size: u8,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<()> {
        let cursor_start = cursor.position();

        self.row_timestamp.push(meta.time_unix_micros);
        self.row_sample_idx.push(meta.sample_idx);
        self.sample_x.push(cursor.read_i16::<LittleEndian>()?);
        self.sample_y.push(cursor.read_i16::<LittleEndian>()?);
        self.sample_z.push(cursor.read_i16::<LittleEndian>()?);

        finish_tdf_read(cursor, cursor_start, size)
    }

    pub fn finish_batch(&mut self) -> std::result::Result<RecordBatch, ArrowError> {
        let schema = self.schema();
        let columns = vec![
            Arc::new(
                TimestampMicrosecondArray::from(std::mem::take(&mut self.row_timestamp))
                    .with_timezone("+00:00"),
            ) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.row_sample_idx))) as ArrayRef,
            Arc::new(StructArray::try_new(
                Fields::from(vec![
                    Field::new("x", DataType::Int16, false),
                    Field::new("y", DataType::Int16, false),
                    Field::new("z", DataType::Int16, false),
                ]),
                vec![
                    Arc::new(Int16Array::from(std::mem::take(&mut self.sample_x))) as ArrayRef,
                    Arc::new(Int16Array::from(std::mem::take(&mut self.sample_y))) as ArrayRef,
                    Arc::new(Int16Array::from(std::mem::take(&mut self.sample_z))) as ArrayRef,
                ],
                None,
            )?) as ArrayRef,
        ];

        RecordBatch::try_new(schema, columns)
    }
}

pub struct Tdf15Gyr250dpsBuilder {
    row_timestamp: Vec<Option<i64>>,
    row_sample_idx: Vec<Option<u16>>,
    sample_x: Vec<i16>,
    sample_y: Vec<i16>,
    sample_z: Vec<i16>,
}

impl Tdf15Gyr250dpsBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            row_timestamp: Vec::with_capacity(capacity),
            row_sample_idx: Vec::with_capacity(capacity),
            sample_x: Vec::with_capacity(capacity),
            sample_y: Vec::with_capacity(capacity),
            sample_z: Vec::with_capacity(capacity),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        tdf_parquet_schema(15).unwrap()
    }

    pub fn rows(&self) -> usize {
        self.row_timestamp.len()
    }

    pub fn append(
        &mut self,
        meta: TdfParquetRowMeta,
        size: u8,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<()> {
        let cursor_start = cursor.position();

        self.row_timestamp.push(meta.time_unix_micros);
        self.row_sample_idx.push(meta.sample_idx);
        self.sample_x.push(cursor.read_i16::<LittleEndian>()?);
        self.sample_y.push(cursor.read_i16::<LittleEndian>()?);
        self.sample_z.push(cursor.read_i16::<LittleEndian>()?);

        finish_tdf_read(cursor, cursor_start, size)
    }

    pub fn finish_batch(&mut self) -> std::result::Result<RecordBatch, ArrowError> {
        let schema = self.schema();
        let columns = vec![
            Arc::new(
                TimestampMicrosecondArray::from(std::mem::take(&mut self.row_timestamp))
                    .with_timezone("+00:00"),
            ) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.row_sample_idx))) as ArrayRef,
            Arc::new(StructArray::try_new(
                Fields::from(vec![
                    Field::new("x", DataType::Int16, false),
                    Field::new("y", DataType::Int16, false),
                    Field::new("z", DataType::Int16, false),
                ]),
                vec![
                    Arc::new(Int16Array::from(std::mem::take(&mut self.sample_x))) as ArrayRef,
                    Arc::new(Int16Array::from(std::mem::take(&mut self.sample_y))) as ArrayRef,
                    Arc::new(Int16Array::from(std::mem::take(&mut self.sample_z))) as ArrayRef,
                ],
                None,
            )?) as ArrayRef,
        ];

        RecordBatch::try_new(schema, columns)
    }
}

pub struct Tdf16Gyr500dpsBuilder {
    row_timestamp: Vec<Option<i64>>,
    row_sample_idx: Vec<Option<u16>>,
    sample_x: Vec<i16>,
    sample_y: Vec<i16>,
    sample_z: Vec<i16>,
}

impl Tdf16Gyr500dpsBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            row_timestamp: Vec::with_capacity(capacity),
            row_sample_idx: Vec::with_capacity(capacity),
            sample_x: Vec::with_capacity(capacity),
            sample_y: Vec::with_capacity(capacity),
            sample_z: Vec::with_capacity(capacity),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        tdf_parquet_schema(16).unwrap()
    }

    pub fn rows(&self) -> usize {
        self.row_timestamp.len()
    }

    pub fn append(
        &mut self,
        meta: TdfParquetRowMeta,
        size: u8,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<()> {
        let cursor_start = cursor.position();

        self.row_timestamp.push(meta.time_unix_micros);
        self.row_sample_idx.push(meta.sample_idx);
        self.sample_x.push(cursor.read_i16::<LittleEndian>()?);
        self.sample_y.push(cursor.read_i16::<LittleEndian>()?);
        self.sample_z.push(cursor.read_i16::<LittleEndian>()?);

        finish_tdf_read(cursor, cursor_start, size)
    }

    pub fn finish_batch(&mut self) -> std::result::Result<RecordBatch, ArrowError> {
        let schema = self.schema();
        let columns = vec![
            Arc::new(
                TimestampMicrosecondArray::from(std::mem::take(&mut self.row_timestamp))
                    .with_timezone("+00:00"),
            ) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.row_sample_idx))) as ArrayRef,
            Arc::new(StructArray::try_new(
                Fields::from(vec![
                    Field::new("x", DataType::Int16, false),
                    Field::new("y", DataType::Int16, false),
                    Field::new("z", DataType::Int16, false),
                ]),
                vec![
                    Arc::new(Int16Array::from(std::mem::take(&mut self.sample_x))) as ArrayRef,
                    Arc::new(Int16Array::from(std::mem::take(&mut self.sample_y))) as ArrayRef,
                    Arc::new(Int16Array::from(std::mem::take(&mut self.sample_z))) as ArrayRef,
                ],
                None,
            )?) as ArrayRef,
        ];

        RecordBatch::try_new(schema, columns)
    }
}

pub struct Tdf17Gyr1000dpsBuilder {
    row_timestamp: Vec<Option<i64>>,
    row_sample_idx: Vec<Option<u16>>,
    sample_x: Vec<i16>,
    sample_y: Vec<i16>,
    sample_z: Vec<i16>,
}

impl Tdf17Gyr1000dpsBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            row_timestamp: Vec::with_capacity(capacity),
            row_sample_idx: Vec::with_capacity(capacity),
            sample_x: Vec::with_capacity(capacity),
            sample_y: Vec::with_capacity(capacity),
            sample_z: Vec::with_capacity(capacity),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        tdf_parquet_schema(17).unwrap()
    }

    pub fn rows(&self) -> usize {
        self.row_timestamp.len()
    }

    pub fn append(
        &mut self,
        meta: TdfParquetRowMeta,
        size: u8,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<()> {
        let cursor_start = cursor.position();

        self.row_timestamp.push(meta.time_unix_micros);
        self.row_sample_idx.push(meta.sample_idx);
        self.sample_x.push(cursor.read_i16::<LittleEndian>()?);
        self.sample_y.push(cursor.read_i16::<LittleEndian>()?);
        self.sample_z.push(cursor.read_i16::<LittleEndian>()?);

        finish_tdf_read(cursor, cursor_start, size)
    }

    pub fn finish_batch(&mut self) -> std::result::Result<RecordBatch, ArrowError> {
        let schema = self.schema();
        let columns = vec![
            Arc::new(
                TimestampMicrosecondArray::from(std::mem::take(&mut self.row_timestamp))
                    .with_timezone("+00:00"),
            ) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.row_sample_idx))) as ArrayRef,
            Arc::new(StructArray::try_new(
                Fields::from(vec![
                    Field::new("x", DataType::Int16, false),
                    Field::new("y", DataType::Int16, false),
                    Field::new("z", DataType::Int16, false),
                ]),
                vec![
                    Arc::new(Int16Array::from(std::mem::take(&mut self.sample_x))) as ArrayRef,
                    Arc::new(Int16Array::from(std::mem::take(&mut self.sample_y))) as ArrayRef,
                    Arc::new(Int16Array::from(std::mem::take(&mut self.sample_z))) as ArrayRef,
                ],
                None,
            )?) as ArrayRef,
        ];

        RecordBatch::try_new(schema, columns)
    }
}

pub struct Tdf18Gyr2000dpsBuilder {
    row_timestamp: Vec<Option<i64>>,
    row_sample_idx: Vec<Option<u16>>,
    sample_x: Vec<i16>,
    sample_y: Vec<i16>,
    sample_z: Vec<i16>,
}

impl Tdf18Gyr2000dpsBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            row_timestamp: Vec::with_capacity(capacity),
            row_sample_idx: Vec::with_capacity(capacity),
            sample_x: Vec::with_capacity(capacity),
            sample_y: Vec::with_capacity(capacity),
            sample_z: Vec::with_capacity(capacity),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        tdf_parquet_schema(18).unwrap()
    }

    pub fn rows(&self) -> usize {
        self.row_timestamp.len()
    }

    pub fn append(
        &mut self,
        meta: TdfParquetRowMeta,
        size: u8,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<()> {
        let cursor_start = cursor.position();

        self.row_timestamp.push(meta.time_unix_micros);
        self.row_sample_idx.push(meta.sample_idx);
        self.sample_x.push(cursor.read_i16::<LittleEndian>()?);
        self.sample_y.push(cursor.read_i16::<LittleEndian>()?);
        self.sample_z.push(cursor.read_i16::<LittleEndian>()?);

        finish_tdf_read(cursor, cursor_start, size)
    }

    pub fn finish_batch(&mut self) -> std::result::Result<RecordBatch, ArrowError> {
        let schema = self.schema();
        let columns = vec![
            Arc::new(
                TimestampMicrosecondArray::from(std::mem::take(&mut self.row_timestamp))
                    .with_timezone("+00:00"),
            ) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.row_sample_idx))) as ArrayRef,
            Arc::new(StructArray::try_new(
                Fields::from(vec![
                    Field::new("x", DataType::Int16, false),
                    Field::new("y", DataType::Int16, false),
                    Field::new("z", DataType::Int16, false),
                ]),
                vec![
                    Arc::new(Int16Array::from(std::mem::take(&mut self.sample_x))) as ArrayRef,
                    Arc::new(Int16Array::from(std::mem::take(&mut self.sample_y))) as ArrayRef,
                    Arc::new(Int16Array::from(std::mem::take(&mut self.sample_z))) as ArrayRef,
                ],
                None,
            )?) as ArrayRef,
        ];

        RecordBatch::try_new(schema, columns)
    }
}

pub struct Tdf19GcsWgs84LlhaBuilder {
    row_timestamp: Vec<Option<i64>>,
    row_sample_idx: Vec<Option<u16>>,
    location_latitude: Vec<f64>,
    location_longitude: Vec<f64>,
    location_height: Vec<f64>,
    h_acc: Vec<f64>,
    v_acc: Vec<f64>,
}

impl Tdf19GcsWgs84LlhaBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            row_timestamp: Vec::with_capacity(capacity),
            row_sample_idx: Vec::with_capacity(capacity),
            location_latitude: Vec::with_capacity(capacity),
            location_longitude: Vec::with_capacity(capacity),
            location_height: Vec::with_capacity(capacity),
            h_acc: Vec::with_capacity(capacity),
            v_acc: Vec::with_capacity(capacity),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        tdf_parquet_schema(19).unwrap()
    }

    pub fn rows(&self) -> usize {
        self.row_timestamp.len()
    }

    pub fn append(
        &mut self,
        meta: TdfParquetRowMeta,
        size: u8,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<()> {
        let cursor_start = cursor.position();

        self.row_timestamp.push(meta.time_unix_micros);
        self.row_sample_idx.push(meta.sample_idx);
        self.location_latitude
            .push(cursor.read_i32::<LittleEndian>()? as f64 / 10000000.0);
        self.location_longitude
            .push(cursor.read_i32::<LittleEndian>()? as f64 / 10000000.0);
        self.location_height
            .push(cursor.read_i32::<LittleEndian>()? as f64 / 1000.0);
        self.h_acc
            .push(cursor.read_i32::<LittleEndian>()? as f64 / 1000.0);
        self.v_acc
            .push(cursor.read_i32::<LittleEndian>()? as f64 / 1000.0);

        finish_tdf_read(cursor, cursor_start, size)
    }

    pub fn finish_batch(&mut self) -> std::result::Result<RecordBatch, ArrowError> {
        let schema = self.schema();
        let columns = vec![
            Arc::new(
                TimestampMicrosecondArray::from(std::mem::take(&mut self.row_timestamp))
                    .with_timezone("+00:00"),
            ) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.row_sample_idx))) as ArrayRef,
            Arc::new(StructArray::try_new(
                Fields::from(vec![
                    Field::new("latitude", DataType::Float64, false),
                    Field::new("longitude", DataType::Float64, false),
                    Field::new("height", DataType::Float64, false),
                ]),
                vec![
                    Arc::new(Float64Array::from(std::mem::take(
                        &mut self.location_latitude,
                    ))) as ArrayRef,
                    Arc::new(Float64Array::from(std::mem::take(
                        &mut self.location_longitude,
                    ))) as ArrayRef,
                    Arc::new(Float64Array::from(std::mem::take(
                        &mut self.location_height,
                    ))) as ArrayRef,
                ],
                None,
            )?) as ArrayRef,
            Arc::new(Float64Array::from(std::mem::take(&mut self.h_acc))) as ArrayRef,
            Arc::new(Float64Array::from(std::mem::take(&mut self.v_acc))) as ArrayRef,
        ];

        RecordBatch::try_new(schema, columns)
    }
}

pub struct Tdf20UbxNavPvtBuilder {
    row_timestamp: Vec<Option<i64>>,
    row_sample_idx: Vec<Option<u16>>,
    itow: Vec<u32>,
    year: Vec<u16>,
    month: Vec<u8>,
    day: Vec<u8>,
    hour: Vec<u8>,
    min: Vec<u8>,
    sec: Vec<u8>,
    valid: Vec<u8>,
    t_acc: Vec<u32>,
    nano: Vec<i32>,
    fix_type: Vec<u8>,
    flags: Vec<u8>,
    flags2: Vec<u8>,
    num_sv: Vec<u8>,
    lon: Vec<f64>,
    lat: Vec<f64>,
    height: Vec<f64>,
    h_msl: Vec<f64>,
    h_acc: Vec<f64>,
    v_acc: Vec<f64>,
    vel_n: Vec<f64>,
    vel_e: Vec<f64>,
    vel_d: Vec<f64>,
    g_speed: Vec<f64>,
    head_mot: Vec<f64>,
    s_acc: Vec<f64>,
    head_acc: Vec<f64>,
    p_dop: Vec<f64>,
    flags3: Vec<u16>,
    reserved0: Vec<u8>,
    head_veh: Vec<f64>,
    mag_dec: Vec<f64>,
    mag_acc: Vec<f64>,
}

impl Tdf20UbxNavPvtBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            row_timestamp: Vec::with_capacity(capacity),
            row_sample_idx: Vec::with_capacity(capacity),
            itow: Vec::with_capacity(capacity),
            year: Vec::with_capacity(capacity),
            month: Vec::with_capacity(capacity),
            day: Vec::with_capacity(capacity),
            hour: Vec::with_capacity(capacity),
            min: Vec::with_capacity(capacity),
            sec: Vec::with_capacity(capacity),
            valid: Vec::with_capacity(capacity),
            t_acc: Vec::with_capacity(capacity),
            nano: Vec::with_capacity(capacity),
            fix_type: Vec::with_capacity(capacity),
            flags: Vec::with_capacity(capacity),
            flags2: Vec::with_capacity(capacity),
            num_sv: Vec::with_capacity(capacity),
            lon: Vec::with_capacity(capacity),
            lat: Vec::with_capacity(capacity),
            height: Vec::with_capacity(capacity),
            h_msl: Vec::with_capacity(capacity),
            h_acc: Vec::with_capacity(capacity),
            v_acc: Vec::with_capacity(capacity),
            vel_n: Vec::with_capacity(capacity),
            vel_e: Vec::with_capacity(capacity),
            vel_d: Vec::with_capacity(capacity),
            g_speed: Vec::with_capacity(capacity),
            head_mot: Vec::with_capacity(capacity),
            s_acc: Vec::with_capacity(capacity),
            head_acc: Vec::with_capacity(capacity),
            p_dop: Vec::with_capacity(capacity),
            flags3: Vec::with_capacity(capacity),
            reserved0: Vec::with_capacity(capacity * 4),
            head_veh: Vec::with_capacity(capacity),
            mag_dec: Vec::with_capacity(capacity),
            mag_acc: Vec::with_capacity(capacity),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        tdf_parquet_schema(20).unwrap()
    }

    pub fn rows(&self) -> usize {
        self.row_timestamp.len()
    }

    pub fn append(
        &mut self,
        meta: TdfParquetRowMeta,
        size: u8,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<()> {
        let cursor_start = cursor.position();

        self.row_timestamp.push(meta.time_unix_micros);
        self.row_sample_idx.push(meta.sample_idx);
        self.itow.push(cursor.read_u32::<LittleEndian>()?);
        self.year.push(cursor.read_u16::<LittleEndian>()?);
        self.month.push(cursor.read_u8()?);
        self.day.push(cursor.read_u8()?);
        self.hour.push(cursor.read_u8()?);
        self.min.push(cursor.read_u8()?);
        self.sec.push(cursor.read_u8()?);
        self.valid.push(cursor.read_u8()?);
        self.t_acc.push(cursor.read_u32::<LittleEndian>()?);
        self.nano.push(cursor.read_i32::<LittleEndian>()?);
        self.fix_type.push(cursor.read_u8()?);
        self.flags.push(cursor.read_u8()?);
        self.flags2.push(cursor.read_u8()?);
        self.num_sv.push(cursor.read_u8()?);
        self.lon
            .push(cursor.read_i32::<LittleEndian>()? as f64 / 10000000.0);
        self.lat
            .push(cursor.read_i32::<LittleEndian>()? as f64 / 10000000.0);
        self.height
            .push(cursor.read_i32::<LittleEndian>()? as f64 / 1000.0);
        self.h_msl
            .push(cursor.read_i32::<LittleEndian>()? as f64 / 1000.0);
        self.h_acc
            .push(cursor.read_u32::<LittleEndian>()? as f64 / 1000.0);
        self.v_acc
            .push(cursor.read_u32::<LittleEndian>()? as f64 / 1000.0);
        self.vel_n
            .push(cursor.read_i32::<LittleEndian>()? as f64 / 1000.0);
        self.vel_e
            .push(cursor.read_i32::<LittleEndian>()? as f64 / 1000.0);
        self.vel_d
            .push(cursor.read_i32::<LittleEndian>()? as f64 / 1000.0);
        self.g_speed
            .push(cursor.read_i32::<LittleEndian>()? as f64 / 1000.0);
        self.head_mot
            .push(cursor.read_i32::<LittleEndian>()? as f64 / 100000.0);
        self.s_acc
            .push(cursor.read_u32::<LittleEndian>()? as f64 / 1000.0);
        self.head_acc
            .push(cursor.read_u32::<LittleEndian>()? as f64 / 100000.0);
        self.p_dop
            .push(cursor.read_u16::<LittleEndian>()? as f64 / 100.0);
        self.flags3.push(cursor.read_u16::<LittleEndian>()?);
        self.reserved0.push(cursor.read_u8()?);
        self.reserved0.push(cursor.read_u8()?);
        self.reserved0.push(cursor.read_u8()?);
        self.reserved0.push(cursor.read_u8()?);
        self.head_veh
            .push(cursor.read_i32::<LittleEndian>()? as f64 / 100000.0);
        self.mag_dec
            .push(cursor.read_i16::<LittleEndian>()? as f64 / 100.0);
        self.mag_acc
            .push(cursor.read_u16::<LittleEndian>()? as f64 / 100.0);

        finish_tdf_read(cursor, cursor_start, size)
    }

    pub fn finish_batch(&mut self) -> std::result::Result<RecordBatch, ArrowError> {
        let schema = self.schema();
        let columns = vec![
            Arc::new(
                TimestampMicrosecondArray::from(std::mem::take(&mut self.row_timestamp))
                    .with_timezone("+00:00"),
            ) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.row_sample_idx))) as ArrayRef,
            Arc::new(UInt32Array::from(std::mem::take(&mut self.itow))) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.year))) as ArrayRef,
            Arc::new(UInt8Array::from(std::mem::take(&mut self.month))) as ArrayRef,
            Arc::new(UInt8Array::from(std::mem::take(&mut self.day))) as ArrayRef,
            Arc::new(UInt8Array::from(std::mem::take(&mut self.hour))) as ArrayRef,
            Arc::new(UInt8Array::from(std::mem::take(&mut self.min))) as ArrayRef,
            Arc::new(UInt8Array::from(std::mem::take(&mut self.sec))) as ArrayRef,
            Arc::new(UInt8Array::from(std::mem::take(&mut self.valid))) as ArrayRef,
            Arc::new(UInt32Array::from(std::mem::take(&mut self.t_acc))) as ArrayRef,
            Arc::new(Int32Array::from(std::mem::take(&mut self.nano))) as ArrayRef,
            Arc::new(UInt8Array::from(std::mem::take(&mut self.fix_type))) as ArrayRef,
            Arc::new(UInt8Array::from(std::mem::take(&mut self.flags))) as ArrayRef,
            Arc::new(UInt8Array::from(std::mem::take(&mut self.flags2))) as ArrayRef,
            Arc::new(UInt8Array::from(std::mem::take(&mut self.num_sv))) as ArrayRef,
            Arc::new(Float64Array::from(std::mem::take(&mut self.lon))) as ArrayRef,
            Arc::new(Float64Array::from(std::mem::take(&mut self.lat))) as ArrayRef,
            Arc::new(Float64Array::from(std::mem::take(&mut self.height))) as ArrayRef,
            Arc::new(Float64Array::from(std::mem::take(&mut self.h_msl))) as ArrayRef,
            Arc::new(Float64Array::from(std::mem::take(&mut self.h_acc))) as ArrayRef,
            Arc::new(Float64Array::from(std::mem::take(&mut self.v_acc))) as ArrayRef,
            Arc::new(Float64Array::from(std::mem::take(&mut self.vel_n))) as ArrayRef,
            Arc::new(Float64Array::from(std::mem::take(&mut self.vel_e))) as ArrayRef,
            Arc::new(Float64Array::from(std::mem::take(&mut self.vel_d))) as ArrayRef,
            Arc::new(Float64Array::from(std::mem::take(&mut self.g_speed))) as ArrayRef,
            Arc::new(Float64Array::from(std::mem::take(&mut self.head_mot))) as ArrayRef,
            Arc::new(Float64Array::from(std::mem::take(&mut self.s_acc))) as ArrayRef,
            Arc::new(Float64Array::from(std::mem::take(&mut self.head_acc))) as ArrayRef,
            Arc::new(Float64Array::from(std::mem::take(&mut self.p_dop))) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.flags3))) as ArrayRef,
            Arc::new(FixedSizeListArray::try_new(
                Arc::new(Field::new_list_field(DataType::UInt8, false)),
                4,
                Arc::new(UInt8Array::from(std::mem::take(&mut self.reserved0))) as ArrayRef,
                None,
            )?) as ArrayRef,
            Arc::new(Float64Array::from(std::mem::take(&mut self.head_veh))) as ArrayRef,
            Arc::new(Float64Array::from(std::mem::take(&mut self.mag_dec))) as ArrayRef,
            Arc::new(Float64Array::from(std::mem::take(&mut self.mag_acc))) as ArrayRef,
        ];

        RecordBatch::try_new(schema, columns)
    }
}

pub struct Tdf21LteConnStatusBuilder {
    row_timestamp: Vec<Option<i64>>,
    row_sample_idx: Vec<Option<u16>>,
    cell_mcc: Vec<u16>,
    cell_mnc: Vec<u16>,
    cell_eci: Vec<u32>,
    cell_tac: Vec<u16>,
    earfcn: Vec<u32>,
    status: Vec<u8>,
    tech: Vec<u8>,
    rsrp: Vec<f64>,
    rsrq: Vec<i8>,
}

impl Tdf21LteConnStatusBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            row_timestamp: Vec::with_capacity(capacity),
            row_sample_idx: Vec::with_capacity(capacity),
            cell_mcc: Vec::with_capacity(capacity),
            cell_mnc: Vec::with_capacity(capacity),
            cell_eci: Vec::with_capacity(capacity),
            cell_tac: Vec::with_capacity(capacity),
            earfcn: Vec::with_capacity(capacity),
            status: Vec::with_capacity(capacity),
            tech: Vec::with_capacity(capacity),
            rsrp: Vec::with_capacity(capacity),
            rsrq: Vec::with_capacity(capacity),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        tdf_parquet_schema(21).unwrap()
    }

    pub fn rows(&self) -> usize {
        self.row_timestamp.len()
    }

    pub fn append(
        &mut self,
        meta: TdfParquetRowMeta,
        size: u8,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<()> {
        let cursor_start = cursor.position();

        self.row_timestamp.push(meta.time_unix_micros);
        self.row_sample_idx.push(meta.sample_idx);
        self.cell_mcc.push(cursor.read_u16::<LittleEndian>()?);
        self.cell_mnc.push(cursor.read_u16::<LittleEndian>()?);
        self.cell_eci.push(cursor.read_u32::<LittleEndian>()?);
        self.cell_tac.push(cursor.read_u16::<LittleEndian>()?);
        self.earfcn.push(cursor.read_u32::<LittleEndian>()?);
        self.status.push(cursor.read_u8()?);
        self.tech.push(cursor.read_u8()?);
        self.rsrp.push(cursor.read_u8()? as f64 / -1.0);
        self.rsrq.push(cursor.read_i8()?);

        finish_tdf_read(cursor, cursor_start, size)
    }

    pub fn finish_batch(&mut self) -> std::result::Result<RecordBatch, ArrowError> {
        let schema = self.schema();
        let columns = vec![
            Arc::new(
                TimestampMicrosecondArray::from(std::mem::take(&mut self.row_timestamp))
                    .with_timezone("+00:00"),
            ) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.row_sample_idx))) as ArrayRef,
            Arc::new(StructArray::try_new(
                Fields::from(vec![
                    Field::new("mcc", DataType::UInt16, false),
                    Field::new("mnc", DataType::UInt16, false),
                    Field::new("eci", DataType::UInt32, false),
                    Field::new("tac", DataType::UInt16, false),
                ]),
                vec![
                    Arc::new(UInt16Array::from(std::mem::take(&mut self.cell_mcc))) as ArrayRef,
                    Arc::new(UInt16Array::from(std::mem::take(&mut self.cell_mnc))) as ArrayRef,
                    Arc::new(UInt32Array::from(std::mem::take(&mut self.cell_eci))) as ArrayRef,
                    Arc::new(UInt16Array::from(std::mem::take(&mut self.cell_tac))) as ArrayRef,
                ],
                None,
            )?) as ArrayRef,
            Arc::new(UInt32Array::from(std::mem::take(&mut self.earfcn))) as ArrayRef,
            Arc::new(UInt8Array::from(std::mem::take(&mut self.status))) as ArrayRef,
            Arc::new(UInt8Array::from(std::mem::take(&mut self.tech))) as ArrayRef,
            Arc::new(Float64Array::from(std::mem::take(&mut self.rsrp))) as ArrayRef,
            Arc::new(Int8Array::from(std::mem::take(&mut self.rsrq))) as ArrayRef,
        ];

        RecordBatch::try_new(schema, columns)
    }
}

pub struct Tdf22GlobalstarPktBuilder {
    row_timestamp: Vec<Option<i64>>,
    row_sample_idx: Vec<Option<u16>>,
    payload: Vec<u8>,
}

impl Tdf22GlobalstarPktBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            row_timestamp: Vec::with_capacity(capacity),
            row_sample_idx: Vec::with_capacity(capacity),
            payload: Vec::with_capacity(capacity * 9),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        tdf_parquet_schema(22).unwrap()
    }

    pub fn rows(&self) -> usize {
        self.row_timestamp.len()
    }

    pub fn append(
        &mut self,
        meta: TdfParquetRowMeta,
        size: u8,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<()> {
        let cursor_start = cursor.position();

        self.row_timestamp.push(meta.time_unix_micros);
        self.row_sample_idx.push(meta.sample_idx);
        self.payload.push(cursor.read_u8()?);
        self.payload.push(cursor.read_u8()?);
        self.payload.push(cursor.read_u8()?);
        self.payload.push(cursor.read_u8()?);
        self.payload.push(cursor.read_u8()?);
        self.payload.push(cursor.read_u8()?);
        self.payload.push(cursor.read_u8()?);
        self.payload.push(cursor.read_u8()?);
        self.payload.push(cursor.read_u8()?);

        finish_tdf_read(cursor, cursor_start, size)
    }

    pub fn finish_batch(&mut self) -> std::result::Result<RecordBatch, ArrowError> {
        let schema = self.schema();
        let columns = vec![
            Arc::new(
                TimestampMicrosecondArray::from(std::mem::take(&mut self.row_timestamp))
                    .with_timezone("+00:00"),
            ) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.row_sample_idx))) as ArrayRef,
            Arc::new(FixedSizeListArray::try_new(
                Arc::new(Field::new_list_field(DataType::UInt8, false)),
                9,
                Arc::new(UInt8Array::from(std::mem::take(&mut self.payload))) as ArrayRef,
                None,
            )?) as ArrayRef,
        ];

        RecordBatch::try_new(schema, columns)
    }
}

pub struct Tdf23AccMagnitudeStdDevBuilder {
    row_timestamp: Vec<Option<i64>>,
    row_sample_idx: Vec<Option<u16>>,
    count: Vec<u32>,
    std_dev: Vec<u32>,
}

impl Tdf23AccMagnitudeStdDevBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            row_timestamp: Vec::with_capacity(capacity),
            row_sample_idx: Vec::with_capacity(capacity),
            count: Vec::with_capacity(capacity),
            std_dev: Vec::with_capacity(capacity),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        tdf_parquet_schema(23).unwrap()
    }

    pub fn rows(&self) -> usize {
        self.row_timestamp.len()
    }

    pub fn append(
        &mut self,
        meta: TdfParquetRowMeta,
        size: u8,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<()> {
        let cursor_start = cursor.position();

        self.row_timestamp.push(meta.time_unix_micros);
        self.row_sample_idx.push(meta.sample_idx);
        self.count.push(cursor.read_u32::<LittleEndian>()?);
        self.std_dev.push(cursor.read_u32::<LittleEndian>()?);

        finish_tdf_read(cursor, cursor_start, size)
    }

    pub fn finish_batch(&mut self) -> std::result::Result<RecordBatch, ArrowError> {
        let schema = self.schema();
        let columns = vec![
            Arc::new(
                TimestampMicrosecondArray::from(std::mem::take(&mut self.row_timestamp))
                    .with_timezone("+00:00"),
            ) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.row_sample_idx))) as ArrayRef,
            Arc::new(UInt32Array::from(std::mem::take(&mut self.count))) as ArrayRef,
            Arc::new(UInt32Array::from(std::mem::take(&mut self.std_dev))) as ArrayRef,
        ];

        RecordBatch::try_new(schema, columns)
    }
}

pub struct Tdf24ActivityMetricBuilder {
    row_timestamp: Vec<Option<i64>>,
    row_sample_idx: Vec<Option<u16>>,
    value: Vec<u32>,
}

impl Tdf24ActivityMetricBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            row_timestamp: Vec::with_capacity(capacity),
            row_sample_idx: Vec::with_capacity(capacity),
            value: Vec::with_capacity(capacity),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        tdf_parquet_schema(24).unwrap()
    }

    pub fn rows(&self) -> usize {
        self.row_timestamp.len()
    }

    pub fn append(
        &mut self,
        meta: TdfParquetRowMeta,
        size: u8,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<()> {
        let cursor_start = cursor.position();

        self.row_timestamp.push(meta.time_unix_micros);
        self.row_sample_idx.push(meta.sample_idx);
        self.value.push(cursor.read_u32::<LittleEndian>()?);

        finish_tdf_read(cursor, cursor_start, size)
    }

    pub fn finish_batch(&mut self) -> std::result::Result<RecordBatch, ArrowError> {
        let schema = self.schema();
        let columns = vec![
            Arc::new(
                TimestampMicrosecondArray::from(std::mem::take(&mut self.row_timestamp))
                    .with_timezone("+00:00"),
            ) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.row_sample_idx))) as ArrayRef,
            Arc::new(UInt32Array::from(std::mem::take(&mut self.value))) as ArrayRef,
        ];

        RecordBatch::try_new(schema, columns)
    }
}

pub struct Tdf25AlgorithmOutputBuilder {
    row_timestamp: Vec<Option<i64>>,
    row_sample_idx: Vec<Option<u16>>,
    algorithm_id: Vec<u32>,
    algorithm_version: Vec<u16>,
    output: Vec<Vec<u8>>,
}

impl Tdf25AlgorithmOutputBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            row_timestamp: Vec::with_capacity(capacity),
            row_sample_idx: Vec::with_capacity(capacity),
            algorithm_id: Vec::with_capacity(capacity),
            algorithm_version: Vec::with_capacity(capacity),
            output: Vec::with_capacity(capacity),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        tdf_parquet_schema(25).unwrap()
    }

    pub fn rows(&self) -> usize {
        self.row_timestamp.len()
    }

    pub fn append(
        &mut self,
        meta: TdfParquetRowMeta,
        size: u8,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<()> {
        let cursor_start = cursor.position();

        self.row_timestamp.push(meta.time_unix_micros);
        self.row_sample_idx.push(meta.sample_idx);
        self.algorithm_id.push(cursor.read_u32::<LittleEndian>()?);
        self.algorithm_version
            .push(cursor.read_u16::<LittleEndian>()?);
        self.output.push(crate::decoders::tdf_field_read_vla(
            cursor,
            cursor_start,
            size,
        )?);

        finish_tdf_read(cursor, cursor_start, size)
    }

    pub fn finish_batch(&mut self) -> std::result::Result<RecordBatch, ArrowError> {
        let schema = self.schema();
        let columns = vec![
            Arc::new(
                TimestampMicrosecondArray::from(std::mem::take(&mut self.row_timestamp))
                    .with_timezone("+00:00"),
            ) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.row_sample_idx))) as ArrayRef,
            Arc::new(UInt32Array::from(std::mem::take(&mut self.algorithm_id))) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(
                &mut self.algorithm_version,
            ))) as ArrayRef,
            Arc::new(BinaryArray::from_iter_values(std::mem::take(
                &mut self.output,
            ))) as ArrayRef,
        ];

        RecordBatch::try_new(schema, columns)
    }
}

pub struct Tdf26RuntimeErrorBuilder {
    row_timestamp: Vec<Option<i64>>,
    row_sample_idx: Vec<Option<u16>>,
    error_id: Vec<u32>,
    error_ctx: Vec<u32>,
}

impl Tdf26RuntimeErrorBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            row_timestamp: Vec::with_capacity(capacity),
            row_sample_idx: Vec::with_capacity(capacity),
            error_id: Vec::with_capacity(capacity),
            error_ctx: Vec::with_capacity(capacity),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        tdf_parquet_schema(26).unwrap()
    }

    pub fn rows(&self) -> usize {
        self.row_timestamp.len()
    }

    pub fn append(
        &mut self,
        meta: TdfParquetRowMeta,
        size: u8,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<()> {
        let cursor_start = cursor.position();

        self.row_timestamp.push(meta.time_unix_micros);
        self.row_sample_idx.push(meta.sample_idx);
        self.error_id.push(cursor.read_u32::<LittleEndian>()?);
        self.error_ctx.push(cursor.read_u32::<LittleEndian>()?);

        finish_tdf_read(cursor, cursor_start, size)
    }

    pub fn finish_batch(&mut self) -> std::result::Result<RecordBatch, ArrowError> {
        let schema = self.schema();
        let columns = vec![
            Arc::new(
                TimestampMicrosecondArray::from(std::mem::take(&mut self.row_timestamp))
                    .with_timezone("+00:00"),
            ) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.row_sample_idx))) as ArrayRef,
            Arc::new(UInt32Array::from(std::mem::take(&mut self.error_id))) as ArrayRef,
            Arc::new(UInt32Array::from(std::mem::take(&mut self.error_ctx))) as ArrayRef,
        ];

        RecordBatch::try_new(schema, columns)
    }
}

pub struct Tdf27ChargerEnControlBuilder {
    row_timestamp: Vec<Option<i64>>,
    row_sample_idx: Vec<Option<u16>>,
    enabled: Vec<u8>,
}

impl Tdf27ChargerEnControlBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            row_timestamp: Vec::with_capacity(capacity),
            row_sample_idx: Vec::with_capacity(capacity),
            enabled: Vec::with_capacity(capacity),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        tdf_parquet_schema(27).unwrap()
    }

    pub fn rows(&self) -> usize {
        self.row_timestamp.len()
    }

    pub fn append(
        &mut self,
        meta: TdfParquetRowMeta,
        size: u8,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<()> {
        let cursor_start = cursor.position();

        self.row_timestamp.push(meta.time_unix_micros);
        self.row_sample_idx.push(meta.sample_idx);
        self.enabled.push(cursor.read_u8()?);

        finish_tdf_read(cursor, cursor_start, size)
    }

    pub fn finish_batch(&mut self) -> std::result::Result<RecordBatch, ArrowError> {
        let schema = self.schema();
        let columns = vec![
            Arc::new(
                TimestampMicrosecondArray::from(std::mem::take(&mut self.row_timestamp))
                    .with_timezone("+00:00"),
            ) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.row_sample_idx))) as ArrayRef,
            Arc::new(UInt8Array::from(std::mem::take(&mut self.enabled))) as ArrayRef,
        ];

        RecordBatch::try_new(schema, columns)
    }
}

pub struct Tdf28GnssFixInfoBuilder {
    row_timestamp: Vec<Option<i64>>,
    row_sample_idx: Vec<Option<u16>>,
    time_fix: Vec<u16>,
    location_fix: Vec<u16>,
    num_sv: Vec<u8>,
}

impl Tdf28GnssFixInfoBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            row_timestamp: Vec::with_capacity(capacity),
            row_sample_idx: Vec::with_capacity(capacity),
            time_fix: Vec::with_capacity(capacity),
            location_fix: Vec::with_capacity(capacity),
            num_sv: Vec::with_capacity(capacity),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        tdf_parquet_schema(28).unwrap()
    }

    pub fn rows(&self) -> usize {
        self.row_timestamp.len()
    }

    pub fn append(
        &mut self,
        meta: TdfParquetRowMeta,
        size: u8,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<()> {
        let cursor_start = cursor.position();

        self.row_timestamp.push(meta.time_unix_micros);
        self.row_sample_idx.push(meta.sample_idx);
        self.time_fix.push(cursor.read_u16::<LittleEndian>()?);
        self.location_fix.push(cursor.read_u16::<LittleEndian>()?);
        self.num_sv.push(cursor.read_u8()?);

        finish_tdf_read(cursor, cursor_start, size)
    }

    pub fn finish_batch(&mut self) -> std::result::Result<RecordBatch, ArrowError> {
        let schema = self.schema();
        let columns = vec![
            Arc::new(
                TimestampMicrosecondArray::from(std::mem::take(&mut self.row_timestamp))
                    .with_timezone("+00:00"),
            ) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.row_sample_idx))) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.time_fix))) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.location_fix))) as ArrayRef,
            Arc::new(UInt8Array::from(std::mem::take(&mut self.num_sv))) as ArrayRef,
        ];

        RecordBatch::try_new(schema, columns)
    }
}

pub struct Tdf29BluetoothConnectionBuilder {
    row_timestamp: Vec<Option<i64>>,
    row_sample_idx: Vec<Option<u16>>,
    address_type: Vec<u8>,
    address_val: Vec<u64>,
    connected: Vec<u8>,
}

impl Tdf29BluetoothConnectionBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            row_timestamp: Vec::with_capacity(capacity),
            row_sample_idx: Vec::with_capacity(capacity),
            address_type: Vec::with_capacity(capacity),
            address_val: Vec::with_capacity(capacity),
            connected: Vec::with_capacity(capacity),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        tdf_parquet_schema(29).unwrap()
    }

    pub fn rows(&self) -> usize {
        self.row_timestamp.len()
    }

    pub fn append(
        &mut self,
        meta: TdfParquetRowMeta,
        size: u8,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<()> {
        let cursor_start = cursor.position();

        self.row_timestamp.push(meta.time_unix_micros);
        self.row_sample_idx.push(meta.sample_idx);
        self.address_type.push(cursor.read_u8()?);
        self.address_val.push(cursor.read_u48::<LittleEndian>()?);
        self.connected.push(cursor.read_u8()?);

        finish_tdf_read(cursor, cursor_start, size)
    }

    pub fn finish_batch(&mut self) -> std::result::Result<RecordBatch, ArrowError> {
        let schema = self.schema();
        let columns = vec![
            Arc::new(
                TimestampMicrosecondArray::from(std::mem::take(&mut self.row_timestamp))
                    .with_timezone("+00:00"),
            ) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.row_sample_idx))) as ArrayRef,
            Arc::new(StructArray::try_new(
                Fields::from(vec![
                    Field::new("type", DataType::UInt8, false),
                    Field::new("val", DataType::UInt64, false),
                ]),
                vec![
                    Arc::new(UInt8Array::from(std::mem::take(&mut self.address_type))) as ArrayRef,
                    Arc::new(UInt64Array::from(std::mem::take(&mut self.address_val))) as ArrayRef,
                ],
                None,
            )?) as ArrayRef,
            Arc::new(UInt8Array::from(std::mem::take(&mut self.connected))) as ArrayRef,
        ];

        RecordBatch::try_new(schema, columns)
    }
}

pub struct Tdf30BluetoothRssiBuilder {
    row_timestamp: Vec<Option<i64>>,
    row_sample_idx: Vec<Option<u16>>,
    address_type: Vec<u8>,
    address_val: Vec<u64>,
    rssi: Vec<i8>,
}

impl Tdf30BluetoothRssiBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            row_timestamp: Vec::with_capacity(capacity),
            row_sample_idx: Vec::with_capacity(capacity),
            address_type: Vec::with_capacity(capacity),
            address_val: Vec::with_capacity(capacity),
            rssi: Vec::with_capacity(capacity),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        tdf_parquet_schema(30).unwrap()
    }

    pub fn rows(&self) -> usize {
        self.row_timestamp.len()
    }

    pub fn append(
        &mut self,
        meta: TdfParquetRowMeta,
        size: u8,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<()> {
        let cursor_start = cursor.position();

        self.row_timestamp.push(meta.time_unix_micros);
        self.row_sample_idx.push(meta.sample_idx);
        self.address_type.push(cursor.read_u8()?);
        self.address_val.push(cursor.read_u48::<LittleEndian>()?);
        self.rssi.push(cursor.read_i8()?);

        finish_tdf_read(cursor, cursor_start, size)
    }

    pub fn finish_batch(&mut self) -> std::result::Result<RecordBatch, ArrowError> {
        let schema = self.schema();
        let columns = vec![
            Arc::new(
                TimestampMicrosecondArray::from(std::mem::take(&mut self.row_timestamp))
                    .with_timezone("+00:00"),
            ) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.row_sample_idx))) as ArrayRef,
            Arc::new(StructArray::try_new(
                Fields::from(vec![
                    Field::new("type", DataType::UInt8, false),
                    Field::new("val", DataType::UInt64, false),
                ]),
                vec![
                    Arc::new(UInt8Array::from(std::mem::take(&mut self.address_type))) as ArrayRef,
                    Arc::new(UInt64Array::from(std::mem::take(&mut self.address_val))) as ArrayRef,
                ],
                None,
            )?) as ArrayRef,
            Arc::new(Int8Array::from(std::mem::take(&mut self.rssi))) as ArrayRef,
        ];

        RecordBatch::try_new(schema, columns)
    }
}

pub struct Tdf31BluetoothDataThroughputBuilder {
    row_timestamp: Vec<Option<i64>>,
    row_sample_idx: Vec<Option<u16>>,
    address_type: Vec<u8>,
    address_val: Vec<u64>,
    throughput: Vec<i32>,
}

impl Tdf31BluetoothDataThroughputBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            row_timestamp: Vec::with_capacity(capacity),
            row_sample_idx: Vec::with_capacity(capacity),
            address_type: Vec::with_capacity(capacity),
            address_val: Vec::with_capacity(capacity),
            throughput: Vec::with_capacity(capacity),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        tdf_parquet_schema(31).unwrap()
    }

    pub fn rows(&self) -> usize {
        self.row_timestamp.len()
    }

    pub fn append(
        &mut self,
        meta: TdfParquetRowMeta,
        size: u8,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<()> {
        let cursor_start = cursor.position();

        self.row_timestamp.push(meta.time_unix_micros);
        self.row_sample_idx.push(meta.sample_idx);
        self.address_type.push(cursor.read_u8()?);
        self.address_val.push(cursor.read_u48::<LittleEndian>()?);
        self.throughput.push(cursor.read_i32::<LittleEndian>()?);

        finish_tdf_read(cursor, cursor_start, size)
    }

    pub fn finish_batch(&mut self) -> std::result::Result<RecordBatch, ArrowError> {
        let schema = self.schema();
        let columns = vec![
            Arc::new(
                TimestampMicrosecondArray::from(std::mem::take(&mut self.row_timestamp))
                    .with_timezone("+00:00"),
            ) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.row_sample_idx))) as ArrayRef,
            Arc::new(StructArray::try_new(
                Fields::from(vec![
                    Field::new("type", DataType::UInt8, false),
                    Field::new("val", DataType::UInt64, false),
                ]),
                vec![
                    Arc::new(UInt8Array::from(std::mem::take(&mut self.address_type))) as ArrayRef,
                    Arc::new(UInt64Array::from(std::mem::take(&mut self.address_val))) as ArrayRef,
                ],
                None,
            )?) as ArrayRef,
            Arc::new(Int32Array::from(std::mem::take(&mut self.throughput))) as ArrayRef,
        ];

        RecordBatch::try_new(schema, columns)
    }
}

pub struct Tdf32AlgorithmClassHistogramBuilder {
    row_timestamp: Vec<Option<i64>>,
    row_sample_idx: Vec<Option<u16>>,
    algorithm_id: Vec<u32>,
    algorithm_version: Vec<u16>,
    classes: Vec<Vec<u8>>,
}

impl Tdf32AlgorithmClassHistogramBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            row_timestamp: Vec::with_capacity(capacity),
            row_sample_idx: Vec::with_capacity(capacity),
            algorithm_id: Vec::with_capacity(capacity),
            algorithm_version: Vec::with_capacity(capacity),
            classes: Vec::with_capacity(capacity),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        tdf_parquet_schema(32).unwrap()
    }

    pub fn rows(&self) -> usize {
        self.row_timestamp.len()
    }

    pub fn append(
        &mut self,
        meta: TdfParquetRowMeta,
        size: u8,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<()> {
        let cursor_start = cursor.position();

        self.row_timestamp.push(meta.time_unix_micros);
        self.row_sample_idx.push(meta.sample_idx);
        self.algorithm_id.push(cursor.read_u32::<LittleEndian>()?);
        self.algorithm_version
            .push(cursor.read_u16::<LittleEndian>()?);
        self.classes.push(crate::decoders::tdf_field_read_vla(
            cursor,
            cursor_start,
            size,
        )?);

        finish_tdf_read(cursor, cursor_start, size)
    }

    pub fn finish_batch(&mut self) -> std::result::Result<RecordBatch, ArrowError> {
        let schema = self.schema();
        let columns = vec![
            Arc::new(
                TimestampMicrosecondArray::from(std::mem::take(&mut self.row_timestamp))
                    .with_timezone("+00:00"),
            ) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.row_sample_idx))) as ArrayRef,
            Arc::new(UInt32Array::from(std::mem::take(&mut self.algorithm_id))) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(
                &mut self.algorithm_version,
            ))) as ArrayRef,
            Arc::new(BinaryArray::from_iter_values(std::mem::take(
                &mut self.classes,
            ))) as ArrayRef,
        ];

        RecordBatch::try_new(schema, columns)
    }
}

pub struct Tdf33AlgorithmClassTimeSeriesBuilder {
    row_timestamp: Vec<Option<i64>>,
    row_sample_idx: Vec<Option<u16>>,
    algorithm_id: Vec<u32>,
    algorithm_version: Vec<u16>,
    values: Vec<Vec<u8>>,
}

impl Tdf33AlgorithmClassTimeSeriesBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            row_timestamp: Vec::with_capacity(capacity),
            row_sample_idx: Vec::with_capacity(capacity),
            algorithm_id: Vec::with_capacity(capacity),
            algorithm_version: Vec::with_capacity(capacity),
            values: Vec::with_capacity(capacity),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        tdf_parquet_schema(33).unwrap()
    }

    pub fn rows(&self) -> usize {
        self.row_timestamp.len()
    }

    pub fn append(
        &mut self,
        meta: TdfParquetRowMeta,
        size: u8,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<()> {
        let cursor_start = cursor.position();

        self.row_timestamp.push(meta.time_unix_micros);
        self.row_sample_idx.push(meta.sample_idx);
        self.algorithm_id.push(cursor.read_u32::<LittleEndian>()?);
        self.algorithm_version
            .push(cursor.read_u16::<LittleEndian>()?);
        self.values.push(crate::decoders::tdf_field_read_vla(
            cursor,
            cursor_start,
            size,
        )?);

        finish_tdf_read(cursor, cursor_start, size)
    }

    pub fn finish_batch(&mut self) -> std::result::Result<RecordBatch, ArrowError> {
        let schema = self.schema();
        let columns = vec![
            Arc::new(
                TimestampMicrosecondArray::from(std::mem::take(&mut self.row_timestamp))
                    .with_timezone("+00:00"),
            ) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.row_sample_idx))) as ArrayRef,
            Arc::new(UInt32Array::from(std::mem::take(&mut self.algorithm_id))) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(
                &mut self.algorithm_version,
            ))) as ArrayRef,
            Arc::new(BinaryArray::from_iter_values(std::mem::take(
                &mut self.values,
            ))) as ArrayRef,
        ];

        RecordBatch::try_new(schema, columns)
    }
}

pub struct Tdf34LteTacCellsBuilder {
    row_timestamp: Vec<Option<i64>>,
    row_sample_idx: Vec<Option<u16>>,
    cell_mcc: Vec<u16>,
    cell_mnc: Vec<u16>,
    cell_eci: Vec<u32>,
    cell_tac: Vec<u16>,
    earfcn: Vec<u32>,
    rsrp: Vec<f64>,
    rsrq: Vec<i8>,
    neighbours_offsets: Vec<i32>,
    neighbours_earfcn: Vec<u32>,
    neighbours_pci: Vec<u16>,
    neighbours_time_diff: Vec<f64>,
    neighbours_rsrp: Vec<f64>,
    neighbours_rsrq: Vec<i8>,
}

impl Tdf34LteTacCellsBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            row_timestamp: Vec::with_capacity(capacity),
            row_sample_idx: Vec::with_capacity(capacity),
            cell_mcc: Vec::with_capacity(capacity),
            cell_mnc: Vec::with_capacity(capacity),
            cell_eci: Vec::with_capacity(capacity),
            cell_tac: Vec::with_capacity(capacity),
            earfcn: Vec::with_capacity(capacity),
            rsrp: Vec::with_capacity(capacity),
            rsrq: Vec::with_capacity(capacity),
            neighbours_offsets: vec![0],
            neighbours_earfcn: Vec::with_capacity(capacity),
            neighbours_pci: Vec::with_capacity(capacity),
            neighbours_time_diff: Vec::with_capacity(capacity),
            neighbours_rsrp: Vec::with_capacity(capacity),
            neighbours_rsrq: Vec::with_capacity(capacity),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        tdf_parquet_schema(34).unwrap()
    }

    pub fn rows(&self) -> usize {
        self.row_timestamp.len()
    }

    fn list_value_field(&self, field_index: usize) -> Arc<Field> {
        let schema = self.schema();
        match schema.field(field_index).data_type() {
            DataType::List(field) => field.clone(),
            _ => unreachable!("generated list field index is not a list"),
        }
    }

    pub fn append(
        &mut self,
        meta: TdfParquetRowMeta,
        size: u8,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<()> {
        let cursor_start = cursor.position();

        self.row_timestamp.push(meta.time_unix_micros);
        self.row_sample_idx.push(meta.sample_idx);
        self.cell_mcc.push(cursor.read_u16::<LittleEndian>()?);
        self.cell_mnc.push(cursor.read_u16::<LittleEndian>()?);
        self.cell_eci.push(cursor.read_u32::<LittleEndian>()?);
        self.cell_tac.push(cursor.read_u16::<LittleEndian>()?);
        self.earfcn.push(cursor.read_u32::<LittleEndian>()?);
        self.rsrp.push(cursor.read_u8()? as f64 / -1.0);
        self.rsrq.push(cursor.read_i8()?);
        {
            let bytes_remaining = crate::decoders::vla_bytes_remaining(cursor, cursor_start, size)?;
            if bytes_remaining % 10 != 0 {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "Variable-length array does not align to element size",
                ));
            }
            let item_count = bytes_remaining / 10;
            for _ in 0..item_count {
                self.neighbours_earfcn
                    .push(cursor.read_u32::<LittleEndian>()?);
                self.neighbours_pci.push(cursor.read_u16::<LittleEndian>()?);
                self.neighbours_time_diff
                    .push(cursor.read_u16::<LittleEndian>()? as f64 / 1000.0);
                self.neighbours_rsrp.push(cursor.read_u8()? as f64 / -1.0);
                self.neighbours_rsrq.push(cursor.read_i8()?);
            }
            self.neighbours_offsets
                .push(*self.neighbours_offsets.last().unwrap() + item_count as i32);
        }

        finish_tdf_read(cursor, cursor_start, size)
    }

    pub fn finish_batch(&mut self) -> std::result::Result<RecordBatch, ArrowError> {
        let schema = self.schema();
        let columns = vec![
            Arc::new(
                TimestampMicrosecondArray::from(std::mem::take(&mut self.row_timestamp))
                    .with_timezone("+00:00"),
            ) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.row_sample_idx))) as ArrayRef,
            Arc::new(StructArray::try_new(
                Fields::from(vec![
                    Field::new("mcc", DataType::UInt16, false),
                    Field::new("mnc", DataType::UInt16, false),
                    Field::new("eci", DataType::UInt32, false),
                    Field::new("tac", DataType::UInt16, false),
                ]),
                vec![
                    Arc::new(UInt16Array::from(std::mem::take(&mut self.cell_mcc))) as ArrayRef,
                    Arc::new(UInt16Array::from(std::mem::take(&mut self.cell_mnc))) as ArrayRef,
                    Arc::new(UInt32Array::from(std::mem::take(&mut self.cell_eci))) as ArrayRef,
                    Arc::new(UInt16Array::from(std::mem::take(&mut self.cell_tac))) as ArrayRef,
                ],
                None,
            )?) as ArrayRef,
            Arc::new(UInt32Array::from(std::mem::take(&mut self.earfcn))) as ArrayRef,
            Arc::new(Float64Array::from(std::mem::take(&mut self.rsrp))) as ArrayRef,
            Arc::new(Int8Array::from(std::mem::take(&mut self.rsrq))) as ArrayRef,
            {
                let offsets = std::mem::replace(&mut self.neighbours_offsets, vec![0]);
                Arc::new(ListArray::try_new(
                    self.list_value_field(6),
                    OffsetBuffer::new(ScalarBuffer::from(offsets)),
                    Arc::new(StructArray::try_new(
                        Fields::from(vec![
                            Field::new("earfcn", DataType::UInt32, false),
                            Field::new("pci", DataType::UInt16, false),
                            Field::new("time_diff", DataType::Float64, false),
                            Field::new("rsrp", DataType::Float64, false),
                            Field::new("rsrq", DataType::Int8, false),
                        ]),
                        vec![
                            Arc::new(UInt32Array::from(std::mem::take(
                                &mut self.neighbours_earfcn,
                            ))) as ArrayRef,
                            Arc::new(UInt16Array::from(std::mem::take(&mut self.neighbours_pci)))
                                as ArrayRef,
                            Arc::new(Float64Array::from(std::mem::take(
                                &mut self.neighbours_time_diff,
                            ))) as ArrayRef,
                            Arc::new(Float64Array::from(std::mem::take(
                                &mut self.neighbours_rsrp,
                            ))) as ArrayRef,
                            Arc::new(Int8Array::from(std::mem::take(&mut self.neighbours_rsrq)))
                                as ArrayRef,
                        ],
                        None,
                    )?) as ArrayRef,
                    None,
                )?) as ArrayRef
            },
        ];

        RecordBatch::try_new(schema, columns)
    }
}

pub struct Tdf35WifiApInfoBuilder {
    row_timestamp: Vec<Option<i64>>,
    row_sample_idx: Vec<Option<u16>>,
    bssid_val: Vec<u64>,
    channel: Vec<u8>,
    rsrp: Vec<i8>,
}

impl Tdf35WifiApInfoBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            row_timestamp: Vec::with_capacity(capacity),
            row_sample_idx: Vec::with_capacity(capacity),
            bssid_val: Vec::with_capacity(capacity),
            channel: Vec::with_capacity(capacity),
            rsrp: Vec::with_capacity(capacity),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        tdf_parquet_schema(35).unwrap()
    }

    pub fn rows(&self) -> usize {
        self.row_timestamp.len()
    }

    pub fn append(
        &mut self,
        meta: TdfParquetRowMeta,
        size: u8,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<()> {
        let cursor_start = cursor.position();

        self.row_timestamp.push(meta.time_unix_micros);
        self.row_sample_idx.push(meta.sample_idx);
        self.bssid_val.push(cursor.read_u48::<BigEndian>()?);
        self.channel.push(cursor.read_u8()?);
        self.rsrp.push(cursor.read_i8()?);

        finish_tdf_read(cursor, cursor_start, size)
    }

    pub fn finish_batch(&mut self) -> std::result::Result<RecordBatch, ArrowError> {
        let schema = self.schema();
        let columns = vec![
            Arc::new(
                TimestampMicrosecondArray::from(std::mem::take(&mut self.row_timestamp))
                    .with_timezone("+00:00"),
            ) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.row_sample_idx))) as ArrayRef,
            Arc::new(StructArray::try_new(
                Fields::from(vec![Field::new("val", DataType::UInt64, false)]),
                vec![Arc::new(UInt64Array::from(std::mem::take(&mut self.bssid_val))) as ArrayRef],
                None,
            )?) as ArrayRef,
            Arc::new(UInt8Array::from(std::mem::take(&mut self.channel))) as ArrayRef,
            Arc::new(Int8Array::from(std::mem::take(&mut self.rsrp))) as ArrayRef,
        ];

        RecordBatch::try_new(schema, columns)
    }
}

pub struct Tdf36DeviceTiltBuilder {
    row_timestamp: Vec<Option<i64>>,
    row_sample_idx: Vec<Option<u16>>,
    cosine: Vec<f32>,
}

impl Tdf36DeviceTiltBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            row_timestamp: Vec::with_capacity(capacity),
            row_sample_idx: Vec::with_capacity(capacity),
            cosine: Vec::with_capacity(capacity),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        tdf_parquet_schema(36).unwrap()
    }

    pub fn rows(&self) -> usize {
        self.row_timestamp.len()
    }

    pub fn append(
        &mut self,
        meta: TdfParquetRowMeta,
        size: u8,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<()> {
        let cursor_start = cursor.position();

        self.row_timestamp.push(meta.time_unix_micros);
        self.row_sample_idx.push(meta.sample_idx);
        self.cosine.push(cursor.read_f32::<LittleEndian>()?);

        finish_tdf_read(cursor, cursor_start, size)
    }

    pub fn finish_batch(&mut self) -> std::result::Result<RecordBatch, ArrowError> {
        let schema = self.schema();
        let columns = vec![
            Arc::new(
                TimestampMicrosecondArray::from(std::mem::take(&mut self.row_timestamp))
                    .with_timezone("+00:00"),
            ) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.row_sample_idx))) as ArrayRef,
            Arc::new(Float32Array::from(std::mem::take(&mut self.cosine))) as ArrayRef,
        ];

        RecordBatch::try_new(schema, columns)
    }
}

pub struct Tdf37Nrf9xGnssPvtBuilder {
    row_timestamp: Vec<Option<i64>>,
    row_sample_idx: Vec<Option<u16>>,
    lat: Vec<f64>,
    lon: Vec<f64>,
    height: Vec<f64>,
    h_acc: Vec<f64>,
    v_acc: Vec<f64>,
    h_speed: Vec<f64>,
    h_speed_acc: Vec<f64>,
    v_speed: Vec<f64>,
    v_speed_acc: Vec<f64>,
    head_mot: Vec<f64>,
    head_acc: Vec<f64>,
    year: Vec<u16>,
    month: Vec<u8>,
    day: Vec<u8>,
    hour: Vec<u8>,
    min: Vec<u8>,
    sec: Vec<u8>,
    ms: Vec<u16>,
    p_dop: Vec<f64>,
    h_dop: Vec<f64>,
    v_dop: Vec<f64>,
    t_dop: Vec<f64>,
    flags: Vec<u8>,
    num_sv: Vec<u8>,
}

impl Tdf37Nrf9xGnssPvtBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            row_timestamp: Vec::with_capacity(capacity),
            row_sample_idx: Vec::with_capacity(capacity),
            lat: Vec::with_capacity(capacity),
            lon: Vec::with_capacity(capacity),
            height: Vec::with_capacity(capacity),
            h_acc: Vec::with_capacity(capacity),
            v_acc: Vec::with_capacity(capacity),
            h_speed: Vec::with_capacity(capacity),
            h_speed_acc: Vec::with_capacity(capacity),
            v_speed: Vec::with_capacity(capacity),
            v_speed_acc: Vec::with_capacity(capacity),
            head_mot: Vec::with_capacity(capacity),
            head_acc: Vec::with_capacity(capacity),
            year: Vec::with_capacity(capacity),
            month: Vec::with_capacity(capacity),
            day: Vec::with_capacity(capacity),
            hour: Vec::with_capacity(capacity),
            min: Vec::with_capacity(capacity),
            sec: Vec::with_capacity(capacity),
            ms: Vec::with_capacity(capacity),
            p_dop: Vec::with_capacity(capacity),
            h_dop: Vec::with_capacity(capacity),
            v_dop: Vec::with_capacity(capacity),
            t_dop: Vec::with_capacity(capacity),
            flags: Vec::with_capacity(capacity),
            num_sv: Vec::with_capacity(capacity),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        tdf_parquet_schema(37).unwrap()
    }

    pub fn rows(&self) -> usize {
        self.row_timestamp.len()
    }

    pub fn append(
        &mut self,
        meta: TdfParquetRowMeta,
        size: u8,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<()> {
        let cursor_start = cursor.position();

        self.row_timestamp.push(meta.time_unix_micros);
        self.row_sample_idx.push(meta.sample_idx);
        self.lat
            .push(cursor.read_i32::<LittleEndian>()? as f64 / 10000000.0);
        self.lon
            .push(cursor.read_i32::<LittleEndian>()? as f64 / 10000000.0);
        self.height
            .push(cursor.read_i32::<LittleEndian>()? as f64 / 1000.0);
        self.h_acc
            .push(cursor.read_u32::<LittleEndian>()? as f64 / 1000.0);
        self.v_acc
            .push(cursor.read_u32::<LittleEndian>()? as f64 / 1000.0);
        self.h_speed
            .push(cursor.read_i32::<LittleEndian>()? as f64 / 1000.0);
        self.h_speed_acc
            .push(cursor.read_u32::<LittleEndian>()? as f64 / 1000.0);
        self.v_speed
            .push(cursor.read_i32::<LittleEndian>()? as f64 / 1000.0);
        self.v_speed_acc
            .push(cursor.read_u32::<LittleEndian>()? as f64 / 1000.0);
        self.head_mot
            .push(cursor.read_i32::<LittleEndian>()? as f64 / 100000.0);
        self.head_acc
            .push(cursor.read_u32::<LittleEndian>()? as f64 / 100000.0);
        self.year.push(cursor.read_u16::<LittleEndian>()?);
        self.month.push(cursor.read_u8()?);
        self.day.push(cursor.read_u8()?);
        self.hour.push(cursor.read_u8()?);
        self.min.push(cursor.read_u8()?);
        self.sec.push(cursor.read_u8()?);
        self.ms.push(cursor.read_u16::<LittleEndian>()?);
        self.p_dop
            .push(cursor.read_u16::<LittleEndian>()? as f64 / 100.0);
        self.h_dop
            .push(cursor.read_u16::<LittleEndian>()? as f64 / 100.0);
        self.v_dop
            .push(cursor.read_u16::<LittleEndian>()? as f64 / 100.0);
        self.t_dop
            .push(cursor.read_u16::<LittleEndian>()? as f64 / 100.0);
        self.flags.push(cursor.read_u8()?);
        self.num_sv.push(cursor.read_u8()?);

        finish_tdf_read(cursor, cursor_start, size)
    }

    pub fn finish_batch(&mut self) -> std::result::Result<RecordBatch, ArrowError> {
        let schema = self.schema();
        let columns = vec![
            Arc::new(
                TimestampMicrosecondArray::from(std::mem::take(&mut self.row_timestamp))
                    .with_timezone("+00:00"),
            ) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.row_sample_idx))) as ArrayRef,
            Arc::new(Float64Array::from(std::mem::take(&mut self.lat))) as ArrayRef,
            Arc::new(Float64Array::from(std::mem::take(&mut self.lon))) as ArrayRef,
            Arc::new(Float64Array::from(std::mem::take(&mut self.height))) as ArrayRef,
            Arc::new(Float64Array::from(std::mem::take(&mut self.h_acc))) as ArrayRef,
            Arc::new(Float64Array::from(std::mem::take(&mut self.v_acc))) as ArrayRef,
            Arc::new(Float64Array::from(std::mem::take(&mut self.h_speed))) as ArrayRef,
            Arc::new(Float64Array::from(std::mem::take(&mut self.h_speed_acc))) as ArrayRef,
            Arc::new(Float64Array::from(std::mem::take(&mut self.v_speed))) as ArrayRef,
            Arc::new(Float64Array::from(std::mem::take(&mut self.v_speed_acc))) as ArrayRef,
            Arc::new(Float64Array::from(std::mem::take(&mut self.head_mot))) as ArrayRef,
            Arc::new(Float64Array::from(std::mem::take(&mut self.head_acc))) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.year))) as ArrayRef,
            Arc::new(UInt8Array::from(std::mem::take(&mut self.month))) as ArrayRef,
            Arc::new(UInt8Array::from(std::mem::take(&mut self.day))) as ArrayRef,
            Arc::new(UInt8Array::from(std::mem::take(&mut self.hour))) as ArrayRef,
            Arc::new(UInt8Array::from(std::mem::take(&mut self.min))) as ArrayRef,
            Arc::new(UInt8Array::from(std::mem::take(&mut self.sec))) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.ms))) as ArrayRef,
            Arc::new(Float64Array::from(std::mem::take(&mut self.p_dop))) as ArrayRef,
            Arc::new(Float64Array::from(std::mem::take(&mut self.h_dop))) as ArrayRef,
            Arc::new(Float64Array::from(std::mem::take(&mut self.v_dop))) as ArrayRef,
            Arc::new(Float64Array::from(std::mem::take(&mut self.t_dop))) as ArrayRef,
            Arc::new(UInt8Array::from(std::mem::take(&mut self.flags))) as ArrayRef,
            Arc::new(UInt8Array::from(std::mem::take(&mut self.num_sv))) as ArrayRef,
        ];

        RecordBatch::try_new(schema, columns)
    }
}

pub struct Tdf38BatteryChargeAccumulatedBuilder {
    row_timestamp: Vec<Option<i64>>,
    row_sample_idx: Vec<Option<u16>>,
    charge: Vec<i32>,
}

impl Tdf38BatteryChargeAccumulatedBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            row_timestamp: Vec::with_capacity(capacity),
            row_sample_idx: Vec::with_capacity(capacity),
            charge: Vec::with_capacity(capacity),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        tdf_parquet_schema(38).unwrap()
    }

    pub fn rows(&self) -> usize {
        self.row_timestamp.len()
    }

    pub fn append(
        &mut self,
        meta: TdfParquetRowMeta,
        size: u8,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<()> {
        let cursor_start = cursor.position();

        self.row_timestamp.push(meta.time_unix_micros);
        self.row_sample_idx.push(meta.sample_idx);
        self.charge.push(cursor.read_i32::<LittleEndian>()?);

        finish_tdf_read(cursor, cursor_start, size)
    }

    pub fn finish_batch(&mut self) -> std::result::Result<RecordBatch, ArrowError> {
        let schema = self.schema();
        let columns = vec![
            Arc::new(
                TimestampMicrosecondArray::from(std::mem::take(&mut self.row_timestamp))
                    .with_timezone("+00:00"),
            ) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.row_sample_idx))) as ArrayRef,
            Arc::new(Int32Array::from(std::mem::take(&mut self.charge))) as ArrayRef,
        ];

        RecordBatch::try_new(schema, columns)
    }
}

pub struct Tdf39InfuseBluetoothRssiBuilder {
    row_timestamp: Vec<Option<i64>>,
    row_sample_idx: Vec<Option<u16>>,
    infuse_id: Vec<u64>,
    rssi: Vec<i8>,
}

impl Tdf39InfuseBluetoothRssiBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            row_timestamp: Vec::with_capacity(capacity),
            row_sample_idx: Vec::with_capacity(capacity),
            infuse_id: Vec::with_capacity(capacity),
            rssi: Vec::with_capacity(capacity),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        tdf_parquet_schema(39).unwrap()
    }

    pub fn rows(&self) -> usize {
        self.row_timestamp.len()
    }

    pub fn append(
        &mut self,
        meta: TdfParquetRowMeta,
        size: u8,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<()> {
        let cursor_start = cursor.position();

        self.row_timestamp.push(meta.time_unix_micros);
        self.row_sample_idx.push(meta.sample_idx);
        self.infuse_id.push(cursor.read_u64::<LittleEndian>()?);
        self.rssi.push(cursor.read_i8()?);

        finish_tdf_read(cursor, cursor_start, size)
    }

    pub fn finish_batch(&mut self) -> std::result::Result<RecordBatch, ArrowError> {
        let schema = self.schema();
        let columns = vec![
            Arc::new(
                TimestampMicrosecondArray::from(std::mem::take(&mut self.row_timestamp))
                    .with_timezone("+00:00"),
            ) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.row_sample_idx))) as ArrayRef,
            Arc::new(UInt64Array::from(std::mem::take(&mut self.infuse_id))) as ArrayRef,
            Arc::new(Int8Array::from(std::mem::take(&mut self.rssi))) as ArrayRef,
        ];

        RecordBatch::try_new(schema, columns)
    }
}

pub struct Tdf40AdcRaw8Builder {
    row_timestamp: Vec<Option<i64>>,
    row_sample_idx: Vec<Option<u16>>,
    val: Vec<i8>,
}

impl Tdf40AdcRaw8Builder {
    pub fn new(capacity: usize) -> Self {
        Self {
            row_timestamp: Vec::with_capacity(capacity),
            row_sample_idx: Vec::with_capacity(capacity),
            val: Vec::with_capacity(capacity),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        tdf_parquet_schema(40).unwrap()
    }

    pub fn rows(&self) -> usize {
        self.row_timestamp.len()
    }

    pub fn append(
        &mut self,
        meta: TdfParquetRowMeta,
        size: u8,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<()> {
        let cursor_start = cursor.position();

        self.row_timestamp.push(meta.time_unix_micros);
        self.row_sample_idx.push(meta.sample_idx);
        self.val.push(cursor.read_i8()?);

        finish_tdf_read(cursor, cursor_start, size)
    }

    pub fn finish_batch(&mut self) -> std::result::Result<RecordBatch, ArrowError> {
        let schema = self.schema();
        let columns = vec![
            Arc::new(
                TimestampMicrosecondArray::from(std::mem::take(&mut self.row_timestamp))
                    .with_timezone("+00:00"),
            ) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.row_sample_idx))) as ArrayRef,
            Arc::new(Int8Array::from(std::mem::take(&mut self.val))) as ArrayRef,
        ];

        RecordBatch::try_new(schema, columns)
    }
}

pub struct Tdf41AdcRaw16Builder {
    row_timestamp: Vec<Option<i64>>,
    row_sample_idx: Vec<Option<u16>>,
    val: Vec<i16>,
}

impl Tdf41AdcRaw16Builder {
    pub fn new(capacity: usize) -> Self {
        Self {
            row_timestamp: Vec::with_capacity(capacity),
            row_sample_idx: Vec::with_capacity(capacity),
            val: Vec::with_capacity(capacity),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        tdf_parquet_schema(41).unwrap()
    }

    pub fn rows(&self) -> usize {
        self.row_timestamp.len()
    }

    pub fn append(
        &mut self,
        meta: TdfParquetRowMeta,
        size: u8,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<()> {
        let cursor_start = cursor.position();

        self.row_timestamp.push(meta.time_unix_micros);
        self.row_sample_idx.push(meta.sample_idx);
        self.val.push(cursor.read_i16::<LittleEndian>()?);

        finish_tdf_read(cursor, cursor_start, size)
    }

    pub fn finish_batch(&mut self) -> std::result::Result<RecordBatch, ArrowError> {
        let schema = self.schema();
        let columns = vec![
            Arc::new(
                TimestampMicrosecondArray::from(std::mem::take(&mut self.row_timestamp))
                    .with_timezone("+00:00"),
            ) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.row_sample_idx))) as ArrayRef,
            Arc::new(Int16Array::from(std::mem::take(&mut self.val))) as ArrayRef,
        ];

        RecordBatch::try_new(schema, columns)
    }
}

pub struct Tdf42AdcRaw32Builder {
    row_timestamp: Vec<Option<i64>>,
    row_sample_idx: Vec<Option<u16>>,
    val: Vec<i32>,
}

impl Tdf42AdcRaw32Builder {
    pub fn new(capacity: usize) -> Self {
        Self {
            row_timestamp: Vec::with_capacity(capacity),
            row_sample_idx: Vec::with_capacity(capacity),
            val: Vec::with_capacity(capacity),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        tdf_parquet_schema(42).unwrap()
    }

    pub fn rows(&self) -> usize {
        self.row_timestamp.len()
    }

    pub fn append(
        &mut self,
        meta: TdfParquetRowMeta,
        size: u8,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<()> {
        let cursor_start = cursor.position();

        self.row_timestamp.push(meta.time_unix_micros);
        self.row_sample_idx.push(meta.sample_idx);
        self.val.push(cursor.read_i32::<LittleEndian>()?);

        finish_tdf_read(cursor, cursor_start, size)
    }

    pub fn finish_batch(&mut self) -> std::result::Result<RecordBatch, ArrowError> {
        let schema = self.schema();
        let columns = vec![
            Arc::new(
                TimestampMicrosecondArray::from(std::mem::take(&mut self.row_timestamp))
                    .with_timezone("+00:00"),
            ) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.row_sample_idx))) as ArrayRef,
            Arc::new(Int32Array::from(std::mem::take(&mut self.val))) as ArrayRef,
        ];

        RecordBatch::try_new(schema, columns)
    }
}

pub struct Tdf43AnnotationBuilder {
    row_timestamp: Vec<Option<i64>>,
    row_sample_idx: Vec<Option<u16>>,
    timestamp: Vec<u32>,
    event: Vec<String>,
}

impl Tdf43AnnotationBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            row_timestamp: Vec::with_capacity(capacity),
            row_sample_idx: Vec::with_capacity(capacity),
            timestamp: Vec::with_capacity(capacity),
            event: Vec::with_capacity(capacity),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        tdf_parquet_schema(43).unwrap()
    }

    pub fn rows(&self) -> usize {
        self.row_timestamp.len()
    }

    pub fn append(
        &mut self,
        meta: TdfParquetRowMeta,
        size: u8,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<()> {
        let cursor_start = cursor.position();

        self.row_timestamp.push(meta.time_unix_micros);
        self.row_sample_idx.push(meta.sample_idx);
        self.timestamp.push(cursor.read_u32::<LittleEndian>()?);
        self.event.push(tdf_field_read_string_to_string(
            cursor,
            cursor_start,
            0,
            size,
        )?);

        finish_tdf_read(cursor, cursor_start, size)
    }

    pub fn finish_batch(&mut self) -> std::result::Result<RecordBatch, ArrowError> {
        let schema = self.schema();
        let columns = vec![
            Arc::new(
                TimestampMicrosecondArray::from(std::mem::take(&mut self.row_timestamp))
                    .with_timezone("+00:00"),
            ) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.row_sample_idx))) as ArrayRef,
            Arc::new(UInt32Array::from(std::mem::take(&mut self.timestamp))) as ArrayRef,
            Arc::new(StringArray::from_iter_values(std::mem::take(
                &mut self.event,
            ))) as ArrayRef,
        ];

        RecordBatch::try_new(schema, columns)
    }
}

pub struct Tdf44LoraRxBuilder {
    row_timestamp: Vec<Option<i64>>,
    row_sample_idx: Vec<Option<u16>>,
    snr: Vec<i8>,
    rssi: Vec<i16>,
    payload: Vec<Vec<u8>>,
}

impl Tdf44LoraRxBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            row_timestamp: Vec::with_capacity(capacity),
            row_sample_idx: Vec::with_capacity(capacity),
            snr: Vec::with_capacity(capacity),
            rssi: Vec::with_capacity(capacity),
            payload: Vec::with_capacity(capacity),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        tdf_parquet_schema(44).unwrap()
    }

    pub fn rows(&self) -> usize {
        self.row_timestamp.len()
    }

    pub fn append(
        &mut self,
        meta: TdfParquetRowMeta,
        size: u8,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<()> {
        let cursor_start = cursor.position();

        self.row_timestamp.push(meta.time_unix_micros);
        self.row_sample_idx.push(meta.sample_idx);
        self.snr.push(cursor.read_i8()?);
        self.rssi.push(cursor.read_i16::<LittleEndian>()?);
        self.payload.push(crate::decoders::tdf_field_read_vla(
            cursor,
            cursor_start,
            size,
        )?);

        finish_tdf_read(cursor, cursor_start, size)
    }

    pub fn finish_batch(&mut self) -> std::result::Result<RecordBatch, ArrowError> {
        let schema = self.schema();
        let columns = vec![
            Arc::new(
                TimestampMicrosecondArray::from(std::mem::take(&mut self.row_timestamp))
                    .with_timezone("+00:00"),
            ) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.row_sample_idx))) as ArrayRef,
            Arc::new(Int8Array::from(std::mem::take(&mut self.snr))) as ArrayRef,
            Arc::new(Int16Array::from(std::mem::take(&mut self.rssi))) as ArrayRef,
            Arc::new(BinaryArray::from_iter_values(std::mem::take(
                &mut self.payload,
            ))) as ArrayRef,
        ];

        RecordBatch::try_new(schema, columns)
    }
}

pub struct Tdf45LoraTxBuilder {
    row_timestamp: Vec<Option<i64>>,
    row_sample_idx: Vec<Option<u16>>,
    payload: Vec<Vec<u8>>,
}

impl Tdf45LoraTxBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            row_timestamp: Vec::with_capacity(capacity),
            row_sample_idx: Vec::with_capacity(capacity),
            payload: Vec::with_capacity(capacity),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        tdf_parquet_schema(45).unwrap()
    }

    pub fn rows(&self) -> usize {
        self.row_timestamp.len()
    }

    pub fn append(
        &mut self,
        meta: TdfParquetRowMeta,
        size: u8,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<()> {
        let cursor_start = cursor.position();

        self.row_timestamp.push(meta.time_unix_micros);
        self.row_sample_idx.push(meta.sample_idx);
        self.payload.push(crate::decoders::tdf_field_read_vla(
            cursor,
            cursor_start,
            size,
        )?);

        finish_tdf_read(cursor, cursor_start, size)
    }

    pub fn finish_batch(&mut self) -> std::result::Result<RecordBatch, ArrowError> {
        let schema = self.schema();
        let columns = vec![
            Arc::new(
                TimestampMicrosecondArray::from(std::mem::take(&mut self.row_timestamp))
                    .with_timezone("+00:00"),
            ) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.row_sample_idx))) as ArrayRef,
            Arc::new(BinaryArray::from_iter_values(std::mem::take(
                &mut self.payload,
            ))) as ArrayRef,
        ];

        RecordBatch::try_new(schema, columns)
    }
}

pub struct Tdf46IdxArrayFreqBuilder {
    row_timestamp: Vec<Option<i64>>,
    row_sample_idx: Vec<Option<u16>>,
    tdf_id: Vec<u16>,
    frequency: Vec<u32>,
}

impl Tdf46IdxArrayFreqBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            row_timestamp: Vec::with_capacity(capacity),
            row_sample_idx: Vec::with_capacity(capacity),
            tdf_id: Vec::with_capacity(capacity),
            frequency: Vec::with_capacity(capacity),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        tdf_parquet_schema(46).unwrap()
    }

    pub fn rows(&self) -> usize {
        self.row_timestamp.len()
    }

    pub fn append(
        &mut self,
        meta: TdfParquetRowMeta,
        size: u8,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<()> {
        let cursor_start = cursor.position();

        self.row_timestamp.push(meta.time_unix_micros);
        self.row_sample_idx.push(meta.sample_idx);
        self.tdf_id.push(cursor.read_u16::<LittleEndian>()?);
        self.frequency.push(cursor.read_u32::<LittleEndian>()?);

        finish_tdf_read(cursor, cursor_start, size)
    }

    pub fn finish_batch(&mut self) -> std::result::Result<RecordBatch, ArrowError> {
        let schema = self.schema();
        let columns = vec![
            Arc::new(
                TimestampMicrosecondArray::from(std::mem::take(&mut self.row_timestamp))
                    .with_timezone("+00:00"),
            ) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.row_sample_idx))) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.tdf_id))) as ArrayRef,
            Arc::new(UInt32Array::from(std::mem::take(&mut self.frequency))) as ArrayRef,
        ];

        RecordBatch::try_new(schema, columns)
    }
}

pub struct Tdf47IdxArrayPeriodBuilder {
    row_timestamp: Vec<Option<i64>>,
    row_sample_idx: Vec<Option<u16>>,
    tdf_id: Vec<u16>,
    period: Vec<u32>,
}

impl Tdf47IdxArrayPeriodBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            row_timestamp: Vec::with_capacity(capacity),
            row_sample_idx: Vec::with_capacity(capacity),
            tdf_id: Vec::with_capacity(capacity),
            period: Vec::with_capacity(capacity),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        tdf_parquet_schema(47).unwrap()
    }

    pub fn rows(&self) -> usize {
        self.row_timestamp.len()
    }

    pub fn append(
        &mut self,
        meta: TdfParquetRowMeta,
        size: u8,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<()> {
        let cursor_start = cursor.position();

        self.row_timestamp.push(meta.time_unix_micros);
        self.row_sample_idx.push(meta.sample_idx);
        self.tdf_id.push(cursor.read_u16::<LittleEndian>()?);
        self.period.push(cursor.read_u32::<LittleEndian>()?);

        finish_tdf_read(cursor, cursor_start, size)
    }

    pub fn finish_batch(&mut self) -> std::result::Result<RecordBatch, ArrowError> {
        let schema = self.schema();
        let columns = vec![
            Arc::new(
                TimestampMicrosecondArray::from(std::mem::take(&mut self.row_timestamp))
                    .with_timezone("+00:00"),
            ) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.row_sample_idx))) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.tdf_id))) as ArrayRef,
            Arc::new(UInt32Array::from(std::mem::take(&mut self.period))) as ArrayRef,
        ];

        RecordBatch::try_new(schema, columns)
    }
}

pub struct Tdf48WifiConnectedBuilder {
    row_timestamp: Vec<Option<i64>>,
    row_sample_idx: Vec<Option<u16>>,
    network_bssid: Vec<u64>,
    network_band: Vec<u8>,
    network_channel: Vec<u8>,
    network_iface_mode: Vec<u8>,
    network_link_mode: Vec<u8>,
    network_security: Vec<u8>,
    network_rssi: Vec<i8>,
    network_beacon_interval: Vec<u16>,
    network_twt_capable: Vec<u8>,
}

impl Tdf48WifiConnectedBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            row_timestamp: Vec::with_capacity(capacity),
            row_sample_idx: Vec::with_capacity(capacity),
            network_bssid: Vec::with_capacity(capacity),
            network_band: Vec::with_capacity(capacity),
            network_channel: Vec::with_capacity(capacity),
            network_iface_mode: Vec::with_capacity(capacity),
            network_link_mode: Vec::with_capacity(capacity),
            network_security: Vec::with_capacity(capacity),
            network_rssi: Vec::with_capacity(capacity),
            network_beacon_interval: Vec::with_capacity(capacity),
            network_twt_capable: Vec::with_capacity(capacity),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        tdf_parquet_schema(48).unwrap()
    }

    pub fn rows(&self) -> usize {
        self.row_timestamp.len()
    }

    pub fn append(
        &mut self,
        meta: TdfParquetRowMeta,
        size: u8,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<()> {
        let cursor_start = cursor.position();

        self.row_timestamp.push(meta.time_unix_micros);
        self.row_sample_idx.push(meta.sample_idx);
        self.network_bssid.push(cursor.read_u48::<BigEndian>()?);
        self.network_band.push(cursor.read_u8()?);
        self.network_channel.push(cursor.read_u8()?);
        self.network_iface_mode.push(cursor.read_u8()?);
        self.network_link_mode.push(cursor.read_u8()?);
        self.network_security.push(cursor.read_u8()?);
        self.network_rssi.push(cursor.read_i8()?);
        self.network_beacon_interval
            .push(cursor.read_u16::<LittleEndian>()?);
        self.network_twt_capable.push(cursor.read_u8()?);

        finish_tdf_read(cursor, cursor_start, size)
    }

    pub fn finish_batch(&mut self) -> std::result::Result<RecordBatch, ArrowError> {
        let schema = self.schema();
        let columns = vec![
            Arc::new(
                TimestampMicrosecondArray::from(std::mem::take(&mut self.row_timestamp))
                    .with_timezone("+00:00"),
            ) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.row_sample_idx))) as ArrayRef,
            Arc::new(StructArray::try_new(
                Fields::from(vec![
                    Field::new("bssid", DataType::UInt64, false),
                    Field::new("band", DataType::UInt8, false),
                    Field::new("channel", DataType::UInt8, false),
                    Field::new("iface_mode", DataType::UInt8, false),
                    Field::new("link_mode", DataType::UInt8, false),
                    Field::new("security", DataType::UInt8, false),
                    Field::new("rssi", DataType::Int8, false),
                    Field::new("beacon_interval", DataType::UInt16, false),
                    Field::new("twt_capable", DataType::UInt8, false),
                ]),
                vec![
                    Arc::new(UInt64Array::from(std::mem::take(&mut self.network_bssid)))
                        as ArrayRef,
                    Arc::new(UInt8Array::from(std::mem::take(&mut self.network_band))) as ArrayRef,
                    Arc::new(UInt8Array::from(std::mem::take(&mut self.network_channel)))
                        as ArrayRef,
                    Arc::new(UInt8Array::from(std::mem::take(
                        &mut self.network_iface_mode,
                    ))) as ArrayRef,
                    Arc::new(UInt8Array::from(std::mem::take(
                        &mut self.network_link_mode,
                    ))) as ArrayRef,
                    Arc::new(UInt8Array::from(std::mem::take(&mut self.network_security)))
                        as ArrayRef,
                    Arc::new(Int8Array::from(std::mem::take(&mut self.network_rssi))) as ArrayRef,
                    Arc::new(UInt16Array::from(std::mem::take(
                        &mut self.network_beacon_interval,
                    ))) as ArrayRef,
                    Arc::new(UInt8Array::from(std::mem::take(
                        &mut self.network_twt_capable,
                    ))) as ArrayRef,
                ],
                None,
            )?) as ArrayRef,
        ];

        RecordBatch::try_new(schema, columns)
    }
}

pub struct Tdf49WifiConnectionFailedBuilder {
    row_timestamp: Vec<Option<i64>>,
    row_sample_idx: Vec<Option<u16>>,
    reason: Vec<u8>,
}

impl Tdf49WifiConnectionFailedBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            row_timestamp: Vec::with_capacity(capacity),
            row_sample_idx: Vec::with_capacity(capacity),
            reason: Vec::with_capacity(capacity),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        tdf_parquet_schema(49).unwrap()
    }

    pub fn rows(&self) -> usize {
        self.row_timestamp.len()
    }

    pub fn append(
        &mut self,
        meta: TdfParquetRowMeta,
        size: u8,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<()> {
        let cursor_start = cursor.position();

        self.row_timestamp.push(meta.time_unix_micros);
        self.row_sample_idx.push(meta.sample_idx);
        self.reason.push(cursor.read_u8()?);

        finish_tdf_read(cursor, cursor_start, size)
    }

    pub fn finish_batch(&mut self) -> std::result::Result<RecordBatch, ArrowError> {
        let schema = self.schema();
        let columns = vec![
            Arc::new(
                TimestampMicrosecondArray::from(std::mem::take(&mut self.row_timestamp))
                    .with_timezone("+00:00"),
            ) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.row_sample_idx))) as ArrayRef,
            Arc::new(UInt8Array::from(std::mem::take(&mut self.reason))) as ArrayRef,
        ];

        RecordBatch::try_new(schema, columns)
    }
}

pub struct Tdf50WifiDisconnectedBuilder {
    row_timestamp: Vec<Option<i64>>,
    row_sample_idx: Vec<Option<u16>>,
    reason: Vec<u8>,
}

impl Tdf50WifiDisconnectedBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            row_timestamp: Vec::with_capacity(capacity),
            row_sample_idx: Vec::with_capacity(capacity),
            reason: Vec::with_capacity(capacity),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        tdf_parquet_schema(50).unwrap()
    }

    pub fn rows(&self) -> usize {
        self.row_timestamp.len()
    }

    pub fn append(
        &mut self,
        meta: TdfParquetRowMeta,
        size: u8,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<()> {
        let cursor_start = cursor.position();

        self.row_timestamp.push(meta.time_unix_micros);
        self.row_sample_idx.push(meta.sample_idx);
        self.reason.push(cursor.read_u8()?);

        finish_tdf_read(cursor, cursor_start, size)
    }

    pub fn finish_batch(&mut self) -> std::result::Result<RecordBatch, ArrowError> {
        let schema = self.schema();
        let columns = vec![
            Arc::new(
                TimestampMicrosecondArray::from(std::mem::take(&mut self.row_timestamp))
                    .with_timezone("+00:00"),
            ) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.row_sample_idx))) as ArrayRef,
            Arc::new(UInt8Array::from(std::mem::take(&mut self.reason))) as ArrayRef,
        ];

        RecordBatch::try_new(schema, columns)
    }
}

pub struct Tdf51NetworkScanCountBuilder {
    row_timestamp: Vec<Option<i64>>,
    row_sample_idx: Vec<Option<u16>>,
    num_wifi: Vec<u8>,
    num_lte: Vec<u8>,
}

impl Tdf51NetworkScanCountBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            row_timestamp: Vec::with_capacity(capacity),
            row_sample_idx: Vec::with_capacity(capacity),
            num_wifi: Vec::with_capacity(capacity),
            num_lte: Vec::with_capacity(capacity),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        tdf_parquet_schema(51).unwrap()
    }

    pub fn rows(&self) -> usize {
        self.row_timestamp.len()
    }

    pub fn append(
        &mut self,
        meta: TdfParquetRowMeta,
        size: u8,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<()> {
        let cursor_start = cursor.position();

        self.row_timestamp.push(meta.time_unix_micros);
        self.row_sample_idx.push(meta.sample_idx);
        self.num_wifi.push(cursor.read_u8()?);
        self.num_lte.push(cursor.read_u8()?);

        finish_tdf_read(cursor, cursor_start, size)
    }

    pub fn finish_batch(&mut self) -> std::result::Result<RecordBatch, ArrowError> {
        let schema = self.schema();
        let columns = vec![
            Arc::new(
                TimestampMicrosecondArray::from(std::mem::take(&mut self.row_timestamp))
                    .with_timezone("+00:00"),
            ) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.row_sample_idx))) as ArrayRef,
            Arc::new(UInt8Array::from(std::mem::take(&mut self.num_wifi))) as ArrayRef,
            Arc::new(UInt8Array::from(std::mem::take(&mut self.num_lte))) as ArrayRef,
        ];

        RecordBatch::try_new(schema, columns)
    }
}

pub struct Tdf52ExceptionStackFrameBuilder {
    row_timestamp: Vec<Option<i64>>,
    row_sample_idx: Vec<Option<u16>>,
    frame_offsets: Vec<i32>,
    frame: Vec<u32>,
}

impl Tdf52ExceptionStackFrameBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            row_timestamp: Vec::with_capacity(capacity),
            row_sample_idx: Vec::with_capacity(capacity),
            frame_offsets: vec![0],
            frame: Vec::with_capacity(capacity),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        tdf_parquet_schema(52).unwrap()
    }

    pub fn rows(&self) -> usize {
        self.row_timestamp.len()
    }

    fn list_value_field(&self, field_index: usize) -> Arc<Field> {
        let schema = self.schema();
        match schema.field(field_index).data_type() {
            DataType::List(field) => field.clone(),
            _ => unreachable!("generated list field index is not a list"),
        }
    }

    pub fn append(
        &mut self,
        meta: TdfParquetRowMeta,
        size: u8,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<()> {
        let cursor_start = cursor.position();

        self.row_timestamp.push(meta.time_unix_micros);
        self.row_sample_idx.push(meta.sample_idx);
        {
            let bytes_remaining = crate::decoders::vla_bytes_remaining(cursor, cursor_start, size)?;
            if bytes_remaining % 4 != 0 {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "Variable-length array does not align to element size",
                ));
            }
            let item_count = bytes_remaining / 4;
            for _ in 0..item_count {
                self.frame.push(cursor.read_u32::<LittleEndian>()?);
            }
            self.frame_offsets
                .push(*self.frame_offsets.last().unwrap() + item_count as i32);
        }

        finish_tdf_read(cursor, cursor_start, size)
    }

    pub fn finish_batch(&mut self) -> std::result::Result<RecordBatch, ArrowError> {
        let schema = self.schema();
        let columns = vec![
            Arc::new(
                TimestampMicrosecondArray::from(std::mem::take(&mut self.row_timestamp))
                    .with_timezone("+00:00"),
            ) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.row_sample_idx))) as ArrayRef,
            {
                let offsets = std::mem::replace(&mut self.frame_offsets, vec![0]);
                Arc::new(ListArray::try_new(
                    self.list_value_field(2),
                    OffsetBuffer::new(ScalarBuffer::from(offsets)),
                    Arc::new(UInt32Array::from(std::mem::take(&mut self.frame))) as ArrayRef,
                    None,
                )?) as ArrayRef
            },
        ];

        RecordBatch::try_new(schema, columns)
    }
}

pub struct Tdf53BatteryVoltageBuilder {
    row_timestamp: Vec<Option<i64>>,
    row_sample_idx: Vec<Option<u16>>,
    voltage: Vec<u16>,
}

impl Tdf53BatteryVoltageBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            row_timestamp: Vec::with_capacity(capacity),
            row_sample_idx: Vec::with_capacity(capacity),
            voltage: Vec::with_capacity(capacity),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        tdf_parquet_schema(53).unwrap()
    }

    pub fn rows(&self) -> usize {
        self.row_timestamp.len()
    }

    pub fn append(
        &mut self,
        meta: TdfParquetRowMeta,
        size: u8,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<()> {
        let cursor_start = cursor.position();

        self.row_timestamp.push(meta.time_unix_micros);
        self.row_sample_idx.push(meta.sample_idx);
        self.voltage.push(cursor.read_u16::<LittleEndian>()?);

        finish_tdf_read(cursor, cursor_start, size)
    }

    pub fn finish_batch(&mut self) -> std::result::Result<RecordBatch, ArrowError> {
        let schema = self.schema();
        let columns = vec![
            Arc::new(
                TimestampMicrosecondArray::from(std::mem::take(&mut self.row_timestamp))
                    .with_timezone("+00:00"),
            ) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.row_sample_idx))) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.voltage))) as ArrayRef,
        ];

        RecordBatch::try_new(schema, columns)
    }
}

pub struct Tdf54BatterySocBuilder {
    row_timestamp: Vec<Option<i64>>,
    row_sample_idx: Vec<Option<u16>>,
    soc: Vec<u8>,
}

impl Tdf54BatterySocBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            row_timestamp: Vec::with_capacity(capacity),
            row_sample_idx: Vec::with_capacity(capacity),
            soc: Vec::with_capacity(capacity),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        tdf_parquet_schema(54).unwrap()
    }

    pub fn rows(&self) -> usize {
        self.row_timestamp.len()
    }

    pub fn append(
        &mut self,
        meta: TdfParquetRowMeta,
        size: u8,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<()> {
        let cursor_start = cursor.position();

        self.row_timestamp.push(meta.time_unix_micros);
        self.row_sample_idx.push(meta.sample_idx);
        self.soc.push(cursor.read_u8()?);

        finish_tdf_read(cursor, cursor_start, size)
    }

    pub fn finish_batch(&mut self) -> std::result::Result<RecordBatch, ArrowError> {
        let schema = self.schema();
        let columns = vec![
            Arc::new(
                TimestampMicrosecondArray::from(std::mem::take(&mut self.row_timestamp))
                    .with_timezone("+00:00"),
            ) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.row_sample_idx))) as ArrayRef,
            Arc::new(UInt8Array::from(std::mem::take(&mut self.soc))) as ArrayRef,
        ];

        RecordBatch::try_new(schema, columns)
    }
}

pub struct Tdf55StateEventSetBuilder {
    row_timestamp: Vec<Option<i64>>,
    row_sample_idx: Vec<Option<u16>>,
    state: Vec<u8>,
}

impl Tdf55StateEventSetBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            row_timestamp: Vec::with_capacity(capacity),
            row_sample_idx: Vec::with_capacity(capacity),
            state: Vec::with_capacity(capacity),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        tdf_parquet_schema(55).unwrap()
    }

    pub fn rows(&self) -> usize {
        self.row_timestamp.len()
    }

    pub fn append(
        &mut self,
        meta: TdfParquetRowMeta,
        size: u8,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<()> {
        let cursor_start = cursor.position();

        self.row_timestamp.push(meta.time_unix_micros);
        self.row_sample_idx.push(meta.sample_idx);
        self.state.push(cursor.read_u8()?);

        finish_tdf_read(cursor, cursor_start, size)
    }

    pub fn finish_batch(&mut self) -> std::result::Result<RecordBatch, ArrowError> {
        let schema = self.schema();
        let columns = vec![
            Arc::new(
                TimestampMicrosecondArray::from(std::mem::take(&mut self.row_timestamp))
                    .with_timezone("+00:00"),
            ) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.row_sample_idx))) as ArrayRef,
            Arc::new(UInt8Array::from(std::mem::take(&mut self.state))) as ArrayRef,
        ];

        RecordBatch::try_new(schema, columns)
    }
}

pub struct Tdf56StateEventClearedBuilder {
    row_timestamp: Vec<Option<i64>>,
    row_sample_idx: Vec<Option<u16>>,
    state: Vec<u8>,
}

impl Tdf56StateEventClearedBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            row_timestamp: Vec::with_capacity(capacity),
            row_sample_idx: Vec::with_capacity(capacity),
            state: Vec::with_capacity(capacity),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        tdf_parquet_schema(56).unwrap()
    }

    pub fn rows(&self) -> usize {
        self.row_timestamp.len()
    }

    pub fn append(
        &mut self,
        meta: TdfParquetRowMeta,
        size: u8,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<()> {
        let cursor_start = cursor.position();

        self.row_timestamp.push(meta.time_unix_micros);
        self.row_sample_idx.push(meta.sample_idx);
        self.state.push(cursor.read_u8()?);

        finish_tdf_read(cursor, cursor_start, size)
    }

    pub fn finish_batch(&mut self) -> std::result::Result<RecordBatch, ArrowError> {
        let schema = self.schema();
        let columns = vec![
            Arc::new(
                TimestampMicrosecondArray::from(std::mem::take(&mut self.row_timestamp))
                    .with_timezone("+00:00"),
            ) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.row_sample_idx))) as ArrayRef,
            Arc::new(UInt8Array::from(std::mem::take(&mut self.state))) as ArrayRef,
        ];

        RecordBatch::try_new(schema, columns)
    }
}

pub struct Tdf57StateDurationBuilder {
    row_timestamp: Vec<Option<i64>>,
    row_sample_idx: Vec<Option<u16>>,
    state: Vec<u8>,
    duration: Vec<u32>,
}

impl Tdf57StateDurationBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            row_timestamp: Vec::with_capacity(capacity),
            row_sample_idx: Vec::with_capacity(capacity),
            state: Vec::with_capacity(capacity),
            duration: Vec::with_capacity(capacity),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        tdf_parquet_schema(57).unwrap()
    }

    pub fn rows(&self) -> usize {
        self.row_timestamp.len()
    }

    pub fn append(
        &mut self,
        meta: TdfParquetRowMeta,
        size: u8,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<()> {
        let cursor_start = cursor.position();

        self.row_timestamp.push(meta.time_unix_micros);
        self.row_sample_idx.push(meta.sample_idx);
        self.state.push(cursor.read_u8()?);
        self.duration.push(cursor.read_u32::<LittleEndian>()?);

        finish_tdf_read(cursor, cursor_start, size)
    }

    pub fn finish_batch(&mut self) -> std::result::Result<RecordBatch, ArrowError> {
        let schema = self.schema();
        let columns = vec![
            Arc::new(
                TimestampMicrosecondArray::from(std::mem::take(&mut self.row_timestamp))
                    .with_timezone("+00:00"),
            ) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.row_sample_idx))) as ArrayRef,
            Arc::new(UInt8Array::from(std::mem::take(&mut self.state))) as ArrayRef,
            Arc::new(UInt32Array::from(std::mem::take(&mut self.duration))) as ArrayRef,
        ];

        RecordBatch::try_new(schema, columns)
    }
}

pub struct Tdf58Pcm16bitChanLeftBuilder {
    row_timestamp: Vec<Option<i64>>,
    row_sample_idx: Vec<Option<u16>>,
    val: Vec<i16>,
}

impl Tdf58Pcm16bitChanLeftBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            row_timestamp: Vec::with_capacity(capacity),
            row_sample_idx: Vec::with_capacity(capacity),
            val: Vec::with_capacity(capacity),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        tdf_parquet_schema(58).unwrap()
    }

    pub fn rows(&self) -> usize {
        self.row_timestamp.len()
    }

    pub fn append(
        &mut self,
        meta: TdfParquetRowMeta,
        size: u8,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<()> {
        let cursor_start = cursor.position();

        self.row_timestamp.push(meta.time_unix_micros);
        self.row_sample_idx.push(meta.sample_idx);
        self.val.push(cursor.read_i16::<LittleEndian>()?);

        finish_tdf_read(cursor, cursor_start, size)
    }

    pub fn finish_batch(&mut self) -> std::result::Result<RecordBatch, ArrowError> {
        let schema = self.schema();
        let columns = vec![
            Arc::new(
                TimestampMicrosecondArray::from(std::mem::take(&mut self.row_timestamp))
                    .with_timezone("+00:00"),
            ) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.row_sample_idx))) as ArrayRef,
            Arc::new(Int16Array::from(std::mem::take(&mut self.val))) as ArrayRef,
        ];

        RecordBatch::try_new(schema, columns)
    }
}

pub struct Tdf59Pcm16bitChanRightBuilder {
    row_timestamp: Vec<Option<i64>>,
    row_sample_idx: Vec<Option<u16>>,
    val: Vec<i16>,
}

impl Tdf59Pcm16bitChanRightBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            row_timestamp: Vec::with_capacity(capacity),
            row_sample_idx: Vec::with_capacity(capacity),
            val: Vec::with_capacity(capacity),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        tdf_parquet_schema(59).unwrap()
    }

    pub fn rows(&self) -> usize {
        self.row_timestamp.len()
    }

    pub fn append(
        &mut self,
        meta: TdfParquetRowMeta,
        size: u8,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<()> {
        let cursor_start = cursor.position();

        self.row_timestamp.push(meta.time_unix_micros);
        self.row_sample_idx.push(meta.sample_idx);
        self.val.push(cursor.read_i16::<LittleEndian>()?);

        finish_tdf_read(cursor, cursor_start, size)
    }

    pub fn finish_batch(&mut self) -> std::result::Result<RecordBatch, ArrowError> {
        let schema = self.schema();
        let columns = vec![
            Arc::new(
                TimestampMicrosecondArray::from(std::mem::take(&mut self.row_timestamp))
                    .with_timezone("+00:00"),
            ) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.row_sample_idx))) as ArrayRef,
            Arc::new(Int16Array::from(std::mem::take(&mut self.val))) as ArrayRef,
        ];

        RecordBatch::try_new(schema, columns)
    }
}

pub struct Tdf60Pcm16bitChanDualBuilder {
    row_timestamp: Vec<Option<i64>>,
    row_sample_idx: Vec<Option<u16>>,
    left: Vec<i16>,
    right: Vec<i16>,
}

impl Tdf60Pcm16bitChanDualBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            row_timestamp: Vec::with_capacity(capacity),
            row_sample_idx: Vec::with_capacity(capacity),
            left: Vec::with_capacity(capacity),
            right: Vec::with_capacity(capacity),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        tdf_parquet_schema(60).unwrap()
    }

    pub fn rows(&self) -> usize {
        self.row_timestamp.len()
    }

    pub fn append(
        &mut self,
        meta: TdfParquetRowMeta,
        size: u8,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<()> {
        let cursor_start = cursor.position();

        self.row_timestamp.push(meta.time_unix_micros);
        self.row_sample_idx.push(meta.sample_idx);
        self.left.push(cursor.read_i16::<LittleEndian>()?);
        self.right.push(cursor.read_i16::<LittleEndian>()?);

        finish_tdf_read(cursor, cursor_start, size)
    }

    pub fn finish_batch(&mut self) -> std::result::Result<RecordBatch, ArrowError> {
        let schema = self.schema();
        let columns = vec![
            Arc::new(
                TimestampMicrosecondArray::from(std::mem::take(&mut self.row_timestamp))
                    .with_timezone("+00:00"),
            ) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.row_sample_idx))) as ArrayRef,
            Arc::new(Int16Array::from(std::mem::take(&mut self.left))) as ArrayRef,
            Arc::new(Int16Array::from(std::mem::take(&mut self.right))) as ArrayRef,
        ];

        RecordBatch::try_new(schema, columns)
    }
}

pub struct Tdf61KvsValueChangedBuilder {
    row_timestamp: Vec<Option<i64>>,
    row_sample_idx: Vec<Option<u16>>,
    key: Vec<u16>,
    value: Vec<Vec<u8>>,
}

impl Tdf61KvsValueChangedBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            row_timestamp: Vec::with_capacity(capacity),
            row_sample_idx: Vec::with_capacity(capacity),
            key: Vec::with_capacity(capacity),
            value: Vec::with_capacity(capacity),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        tdf_parquet_schema(61).unwrap()
    }

    pub fn rows(&self) -> usize {
        self.row_timestamp.len()
    }

    pub fn append(
        &mut self,
        meta: TdfParquetRowMeta,
        size: u8,
        cursor: &mut Cursor<&[u8]>,
    ) -> Result<()> {
        let cursor_start = cursor.position();

        self.row_timestamp.push(meta.time_unix_micros);
        self.row_sample_idx.push(meta.sample_idx);
        self.key.push(cursor.read_u16::<LittleEndian>()?);
        self.value.push(crate::decoders::tdf_field_read_vla(
            cursor,
            cursor_start,
            size,
        )?);

        finish_tdf_read(cursor, cursor_start, size)
    }

    pub fn finish_batch(&mut self) -> std::result::Result<RecordBatch, ArrowError> {
        let schema = self.schema();
        let columns = vec![
            Arc::new(
                TimestampMicrosecondArray::from(std::mem::take(&mut self.row_timestamp))
                    .with_timezone("+00:00"),
            ) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.row_sample_idx))) as ArrayRef,
            Arc::new(UInt16Array::from(std::mem::take(&mut self.key))) as ArrayRef,
            Arc::new(BinaryArray::from_iter_values(std::mem::take(
                &mut self.value,
            ))) as ArrayRef,
        ];

        RecordBatch::try_new(schema, columns)
    }
}
