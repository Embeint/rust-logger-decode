use std::sync::Arc;

use arrow_schema::{DataType, Field, Fields, Schema, TimeUnit};

fn timestamp_field() -> Field {
    Field::new(
        "timestamp",
        DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
        false,
    )
}

pub fn tdf_parquet_schemas() -> Vec<(u16, &'static str, Schema)> {
    vec![
        (1, "ANNOUNCE", tdf_parquet_schema(&1).unwrap()),
        (2, "BATTERY_STATE", tdf_parquet_schema(&2).unwrap()),
        (3, "AMBIENT_TEMP_PRES_HUM", tdf_parquet_schema(&3).unwrap()),
        (4, "AMBIENT_TEMPERATURE", tdf_parquet_schema(&4).unwrap()),
        (5, "TIME_SYNC", tdf_parquet_schema(&5).unwrap()),
        (6, "REBOOT_INFO", tdf_parquet_schema(&6).unwrap()),
        (7, "ANNOUNCE_V2", tdf_parquet_schema(&7).unwrap()),
        (8, "SOC_TEMPERATURE", tdf_parquet_schema(&8).unwrap()),
        (10, "ACC_2G", tdf_parquet_schema(&10).unwrap()),
        (11, "ACC_4G", tdf_parquet_schema(&11).unwrap()),
        (12, "ACC_8G", tdf_parquet_schema(&12).unwrap()),
        (13, "ACC_16G", tdf_parquet_schema(&13).unwrap()),
        (14, "GYR_125DPS", tdf_parquet_schema(&14).unwrap()),
        (15, "GYR_250DPS", tdf_parquet_schema(&15).unwrap()),
        (16, "GYR_500DPS", tdf_parquet_schema(&16).unwrap()),
        (17, "GYR_1000DPS", tdf_parquet_schema(&17).unwrap()),
        (18, "GYR_2000DPS", tdf_parquet_schema(&18).unwrap()),
        (19, "GCS_WGS84_LLHA", tdf_parquet_schema(&19).unwrap()),
        (20, "UBX_NAV_PVT", tdf_parquet_schema(&20).unwrap()),
        (21, "LTE_CONN_STATUS", tdf_parquet_schema(&21).unwrap()),
        (22, "GLOBALSTAR_PKT", tdf_parquet_schema(&22).unwrap()),
        (
            23,
            "ACC_MAGNITUDE_STD_DEV",
            tdf_parquet_schema(&23).unwrap(),
        ),
        (24, "ACTIVITY_METRIC", tdf_parquet_schema(&24).unwrap()),
        (25, "ALGORITHM_OUTPUT", tdf_parquet_schema(&25).unwrap()),
        (26, "RUNTIME_ERROR", tdf_parquet_schema(&26).unwrap()),
        (27, "CHARGER_EN_CONTROL", tdf_parquet_schema(&27).unwrap()),
        (28, "GNSS_FIX_INFO", tdf_parquet_schema(&28).unwrap()),
        (29, "BLUETOOTH_CONNECTION", tdf_parquet_schema(&29).unwrap()),
        (30, "BLUETOOTH_RSSI", tdf_parquet_schema(&30).unwrap()),
        (
            31,
            "BLUETOOTH_DATA_THROUGHPUT",
            tdf_parquet_schema(&31).unwrap(),
        ),
        (
            32,
            "ALGORITHM_CLASS_HISTOGRAM",
            tdf_parquet_schema(&32).unwrap(),
        ),
        (
            33,
            "ALGORITHM_CLASS_TIME_SERIES",
            tdf_parquet_schema(&33).unwrap(),
        ),
        (34, "LTE_TAC_CELLS", tdf_parquet_schema(&34).unwrap()),
        (35, "WIFI_AP_INFO", tdf_parquet_schema(&35).unwrap()),
        (36, "DEVICE_TILT", tdf_parquet_schema(&36).unwrap()),
        (37, "NRF9X_GNSS_PVT", tdf_parquet_schema(&37).unwrap()),
        (
            38,
            "BATTERY_CHARGE_ACCUMULATED",
            tdf_parquet_schema(&38).unwrap(),
        ),
        (
            39,
            "INFUSE_BLUETOOTH_RSSI",
            tdf_parquet_schema(&39).unwrap(),
        ),
        (40, "ADC_RAW_8", tdf_parquet_schema(&40).unwrap()),
        (41, "ADC_RAW_16", tdf_parquet_schema(&41).unwrap()),
        (42, "ADC_RAW_32", tdf_parquet_schema(&42).unwrap()),
        (43, "ANNOTATION", tdf_parquet_schema(&43).unwrap()),
        (44, "LORA_RX", tdf_parquet_schema(&44).unwrap()),
        (45, "LORA_TX", tdf_parquet_schema(&45).unwrap()),
        (46, "IDX_ARRAY_FREQ", tdf_parquet_schema(&46).unwrap()),
        (47, "IDX_ARRAY_PERIOD", tdf_parquet_schema(&47).unwrap()),
        (48, "WIFI_CONNECTED", tdf_parquet_schema(&48).unwrap()),
        (
            49,
            "WIFI_CONNECTION_FAILED",
            tdf_parquet_schema(&49).unwrap(),
        ),
        (50, "WIFI_DISCONNECTED", tdf_parquet_schema(&50).unwrap()),
        (51, "NETWORK_SCAN_COUNT", tdf_parquet_schema(&51).unwrap()),
        (
            52,
            "EXCEPTION_STACK_FRAME",
            tdf_parquet_schema(&52).unwrap(),
        ),
        (53, "BATTERY_VOLTAGE", tdf_parquet_schema(&53).unwrap()),
        (54, "BATTERY_SOC", tdf_parquet_schema(&54).unwrap()),
        (55, "STATE_EVENT_SET", tdf_parquet_schema(&55).unwrap()),
        (56, "STATE_EVENT_CLEARED", tdf_parquet_schema(&56).unwrap()),
        (57, "STATE_DURATION", tdf_parquet_schema(&57).unwrap()),
        (58, "PCM_16BIT_CHAN_LEFT", tdf_parquet_schema(&58).unwrap()),
        (59, "PCM_16BIT_CHAN_RIGHT", tdf_parquet_schema(&59).unwrap()),
        (60, "PCM_16BIT_CHAN_DUAL", tdf_parquet_schema(&60).unwrap()),
        (61, "KVS_VALUE_CHANGED", tdf_parquet_schema(&61).unwrap()),
    ]
}

pub fn tdf_parquet_schema(tdf_id: &u16) -> Option<Schema> {
    match tdf_id {
        1 => Some(Schema::new(vec![
            timestamp_field(),
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
        ])),
        2 => Some(Schema::new(vec![
            timestamp_field(),
            Field::new("voltage_mv", DataType::UInt32, false),
            Field::new("current_ua", DataType::Int32, false),
            Field::new("soc", DataType::UInt8, false),
        ])),
        3 => Some(Schema::new(vec![
            timestamp_field(),
            Field::new("temperature", DataType::Float64, false),
            Field::new("pressure", DataType::Float64, false),
            Field::new("humidity", DataType::Float64, false),
        ])),
        4 => Some(Schema::new(vec![
            timestamp_field(),
            Field::new("temperature", DataType::Float64, false),
        ])),
        5 => Some(Schema::new(vec![
            timestamp_field(),
            Field::new("source", DataType::UInt8, false),
            Field::new("shift", DataType::Float64, false),
        ])),
        6 => Some(Schema::new(vec![
            timestamp_field(),
            Field::new("reason", DataType::UInt8, false),
            Field::new("hardware_flags", DataType::UInt32, false),
            Field::new("count", DataType::UInt32, false),
            Field::new("uptime", DataType::UInt32, false),
            Field::new("param_1", DataType::UInt32, false),
            Field::new("param_2", DataType::UInt32, false),
            Field::new("thread", DataType::Utf8, false),
        ])),
        7 => Some(Schema::new(vec![
            timestamp_field(),
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
        ])),
        8 => Some(Schema::new(vec![
            timestamp_field(),
            Field::new("temperature", DataType::Float64, false),
        ])),
        10 => Some(Schema::new(vec![
            timestamp_field(),
            Field::new(
                "sample",
                DataType::Struct(Fields::from(vec![
                    Field::new("x", DataType::Int16, false),
                    Field::new("y", DataType::Int16, false),
                    Field::new("z", DataType::Int16, false),
                ])),
                false,
            ),
        ])),
        11 => Some(Schema::new(vec![
            timestamp_field(),
            Field::new(
                "sample",
                DataType::Struct(Fields::from(vec![
                    Field::new("x", DataType::Int16, false),
                    Field::new("y", DataType::Int16, false),
                    Field::new("z", DataType::Int16, false),
                ])),
                false,
            ),
        ])),
        12 => Some(Schema::new(vec![
            timestamp_field(),
            Field::new(
                "sample",
                DataType::Struct(Fields::from(vec![
                    Field::new("x", DataType::Int16, false),
                    Field::new("y", DataType::Int16, false),
                    Field::new("z", DataType::Int16, false),
                ])),
                false,
            ),
        ])),
        13 => Some(Schema::new(vec![
            timestamp_field(),
            Field::new(
                "sample",
                DataType::Struct(Fields::from(vec![
                    Field::new("x", DataType::Int16, false),
                    Field::new("y", DataType::Int16, false),
                    Field::new("z", DataType::Int16, false),
                ])),
                false,
            ),
        ])),
        14 => Some(Schema::new(vec![
            timestamp_field(),
            Field::new(
                "sample",
                DataType::Struct(Fields::from(vec![
                    Field::new("x", DataType::Int16, false),
                    Field::new("y", DataType::Int16, false),
                    Field::new("z", DataType::Int16, false),
                ])),
                false,
            ),
        ])),
        15 => Some(Schema::new(vec![
            timestamp_field(),
            Field::new(
                "sample",
                DataType::Struct(Fields::from(vec![
                    Field::new("x", DataType::Int16, false),
                    Field::new("y", DataType::Int16, false),
                    Field::new("z", DataType::Int16, false),
                ])),
                false,
            ),
        ])),
        16 => Some(Schema::new(vec![
            timestamp_field(),
            Field::new(
                "sample",
                DataType::Struct(Fields::from(vec![
                    Field::new("x", DataType::Int16, false),
                    Field::new("y", DataType::Int16, false),
                    Field::new("z", DataType::Int16, false),
                ])),
                false,
            ),
        ])),
        17 => Some(Schema::new(vec![
            timestamp_field(),
            Field::new(
                "sample",
                DataType::Struct(Fields::from(vec![
                    Field::new("x", DataType::Int16, false),
                    Field::new("y", DataType::Int16, false),
                    Field::new("z", DataType::Int16, false),
                ])),
                false,
            ),
        ])),
        18 => Some(Schema::new(vec![
            timestamp_field(),
            Field::new(
                "sample",
                DataType::Struct(Fields::from(vec![
                    Field::new("x", DataType::Int16, false),
                    Field::new("y", DataType::Int16, false),
                    Field::new("z", DataType::Int16, false),
                ])),
                false,
            ),
        ])),
        19 => Some(Schema::new(vec![
            timestamp_field(),
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
        ])),
        20 => Some(Schema::new(vec![
            timestamp_field(),
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
        ])),
        21 => Some(Schema::new(vec![
            timestamp_field(),
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
        ])),
        22 => Some(Schema::new(vec![
            timestamp_field(),
            Field::new(
                "payload",
                DataType::FixedSizeList(Arc::new(Field::new_list_field(DataType::UInt8, false)), 9),
                false,
            ),
        ])),
        23 => Some(Schema::new(vec![
            timestamp_field(),
            Field::new("count", DataType::UInt32, false),
            Field::new("std_dev", DataType::UInt32, false),
        ])),
        24 => Some(Schema::new(vec![
            timestamp_field(),
            Field::new("value", DataType::UInt32, false),
        ])),
        25 => Some(Schema::new(vec![
            timestamp_field(),
            Field::new("algorithm_id", DataType::UInt32, false),
            Field::new("algorithm_version", DataType::UInt16, false),
            Field::new("output", DataType::Binary, false),
        ])),
        26 => Some(Schema::new(vec![
            timestamp_field(),
            Field::new("error_id", DataType::UInt32, false),
            Field::new("error_ctx", DataType::UInt32, false),
        ])),
        27 => Some(Schema::new(vec![
            timestamp_field(),
            Field::new("enabled", DataType::UInt8, false),
        ])),
        28 => Some(Schema::new(vec![
            timestamp_field(),
            Field::new("time_fix", DataType::UInt16, false),
            Field::new("location_fix", DataType::UInt16, false),
            Field::new("num_sv", DataType::UInt8, false),
        ])),
        29 => Some(Schema::new(vec![
            timestamp_field(),
            Field::new(
                "address",
                DataType::Struct(Fields::from(vec![
                    Field::new("type", DataType::UInt8, false),
                    Field::new("val", DataType::UInt64, false),
                ])),
                false,
            ),
            Field::new("connected", DataType::UInt8, false),
        ])),
        30 => Some(Schema::new(vec![
            timestamp_field(),
            Field::new(
                "address",
                DataType::Struct(Fields::from(vec![
                    Field::new("type", DataType::UInt8, false),
                    Field::new("val", DataType::UInt64, false),
                ])),
                false,
            ),
            Field::new("rssi", DataType::Int8, false),
        ])),
        31 => Some(Schema::new(vec![
            timestamp_field(),
            Field::new(
                "address",
                DataType::Struct(Fields::from(vec![
                    Field::new("type", DataType::UInt8, false),
                    Field::new("val", DataType::UInt64, false),
                ])),
                false,
            ),
            Field::new("throughput", DataType::Int32, false),
        ])),
        32 => Some(Schema::new(vec![
            timestamp_field(),
            Field::new("algorithm_id", DataType::UInt32, false),
            Field::new("algorithm_version", DataType::UInt16, false),
            Field::new("classes", DataType::Binary, false),
        ])),
        33 => Some(Schema::new(vec![
            timestamp_field(),
            Field::new("algorithm_id", DataType::UInt32, false),
            Field::new("algorithm_version", DataType::UInt16, false),
            Field::new("values", DataType::Binary, false),
        ])),
        34 => Some(Schema::new(vec![
            timestamp_field(),
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
        ])),
        35 => Some(Schema::new(vec![
            timestamp_field(),
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
        ])),
        36 => Some(Schema::new(vec![
            timestamp_field(),
            Field::new("cosine", DataType::Float32, false),
        ])),
        37 => Some(Schema::new(vec![
            timestamp_field(),
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
        ])),
        38 => Some(Schema::new(vec![
            timestamp_field(),
            Field::new("charge", DataType::Int32, false),
        ])),
        39 => Some(Schema::new(vec![
            timestamp_field(),
            Field::new("infuse_id", DataType::UInt64, false),
            Field::new("rssi", DataType::Int8, false),
        ])),
        40 => Some(Schema::new(vec![
            timestamp_field(),
            Field::new("val", DataType::Int8, false),
        ])),
        41 => Some(Schema::new(vec![
            timestamp_field(),
            Field::new("val", DataType::Int16, false),
        ])),
        42 => Some(Schema::new(vec![
            timestamp_field(),
            Field::new("val", DataType::Int32, false),
        ])),
        43 => Some(Schema::new(vec![
            timestamp_field(),
            Field::new("timestamp", DataType::UInt32, false),
            Field::new("event", DataType::Utf8, false),
        ])),
        44 => Some(Schema::new(vec![
            timestamp_field(),
            Field::new("snr", DataType::Int8, false),
            Field::new("rssi", DataType::Int16, false),
            Field::new("payload", DataType::Binary, false),
        ])),
        45 => Some(Schema::new(vec![
            timestamp_field(),
            Field::new("payload", DataType::Binary, false),
        ])),
        46 => Some(Schema::new(vec![
            timestamp_field(),
            Field::new("tdf_id", DataType::UInt16, false),
            Field::new("frequency", DataType::UInt32, false),
        ])),
        47 => Some(Schema::new(vec![
            timestamp_field(),
            Field::new("tdf_id", DataType::UInt16, false),
            Field::new("period", DataType::UInt32, false),
        ])),
        48 => Some(Schema::new(vec![
            timestamp_field(),
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
        ])),
        49 => Some(Schema::new(vec![
            timestamp_field(),
            Field::new("reason", DataType::UInt8, false),
        ])),
        50 => Some(Schema::new(vec![
            timestamp_field(),
            Field::new("reason", DataType::UInt8, false),
        ])),
        51 => Some(Schema::new(vec![
            timestamp_field(),
            Field::new("num_wifi", DataType::UInt8, false),
            Field::new("num_lte", DataType::UInt8, false),
        ])),
        52 => Some(Schema::new(vec![
            timestamp_field(),
            Field::new(
                "frame",
                DataType::List(Arc::new(Field::new_list_field(DataType::UInt32, false))),
                false,
            ),
        ])),
        53 => Some(Schema::new(vec![
            timestamp_field(),
            Field::new("voltage", DataType::UInt16, false),
        ])),
        54 => Some(Schema::new(vec![
            timestamp_field(),
            Field::new("soc", DataType::UInt8, false),
        ])),
        55 => Some(Schema::new(vec![
            timestamp_field(),
            Field::new("state", DataType::UInt8, false),
        ])),
        56 => Some(Schema::new(vec![
            timestamp_field(),
            Field::new("state", DataType::UInt8, false),
        ])),
        57 => Some(Schema::new(vec![
            timestamp_field(),
            Field::new("state", DataType::UInt8, false),
            Field::new("duration", DataType::UInt32, false),
        ])),
        58 => Some(Schema::new(vec![
            timestamp_field(),
            Field::new("val", DataType::Int16, false),
        ])),
        59 => Some(Schema::new(vec![
            timestamp_field(),
            Field::new("val", DataType::Int16, false),
        ])),
        60 => Some(Schema::new(vec![
            timestamp_field(),
            Field::new("left", DataType::Int16, false),
            Field::new("right", DataType::Int16, false),
        ])),
        61 => Some(Schema::new(vec![
            timestamp_field(),
            Field::new("key", DataType::UInt16, false),
            Field::new("value", DataType::Binary, false),
        ])),
        _ => None,
    }
}
