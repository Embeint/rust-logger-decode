#!/usr/bin/env python3

import argparse
import decimal
import json
import os
import pathlib
import re
from numpy import format_float_positional as float_format

from jinja2 import Environment, FileSystemLoader, select_autoescape

# C type: (rust_type, ::<LittleEndian>?)
rust_type = {
    "char": ("u8", False),
    "int8_t": ("i8", False),
    "uint8_t": ("u8", False),
    "int16_t": ("i16", True),
    "uint16_t": ("u16", True),
    "int32_t": ("i32", True),
    "uint32_t": ("u32", True),
    "int64_t": ("i64", True),
    "uint64_t": ("u64", True),
    "float": ("f32", True),
    "float32_t": ("f32", True),
    "float64_t": ("f64", True),
}

arrow_int_type = {
    "int8_t": "DataType::Int8",
    "uint8_t": "DataType::UInt8",
    "int16_t": "DataType::Int16",
    "uint16_t": "DataType::UInt16",
    "int32_t": "DataType::Int32",
    "uint32_t": "DataType::UInt32",
    "int64_t": "DataType::Int64",
    "uint64_t": "DataType::UInt64",
}

arrow_float_type = {
    "float": "DataType::Float32",
    "float32_t": "DataType::Float32",
    "float64_t": "DataType::Float64",
}


def rust_str(value):
    return json.dumps(value)


def arrow_name(name):
    name = re.sub(r"\W", "_", name)
    if not name or name[0].isdigit():
        name = f"_{name}"
    return name


def indent_block(value, indent):
    pad = " " * indent
    return "\n".join(f"{pad}{line}" for line in value.splitlines())


def arrow_scalar_type(field):
    c_type = field["type"]
    conv = field.get("conversion", {})

    if c_type == "char":
        return "DataType::Utf8"

    if "m" in conv or "c" in conv:
        return "DataType::Float64"

    if "int" in conv:
        assert "num" in field
        byte_len = field["num"]
        if byte_len <= 1:
            return "DataType::UInt8"
        if byte_len <= 2:
            return "DataType::UInt16"
        if byte_len <= 4:
            return "DataType::UInt32"
        if byte_len <= 8:
            return "DataType::UInt64"
        return f"DataType::FixedSizeBinary({byte_len})"

    if c_type in arrow_int_type:
        return arrow_int_type[c_type]

    if c_type in arrow_float_type:
        return arrow_float_type[c_type]

    raise RuntimeError(f"Bad type '{c_type}'")


def arrow_data_type_expr(field, structs, indent):
    c_type = field["type"]
    conv = field.get("conversion", {})

    if c_type.startswith("struct "):
        struct_name = c_type.removeprefix("struct ")
        if struct_name not in structs:
            raise RuntimeError(f"Bad type '{c_type}'")
        nested = ",\n".join(
            arrow_field_expr(nested, structs, indent + 8)
            for nested in structs[struct_name]["fields"]
        )
        base = "DataType::Struct(Fields::from(vec![\n"
        base += f"{indent_block(nested, indent + 4)}\n"
        base += f"{' ' * indent}]))"
    else:
        base = arrow_scalar_type(field)

    if "num" not in field or field["type"] == "char" or "int" in conv:
        return base

    num = field["num"]
    if num == 0:
        if c_type == "uint8_t":
            return "DataType::Binary"
        return (
            "DataType::List(Arc::new(Field::new_list_field(\n"
            f"{indent_block(base, indent + 4)},\n"
            f"{' ' * (indent + 4)}false,\n"
            f"{' ' * indent})))"
        )

    if c_type == "uint8_t":
        return f"DataType::FixedSizeList(Arc::new(Field::new_list_field(DataType::UInt8, false)), {num})"
    return (
        "DataType::FixedSizeList(Arc::new(Field::new_list_field(\n"
        f"{indent_block(base, indent + 4)},\n"
        f"{' ' * (indent + 4)}false,\n"
        f"{' ' * indent})), "
        f"{num})"
    )


def arrow_field_expr(field, structs, indent):
    field_name = rust_str(arrow_name(field["name"]))
    data_type = arrow_data_type_expr(field, structs, indent + 4)
    if "\n" not in data_type:
        return f"Field::new({field_name}, {data_type}, false)"
    return (
        "Field::new(\n"
        f"{' ' * (indent + 4)}{field_name},\n"
        f"{indent_block(data_type, indent + 4)},\n"
        f"{' ' * (indent + 4)}false,\n"
        f"{' ' * indent})"
    )


def arrow_schema_expr(info, structs):
    fields = ",\n".join(
        arrow_field_expr(field, structs, 12) for field in info["fields"]
    )
    return (
        "Schema::new(vec![\n"
        + indent_block("timestamp_field(),", 8)
        + "\n"
        + indent_block(fields, 8)
        + "\n    ])"
    )


def decoders_gen(tdf_defs, output):
    env = Environment(
        loader=FileSystemLoader(pathlib.Path(__file__).parent),
        autoescape=select_autoescape(),
        trim_blocks=True,
        lstrip_blocks=True,
    )
    common_template = env.get_template("tdf_decoder.rs.jinja")
    csv_template = env.get_template("tdf_decoder_csv.rs.jinja")
    parquet_template = env.get_template("tdf_decoder_parquet.rs.jinja")

    for _tdf_id, info in tdf_defs["definitions"].items():
        info["arrow_schema"] = arrow_schema_expr(info, tdf_defs["structs"])

    def field_conv_func(field, name_prefix=None):
        t = rust_type[field["type"]]
        func = f"cursor.read_{t[0]}"
        if t[1]:
            func += "::<LittleEndian>"
        func += "()?"
        if c := field.get("conversion"):
            if endian := c.get("int", None):
                assert "num" in field
                assert t[0] == "u8"
                e = "LittleEndian" if endian == "little" else "BigEndian"
                if field["num"] == 3:
                    t = "u24"
                elif field["num"] == 6:
                    t = "u48"
                else:
                    raise RuntimeError("Unknown integer length")

                func = f"cursor.read_{t}::<{e}>()?"
                del field["num"]

            if "m" in c or "c" in c:
                func += " as f64"
                if "m" in c and c["m"] != 0:
                    val = c["m"]
                    inverse_ratio = (1 / val).as_integer_ratio()
                    # If number can be represented as a whole number division, use that
                    # instead for numerical stability (/ 10) is better than (* 0.1) as
                    # the former can be represented without loss of precision.
                    if inverse_ratio[1] == 1:
                        func += f" / {inverse_ratio[0]}.0"
                    else:
                        func += f" * {float_format(c['m'])}"
                if "c" in c and c["c"] != 0:
                    func += f" + {float_format(c['c'])}"

        n = field["name"]
        if name_prefix is not None:
            n = f"{name_prefix}." + n

        if "num" in field:
            if field["type"] == "char":
                return [
                    (
                        n,
                        f"tdf_field_read_string_to_str(cursor, cursor_start, {field['num']}, size)?",
                    )
                ]
            else:
                if field["num"] == 0:
                    return [
                        (n, "tdf_field_read_vla_to_str(cursor, cursor_start, size)?")
                    ]
                else:
                    return [(n + f"[{idx}]", func) for idx in range(field["num"])]
        else:
            return [(n, func)]

    def field_fmt(field):
        if field["type"] == "char":
            return ["{}"]
        if "display" in field and field["display"].get("fmt", "") == "hex":
            if digits := field["display"].get("digits", None):
                single = [f"0x{{:0{digits}x}}"]
            else:
                single = ["0x{:x}"]
        else:
            single = ["{}"]
        if field.get("num", None) == 0:
            return ["{}"]
        return single * field.get("num", 1)

    structs = {}
    struct_fmts = {}
    for name, struct in tdf_defs["structs"].items():
        funcs = []
        fmts = []
        for f in struct["fields"]:
            funcs += field_conv_func(f)
            fmts += field_fmt(f)
        structs[f"struct {name}"] = funcs
        struct_fmts[f"struct {name}"] = fmts

    # Generate rust conversion functions
    for _tdf_id, info in tdf_defs["definitions"].items():
        info["rust_convs"] = []
        fmt = []
        for f in info["fields"]:
            if f["type"] in structs:
                info["rust_convs"] += structs[f["type"]]
                fmt += struct_fmts[f["type"]]
            elif f["type"] in rust_type:
                info["rust_convs"] += field_conv_func(f)
                fmt += field_fmt(f)
            else:
                raise RuntimeError(f"Bad type '{f['type']}'")

        info["rust_head"] = ",".join([f'"{c[0]}"' for c in info["rust_convs"]])
        info["rust_fmt"] = ",".join(fmt)

    common_output = pathlib.Path(output) / "decoders.rs"
    csv_output = pathlib.Path(output) / "decoders_csv.rs"
    parquet_output = pathlib.Path(output) / "decoders_parquet.rs"

    def write_rendered(path, template):
        with path.open("w", newline="\n") as f:
            rendered = template.render(
                structs=tdf_defs["structs"], definitions=tdf_defs["definitions"]
            )
            f.write(rendered)
            f.write(os.linesep)

    write_rendered(common_output, common_template)
    write_rendered(csv_output, csv_template)
    write_rendered(parquet_output, parquet_template)


if __name__ == "__main__":
    parser = argparse.ArgumentParser("Generate rust TDF decoders", allow_abbrev=False)
    parser.add_argument("--json", required=True, type=str, help="TDF json description")
    parser.add_argument("--out", required=True, type=str, help="Output folder")
    args = parser.parse_args()

    with open(args.json) as f:
        definitions = json.load(f, parse_float=decimal.Decimal)
    decoders_gen(definitions, args.out)
