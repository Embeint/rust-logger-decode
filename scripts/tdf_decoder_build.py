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
        + indent_block("sample_idx_field(),", 8)
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
        int_endian = None
        if c := field.get("conversion"):
            int_endian = c.get("int", None)
            if endian := int_endian:
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

        if "num" in field and not int_endian:
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
        if field.get("conversion", {}).get("int", None):
            return single
        if field.get("num", None) == 0:
            return ["{}"]
        return single * field.get("num", 1)

    structs = {}
    struct_fmts = {}
    for name, struct in tdf_defs["structs"].items():
        fmts = []
        for f in struct["fields"]:
            fmts += field_fmt(f)
        structs[f"struct {name}"] = struct["fields"]
        struct_fmts[f"struct {name}"] = fmts

    # Generate rust conversion functions
    for _tdf_id, info in tdf_defs["definitions"].items():
        info["rust_convs"] = []
        fmt = []
        for f in info["fields"]:
            if f["type"] in structs:
                for struct_field in structs[f["type"]]:
                    info["rust_convs"] += field_conv_func(
                        struct_field, name_prefix=f["name"]
                    )
                fmt += struct_fmts[f["type"]]
            elif f["type"] in rust_type:
                info["rust_convs"] += field_conv_func(f)
                fmt += field_fmt(f)
            else:
                raise RuntimeError(f"Bad type '{f['type']}'")

        info["rust_head"] = ",".join([f'"{c[0]}"' for c in info["rust_convs"]])
        info["rust_fmt"] = ",".join(fmt)

    rust_array_type = {
        "i8": "Int8Array",
        "u8": "UInt8Array",
        "i16": "Int16Array",
        "u16": "UInt16Array",
        "i32": "Int32Array",
        "u32": "UInt32Array",
        "i64": "Int64Array",
        "u64": "UInt64Array",
        "f32": "Float32Array",
        "f64": "Float64Array",
    }

    rust_vec_type = {
        "char": "String",
        "int8_t": "i8",
        "uint8_t": "u8",
        "int16_t": "i16",
        "uint16_t": "u16",
        "int32_t": "i32",
        "uint32_t": "u32",
        "int64_t": "i64",
        "uint64_t": "u64",
        "float": "f32",
        "float32_t": "f32",
        "float64_t": "f64",
    }

    def rust_type_after_conversion(field):
        conv = field.get("conversion", {})
        if "m" in conv or "c" in conv:
            return "f64"
        if field["type"] == "char":
            return "String"
        if "int" in conv:
            byte_len = field["num"]
            if byte_len <= 1:
                return "u8"
            if byte_len <= 2:
                return "u16"
            if byte_len <= 4:
                return "u32"
            if byte_len <= 8:
                return "u64"
        return rust_vec_type[field["type"]]

    def rust_pascal(value):
        return "".join(part.capitalize() for part in re.split(r"\W|_", value) if part)

    def rust_field_ident(path):
        return arrow_name("_".join(path))

    def primitive_read_expr(field):
        t = rust_type[field["type"]]
        func = f"cursor.read_{t[0]}"
        if t[1]:
            func += "::<LittleEndian>"
        func += "()?"

        if c := field.get("conversion"):
            if endian := c.get("int", None):
                e = "LittleEndian" if endian == "little" else "BigEndian"
                if field["num"] == 3:
                    t_name = "u24"
                elif field["num"] == 6:
                    t_name = "u48"
                else:
                    raise RuntimeError("Unknown integer length")
                func = f"cursor.read_{t_name}::<{e}>()?"

            if "m" in c or "c" in c:
                func += " as f64"
                if "m" in c and c["m"] != 0:
                    val = c["m"]
                    inverse_ratio = (1 / val).as_integer_ratio()
                    if inverse_ratio[1] == 1:
                        func += f" / {inverse_ratio[0]}.0"
                    else:
                        func += f" * {float_format(c['m'])}"
                if "c" in c and c["c"] != 0:
                    func += f" + {float_format(c['c'])}"
        return func

    def field_byte_size(field):
        c_type = field["type"]
        conv = field.get("conversion", {})
        if "int" in conv:
            return field["num"]
        if c_type.startswith("struct "):
            struct_name = c_type.removeprefix("struct ")
            return sum(field_byte_size(f) for f in tdf_defs["structs"][struct_name]["fields"])
        if c_type == "char":
            return field.get("num", 0)
        base = {
            "int8_t": 1,
            "uint8_t": 1,
            "int16_t": 2,
            "uint16_t": 2,
            "int32_t": 4,
            "uint32_t": 4,
            "int64_t": 8,
            "uint64_t": 8,
            "float": 4,
            "float32_t": 4,
            "float64_t": 8,
        }[c_type]
        return base * field.get("num", 1)

    def field_model(field, path):
        c_type = field["type"]
        num = field.get("num", None)
        conv = field.get("conversion", {})

        if c_type.startswith("struct "):
            struct_name = c_type.removeprefix("struct ")
            child_fields = tdf_defs["structs"][struct_name]["fields"]
            children = [field_model(child, path + [arrow_name(child["name"])]) for child in child_fields]
            if num == 0:
                return {
                    "kind": "list",
                    "path": path,
                    "child": {"kind": "struct", "path": path, "children": children},
                    "item_size": field_byte_size(field),
                    "field_expr": arrow_field_expr(field, tdf_defs["structs"], 12),
                }
            return {
                "kind": "struct",
                "path": path,
                "children": children,
                "field_expr": arrow_field_expr(field, tdf_defs["structs"], 12),
            }

        if c_type == "char":
            return {
                "kind": "string",
                "path": path,
                "field_expr": arrow_field_expr(field, tdf_defs["structs"], 12),
                "read": f"tdf_field_read_string_to_string(cursor, cursor_start, {num or 0}, size)?",
            }

        if num == 0 and c_type == "uint8_t" and "int" not in conv:
            return {
                "kind": "binary",
                "path": path,
                "field_expr": arrow_field_expr(field, tdf_defs["structs"], 12),
            }

        if num == 0:
            return {
                "kind": "list",
                "path": path,
                "child": field_model({k: v for k, v in field.items() if k != "num"}, path),
                "item_size": field_byte_size({k: v for k, v in field.items() if k != "num"}),
                "field_expr": arrow_field_expr(field, tdf_defs["structs"], 12),
            }

        if num is not None and "int" not in conv:
            return {
                "kind": "fixed_list",
                "path": path,
                "child": field_model({k: v for k, v in field.items() if k != "num"}, path),
                "num": num,
                "field_expr": arrow_field_expr(field, tdf_defs["structs"], 12),
            }

        rust_type_name = rust_type_after_conversion(field)
        return {
            "kind": "primitive",
            "path": path,
            "rust_type": rust_type_name,
            "array_type": rust_array_type[rust_type_name],
            "field_expr": arrow_field_expr(field, tdf_defs["structs"], 12),
            "read": primitive_read_expr(field),
        }

    def model_storage_fields(model, out):
        kind = model["kind"]
        if kind == "primitive":
            out.append((rust_field_ident(model["path"]), f"Vec<{model['rust_type']}>"))
        elif kind == "string":
            out.append((rust_field_ident(model["path"]), "Vec<String>"))
        elif kind == "binary":
            out.append((rust_field_ident(model["path"]), "Vec<Vec<u8>>"))
        elif kind == "fixed_list":
            model_storage_fields(model["child"], out)
        elif kind == "list":
            out.append((rust_field_ident(model["path"]) + "_offsets", "Vec<i32>"))
            model_storage_fields(model["child"], out)
        elif kind == "struct":
            for child in model["children"]:
                model_storage_fields(child, out)
        else:
            raise RuntimeError(f"Bad model kind {kind}")

    def model_init_fields(model, out, capacity):
        kind = model["kind"]
        if kind == "primitive":
            out.append(f"{rust_field_ident(model['path'])}: Vec::with_capacity({capacity})")
        elif kind == "string":
            out.append(f"{rust_field_ident(model['path'])}: Vec::with_capacity({capacity})")
        elif kind == "binary":
            out.append(f"{rust_field_ident(model['path'])}: Vec::with_capacity({capacity})")
        elif kind == "fixed_list":
            model_init_fields(model["child"], out, f"{capacity} * {model['num']}")
        elif kind == "list":
            out.append(f"{rust_field_ident(model['path'])}_offsets: vec![0]")
            model_init_fields(model["child"], out, capacity)
        elif kind == "struct":
            for child in model["children"]:
                model_init_fields(child, out, capacity)
        else:
            raise RuntimeError(f"Bad model kind {kind}")

    def model_append_lines(model, out, cursor_name="cursor"):
        kind = model["kind"]
        if kind == "primitive":
            out.append(f"self.{rust_field_ident(model['path'])}.push({model['read']});")
        elif kind == "string":
            out.append(f"self.{rust_field_ident(model['path'])}.push({model['read']});")
        elif kind == "binary":
            out.append(
                f"self.{rust_field_ident(model['path'])}.push(crate::decoders::tdf_field_read_vla({cursor_name}, cursor_start, size)?);"
            )
        elif kind == "fixed_list":
            for _ in range(model["num"]):
                model_append_lines(model["child"], out, cursor_name)
        elif kind == "list":
            out.append("{")
            out.append(
                f"    let bytes_remaining = crate::decoders::vla_bytes_remaining({cursor_name}, cursor_start, size)?;"
            )
            out.append(f"    if bytes_remaining % {model['item_size']} != 0 {{")
            out.append(
                '        return Err(Error::new(ErrorKind::InvalidData, "Variable-length array does not align to element size"));'
            )
            out.append("    }")
            out.append(f"    let item_count = bytes_remaining / {model['item_size']};")
            out.append("    for _ in 0..item_count {")
            child_lines = []
            model_append_lines(model["child"], child_lines, cursor_name)
            out.extend([f"        {line}" for line in child_lines])
            out.append("    }")
            out.append(
                f"    self.{rust_field_ident(model['path'])}_offsets.push(*self.{rust_field_ident(model['path'])}_offsets.last().unwrap() + item_count as i32);"
            )
            out.append("}")
        elif kind == "struct":
            for child in model["children"]:
                model_append_lines(child, out, cursor_name)
        else:
            raise RuntimeError(f"Bad model kind {kind}")

    def model_finish_expr(model):
        kind = model["kind"]
        if kind == "primitive":
            return f"Arc::new({model['array_type']}::from(std::mem::take(&mut self.{rust_field_ident(model['path'])}))) as ArrayRef"
        if kind == "string":
            return f"Arc::new(StringArray::from_iter_values(std::mem::take(&mut self.{rust_field_ident(model['path'])}))) as ArrayRef"
        if kind == "binary":
            return f"Arc::new(BinaryArray::from_iter_values(std::mem::take(&mut self.{rust_field_ident(model['path'])}))) as ArrayRef"
        if kind == "fixed_list":
            child = model_finish_expr(model["child"])
            child_field = arrow_data_type_expr(
                {k: v for k, v in model.get("raw_field", {}).items() if k != "num"},
                tdf_defs["structs"],
                16,
            )
            return (
                "Arc::new(FixedSizeListArray::try_new(\n"
                f"            Arc::new(Field::new_list_field({child_field}, false)),\n"
                f"            {model['num']},\n"
                f"            {child},\n"
                "            None,\n"
                "        )?) as ArrayRef"
            )
        if kind == "list":
            child = model_finish_expr(model["child"])
            offsets = rust_field_ident(model["path"]) + "_offsets"
            return (
                "{\n"
                f"            let offsets = std::mem::replace(&mut self.{offsets}, vec![0]);\n"
                f"            Arc::new(ListArray::try_new(\n"
                f"                self.list_value_field({model['field_index']}),\n"
                "                OffsetBuffer::new(ScalarBuffer::from(offsets)),\n"
                f"                {child},\n"
                "                None,\n"
                "            )?) as ArrayRef\n"
                "        }"
            )
        if kind == "struct":
            child_fields = ",\n            ".join(child["field_expr"] for child in model["children"])
            child_arrays = ",\n            ".join(model_finish_expr(child) for child in model["children"])
            return (
                "Arc::new(StructArray::try_new(\n"
                f"            Fields::from(vec![\n            {child_fields}\n            ]),\n"
                f"            vec![\n            {child_arrays}\n            ],\n"
                "            None,\n"
                "        )?) as ArrayRef"
            )
        raise RuntimeError(f"Bad model kind {kind}")

    def model_set_raw_field(model, field):
        model["raw_field"] = field
        if model["kind"] in ("fixed_list", "list"):
            child = {k: v for k, v in field.items() if k != "num"}
            model_set_raw_field(model["child"], child)

    def model_has_list(model):
        if model["kind"] == "list":
            return True
        if model["kind"] == "fixed_list":
            return model_has_list(model["child"])
        if model["kind"] == "struct":
            return any(model_has_list(child) for child in model["children"])
        return False

    for tdf_id, info in tdf_defs["definitions"].items():
        info["rust_builder_name"] = f"Tdf{tdf_id}{rust_pascal(info['name'])}Builder"
        info["rust_variant_name"] = f"Tdf{tdf_id}{rust_pascal(info['name'])}"
        info["parquet_fields"] = []
        models = []
        for idx, field in enumerate(info["fields"]):
            model = field_model(field, [arrow_name(field["name"])])
            model["field_index"] = idx + 2
            model_set_raw_field(model, field)
            models.append(model)
        info["parquet_models"] = models
        info["parquet_has_lists"] = any(model_has_list(model) for model in models)

        storage_fields = [
            ("row_timestamp", "Vec<Option<i64>>"),
            ("row_sample_idx", "Vec<Option<u16>>"),
        ]
        for model in models:
            model_storage_fields(model, storage_fields)
        info["parquet_storage_fields"] = storage_fields

        init_fields = [
            "row_timestamp: Vec::with_capacity(capacity)",
            "row_sample_idx: Vec::with_capacity(capacity)",
        ]
        for model in models:
            model_init_fields(model, init_fields, "capacity")
        info["parquet_init_fields"] = init_fields

        append_lines = [
            "self.row_timestamp.push(meta.time_unix_micros);",
            "self.row_sample_idx.push(meta.sample_idx);",
        ]
        for model in models:
            model_append_lines(model, append_lines)
        info["parquet_append_lines"] = append_lines

        finish_arrays = [
            'Arc::new(TimestampMicrosecondArray::from(std::mem::take(&mut self.row_timestamp)).with_timezone("+00:00")) as ArrayRef',
            "Arc::new(UInt16Array::from(std::mem::take(&mut self.row_sample_idx))) as ArrayRef",
        ]
        for idx, model in enumerate(models):
            model["field_index"] = idx + 2
            finish_arrays.append(model_finish_expr(model))
        info["parquet_finish_arrays"] = finish_arrays

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
