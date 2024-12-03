#!/usr/bin/env python3

import argparse
import decimal
import json
import os
import pathlib
import math
from numpy import format_float_positional as float_format

from jinja2 import Environment, FileSystemLoader, select_autoescape


rust_type = {
    'char': ('u8', False),
    'int8_t': ('i8', False),
    'uint8_t': ('u8', False),
    'int16_t': ('i16', True),
    'uint16_t': ('u16', True),
    'int32_t': ('i32', True),
    'uint32_t': ('u32', True),
    'int64_t': ('i64', True),
    'uint64_t': ('u64', True),
    'float32_t': ('f32', True),
    'float64_t': ('f64', True),
}

def decoders_gen(tdf_defs, output):
    env = Environment(
        loader=FileSystemLoader(pathlib.Path(__file__).parent),
        autoescape=select_autoescape(),
        trim_blocks=True,
        lstrip_blocks=True,
    )
    tdf_template = env.get_template("tdf_decoder.rs.jinja")

    def field_conv_func(field, name_prefix=None):
        t = rust_type[field['type']]
        func = f"cursor.read_{t[0]}"
        if t[1]:
            func += "::<LittleEndian>"
        func += "()?"
        if c := field.get('conversion'):
            if endian := c.get('int', None):
                assert 'num' in field
                assert t[0] == 'u8'
                e = 'LittleEndian' if endian == 'little' else 'BigEndian'
                if field['num'] == 3:
                    t = 'u24'
                elif field['num'] == 6:
                    t = 'u48'
                else:
                    raise RuntimeError("Unknown integer length")

                func = f"cursor.read_{t}::<{e}>()?"
                del field['num']

            if 'm' in c or 'c' in c:
                func += ' as f64'
                if 'm' in c and c['m'] != 0:
                    val = c['m']
                    inverse_ratio = (1 / val).as_integer_ratio()
                    # If number can be represented as a whole number division, use that
                    # instead for numerical stability (/ 10) is better than (* 0.1) as
                    # the former can be represented without loss of precision.
                    if inverse_ratio[1] == 1:
                        func += f" / {inverse_ratio[0]}.0"
                    else:
                        func += f" * {float_format(c['m'])}"
                if 'c' in c and c['c'] != 0:
                    func += f" + {float_format(c['c'])}"

        n = field['name']
        if name_prefix is not None:
            n = f"{name_prefix}." + n

        if 'num' in field:
            if field['type'] == 'char':
                return [(n, f"tdf_field_read_string(cursor, {field['num']})?")]
            else:
                return [(n + f'[{idx}]', func) for idx in range(field['num'])]
        else:
            return [(n, func)]

    def field_fmt(field):
        if field['type'] == 'char':
            return ["{}"]
        if 'display' in field and field['display'].get('fmt', '') == "hex":
            single = ["0x{:x}"]
        else:
            single = ["{}"]
        return single * field.get('num', 1)


    structs = {}
    struct_fmts = {}
    for name, struct in tdf_defs['structs'].items():
        funcs = []
        fmts = []
        for f in struct['fields']:
            funcs += field_conv_func(f)
            fmts += field_fmt(f)
        structs[f"struct {name}"] = funcs
        struct_fmts[f"struct {name}"] = fmts

    # Generate rust conversion functions
    for tdf_id, info in tdf_defs['definitions'].items():
        info['rust_convs'] = []
        fmt = []
        for f in info['fields']:
            if f['type'] in structs:
                info['rust_convs'] += structs[f['type']]
                fmt += struct_fmts[f['type']]
            elif f['type'] in rust_type:
                info['rust_convs'] += field_conv_func(f)
                fmt += field_fmt(f)
            else:
                raise RuntimeError(f"Bad type {info['type']}")

        info['rust_head'] = ",".join([f"\"{c[0]}\"" for c in info['rust_convs']])
        info['rust_fmt'] = ",".join(fmt)


    tdf_output = pathlib.Path(output) / 'decoders.rs'
    with tdf_output.open("w") as f:
        f.write(
            tdf_template.render(
                    structs=tdf_defs["structs"], definitions=tdf_defs["definitions"]
            )
        )
        f.write(os.linesep)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        "Generate rust TDF decoders", allow_abbrev=False
    )
    parser.add_argument("--json", required=True, type=str, help="TDF json description")
    parser.add_argument("--out", required=True, type=str, help="Output folder")
    args = parser.parse_args()

    with open(args.json) as f:
        definitions = json.load(f, parse_float=decimal.Decimal)
    decoders_gen(definitions, args.out)
