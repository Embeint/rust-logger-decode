# Custom TDF Decoding

To create a build of the tool with support for custom TDFs, the decoders need to be rebuilt
with `tdf_decoder_build.py` being provided the custom definition file. For example:

```
./scripts/tdf_decoder_build.py --json ./scripts/tdf.json --out ./tdf/src/ --extensions ~/code/extensions/tdf.json
```

This will update the `decoders_csv.rs` and `decoders_parquet.rs` files, and the GUI and CLI
applications can be rebuilt with a simple `cargo build --release`.

> ⚠️ For MacOS builds, the resulting binaries must be notorized through Apple before
  they can be run on other machines without warnings.
