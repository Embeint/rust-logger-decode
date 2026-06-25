# Changelog

All notable changes to this project will be documented in this file.

This project adheres to [Semantic Versioning](https://semver.org).

## [1.10.0] - 2026-06-25

 - Nested TDF definitions inherit the parent field name in the CSV header column
 - Variable length array structs as the last element in a TDF generate a new row per instance
 - Handle 0 length trailing VLAs

## [1.9.0] - 2026-06-16

 - New output format [Apache Parquet](https://parquet.apache.org/)
 - Option to limit output files to a certain number of readings
    * If the limit is hit, output files have a numeric postfix
 - Option to skip merge step to optimize decoding times

## [1.8.1] - 2026-06-05

 - MacOS GUI distributed as notorized `.dmg`

## [1.8.0] - 2026-05-01

 - Fix decoding of constant length strings (Introduced in `1.7.0`)
 - Fix temporary decoding files potentially being left on the filesystem

## [1.7.0] - 2026-04-14

 - Add button to open output folder in system viewer
 - Fix decoding of ANNOTATION TDF
 - Update TDF definitions

## [1.6.0] - 2026-01-16

 - Fix GUI crash on certain types of invalid data
 - Block size for decoding can now be configured

## [1.5.0] - 2025-12-18

 - Fix bug that caused a variable number of blocks at the end of a file to not be decoded
 - Limit the number of threads used for small files
 - Update TDF definitions

## [1.4.1] - 2025-11-27

 - Update MacOS signing certificate

## [1.4.0] - 2025-11-26

 - Binary signing for MacOS
 - Update TDF definitions
 - Update dependencies

## [1.3.0] - 2025-09-16

 - Fix crashes on invalid headers lengths and sizes
 - Update TDF definitions
 - Update dependencies

## [1.2.0] - 2024-07-06

 - Add support for the DIFF and IDX encoded array types
 - Fix the handling of array periods over 0.5 seconds
 - Update TDF definitions

## [1.1.0] - 2024-03-26

 - Reset input file path if path no longer exists
 - Update TDF definitions

## [1.0.0] - 2024-12-19

 - Added missing Windows release target
 - Extended TDF time period support
 - Added support for `u24` and `u48` integer types
 - Populated `README.md`
 - Updated all dependencies
 - Handling of read underflow on TDFs
 - Format hex values with requested number of digits
 - Basic Variable-Length-Array TDF support

## [0.2.1] - 2024-12-01

 - Fixed Github release generation action

## [0.2.0] - 2024-11-30

 - Added support for the `TDF_REMOTE` block type
 - GUI: Automatically populate output prefix from input file for single file decoding
 - GUI: Sort output file list
 - GUI: Improve table column alignment
