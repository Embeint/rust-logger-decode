# Changelog

All notable changes to this project will be documented in this file.

This project adheres to [Semantic Versioning](https://semver.org).

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
