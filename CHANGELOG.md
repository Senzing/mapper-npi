# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
[markdownlint](https://dlaa.me/markdownlint/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- Read `Provider Sex Code` (NPPES renamed `Provider Gender Code`), falling back to the old name; the mapper
  previously raised `KeyError` on the first row of 2025+ dissemination files and produced no output.
- Load the othername/pl/endpoint reference files as strings: postal codes, phone and fax numbers were emitted
  as JSON integers/floats, dropping leading-zero ZIP codes and rendering faxes as `nnnnnnnnnn.0`.
- Emit DBA/former/other organization names from `othername_pfile`; an int-vs-string type-code comparison had
  silently dropped every row. NPPES placeholder `<UNAVAIL>` is filtered alongside `NONE`.
- Map `Replacement NPI` as a second `NPI_NUMBER` value (`NPI_NUMBERS` sub-list) instead of the unregistered
  `REPL_NPI_NUMBER` attribute.
- Deactivated NPIs (blank Entity Type Code) are no longer stamped `RECORD_TYPE=ORGANIZATION` with an empty
  `PRIMARY_NAME_ORG`; empty-string attributes are never emitted.
- `NPI-LOCATIONS` / `NPI-AFFILIATIONS` `RECORD_ID`s are now deterministic (`<NPI>-<sha1 prefix>` of the row's
  source fields) instead of a cursor ordinal, and output is byte-identical across runs.
- The temporary `NPPES.db` is created in a temp directory (or `-w/--workDir`), never in the source directory.

## [1.0.1] - yyyy-mm-dd

### Added to 1.0.1

- Thing 3

### Fixed in 1.0.1

- Thing 2

## [1.0.0] - yyyy-mm-dd

### Added to 1.0.0

- Thing 2
- Thing 1
