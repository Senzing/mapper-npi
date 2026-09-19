# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
[markdownlint](https://dlaa.me/markdownlint/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- Currently-deactivated NPIs (a deactivation date and no reactivation date) are now their own data
  source, `NPI_DEACTIVE`, written to a fifth output file `NPI_DEACTIVE_<period>.json`, so an entity
  that touches a retired NPI is directly queryable. They keep `NPI_NUMBER`, `REF_NPI_ID` and
  `REL_ANCHOR` and carry no `RECORD_TYPE`. An NPI that was deactivated and later reactivated is
  active again and stays in `NPI-PROVIDERS`.
- Every resolution feature is now emitted in a `FEATURES` array, one object per feature instance,
  instead of at the record root with the usage type encoded as a name prefix. `DATA_SOURCE`,
  `RECORD_ID` and the payload attributes stay at the root. This retires the `PROVIDER_IDS`,
  `PROVIDER_LICENSE_NUMS`, `NPI_NUMBERS`, `REF_NPI_IDS`, `OTHER_NAMES` and `ENDPOINT_LIST`
  sub-lists, which the Senzing analyzer treated as opaque payload and never examined.
- Removed the invented compound phone usage types `BUSINESS-LOCATION_`, `MAILING-LOCATION_`,
  `BUSINESS-FAX_` and `MAILING-FAX_`. A telephone number now carries no `PHONE_TYPE` (NPPES
  publishes no mobile numbers, and `MOBILE` is the only phone label the engine weighs) and a fax
  carries `PHONE_TYPE: FAX`. PHONE objects are de-duplicated by (`PHONE_TYPE`, `PHONE_NUMBER`),
  because the mailing and practice-location numbers are frequently the same number.
- Empty elements are now dropped inside the provider-identifier and provider-license features too,
  so no `"KEY": ""` is emitted anywhere in a record.

### Fixed

- Read `Provider Sex Code` (NPPES renamed `Provider Gender Code`), falling back to the old name; the mapper
  previously raised `KeyError` on the first row of 2025+ dissemination files and produced no output.
- Load the othername/pl/endpoint reference files as strings: postal codes, phone and fax numbers were emitted
  as JSON integers/floats, dropping leading-zero ZIP codes and rendering faxes as `nnnnnnnnnn.0`.
- Emit DBA/former/other organization names from `othername_pfile`; an int-vs-string type-code comparison had
  silently dropped every row. NPPES placeholder `<UNAVAIL>` is filtered alongside `NONE`.
- Map `Replacement NPI` as a second `NPI_NUMBER` value (a second `NPI_NUMBER` object in `FEATURES`)
  instead of the unregistered `REPL_NPI_NUMBER` attribute.
- Deactivated NPIs (blank Entity Type Code) are no longer stamped `RECORD_TYPE=ORGANIZATION` with an empty
  organization name; empty-string attributes are never emitted.
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
