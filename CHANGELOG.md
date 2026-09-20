# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
[markdownlint](https://dlaa.me/markdownlint/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `ShuffleWriter`, a bounded-memory reservoir that shuffles records as they are written, plus
  `-S/--shuffleBuffer` to size it (default 250000 records per output file; 0 writes straight
  through and disables shuffling). NPPES ships sorted by NPI, so the records belonging to one
  entity are adjacent in the file; loaded in that order every consumer thread works the same
  entity at once and they all queue on one lock. Adjacent input records land in independent
  reservoir slots, which is what breaks that adjacency (there is no hard bound on how far one
  record can move -- eviction is random), and no second copy of the file is ever written. The drain order at close is shuffled too, so it cannot reproduce insertion order.

### Changed

- `NPI-AFFILIATIONS` records are now clustered by ER content. `er_record_id()` derives the
  `RECORD_ID` as a sha1 over the record's normalized (strip, collapse whitespace, upper),
  sorted features, EXCLUDING the `REL_*` disclosure features, so two rows that describe the
  same organization and differ only in which provider they point at collapse onto one
  `RECORD_ID`. Affiliates are accumulated for the whole run and emitted once each by
  `flush_affiliate_records()`, which aggregates every provider pointer that landed on that
  organization onto the single record as `REL_POINTER` entries. Those entries are emitted in
  sorted order, because a set iterates in per-process hash order and the record bytes would
  otherwise differ between two runs on identical input.
- Provider license numbers are now filtered by `check_id_value()` like every other identifier,
  instead of only rejecting the literal `=========`. `check_id_value()` splits the value and
  rejects it if any word is in the ignore list, which now carries all three CMS masks published
  in the NPPES readme (CMS-6060-N) -- `$$$$$$$$$` for a self-reported SSN, `*********` for an
  ITIN and `=========` for an EIN -- alongside `PENDING`, `NA`, `ENROLLED` and `NONE`. Only the
  EIN mask was previously stripped. Engine genericity detection would keep a shared mask out of
  resolution regardless, so this is data hygiene rather than an ER fix.
- `npi_config_updates.g2c` replaces the `OTHER_PROVIDER_ID` and `MEDICAID_PROVIDER_ID` features
  with a single `PROVIDER_ID` feature, behavior `FF` with `ISSUER` compared (both predecessors
  were `F1` and did not compare `ISSUER`), carrying `PROVIDER_ID_NUMBER`, `PROVIDER_ID_STATE`
  and `PROVIDER_ID_ISSUER`. It also registers `REF_NPI_ID`, the provider's own NPI asserted a
  second time as an `A1ES` exclusive feature via `templateAdd`/`GLOBAL_ID`, so that reference
  lists of NPIs carried by other sources cannot resolve distinct providers together.
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

- Read `Provider Sex Code` (NPPES renamed `Provider Gender Code`), falling back to the old name;
  the mapper previously raised `KeyError` on the first row of 2025+ dissemination files and
  produced no output.
- Load the othername/pl/endpoint reference files as strings: postal codes, phone and fax numbers
  were emitted as JSON integers/floats, dropping leading-zero ZIP codes and rendering faxes as
  `nnnnnnnnnn.0`.
- Emit DBA/former/other organization names from `othername_pfile`; an int-vs-string type-code
  comparison had silently dropped every row. NPPES placeholder `<UNAVAIL>` is filtered alongside
  `NONE`.
- Map `Replacement NPI` as a second `NPI_NUMBER` value (a second `NPI_NUMBER` object in
  `FEATURES`) instead of the unregistered `REPL_NPI_NUMBER` attribute.
- Deactivated NPIs (blank Entity Type Code) are no longer stamped `RECORD_TYPE=ORGANIZATION` with
  an empty organization name; empty-string attributes are never emitted.
- `NPI-LOCATIONS` `RECORD_ID`s are now deterministic (`<NPI>-<sha1 prefix>` of the row's source
  fields) instead of a cursor ordinal, and output is byte-identical across runs.
- The temporary `NPPES.db` is created in a temp directory (or `-w/--workDir`), never in the source
  directory.

### Operator notes

- The `RECORD_ID` scheme changed for `NPI-LOCATIONS` and `NPI-AFFILIATIONS`. Senzing keys records
  on (`DATA_SOURCE`, `RECORD_ID`), so re-loading these outputs onto an existing repository leaves
  the previously loaded copies behind as duplicates. Purge those two data sources before the
  re-load.
- Re-run `npi_config_updates.g2c` on existing instances to register `PROVIDER_ID` and
  `REF_NPI_ID`. The features `PROVIDER_ID` supersedes are left registered on purpose: deleting a
  feature from a configuration that already has data loaded against it is not safe.

## [1.0.1] - yyyy-mm-dd

### Added to 1.0.1

- Thing 3

### Fixed in 1.0.1

- Thing 2

## [1.0.0] - yyyy-mm-dd

### Added to 1.0.0

- Thing 2
- Thing 1
