# Changelog

All notable changes to CairnDB are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project
uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html). Before
1.0, minor versions may contain breaking changes. They are marked
**Breaking** below.

## [Unreleased]

### Added

- `db.attach_lease(key, holder=..., ttl=...)` (and `attach_lease_sync`):
  re-attach to an ownership period you already hold, from any process.
  One read, no write, no epoch bump.
- `db.cooperative_write(key, fn)`: write into a lease's state from
  outside the lease without fencing the holder, for example a cancel
  flag. Guarded lease writes absorb it instead of clobbering it.
- `state_fn` on `db.lease(...)`: make acquisition an atomic state
  transition. An exception in `state_fn` aborts the acquisition with
  nothing written.
- `BlobStorage.append_object_sync` / `append_object`: atomic appends,
  native on the filesystem backend and on Azure (Append Blobs), with a
  compare-and-swap fallback elsewhere.
- `Timestamp`: ordering, `timedelta` arithmetic, and a UUIDv7 round trip
  (`to_uuid7` / `from_uuid7`).
- `--log NAME` option (or `CAIRNDB_LOG`) on `cairndb snapshot`, `gc`,
  and `rebuild`, to run the jobs against a named log.
- `db.projection(..., registry=...)`: build a projection on an existing
  `HandlerRegistry`, so the application and the snapshot job share one
  set of handlers.
- `cairndb.engine.log_storage(storage, name)`: the storage view of a
  named log, for the lower-level API and the jobs.
- `CAIRNDB_GCS_BUCKET` environment variable for the GCS backend.
- `DEFAULT_SCHEMA_VERSION` constant in `cairndb.storage.base`.
- Azure templates: a `snapshotLogs` parameter (one snapshot and GC job
  pair per log) and a `schemaVersion` parameter. The snapshot job now
  passes `--init-schema`.
- Documentation site (Sphinx + MyST): quickstart, concepts, guides,
  deployment guides for local, AWS, Google Cloud, and Azure, and a
  generated API and CLI reference.
- Opt-in live integration suite against real Azure Blob Storage
  (`make azure-integration`).
- Mutation testing with mutmut. The codebase is mutant-clean.

### Changed

- **Breaking:** the default projection schema version is now `"1"`
  everywhere (`ClientConfig`, `SnapshotBuilder`, `CAIRNDB_SCHEMA_VERSION`,
  and the CLI's `--schema-version`), matching `db.projection()`.
  Previously some components defaulted to `"1.0.0"` and others to `"1"`,
  so snapshots built with the defaults were never found by default
  projections. Pass `1.0.0` explicitly to keep using existing
  `snapshots/v1.0.0/` snapshots.
- **Breaking:** `Projection.registry` is read-only. Assigning to it had no
  effect before; it now raises `AttributeError`. Pass `registry=`
  instead.
- **Breaking:** the Azure templates' `snapshotHandlersRef` parameter is
  replaced by `snapshotLogs`.
- Client and storage configurations are stdlib dataclasses validated at
  construction. The `pydantic` dependency is dropped.

### Deprecated

- Setting the GCS bucket through `CAIRNDB_S3_BUCKET`. It still works when
  `CAIRNDB_GCS_BUCKET` is unset.

### Fixed

- Azure: on hierarchical-namespace (ADLS Gen2) accounts, listings no
  longer report directory stubs as objects.
- Azure: an unconditional `put_object` over an append blob replaces it,
  instead of misreporting a lost precondition.
- The Azure snapshot job no longer runs without a schema initializer.

## [0.4.0] - 2026-08-09

First release under the CairnDB name (formerly ChroniQL), repositioned
as a serverless database engine on blob storage.

### Added

- The `CairnDB` engine facade over five layers: conditional objects;
  coordination (`claim`, `lease`, `doc`); named commit logs;
  optimistic multi-key transactions; and declarative SQLite projections
  with snapshots and time travel.
- Storage backends: filesystem, Amazon S3, Google Cloud Storage, and
  Azure Blob Storage.
- `cairndb` CLI with the `snapshot`, `gc`, and `rebuild` jobs, a jobs
  container image, and Azure Container Apps Bicep templates.

[Unreleased]: https://github.com/Quadratic-Labs/cairndb/compare/481d105...HEAD
[0.4.0]: https://github.com/Quadratic-Labs/cairndb/commit/481d105
