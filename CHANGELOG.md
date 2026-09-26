# Changelog

All notable changes to CairnDB are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project
uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html). Before
1.0, minor versions may contain breaking changes. They are marked
**Breaking** below.

## [Unreleased]

### Added

- `LICENSE` file with the MIT license text, shipped in the sdist and
  wheel.
- `py.typed` marker (PEP 561), so type checkers use CairnDB's inline
  type hints.
- Publishing: pushing a `vX.Y.Z` tag publishes the release to PyPI
  (Trusted Publishing) and creates the GitHub Release.

### Changed

- Package metadata declares the license as an SPDX expression
  (`license = "MIT"`, PEP 639) instead of the deprecated table and
  classifier.
- README links are absolute, so they work on the PyPI project page.

## [0.4.0] - 2026-09-26

First public release. CairnDB (formerly ChroniQL) is a serverless
database engine on blob storage: all state lives in a bucket, and the
bucket's conditional writes arbitrate concurrency.

### Added

- **Engine facade** `CairnDB`, over five layers:
  - **Objects**: an etag-guarded key-value store with compare-and-swap,
    put-if-absent, and `wait_for` polling.
  - **Coordination**:
    - `claim`: a unique constraint whose losers converge on the winner's
      value.
    - `lease`: expiring, epoch-fenced ownership. It supports `state_fn`
      for atomic acquire-with-transition, `cooperative_write` for
      unfenced writes from outside the lease, and `attach_lease` to
      re-enter an ownership period from another process.
    - `doc`: a typed document with a retrying read-modify-write loop.
  - **Logs**: named, dense, totally ordered commit logs, with group
    commit, a durable acknowledgement, and revalidate hooks.
  - **Transactions**: optimistic multi-key atomicity, coordinated by a
    system log, with crash recovery.
  - **Projections**: declarative SQLite projections, built by
    deterministic replay with atomic swaps. They support snapshots, time
    travel (`as_of`), read-your-writes (`wait_for`), and handler
    registries shared with the snapshot job.
- **Storage backends**: filesystem, Amazon S3 (and S3-compatible stores
  with conditional writes), Google Cloud Storage, and Azure Blob Storage,
  including hierarchical-namespace accounts. `append_object` is native on
  the filesystem backend and on Azure, with a compare-and-swap fallback
  elsewhere.
- **`cairndb` CLI**: `snapshot`, `gc`, and `rebuild` jobs, each able to
  target a named log with `--log`. Configuration comes from `CAIRNDB_*`
  environment variables.
- **Deployment**: a jobs container image, and Azure Container Apps Bicep
  templates with one snapshot and GC job pair per log.
- **Documentation site** (Sphinx + MyST): quickstart, concepts, guides,
  deployment guides for local, AWS, Google Cloud, and Azure, and a
  generated API and CLI reference. It is published to GitHub Pages from
  the latest `release/X.Y` branch, and built as a check on pull requests.
- **Testing**: race tests on the filesystem backend, mocked-SDK contract
  tests for every cloud backend, end-to-end crash-and-recovery tests, an
  opt-in live Azure suite, and mutation testing (the codebase is
  mutant-clean).

[Unreleased]: https://github.com/Quadratic-Labs/cairndb/compare/v0.4.0...HEAD
[0.4.0]: https://github.com/Quadratic-Labs/cairndb/releases/tag/v0.4.0
