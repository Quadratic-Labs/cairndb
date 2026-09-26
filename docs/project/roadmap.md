# Roadmap

CairnDB is **alpha** software. The engine and its primitives are
implemented and extensively tested. Now they need to prove themselves in
real use. This page describes where the project is and what comes next.
It states intent, not commitments: priorities shift with what we learn.

## Current phase: testing and validation

The API is not frozen yet. Before 1.0, we want evidence that the
primitives are correct, sufficient, and pleasant to build on.

### Finding bugs

- Keep the test suite exhaustive. It already covers concurrency races on
  the filesystem backend, mocked SDK contracts for every cloud backend,
  end-to-end crash-and-recovery scenarios, and mutation testing.
- Hunt for the bugs that tests miss: edge cases under real contention,
  long-running processes, large logs and projections, and unusual
  failure modes of real object stores.
- Report anything surprising. See [Contributing](contributing.md).

### Building applications on top of CairnDB

The best test of an API is building real things with it. Applications
such as **Flowli** are being developed on CairnDB to answer two questions:

- **Is the API useful?** Do the existing layers (objects, coordination,
  logs, transactions, projections) express real workloads naturally, or
  do applications keep re-implementing the same patterns around them?
- **Are primitives missing?** Patterns that recur across applications are
  candidates for new primitives, or for promotion from recipes (see
  [Coordination patterns](../guides/coordination-patterns.md)) into the
  engine.

The API may change as a result, with breaking changes recorded in the
[Changelog](changelog.md).

## Next: broader platform validation

### Testing on every cloud platform

Cloud backends are unit-tested against mocked SDK clients. Only Azure
Blob Storage has a live integration suite so far. Planned:

- Live integration suites for **Amazon S3** and **Google Cloud Storage**,
  matching the Azure suite: put-if-absent atomicity under concurrent
  writers, the compare-and-swap ladder, listing and GC semantics, and an
  end-to-end pass through the coordination kernel.
- Running the live suites regularly, so that changes in provider
  behaviour are caught early.
- Covering provider variants: S3 Express One Zone, Azure accounts with
  and without a hierarchical namespace, and GCS dual-region buckets.

### Open-source distributed storage

CairnDB should run on storage you operate yourself, not only on public
clouds.

- **Ceph**: a backend adapter, or a validated configuration of the S3
  backend against the Ceph Object Gateway (RGW), depending on how
  completely RGW supports the conditional writes CairnDB relies on.
- **Other open-source stores**: evaluate S3-compatible systems such as
  MinIO, SeaweedFS, and Garage. For each, verify put-if-absent and
  compare-and-swap semantics, then either document it as supported or
  provide a dedicated adapter.
- A **conformance suite** any backend can be run against, so that
  "supported" means "passes the suite" rather than "believed to work".

## Later

- Declarative retention policies per prefix, beyond today's `gc` job and
  bucket lifecycle rules.
- A 1.0 release with a stable API, once the validation phase has settled
  the primitives.
