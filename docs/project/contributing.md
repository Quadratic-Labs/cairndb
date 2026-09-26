# Contributing

Contributions are welcome: bug reports, fixes, documentation, and
feedback from building on CairnDB. See the [Roadmap](roadmap.md) for
where help matters most. Everyone taking part is expected to follow the
[Code of Conduct](code-of-conduct.md).

## Reporting bugs

Open an issue on [GitHub](https://github.com/Quadratic-Labs/cairndb/issues)
with the CairnDB version, the storage backend, and a minimal
reproduction. Concurrency bugs are easiest to act on when they reproduce
against the filesystem backend.

## Setup

```bash
python3.14 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

The `dev` extra includes every storage SDK (boto3, google-cloud-storage,
azure-storage-blob) and the CLI, so all backends' unit tests can run.

## Common tasks

| Command | What it does |
|---|---|
| `make test` | run the full test suite |
| `make coverage` | tests with a coverage report (`htmlcov/`) |
| `make lint` / `make format` / `make type-check` | ruff, black, mypy (strict) |
| `make all` | format, lint, type-check, test |
| `make mutation` / `make mutation-results` | mutation testing with mutmut |
| `make azure-integration` | live Azure suite (needs `CAIRNDB_AZURE_*`) |
| `make demo` | writer + reader demo on the filesystem backend |
| `make docs` / `make docs-serve` | build this documentation / live-reload it |

## Test design

- **The filesystem backend is the race simulator.** Its put-if-absent is
  a real atomic `os.link`, so concurrency tests (committer races, the
  end-to-end acceptance test) exercise genuine contention without cloud
  credentials.
- **Cloud backends are unit-tested against mocked SDK clients.** The
  tests pin the exact conditional-write parameters (`IfNoneMatch="*"`,
  `if_generation_match=0`, `overwrite=False`) and the error mappings.
- `tests/integration/test_end_to_end.py` is the acceptance test: three
  concurrent writers, a mid-run snapshot, a killed-and-replaced committer,
  and a fresh reader that must see every acknowledged event exactly once
  over a dense, gap-free log.
- `tests/integration/test_snapshot_bootstrap.py` is the regression suite
  for the v1 bug in which fresh clients silently lost pre-snapshot
  history.
- `tests/integration/test_azure_live.py` runs against a real Azure
  container. It is opt-in with `CAIRNDB_AZURE_INTEGRATION=1`.

To smoke-test a real S3-compatible store, see
[Local and self-hosted → MinIO](../deployment/local.md#minio-s3-compatible-self-hosted).

## Code layout

```text
src/cairndb/
├── engine/             # the CairnDB facade and its layers
│   ├── core.py         #   CairnDB
│   ├── objects.py      #   layer 0: Objects
│   ├── coordination.py #   layer 1: claim, Lease, Document
│   ├── logs.py         #   layer 2: Log, NamespacedStorage
│   ├── transactions.py #   layer 2: Transaction, TransactionManager
│   └── projection.py   #   layer 3: Projection
├── committer.py        # write path: group commit, durable ack, races
├── cli.py              # cairndb snapshot | gc | rebuild
├── core/               # Event, Commit, SequenceNumber, Timestamp, exceptions
├── storage/            # BlobStorage interface, configs, filesystem|s3|gcs|azure
├── client/             # CairnDBClient, updater, projector, replay, registry
└── jobs/               # snapshot builder, garbage collection
```

## Invariants: do not break these

1. `put_commit` must be put-if-absent on every backend. The bucket is the
   only arbiter of ordering.
2. An `append()` future may resolve only after its PUT succeeded.
3. Replay must be deterministic, and handlers see events in sequence
   order.
4. Projection updates are atomic swaps. Readers never see partial state.
5. Commit numbers are dense. Never create one except through a won
   conditional PUT, and never delete one except strictly below a kept
   snapshot.

## Writing documentation

The docs are Markdown ([MyST](https://myst-parser.readthedocs.io/)) built
with Sphinx. The API reference is generated from docstrings, so
**document behaviour in the docstring**, in Google style (`Args:`,
`Returns:`, `Raises:`). The reference pages pick it up automatically.

```bash
pip install -e ".[docs]"
make docs          # builds docs/_build/html, and fails on warnings
make docs-serve    # live-reloading server on http://127.0.0.1:8000
```

- Concept pages explain *what is guaranteed and why*. Guides show *how*.
  The reference is *what exists*.
- Code samples should run. The quickstart's complete script is kept in
  sync with the snippets above it.
- New pages must be added to a `toctree` in `docs/index.md`.

### Publishing

The site at <https://quadratic-labs.github.io/cairndb/> is published by
the `Docs` GitHub Actions workflow (`.github/workflows/docs.yml`):

- **Pull requests** to `main` or a release branch build the docs as a
  check. Warnings fail the build. Nothing is published.
- **Pushes to a `release/X.Y` branch** build the docs, and publish them
  only if that branch is the **latest** release branch, meaning the
  highest `X.Y` on GitHub. Pushes to older release branches, such as
  backports, never overwrite the published site.
- To republish without a new commit, run the workflow manually on the
  latest release branch from the Actions tab.

Cutting a release branch therefore publishes its docs. See
[Releasing](#releasing).

## Releasing

Releases combine two kinds of Git refs:

- **Release branches** (`release/X.Y`) are lines of maintenance. Patch
  fixes land on them, and the docs are published from the head of the
  latest one.
- **Tags** (`vX.Y.Z`) mark the exact commit of each release, forever.
  GitHub Releases, `pip install git+…@vX.Y.Z`, and the changelog's
  version links point at tags.

Both are protected by repository rulesets. Only the release manager can
create or push release branches, and create release tags. Nobody can
move or delete a tag, and nobody can delete a release branch or
force-push to it. Commits on release branches must be signed.

### A new minor release (X.Y.0)

1. On `main`, set the version in `pyproject.toml` and
   `src/cairndb/__init__.py`. In `CHANGELOG.md`, move the
   `[Unreleased]` entries under a new `## [X.Y.0] - YYYY-MM-DD` heading,
   and add its link at the bottom:
   `[X.Y.0]: https://github.com/Quadratic-Labs/cairndb/releases/tag/vX.Y.0`.
   Point `[Unreleased]` at `compare/vX.Y.0...HEAD`. Merge this through a
   pull request.
2. Cut the release branch from `main`. Pushing it publishes the docs:

   ```bash
   git switch main && git pull --ff-only
   git switch -c release/X.Y
   git push -u origin release/X.Y
   ```

3. Tag the release with a signed tag, then push the tag:

   ```bash
   git tag -s vX.Y.0 -m "CairnDB X.Y.0"
   git push origin vX.Y.0
   ```

4. Create the GitHub Release, using the changelog section as notes:

   ```bash
   V=X.Y.0
   awk -v v="$V" '$0 ~ "^## \\[" v "\\]" {f=1; next} /^## \[/ {f=0} f && !/^\[[^]]*\]: /' \
     CHANGELOG.md > /tmp/notes.md
   gh release create "v$V" --title "CairnDB $V" --notes-file /tmp/notes.md
   ```

### A patch release (X.Y.Z)

1. Land the fix on `main` first when it applies there, then cherry-pick
   it onto `release/X.Y`. A fix that only concerns the old line goes
   straight to the release branch.
2. On `release/X.Y`, bump the version to X.Y.Z, add the changelog entry
   and its link, commit, and push. The docs republish if this is still
   the latest release branch.
3. Tag `vX.Y.Z` on the release branch, push the tag, and create the
   GitHub Release as above.
4. Bring the changelog entry back to `main`, so `main`'s changelog lists
   every release.

### Artifacts

```bash
python -m build                  # sdist and wheel, into dist/
docker build -t cairndb-jobs .   # jobs image (cairndb CLI entrypoint)
```

Infrastructure templates for Azure live in `infra/`. See
[Azure](../deployment/azure.md).
