# Command-line interface

The `cairndb` command runs CairnDB's maintenance jobs. Install it with
the `cli` extra (`pip install "cairndb[cli]"`). It reads storage settings
from the [`CAIRNDB_*` environment variables](configuration.md#environment-variables).
It loads application code (handler registries, schema initializers) from
`module:attribute` references, so your package must be importable, for
example installed in the same environment or on `PYTHONPATH`.

Every command operates on **one log**: the root log, or the named log
given by `--log` or `CAIRNDB_LOG` (see
[Operations → Named logs](../guides/operations.md#named-logs)). The
output is structured JSON logs followed by a one-line summary. The exit
status is non-zero on failure.

```{eval-rst}
.. typer-cli:: cairndb.cli:app
   :prog: cairndb
```
