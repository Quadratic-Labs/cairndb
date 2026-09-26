"""CairnDB CLI: snapshot building, garbage collection, projection rebuild.

Storage is configured via CAIRNDB_* environment variables (see
StorageConfig.from_env). Every command acts on one log: the root log, or
the named log given by --log (or CAIRNDB_LOG). Application code (handlers,
schema initializer) is loaded from "module:attribute" references, e.g.::

    cairndb snapshot --log orders --handlers myapp.projections:registry \\
                      --init-schema myapp.projections:init_schema

Designed to run as scheduled serverless jobs (container entrypoint).
"""

import asyncio
import importlib
import sys
from typing import Any

try:
    import typer
except ImportError:
    print("CLI dependencies not installed. Install with: pip install cairndb[cli]")
    sys.exit(1)

import structlog

from cairndb.engine.logs import log_storage
from cairndb.storage.base import DEFAULT_SCHEMA_VERSION, BlobStorage
from cairndb.storage.config import StorageConfig

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.processors.JSONRenderer(),
    ]
)

logger = structlog.get_logger(__name__)

app = typer.Typer(
    name="cairndb",
    help="CairnDB jobs: snapshots, garbage collection, projection rebuild",
    no_args_is_help=True,
)


def _load_ref(ref: str):
    """Load 'module.path:attribute' and return the attribute."""
    try:
        module_path, attribute = ref.split(":", 1)
    except ValueError:
        raise typer.BadParameter(f"Expected 'module:attribute', got {ref!r}")

    module = importlib.import_module(module_path)
    try:
        return getattr(module, attribute)
    except AttributeError:
        raise typer.BadParameter(f"{module_path!r} has no attribute {attribute!r}")


def _log_option() -> Any:
    """A fresh --log option (Typer OptionInfo) for one command."""
    return typer.Option(
        None,
        "--log",
        envvar="CAIRNDB_LOG",
        help="Named log to operate on (logs/{name}/); the root log when omitted",
    )


def _storage_for(base: BlobStorage, log: str | None) -> BlobStorage:
    """The storage view of `log`, rejecting invalid log names as bad input."""
    try:
        return log_storage(base, log)
    except ValueError as e:
        raise typer.BadParameter(str(e), param_hint="--log") from None


@app.command()
def snapshot(
    handlers: str = typer.Option(
        ...,
        help="HandlerRegistry reference, e.g. 'myapp.projections:registry'",
    ),
    init_schema: str | None = typer.Option(
        None,
        help="Schema initializer reference (async callable taking a db path)",
    ),
    schema_version: str = typer.Option(
        DEFAULT_SCHEMA_VERSION,
        help="Projection schema version (snapshots/v{version}/ prefix); "
        "must equal the projection's version",
    ),
    end_at: int | None = typer.Option(None, help="Last commit to include (point-in-time snapshot)"),
    log: str | None = _log_option(),
) -> None:
    """Build a snapshot by replaying the commit log, and upload it."""
    from cairndb.jobs.snapshot import SnapshotBuilder

    registry = _load_ref(handlers)
    initializer = _load_ref(init_schema) if init_schema else None

    storage = _storage_for(StorageConfig.from_env().create_storage(), log)
    builder = SnapshotBuilder(
        storage, registry, init_schema=initializer, schema_version=schema_version
    )

    commit = asyncio.run(builder.build(end_at=end_at))
    where = f"log {log!r}" if log else "root log"
    typer.echo(f"Snapshot built through commit {commit} ({where}, schema v{schema_version})")


@app.command()
def gc(
    schema_version: str = typer.Option(DEFAULT_SCHEMA_VERSION, help="Projection schema version"),
    keep_snapshots: int = typer.Option(3, min=1, help="Snapshots to keep"),
    prune_log: bool = typer.Option(
        False,
        "--prune-log",
        help="Also delete commits covered by the oldest kept snapshot "
        "(destroys replayable history before it!)",
    ),
    log: str | None = _log_option(),
) -> None:
    """Apply retention policy to snapshots (and optionally the log)."""
    from cairndb.jobs.gc import collect_garbage

    storage = _storage_for(StorageConfig.from_env().create_storage(), log)

    result = asyncio.run(
        collect_garbage(
            storage,
            schema_version,
            keep_snapshots=keep_snapshots,
            prune_log=prune_log,
        )
    )
    typer.echo(
        f"GC done: {result.snapshots_deleted} snapshots and "
        f"{result.commits_deleted} commits deleted "
        f"(oldest kept snapshot: {result.oldest_kept_snapshot})"
    )


@app.command()
def rebuild(
    handlers: str = typer.Option(
        ...,
        help="HandlerRegistry reference, e.g. 'myapp.projections:registry'",
    ),
    init_schema: str | None = typer.Option(None, help="Schema initializer reference"),
    log: str | None = _log_option(),
) -> None:
    """Rebuild the local projection from the ledger (debug/ops).

    Client settings (db path, storage) come from CAIRNDB_* env vars.
    """
    from cairndb.client.config import ClientConfig
    from cairndb.client.projector import Projector

    registry = _load_ref(handlers)
    initializer = _load_ref(init_schema) if init_schema else None

    config = ClientConfig.from_env()
    storage = _storage_for(config.create_storage(), log)
    projector = Projector(config, storage, registry, init_schema=initializer)

    last = asyncio.run(projector.rebuild_from_scratch())
    typer.echo(f"Projection rebuilt at {config.db_path} (sequence: {last})")


if __name__ == "__main__":
    app()
