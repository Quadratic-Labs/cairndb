# Configuration

## Storage

Pass storage settings to `CairnDB.configure` as a dict with a `type`
discriminator, which defaults to `"filesystem"`:

```python
CairnDB.configure({"storage": {"type": "s3", "bucket": "myapp", "region": "eu-west-1"}})
CairnDB.configure(S3StorageConfig(bucket="myapp"))          # or a config object
CairnDB(storage)                                            # or an existing backend
```

Configs are validated when they are constructed: a config object that
exists is usable. An unknown `type` raises `ConfigurationError`, and
missing or unknown fields raise `ValueError`.

### `filesystem`

| Field | Type | Default | Meaning |
|---|---|---|---|
| `path` | str | *required* | root directory |

### `s3`

| Field | Type | Default | Meaning |
|---|---|---|---|
| `bucket` | str | *required* | bucket name |
| `prefix` | str | `""` | key prefix for everything this engine stores |
| `region` | str \| None | `None` | AWS region; boto3's default otherwise |
| `endpoint_url` | str \| None | `None` | custom endpoint for S3-compatible stores (MinIO, R2, LocalStack) |

Credentials come from boto3's default chain.

### `gcs`

| Field | Type | Default | Meaning |
|---|---|---|---|
| `bucket` | str | *required* | bucket name |
| `prefix` | str | `""` | key prefix |
| `project` | str \| None | `None` | GCP project id |
| `credentials_path` | str \| None | `None` | service-account JSON key; Application Default Credentials otherwise |

### `azure`

| Field | Type | Default | Meaning |
|---|---|---|---|
| `container` | str | *required* | blob container name |
| `prefix` | str | `""` | blob name prefix |
| `connection_string` | str \| None | `None` | full connection string |
| `account_url` | str \| None | `None` | `https://<account>.blob.core.windows.net`, authenticated with `DefaultAzureCredential` |

Exactly one of `connection_string` and `account_url` is required. If
both are given, `connection_string` wins.

## Environment variables

`StorageConfig.from_env()`, `ClientConfig.from_env()`, and the CLI read
these variables.

### Storage variables

| Variable | Backend | Maps to |
|---|---|---|
| `CAIRNDB_STORAGE_TYPE` | all | `type`: `filesystem` (default), `s3`, `gcs`, `azure` |
| `CAIRNDB_STORAGE_PATH` | filesystem | `path` |
| `CAIRNDB_STORAGE_PREFIX` | s3, gcs, azure | `prefix` |
| `CAIRNDB_S3_BUCKET` | s3 (and gcs, as a fallback) | `bucket` |
| `CAIRNDB_GCS_BUCKET` | gcs | `bucket` |
| `CAIRNDB_S3_REGION` | s3 | `region` |
| `CAIRNDB_S3_ENDPOINT_URL` | s3 | `endpoint_url` |
| `CAIRNDB_GCS_PROJECT` | gcs | `project` |
| `CAIRNDB_GCS_CREDENTIALS_PATH` | gcs | `credentials_path` |
| `CAIRNDB_AZURE_CONTAINER` | azure | `container` |
| `CAIRNDB_AZURE_CONNECTION_STRING` | azure | `connection_string` |
| `CAIRNDB_AZURE_ACCOUNT_URL` | azure | `account_url` |

For GCS, `CAIRNDB_GCS_BUCKET` wins. `CAIRNDB_S3_BUCKET` is read as a
fallback when it is unset.

### CLI variables

| Variable | Default | Meaning |
|---|---|---|
| `CAIRNDB_LOG` | unset (root log) | named log the jobs act on; same as `--log` |

### Client variables

These are used by `ClientConfig.from_env()` and `cairndb rebuild`:

| Variable | Default | Meaning |
|---|---|---|
| `CAIRNDB_DB_PATH` | `./projection.db` | local SQLite projection path |
| `CAIRNDB_POLL_INTERVAL` | `5.0` | seconds between polls, in (0, 3600] |
| `CAIRNDB_SCHEMA_VERSION` | `1` | projection schema version (`snapshots/v{version}/`) |

Provider SDK variables apply as usual, for example `AWS_PROFILE`,
`AWS_DEFAULT_REGION`, `GOOGLE_APPLICATION_CREDENTIALS`, and
`AZURE_CLIENT_ID`.

## Projection options

These are the arguments of `db.projection(name, ...)`:

| Argument | Default | Meaning |
|---|---|---|
| `log` | `None` (root log) | the named log to project |
| `version` | `"1"` | projection schema version; selects `snapshots/v{version}/` |
| `db_path` | `./{name}.v{version}.sqlite` | local SQLite file |
| `poll_interval` | `5.0` | seconds between background polls |
| `init_schema` | `None` | `async (db_path: str) -> None`, creating tables on a fresh file |
| `registry` | `None` (a fresh one) | an existing `HandlerRegistry` to replay with, e.g. the one the snapshot job loads |

Every component that names snapshots defaults to the same projection
schema version, `"1"`: `db.projection`, `ClientConfig`, `SnapshotBuilder`,
and the CLI. It is available as
`cairndb.storage.base.DEFAULT_SCHEMA_VERSION`.

## Client options

`ClientConfig` configures the lower-level
{class}`~cairndb.client.CairnDBClient`:

| Field | Default | Meaning |
|---|---|---|
| `storage` | `None` | a `StorageConfig` (or, in `from_dict`, a dict with `type`) |
| `db_path` | `./projection.db` | projection path |
| `poll_interval_seconds` | `5.0` | in (0, 3600] |
| `use_reflink` | `True` | copy-on-write copies during the atomic swap, when supported |
| `schema_version` | `"1"` | projection schema version |

## Committer options

`CommitterConfig` tunes the write path. Pass it to `Committer`, or to
`Log(..., committer_config=...)`:

| Field | Default | Range | Meaning |
|---|---|---|---|
| `max_events_per_commit` | `1000` | 1 – 100 000 | cap on events per commit object |
| `max_batch_wait_seconds` | `0.0` | 0 – 10 | linger before committing to grow batches; 0 still batches what arrives during a PUT |
| `max_commit_attempts` | `20` | ≥ 1 | attempts per batch before appends fail with `CommitError` |
| `lost_race_backoff_seconds` | `0.05` | 0 – 5 | backoff when a PUT was rejected but the winner is not visible yet |
| `tail_hint` | `0` | ≥ 0 | commits at or below this are known to exist; speeds up cold-start tail discovery |

## Coordination defaults

| Setting | Value |
|---|---|
| `doc.update(max_attempts=...)` | 10 |
| Lease `ttl` | required, > 0 seconds |
| Lease `steal_if_expired` | `True` |
