# Local and self-hosted

## Filesystem backend

The filesystem backend stores the bucket layout in a directory. It needs
no extra dependency:

```python
db = CairnDB.configure({"storage": {"type": "filesystem", "path": "/var/lib/myapp/cairndb"}})
```

```bash
CAIRNDB_STORAGE_TYPE=filesystem
CAIRNDB_STORAGE_PATH=/var/lib/myapp/cairndb
```

Put-if-absent is a write to a temp file followed by an atomic `os.link`.
Compare-and-swap takes an exclusive `flock` on a sibling `.{name}.lock`
file. The kernel releases the lock if its holder dies, so crashes never
leave stale locks. As a result, **many processes on one machine** can
share a directory safely. The test suite relies on exactly this to
simulate real contention.

Limits:

- POSIX only (Linux, macOS). No Windows.
- One machine. Do not share the directory over NFS, SMB, or similar
  unless you have verified `flock` and `link` semantics on that
  filesystem.
- Durability is your disk's durability. Back up the directory. Its
  contents are a plain file tree and copy well with `rsync` or restic.

The filesystem backend has no `prefix` option. To keep several
independent engines apart, give each its own directory.

## MinIO (S3-compatible, self-hosted)

Use a MinIO release with conditional-write support (late 2024 or newer):

```bash
docker run -d --name minio -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data --console-address :9001

# create the bucket (via the console on :9001, or the mc client)
docker run --rm --network host --entrypoint sh minio/mc -c \
  "mc alias set local http://localhost:9000 minioadmin minioadmin && mc mb local/myapp"
```

```bash
export CAIRNDB_STORAGE_TYPE=s3 \
       CAIRNDB_S3_BUCKET=myapp \
       CAIRNDB_S3_ENDPOINT_URL=http://localhost:9000 \
       AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
       AWS_DEFAULT_REGION=us-east-1

python -m cairndb.client.run        # the demo, now against MinIO
```

Before relying on any S3-compatible store, run a quick contention check:

```python
import asyncio
from cairndb import CairnDB

async def check():
    db = CairnDB.configure({"storage": {"type": "s3", "bucket": "myapp",
                                        "endpoint_url": "http://localhost:9000"}})
    await db.objects.delete("probe/key")
    results = await asyncio.gather(*[
        db.objects.put("probe/key", str(i).encode(), if_absent=True) for i in range(20)
    ])
    wins = sum(r is not None for r in results)
    assert wins == 1, f"{wins} writers won put-if-absent: conditional writes NOT enforced"
    print("conditional writes enforced")

asyncio.run(check())
```

## Running the jobs locally

### With cron

```text
# m h dom mon dow  command
0 3 * * *  cd /srv/myapp && CAIRNDB_STORAGE_TYPE=filesystem CAIRNDB_STORAGE_PATH=/var/lib/myapp/cairndb \
             .venv/bin/cairndb snapshot --handlers myapp.projections:registry
0 4 * * 0  cd /srv/myapp && CAIRNDB_STORAGE_TYPE=filesystem CAIRNDB_STORAGE_PATH=/var/lib/myapp/cairndb \
             .venv/bin/cairndb gc --keep-snapshots 3
```

### With systemd timers

```ini
# /etc/systemd/system/cairndb-snapshot.service
[Service]
Type=oneshot
User=myapp
WorkingDirectory=/srv/myapp
Environment=CAIRNDB_STORAGE_TYPE=filesystem
Environment=CAIRNDB_STORAGE_PATH=/var/lib/myapp/cairndb
ExecStart=/srv/myapp/.venv/bin/cairndb snapshot --handlers myapp.projections:registry

# /etc/systemd/system/cairndb-snapshot.timer
[Timer]
OnCalendar=*-*-* 03:00:00
Persistent=true

[Install]
WantedBy=timers.target
```

Enable it with `systemctl enable --now cairndb-snapshot.timer`. Create a
`cairndb-gc` pair the same way.

### With Docker

```bash
docker build -t cairndb-jobs .                   # from the cairndb repository
docker build -t myapp-jobs -f Dockerfile.jobs .  # your image, FROM cairndb-jobs (see Deployment overview)

docker run --rm \
  -e CAIRNDB_STORAGE_TYPE=filesystem -e CAIRNDB_STORAGE_PATH=/data \
  -v /var/lib/myapp/cairndb:/data \
  myapp-jobs snapshot --handlers myapp.projections:registry
```

The image runs as UID 1000 (`cairndb`). Make sure that user can write to
the mounted directory.

## Local development tips

- Every test can get its own `tmp_path` bucket. Concurrency tests on the
  filesystem backend exercise real races, with no mocks needed.
- Delete projection files freely. They are rebuilt from the log.
- Inspect state with `ls` and `cat`: documents are JSON, and commits are
  msgpack (`python -c "import msgpack,sys; print(msgpack.unpackb(open(sys.argv[1],'rb').read()))" data/log/000000000001.msgpack`).
