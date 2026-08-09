# CairnDB jobs image: runs `cairndb snapshot` / `cairndb gc` as
# scheduled serverless jobs. There is no server to run.

# ---- Builder stage ----
FROM python:3.14-slim AS builder

WORKDIR /build

RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential && \
    rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md ./
COPY src/ src/

RUN pip install --no-cache-dir build && \
    python -m build --wheel --outdir /build/dist

# ---- Runtime stage ----
FROM python:3.14-slim

WORKDIR /app

# Install the wheel with CLI + all storage backends
COPY --from=builder /build/dist/*.whl /tmp/
RUN pip install --no-cache-dir /tmp/*.whl'[cli,s3,gcs,azure]' && \
    rm -f /tmp/*.whl

# Create non-root user
RUN groupadd --gid 1000 cairndb && \
    useradd --uid 1000 --gid cairndb --create-home cairndb

USER cairndb

# Handlers for snapshot builds are provided by the deployment:
# mount/install the application package and set --handlers accordingly,
# e.g. CMD ["cairndb", "snapshot", "--handlers", "myapp.projections:registry"]
ENTRYPOINT ["cairndb"]
CMD ["--help"]
