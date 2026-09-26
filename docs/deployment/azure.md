# Azure

On Azure, the bucket is a **Blob Storage** container, and the jobs run
as scheduled **Container Apps jobs**. The repository ships Bicep templates
in `infra/` that provision everything except your application.

```bash
pip install "cairndb[azure]"     # azure-storage-blob + azure-identity
```

Blob Storage enforces CairnDB's preconditions natively, through
`If-None-Match: *` and `If-Match` etags. Accounts with a hierarchical
namespace (ADLS Gen2) are supported too.

## What the templates deploy

`infra/main.bicep` creates:

| Resource | Purpose |
|---|---|
| User-assigned managed identity | the identity the jobs run as |
| Storage account (`Standard_LRS`, TLS 1.2, no public access) + container | the bucket; the identity gets **Storage Blob Data Contributor** |
| Key Vault (RBAC) | holds the storage connection string; the identity gets **Key Vault Secrets User** |
| Container Registry (Basic) | hosts the jobs image; the identity gets **AcrPull** |
| Container Apps environment + Log Analytics | runs the jobs and collects their logs |
| Container Apps jobs `{baseName}-snapshot[-{log}]` | `cairndb snapshot`, one per entry of `snapshotLogs`, on a cron schedule (default `0 3 * * *` UTC) |
| Container Apps jobs `{baseName}-gc[-{log}]` | `cairndb gc --keep-snapshots N`, one per entry of `snapshotLogs` (default `0 4 * * 0` UTC) |

There are no long-running services. Your application processes are not
part of the template: deploy them however you like, and give them access
to the storage account.

## Parameters

Edit `infra/main.bicepparam`:

| Parameter | Default | Meaning |
|---|---|---|
| `baseName` | `cairndb` | prefix for every resource name. Storage account and registry names must be globally unique, so change it |
| `imageTag` | `latest` | tag of `{acr}/cairndb` the jobs run |
| `blobContainerName` | `cairndb-data` | the container that holds the database |
| `snapshotLogs` | the root log, with the demo handlers | one entry per log with projections: `{ log, handlers, initSchema }` (see below) |
| `schemaVersion` | `1` | projection schema version passed to both jobs; must equal your projections' `version` |
| `snapshotCron` | `0 3 * * *` | snapshot schedule (UTC) |
| `gcCron` | `0 4 * * 0` | GC schedule (UTC) |
| `gcKeepSnapshots` | `3` | snapshots GC keeps |

Each `snapshotLogs` entry gets its own snapshot job and GC job:

```text
param snapshotLogs = [
  {
    log: ''                                        // '' = the root log
    handlers: 'myapp.projections:registry'         // HandlerRegistry (module:attribute)
    initSchema: 'myapp.projections:init_schema'    // creates the projection tables
  }
  {
    log: 'orders'                                  // named log: runs the jobs with --log orders
    handlers: 'myapp.projections:orders_registry'
    initSchema: 'myapp.projections:init_orders'
  }
]
```

The log name becomes part of the job names (`{baseName}-snapshot-orders`).
Azure resource names allow only lowercase letters, digits, and `-`, so
named logs deployed through these templates must stick to those
characters, even though CairnDB also accepts `.` and `_` in log names.
Job names are also limited to 32 characters, so keep
`{baseName}-snapshot-{log}` within that.

## Deploy

```bash
az group create --name myapp-rg --location westeurope

# 1. Infrastructure (creates the registry the image goes to)
az deployment group create \
  --resource-group myapp-rg \
  --template-file infra/main.bicep \
  --parameters infra/main.bicepparam \
  --parameters baseName=myappdb

# 2. The jobs image, with your handlers installed (see "The jobs image").
#    The jobs expect it as {acr}/cairndb:{imageTag}.
ACR=$(az deployment group show -g myapp-rg -n main --query properties.outputs.acrLoginServer.value -o tsv)
az acr login --name "${ACR%%.*}"
docker build -t "$ACR/cairndb:latest" -f Dockerfile.jobs .
docker push "$ACR/cairndb:latest"

# 3. Optional: run a job now instead of waiting for its schedule
az containerapp job start --resource-group myapp-rg --name myappdb-snapshot
```

The jobs receive their configuration as environment variables:
`CAIRNDB_STORAGE_TYPE=azure`, `CAIRNDB_AZURE_CONTAINER`, and
`CAIRNDB_AZURE_CONNECTION_STRING`, the last one pulled from Key Vault
through the managed identity. Inspect runs with `az containerapp job
execution list` and in Log Analytics.

## Configure your application

Prefer **managed identity** over connection strings for application
processes. Grant their identity *Storage Blob Data Contributor* on the
storage account, then:

```python
db = CairnDB.configure({"storage": {
    "type": "azure",
    "container": "cairndb-data",
    "account_url": "https://<account>.blob.core.windows.net",   # DefaultAzureCredential
}})
```

or, from the environment:

```bash
CAIRNDB_STORAGE_TYPE=azure
CAIRNDB_AZURE_CONTAINER=cairndb-data
CAIRNDB_AZURE_ACCOUNT_URL=https://<account>.blob.core.windows.net
AZURE_CLIENT_ID=<client id of a user-assigned identity>     # when using one
```

With `account_url`, CairnDB authenticates through `DefaultAzureCredential`:
a managed identity in Azure, and your `az login` session on a
workstation. `connection_string` also works, but it embeds the account
key.

## Testing against real Azure

The repository has a live integration suite that runs against an
existing container:

```bash
export CAIRNDB_AZURE_CONTAINER=<container>
export CAIRNDB_AZURE_CONNECTION_STRING='...'     # or CAIRNDB_AZURE_ACCOUNT_URL
make azure-integration                           # or: make azure-integration AZURE_ENV=path/to/envfile
```
