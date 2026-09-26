# Google Cloud

On Google Cloud, the bucket is a **Cloud Storage** bucket, your processes
run on Cloud Run, GKE, Compute Engine, or Cloud Functions, and the jobs
run as **Cloud Run jobs** triggered by **Cloud Scheduler**.

```bash
pip install "cairndb[gcs]"
```

GCS enforces CairnDB's preconditions natively. Put-if-absent is
`if_generation_match=0`, and compare-and-swap is `if_generation_match`
on the object's current generation, which serves as its etag.

## 1. Create the bucket

```bash
gcloud storage buckets create gs://myapp-cairndb \
  --location=europe-west1 --uniform-bucket-level-access --public-access-prevention
gcloud storage buckets update gs://myapp-cairndb --soft-delete-duration=7d   # or --versioning
```

## 2. Grant access

Create a service account for your workloads and grant it object access
on the bucket:

```bash
gcloud iam service-accounts create cairndb
gcloud storage buckets add-iam-policy-binding gs://myapp-cairndb \
  --member=serviceAccount:cairndb@<project>.iam.gserviceaccount.com \
  --role=roles/storage.objectUser
```

Attach the service account to your Cloud Run services and jobs, GKE
workloads (Workload Identity), or VMs. CairnDB then authenticates through
Application Default Credentials. Use a key file (`credentials_path`) only
outside Google Cloud.

## 3. Configure your application

```python
db = CairnDB.configure({"storage": {
    "type": "gcs",
    "bucket": "myapp-cairndb",
    "project": "<project>",     # optional
    "prefix": "prod",           # optional
}})
```

or, from the environment:

```bash
CAIRNDB_STORAGE_TYPE=gcs
CAIRNDB_GCS_BUCKET=myapp-cairndb
CAIRNDB_GCS_PROJECT=<project>
CAIRNDB_STORAGE_PREFIX=prod
# CAIRNDB_GCS_CREDENTIALS_PATH=/path/key.json   # only outside Google Cloud
```

`CAIRNDB_S3_BUCKET` is also accepted as a fallback for the GCS bucket
when `CAIRNDB_GCS_BUCKET` is unset.

### Cloud Run services

- Projections live on the instance's in-memory filesystem. Budget memory
  for the projection plus one copy during the atomic swap, or mount a
  volume.
- With CPU allocated only during requests, background polling
  (`proj.start()`) stalls between requests. Call `await proj.refresh()`
  in requests that read, or enable always-on CPU.

## 4. Schedule the jobs

Push your jobs image (see [The jobs image](index.md#the-jobs-image)) to
Artifact Registry, and create one Cloud Run job per task:

```bash
gcloud artifacts repositories create cairndb --repository-format=docker --location=europe-west1
docker tag myapp-jobs europe-west1-docker.pkg.dev/<project>/cairndb/myapp-jobs:latest
docker push europe-west1-docker.pkg.dev/<project>/cairndb/myapp-jobs:latest

gcloud run jobs create cairndb-snapshot \
  --region=europe-west1 \
  --image=europe-west1-docker.pkg.dev/<project>/cairndb/myapp-jobs:latest \
  --service-account=cairndb@<project>.iam.gserviceaccount.com \
  --set-env-vars=CAIRNDB_STORAGE_TYPE=gcs,CAIRNDB_GCS_BUCKET=myapp-cairndb \
  --args=snapshot,--handlers,myapp.projections:registry,--init-schema,myapp.projections:init_schema \
  --memory=1Gi --task-timeout=30m --max-retries=1

gcloud run jobs create cairndb-gc \
  --region=europe-west1 \
  --image=europe-west1-docker.pkg.dev/<project>/cairndb/myapp-jobs:latest \
  --service-account=cairndb@<project>.iam.gserviceaccount.com \
  --set-env-vars=CAIRNDB_STORAGE_TYPE=gcs,CAIRNDB_GCS_BUCKET=myapp-cairndb \
  --args=gc,--keep-snapshots,3
```

Trigger them on a schedule with Cloud Scheduler. The scheduler's service
account needs `roles/run.invoker` on the jobs:

```bash
gcloud scheduler jobs create http cairndb-snapshot-nightly \
  --location=europe-west1 --schedule="0 3 * * *" --time-zone=UTC \
  --http-method=POST \
  --uri="https://run.googleapis.com/v2/projects/<project>/locations/europe-west1/jobs/cairndb-snapshot:run" \
  --oauth-service-account-email=scheduler@<project>.iam.gserviceaccount.com
```

Create the GC trigger the same way (for example, `--schedule="0 4 * * 0"`).
