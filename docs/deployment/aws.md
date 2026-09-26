# AWS

On AWS, the bucket is an **S3** bucket, your processes run wherever you
already run Python (ECS/Fargate, Lambda, EC2, EKS), and the jobs run as
scheduled tasks.

```bash
pip install "cairndb[s3]"      # boto3 >= 1.35 (conditional writes)
```

## 1. Create the bucket

```bash
aws s3api create-bucket --bucket myapp-cairndb --region eu-west-1 \
  --create-bucket-configuration LocationConstraint=eu-west-1
aws s3api put-bucket-versioning --bucket myapp-cairndb \
  --versioning-configuration Status=Enabled
aws s3api put-public-access-block --bucket myapp-cairndb \
  --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
```

Amazon S3 supports the conditional writes CairnDB needs, `If-None-Match`
and `If-Match` on PUT, natively on general-purpose buckets. S3 Express
One Zone directory buckets give single-digit-millisecond latency, but
check that they support the conditional operations your version needs
before switching.

## 2. Grant access

Attach a policy like this to the role your processes and jobs use (task
role, Lambda execution role, or instance profile). Scope it to a prefix
if several applications share a bucket:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      "Resource": "arn:aws:s3:::myapp-cairndb/*"
    },
    {
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::myapp-cairndb"
    }
  ]
}
```

boto3 picks up credentials from its default chain: the task role, the
execution role, the instance profile, or `AWS_*` environment variables.
CairnDB needs no keys in its own configuration.

## 3. Configure your application

```python
db = CairnDB.configure({"storage": {
    "type": "s3",
    "bucket": "myapp-cairndb",
    "region": "eu-west-1",
    "prefix": "prod",           # optional: several engines in one bucket
}})
```

or, from the environment:

```bash
CAIRNDB_STORAGE_TYPE=s3
CAIRNDB_S3_BUCKET=myapp-cairndb
CAIRNDB_S3_REGION=eu-west-1
CAIRNDB_STORAGE_PREFIX=prod
```

### Lambda notes

- Keep one `CairnDB` instance per execution environment, created outside
  the handler, and reuse it across invocations.
- Projections write to local disk. Point `db_path` at `/tmp` and size
  ephemeral storage for your projection plus a copy (the atomic swap
  briefly needs both).
- `append()` returns only after the commit is durable. Await it before
  the handler returns: a frozen environment cannot finish a pending
  write.
- Call `await proj.refresh()` at the start of a request that reads. Do not
  rely on `proj.start()`: background tasks do not run while the
  environment is frozen.

## 4. Schedule the jobs

Push your jobs image (see
[The jobs image](index.md#the-jobs-image)) to ECR, create an ECS task
definition that runs it on Fargate, and trigger it with **EventBridge
Scheduler**:

```bash
aws ecr create-repository --repository-name myapp-jobs
docker tag myapp-jobs:latest <account>.dkr.ecr.eu-west-1.amazonaws.com/myapp-jobs:latest
docker push <account>.dkr.ecr.eu-west-1.amazonaws.com/myapp-jobs:latest
```

Task definition excerpt (one per job, or override `command` per schedule):

```json
{
  "family": "cairndb-snapshot",
  "requiresCompatibilities": ["FARGATE"],
  "networkMode": "awsvpc",
  "cpu": "512",
  "memory": "1024",
  "taskRoleArn": "arn:aws:iam::<account>:role/cairndb-jobs",
  "executionRoleArn": "arn:aws:iam::<account>:role/ecsTaskExecutionRole",
  "containerDefinitions": [{
    "name": "snapshot",
    "image": "<account>.dkr.ecr.eu-west-1.amazonaws.com/myapp-jobs:latest",
    "command": ["snapshot", "--handlers", "myapp.projections:registry",
                "--init-schema", "myapp.projections:init_schema"],
    "environment": [
      {"name": "CAIRNDB_STORAGE_TYPE", "value": "s3"},
      {"name": "CAIRNDB_S3_BUCKET", "value": "myapp-cairndb"},
      {"name": "CAIRNDB_S3_REGION", "value": "eu-west-1"}
    ],
    "logConfiguration": {"logDriver": "awslogs", "options": {
      "awslogs-group": "/cairndb/jobs", "awslogs-region": "eu-west-1",
      "awslogs-stream-prefix": "snapshot"}}
  }]
}
```

Then schedule it, for example nightly at 03:00 UTC with EventBridge
Scheduler (`cron(0 3 * * ? *)`), with an ECS target on your cluster, the
task definition, and a Fargate network configuration. Create a GC schedule
the same way (`cron(0 4 ? * SUN *)`), with the command `["gc",
"--keep-snapshots", "3"]`. For a named log, append `"--log", "orders"` to
both commands, or set `CAIRNDB_LOG` in the container environment.

Snapshot and GC jobs are idempotent, so EventBridge's at-least-once
delivery and retries are harmless.

## Costs to expect

With no idle compute, a typical small deployment costs:

- S3 storage for the log and snapshots, plus request charges: one PUT per
  commit, and one GET per poll per client.
- A few minutes of Fargate per day for the jobs.

A client polling every 5 seconds makes about 520,000 GETs a month. At S3
standard GET pricing, that is well under a dollar. Lengthen the poll
interval if you have many idle clients.
