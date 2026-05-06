# Reference

Complete flag, field, and permission lists. For the common path, start with
[Usage](usage.md).

## CLI

```
braid init                        Create AWS resources and write config
braid sync                        Build dep layer from uv.lock and deploy worker
braid sync --force                Rebuild layer even if cached
braid sync --exclude PKG          Strip a package from the layer (repeatable)
braid status                      Check backend resources and SQS are reachable
braid layers                      List cached layer ARNs
braid dry-run                     Serialize a function and print payload sizes
braid clean          <job_id>     Delete S3 artifacts for a completed job
braid jobs                        Show recent Batch jobs and live AWS status
braid jobs --watch                Auto-refresh every 10 s (shows accruing cost)
braid cancel  <batch_job_id>      Terminate a running or pending Batch job
```

`braid --verbose <command>` raises the log level to DEBUG.

### `braid init` options

| Flag | Default | Description |
|------|---------|-------------|
| `--bucket` | required | S3 bucket for layers and job artifacts |
| `--bucket-prefix` | `""` | Key prefix within the bucket |
| `--region` | auto-detected | AWS region |
| `--backend` | `lambda` | `lambda` or `batch` |
| `--arch` | `x86_64` | `x86_64` or `arm64` |
| `--role-arn` | prompted | IAM execution role ARN |
| `--create-role` | — | Create the execution role automatically |
| `--extra-bucket` | — | Additional S3 bucket the worker role needs (repeatable) |
| **Lambda only** | | |
| `--memory-mb` | `512` | Lambda memory in MB (128–10240) |
| `--lambda-timeout-s` | `300` | Per-invocation timeout in seconds (max 900) |
| **Batch only** | | |
| `--batch-vcpu` | `2.0` | vCPU per task |
| `--batch-memory-mb` | `4096` | Memory per task (MB) |
| `--instance-types` | Fargate | Comma-separated EC2 instance types (e.g. `m8g.large,m8g.xlarge`); omit for Fargate |
| `--spot` | off | Spot pricing (Fargate Spot, or EC2 Spot with `--instance-types`) |
| `--batch-image` | uv slim | Custom base container image |
| `--subnet-ids` | auto-detected | Comma-separated subnet IDs |
| `--security-group-ids` | auto-detected | Comma-separated security group IDs |

Subnets and security groups are auto-detected from your default VPC. Pass them explicitly
if you have no default VPC — `braid init` will tell you when that happens.

Resource names (`worker_function_name`, `batch_job_queue`, `batch_job_definition`) are not
CLI flags. Set them in `.braid/config.toml`, or via
[`braid.init`](api.md#braid.api.init) from Python.

## Config fields

`.braid/config.toml`, written by `braid init`. See
[`Config`](api.md#braid.config.Config) for types and validation bounds.

```toml
region = "us-west-2"
bucket = "my-bucket"
bucket_prefix = ""               # key prefix within the bucket
sqs_queue_url = "https://sqs.us-west-2.amazonaws.com/123456789/braid-results"
arch = "x86_64"                  # x86_64 or arm64
python_version = "3.13"          # must match your worker image, if custom
backend = "lambda"               # or "batch"

# Runtime defaults, overridable per fan call
batch_size = 10
collect_timeout_s = 300          # how long fan waits for results
max_concurrency = 200
max_retries = 2
exclude_packages = []            # packages to strip from the layer

# Lambda only
worker_function_name = "braid-worker"
memory_mb = 512                  # 128–10240
lambda_timeout_s = 300           # the Lambda function's own timeout, max 900

# Batch only
batch_job_queue = "braid-queue"
batch_job_definition = "braid-job"
batch_vcpu = 2.0
batch_memory_mb = 4096
batch_ephemeral_storage_gib = 21        # 21–200
batch_image = ""                        # custom base image; default is the uv slim image
batch_instance_types = []               # empty = Fargate; set = EC2
batch_execution_role_arn = ""
batch_subnet_ids = []                   # auto-detected by init
batch_security_group_ids = []           # auto-detected by init
```

`region`, `bucket`, and `sqs_queue_url` can be overridden by the `BRAID_REGION`,
`BRAID_BUCKET`, and `BRAID_SQS_QUEUE_URL` environment variables.

### Two timeouts

`lambda_timeout_s` is how long a single Lambda invocation may run, capped by AWS at 900 s.
`collect_timeout_s` is how long `fan` waits for all results to arrive, and is what
`fan(timeout_s=...)` overrides. `braid init` seeds both from `--lambda-timeout-s` on
Lambda; on Batch, where there is no invocation limit, `collect_timeout_s` defaults to
3600.

## Using existing AWS resources

If you already have a Lambda function and SQS queue, skip `braid init` and write
`.braid/config.toml` directly:

```toml
region = "us-west-2"
bucket = "my-bucket"
sqs_queue_url = "https://sqs.us-west-2.amazonaws.com/123456789012/my-queue"
worker_function_name = "my-existing-function"
```

Then run `braid sync`. braid will not create or modify the function itself, only update
its layer and environment.

## IAM and auth

braid uses the standard boto3 credential chain — `aws configure`, environment variables,
or an instance/SSO profile. Two sets of permissions matter: the **execution role** the
workers assume, and the **local credentials** you run `braid init` / `braid sync` with.
`braid init --create-role` creates the execution role for you; the policies below are what
it attaches.

??? note "Lambda execution role"

    ```json
    {
      "Version": "2012-10-17",
      "Statement": [
        {
          "Effect": "Allow",
          "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"],
          "Resource": ["arn:aws:s3:::my-bucket", "arn:aws:s3:::my-bucket/*"]
        },
        {
          "Effect": "Allow",
          "Action": ["sqs:SendMessage"],
          "Resource": "arn:aws:sqs:us-west-2:123456789:braid-results"
        }
      ]
    }
    ```

    Plus `AWSLambdaBasicExecutionRole` for CloudWatch logs.

??? note "Batch execution role"

    Trust policy: `ecs-tasks.amazonaws.com`.

    ```json
    {
      "Version": "2012-10-17",
      "Statement": [
        {
          "Effect": "Allow",
          "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"],
          "Resource": ["arn:aws:s3:::my-bucket", "arn:aws:s3:::my-bucket/*"]
        },
        {
          "Effect": "Allow",
          "Action": ["sqs:SendMessage"],
          "Resource": "arn:aws:sqs:us-west-2:123456789:braid-results"
        },
        {
          "Effect": "Allow",
          "Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
          "Resource": "*"
        }
      ]
    }
    ```

    Plus `AmazonECSTaskExecutionRolePolicy`.

??? note "Local credentials"

    For Lambda:

    - `lambda:CreateFunction`, `lambda:InvokeFunction`, `lambda:UpdateFunctionConfiguration`,
      `lambda:UpdateFunctionCode`, `lambda:GetFunctionConfiguration`,
      `lambda:PublishLayerVersion`
    - `iam:PassRole` (to assign the role to the Lambda function)
    - `iam:CreateRole`, `iam:PutRolePolicy`, `iam:AttachRolePolicy` (only with `--create-role`)
    - `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject`, `s3:ListBucket` on your bucket
    - `sqs:CreateQueue`, `sqs:SendMessage`, `sqs:ReceiveMessage`, `sqs:DeleteMessage`

    For Batch, additionally:

    - `batch:CreateComputeEnvironment`, `batch:CreateJobQueue`, `batch:RegisterJobDefinition`,
      `batch:SubmitJob`, `batch:DescribeJobs`, `batch:DescribeJobQueues`
    - `ec2:DescribeVpcs`, `ec2:DescribeSubnets`, `ec2:DescribeSecurityGroups`
    - `iam:CreateRole`, `iam:PutRolePolicy`, `iam:AttachRolePolicy` (only with `--create-role`)

## Heavy dependencies (GDAL, HDF5, PROJ)

Lambda layers are cross-compiled locally from pure Python and manylinux wheels. Workloads
that need non-Python system libraries can't be bundled that way — use Batch with a
purpose-built base image, and let braid layer your `uv.lock` deps on top at container
start.

braid's default base (`ghcr.io/astral-sh/uv:python3.13-bookworm-slim`) covers most
workloads. For a hard system dependency, add `uv` to the vendor's image:

```dockerfile
FROM ghcr.io/osgeo/gdal:ubuntu-small-latest

# Add uv — no curl/wget needed
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# uv manages its own Python — no system Python required.
# Version must match python_version in .braid/config.toml (default: 3.13).
RUN uv python install 3.13
```

Use `ghcr.io/osgeo/gdal:ubuntu-full-latest` if you also need HDF5/NetCDF. Push to ECR and
point braid at it:

```bash
docker build -t my-gdal-worker .
docker tag my-gdal-worker 123456789012.dkr.ecr.us-west-2.amazonaws.com/my-gdal-worker:latest
aws ecr get-login-password | docker login --username AWS \
    --password-stdin 123456789012.dkr.ecr.us-west-2.amazonaws.com
docker push 123456789012.dkr.ecr.us-west-2.amazonaws.com/my-gdal-worker:latest

uv run braid init --bucket my-bucket --backend batch \
    --batch-image 123456789012.dkr.ecr.us-west-2.amazonaws.com/my-gdal-worker:latest \
    --create-role
```

Rebuild the image only when system libraries change — Python dependency changes are still
handled from `uv.lock` at runtime.

## Limits

| Limit | Value | Raised as | Workaround |
|---|---|---|---|
| Lambda layer size | 250 MB unzipped | `LayerBuildError` | Switch to Batch |
| Function payload | 200 KB | `PayloadTooLargeError` | Move large objects to S3, pass keys instead |
| Lambda timeout | 900 s | `JobTimeoutError` | Switch to Batch — no timeout limit |
| Lambda concurrency | Account burst limit, default 1000 | throttling | Set `max_concurrency` below the limit |

Batch has no timeout or dependency-size limit and scales independently of your Lambda
concurrency quota.
