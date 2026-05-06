# Usage

## Setup

Two commands, run once per project. `braid init` creates the AWS resources and writes
`.braid/config.toml`; `braid sync` builds the dependency bundle from `uv.lock`.

### 1. Initialize

```bash
# Lambda (default)
uv run braid init --bucket my-bucket --region us-west-2 --create-role

# Batch — Fargate, no instance management
uv run braid init --bucket my-bucket --backend batch --create-role

# Batch — EC2, for GPUs or >16 vCPU / >120 GB
uv run braid init --bucket my-bucket --backend batch \
    --instance-types m8g.large,m8g.xlarge --create-role

# Spot pricing (Fargate Spot, or EC2 Spot with --instance-types)
uv run braid init --bucket my-bucket --backend batch --spot --create-role
```

The flags that matter:

| Flag | Description |
|------|-------------|
| `--bucket` | S3 bucket for layers and job artifacts (required) |
| `--region` | AWS region; auto-detected from your boto3 session |
| `--backend` | `lambda` (default) or `batch` |
| `--create-role` | Create the IAM execution role automatically |
| `--role-arn` | Use an existing execution role instead |

`--create-role` needs `iam:CreateRole` and `iam:PutRolePolicy` on your local credentials.
For EC2 Batch it also creates `ecsInstanceRole` if it doesn't exist.

Run `braid init --help`, or see the [reference](reference.md#braid-init-options), for the
remaining flags — worker sizing, networking, and custom images.

**Lambda creates** an SQS queue (`braid-results`), a Lambda function (`braid-worker`), and
`.braid/config.toml`.

**Batch creates** an SQS queue, a Fargate or EC2 compute environment and job queue
(`braid-queue`), an execution role (`braid-batch-execution-role`), and
`.braid/config.toml`.

### 2. Sync dependencies

```bash
uv run braid sync
```

Re-run after adding dependencies or modifying local packages. Not needed for code changes
— cloudpickle handles those at call time.

```bash
uv run braid sync --force          # rebuild even if the layer is cached
uv run braid sync --exclude numpy  # strip a package from the layer (repeatable)
```

On Lambda, `sync` cross-compiles your `uv.lock` for Amazon Linux and publishes it as a
layer, keyed by lockfile hash. On Batch it exports a `requirements.txt` that workers
install natively at job start — no cross-compilation and no size limit.

### Local / editable packages

If your project depends on local editable packages (`source = { editable = "../mylib" }`
in `uv.lock`), `braid sync` zips their source directories and uploads them automatically.
Workers download them and inject them into `sys.path` on cold start.

### Setup from Python

`init` and `sync` are also available as functions, useful in notebooks:

```python
import braid

config = braid.init(bucket="my-bucket", region="us-west-2", create_role=True)

braid.sync()              # equivalent to `braid sync`
braid.sync(force=True)    # rebuild even if cached
```

`braid.init` raises `ValueError` if neither `role_arn` nor `create_role=True` is given.
All output goes to the `braid` logger — set it to `INFO` to see progress.

### Joining an existing project

`.braid/config.toml` is committed to the repo, so there is nothing to initialize — just
sync your deps:

```bash
uv run braid sync
```

## Choosing a backend

| | Lambda | Batch Fargate | Batch EC2 |
|---|---|---|---|
| Timeout | ≤15 min | Unlimited | Unlimited |
| Memory | ≤10 GB | ≤120 GB | Instance size |
| vCPU | ≤6 | ≤16 | Instance size |
| Deps size | ≤250 MB layer | No limit | No limit |
| GPU | No | No | Yes |
| Spot | No | Yes (`--spot`) | Yes (`--spot`) |
| Cold start | ~1–5 s | ~30–60 s | ~2–5 min (fleet scale-up) |

**Lambda** — the default. Best for fast, lightweight tasks (seconds to minutes, under
10 GB RAM). Zero cluster overhead.

**Batch Fargate** — no instance management; AWS allocates capacity per task. Best for
medium tasks that exceed Lambda's limits or need an unlimited timeout. Fargate Spot
(`--spot`) is roughly 70% cheaper, with task-level interruption.

**Batch EC2** — you pick instance types. Required for GPUs or more than 16 vCPU / 120 GB.
AWS manages the fleet and ECS cluster — no AMI patching or cluster ops. EC2 Spot is
typically 60–90% cheaper than on-demand; pair it with `max_retries=2` so interruptions are
handled transparently.

If your workload needs non-Python system libraries (GDAL, HDF5, PROJ), use Batch with a
[custom base image](reference.md#heavy-dependencies-gdal-hdf5-proj).

The backend is set in `.braid/config.toml` and can be overridden per call:

```python
results = fan(fn, inputs, backend="batch")
```

## Dispatching jobs

```python
from braid import fan

results = fan(fn, inputs)
```

`fan` blocks until every result is collected, then returns them as a list in input order.
Any worker failure raises `WorkerError` with the remote traceback attached.

### Knobs

```python
results = fan(
    fn,
    inputs,
    batch_size=10,        # inputs per worker invocation
    timeout_s=300,        # how long to wait for results
    max_concurrency=200,  # max simultaneous invocations dispatched
    max_retries=2,        # retry failed batches
    cleanup=True,         # delete S3 job artifacts after collection
    backend="batch",      # override the configured backend for this call
)
```

Each knob falls back to the corresponding `.braid/config.toml` value when omitted.
`timeout_s` bounds how long `fan` waits for results; it does not change the Lambda
function's own timeout, which is set by `braid init --lambda-timeout-s`.

### Static arguments

`fan` maps one argument. For constants shared by every call, use `functools.partial`:

```python
from functools import partial

results = fan(partial(fn, model_path=path), inputs)
```

### Dry run

Serialize the function and one input, print the payload sizes, and return without
invoking anything:

```python
fan(fn, inputs, dry_run=True)
```

### Streaming results

`fan_iter` yields `(original_index, result)` pairs as results arrive rather than waiting
for the full set:

```python
from braid import fan_iter

for idx, result in fan_iter(fn, inputs):
    print(idx, result)
```

Lambda backend only. No retry support — any failure raises `WorkerError` immediately.

## Tuning `batch_size`

`batch_size` sets how many inputs each worker invocation processes, which determines how
many run in parallel:

```
invocations = ceil(len(inputs) / batch_size)
```

The default is `10`. braid caps it at `len(inputs)`, so mapping over 3 inputs always
dispatches 3 invocations regardless of the configured default.

Smaller batches mean more parallelism but more cold-start overhead (each worker pays
~2–5 s to import your dependencies on first call). Larger batches mean the reverse.

| Per-item duration | Recommended `batch_size` |
|---|---|
| <5 s | 1–5 |
| 5–30 s | 5–20 |
| >30 s | 20–50 |

**Concurrency limits.** AWS accounts have a default burst limit of 1000 concurrent Lambda
executions; exceeding it causes throttling, where invocations queue instead of running in
parallel. At `batch_size=10` with 10,000 inputs that's exactly 1000 invocations, right at
the limit. For any workload over ~800 invocations, cap it:

```python
results = fan(fn, inputs, batch_size=10, max_concurrency=800)
```

**On the Batch backend, `batch_size` and `max_concurrency` are ignored.** A Batch call
submits one array job with one task per input — AWS Batch schedules concurrency itself.
Instead, set `timeout_s` (or `collect_timeout_s` in config) generously: cold start alone
takes 30–60 s on Fargate, several minutes on EC2 while the fleet scales up, so a
Lambda-sized timeout will expire before the first result lands.

## Detached jobs

`fan` blocks until every result is collected, and terminates the underlying job if
`timeout_s` expires. That's wrong for long-running Batch jobs (hours) where the client
shouldn't need to stay connected, and a laptop going to sleep shouldn't kill work that's
progressing fine on the server.

`submit` returns a handle immediately instead of blocking. Results are written to S3, not
SQS, so collection is non-destructive — call `.result()` from any process, any number of
times, including after resuming from a job_id saved earlier:

```python
from braid import submit, attach

handle = submit(fn, inputs)     # returns immediately; Batch backend only
print(handle.job_id)

handle.status()                 # {done, failed, pending, batch_state, complete} — no downloads
handle.wait(poll_interval_s=5)  # blocks until complete, still no downloads

results = handle.result()       # blocks until complete, then downloads everything
```

Reattach from another process (or after restarting) with the `job_id`:

```python
handle = attach(job_id)
results = handle.result(timeout_s=3600)
```

`result(timeout_s=...)` raises `JobTimeoutError` on expiry but **leaves the Batch job
running** — unlike `fan`, which terminates on timeout. Call `.cancel()` to stop a job
explicitly, and `.clean()` to delete its S3 artifacts once you're done with it; neither
happens automatically.

A job that stops without producing every result — spot reclaim, OOM, container crash —
raises `JobFailedError` instead, carrying the terminal `batch_state`. AWS drops completed
jobs from `describe_jobs` after roughly 24 hours, so `status()` on an old job reports
`batch_state="UNKNOWN"`; results already in S3 still collect normally, but an *incomplete*
job that has aged out also raises `JobFailedError` rather than polling forever.

Use `fan` for anything that finishes in minutes and fits in one blocking call. Reach for
`submit`/`attach` once a job needs to outlive the process that started it.

## Running scripts on Batch

`fan`/`submit` map a function over inputs. `braid batch run` is different: it submits a
whole Python **script** as a single fire-and-forget Batch job — like `coiled batch run`.
There is no map, no manifest, and no result collection; the job's outcome is its exit
code.

```bash
uv run braid batch run train.py
```

This prints both ids and returns immediately:

```
Submitted: train.py
  braid job id: 3f9c1e2a-...
  batch job id: 8b7a...
Monitor: braid status 3f9c1e2a-...   |   braid jobs --watch
Cancel:  braid cancel 8b7a...
```

Requires a project already `braid init`'d with the Batch backend (or one that has Batch
resources even if `backend = "lambda"`) — `batch_execution_role_arn` must be set.

### Dependencies: PEP 723 vs. project env

`braid batch run` auto-detects how to install dependencies from the script itself:

- **PEP 723 inline metadata** (a `# /// script` ... `# ///` header) — deps run in an
  isolated environment via `uv run`, independent of your project's `uv.lock`:

  ```python
  # /// script
  # dependencies = ["requests", "numpy"]
  # ///
  import requests, numpy
  ...
  ```

- **No header** — the script runs against your project's environment: `braid sync`'s
  `uv.lock` export and any editable packages are synced first, same as `fan`/`submit`.

### Monitoring and control

A script job is a single (non-array) Batch job, so the existing tooling works unchanged:

```bash
uv run braid status <job_id>          # done/failed/pending — done=1 means exit code 0
uv run braid jobs --watch             # recent jobs, AWS state, cost estimate
uv run braid cancel <batch_job_id>    # terminate a running job
uv run braid clean <job_id>           # delete its S3 artifacts
```

### Resource overrides and passthrough env vars

```bash
uv run braid batch run train.py \
    --vcpu 4 --memory 16384 --disk 50 \
    --instance-type m8g.xlarge \
    --env WANDB_PROJECT=my-project --env EPOCHS=10
```

`--instance-type` (repeatable) implies EC2 mode for this job. `--env` (repeatable) passes
`KEY=VALUE` pairs straight through to the script's environment.

### Mental model

Keep `fan`/`submit` and `batch run` separate in your head: `fan`/`submit` parallelize one
function over many inputs and give you results back; `batch run` runs one script to
completion and gives you an exit code. Don't reach for `batch run` expecting `.result()` —
there isn't one. Use `fan`/`submit` for anything shaped like a map.

## Configuration

`braid init` writes `.braid/config.toml`. The values that identify your resources:

```toml
region = "us-west-2"
bucket = "my-bucket"
sqs_queue_url = "https://sqs.us-west-2.amazonaws.com/123456789/braid-results"
worker_function_name = "braid-worker"
backend = "lambda"
```

Everything else has a default. Runtime defaults set here (`batch_size`,
`collect_timeout_s`, `max_concurrency`, `max_retries`) are overridden by the matching
argument to `fan`. See the [reference](reference.md#config-fields) for the full list, or
[`Config`](api.md#braid.config.Config) for the authoritative field definitions.

## Workflow

```
first time         →  braid init && braid sync
add dependency     →  braid sync
change local pkg   →  braid sync
code change only   →  nothing (cloudpickle handles it at call time)
```

`fan` skips the layer/environment update step when nothing has changed since the last
call, so repeated calls in one session start dispatching immediately.
