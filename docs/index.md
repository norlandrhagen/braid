# braid

Parallel map over AWS Lambda and AWS Batch, with automatic dependency sync via `uv`.

```python
from braid import fan

results = fan(fn, inputs, batch_size=10)
```


!!! warning "Experimental"
    APIs may change without notice.

## How it works

- Your function is serialized with cloudpickle and uploaded to S3
- Inputs are dispatched across workers in batches
- Workers send results back via SQS (inline for small results, S3 for large)
- Results are collected in order and returned as a list

**Dependency sync (Lambda)** — `braid sync` reads your `uv.lock`, cross-compiles all
dependencies for Amazon Linux, and uploads them as a Lambda layer. The layer is keyed by
`uv.lock` hash, so it only rebuilds when the lockfile changes. The ARN is cached locally
and in S3 so other machines skip the build too.

**Dependency sync (Batch)** — `braid sync` exports a `requirements.txt` from `uv.lock`
(via `uv export`) and uploads it to S3. Workers run `uv pip install` natively on Amazon
Linux at job start — no cross-compilation, no layer size limit.

Your function code is re-serialized fresh on every `fan` call, so **code changes never
require a sync**. Only dependency changes do.

## Prerequisites

- Python 3.13+ and [uv](https://docs.astral.sh/uv/)
- AWS credentials configured (`aws configure` or environment variables)
- An S3 bucket
- An IAM execution role — `braid init --create-role` can create one for you
  (see [IAM and auth](reference.md#iam-and-auth))

## Installation

```bash
uv add braid
```

## Quickstart

```bash
# 1. Create AWS resources and write .braid/config.toml
uv run braid init --bucket my-bucket --region us-west-2 --create-role

# 2. Build the dependency layer from uv.lock and deploy the worker
uv run braid sync
```

```python
# 3. Dispatch
from braid import fan

def process(path: str) -> dict:
    import some_library
    ...

results = fan(process, paths, batch_size=10)
```

## Backends

Lambda is the default. Switch to Batch (`--backend batch`) when a job needs more than
15 minutes, more than 10 GB of RAM, dependencies over the 250 MB layer limit, or a GPU.
See [choosing a backend](usage.md#choosing-a-backend) for the full comparison.

## Next steps

- [Usage](usage.md) — setup, backends, dispatching, tuning
- [Reference](reference.md) — every CLI flag, config field, IAM policy, and limit
- [API Reference](api.md) — `fan`, `fan_iter`, `init`, `sync`, `Config`
