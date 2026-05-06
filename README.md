# braid

Parallel map over AWS Lambda, Fargate and Batch, with automatic dependency sync via `uv`.

```python
from braid import fan

results = fan(fn, inputs)
```


**Docs:** https://norlandrhagen.github.io/braid/

> Experimental. APIs may change without notice.

## How it works

Your function is cloudpickled and uploaded to S3, inputs are dispatched across workers in
batches, and results come back over SQS in input order. `braid sync` builds your
dependencies from `uv.lock` once; after that, code changes need no sync — the function is
re-serialized on every `fan` call.

Also supports basic batch dispatch functionality. 


## Prerequisites

- Python 3.13+ and [uv](https://docs.astral.sh/uv/)
- AWS credentials (`aws configure` or environment variables)
- An S3 bucket

## Install

```bash
uv add braid
```

## Quickstart

```bash
# Create AWS resources and write .braid/config.toml
uv run braid init --bucket my-bucket --region us-west-2 --create-role

# Build the dependency layer from uv.lock and deploy the worker
uv run braid sync
```

```python
from braid import fan

def process(path: str) -> dict:
    import some_library
    ...

results = fan(process, paths, batch_size=10)
```
