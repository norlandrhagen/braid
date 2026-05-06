# braid as a Cubed executor

## Why it fits

[Cubed](https://github.com/cubed-dev/cubed) - Rather than returning large arrays to the caller, Cubed materializes intermediates to a zarr store between stages. 

```
cubed plan:  [stage 1: map] → zarr → [stage 2: reduce round 1] → zarr → [stage 3: reduce round 2]
braid role:                 ↑ execute each stage via remote_parallel_map
```

Workers read from S3 zarr, compute, write results back to S3 zarr — no large payloads cross Lambda's 200KB invocation limit.

## Cubed executor interface

Executors subclass `DagExecutor` and implement `execute_dag`:

```python
class DagExecutor:
    @property
    def name(self) -> str: ...

    def execute_dag(
        self,
        dag,                    # networkx DAG of tasks
        callbacks=None,
        spec=None,
        compute_id=None,
        **kwargs,
    ) -> None: ...
```

The DAG nodes are independent within each stage (topological level). The executor walks levels, dispatches all tasks in a level in parallel, waits for completion, then advances.

Each task is a callable (already partially applied with its zarr input/output paths). The executor's job is to run `callable(arg)` for each task in a stage.

## Implementation sketch

```python
from cubed.runtime.executors.lithops import LithopsExecutor  # reference
import networkx as nx
from braid import remote_parallel_map


class BraidExecutor:
    @property
    def name(self) -> str:
        return "braid"

    def __init__(self, **braid_kwargs):
        self.braid_kwargs = braid_kwargs  # memory_mb, timeout_s, max_concurrency, etc.

    def execute_dag(self, dag, callbacks=None, spec=None, compute_id=None, **kwargs):
        # walk DAG in topological order, one level at a time
        for level_nodes in nx.topological_generations(dag):
            tasks = [dag.nodes[n]["task"] for n in level_nodes if "task" in dag.nodes[n]]
            if not tasks:
                continue

            # each task is (callable, input_arg) — exact shape TBD from Cubed internals
            # assumption: task is callable with no args (already bound) or (fn, arg) tuple
            def run_task(task):
                fn, arg = task
                return fn(arg)

            remote_parallel_map(run_task, tasks, **self.braid_kwargs)
```

Usage:

```python
import cubed
import cubed.array_api as xp

executor = BraidExecutor(memory_mb=2048, timeout_s=300, max_concurrency=500)
spec = cubed.Spec(work_dir="s3://my-bucket/cubed-tmp", executor=executor)

a = xp.ones((10_000, 10_000), chunks=(1000, 1000), spec=spec)
b = xp.mean(a, axis=0)   # reduction — Cubed plans multi-stage tree reduce
b.compute()               # braid executes each stage on Lambda
```

## What this unlocks

| Operation | braid alone | braid + Cubed |
|---|---|---|
| Element-wise transform | yes | yes |
| Global reduction (mean, sum) | no | yes |
| Multi-step pipeline | manual chaining | yes |
| Rechunking | no | yes |
| Linear algebra (matmul) | no | yes |

## Open questions

1. **Exact task shape**: Cubed task nodes may be pre-bound callables (no arg), `(fn, arg)` tuples, or something else. Check `lithops` or `modal` executor source for the actual unpacking pattern.

2. **Callbacks**: Cubed passes progress/event callbacks to `execute_dag`. These fire per-task (for progress bars, retries, etc.). braid would need to call them after each `remote_parallel_map` round completes — or per-result if streaming.

3. **Error propagation**: Cubed expects exceptions to surface with task identity. braid's `WorkerError` would need to be mapped to Cubed's error type.

4. **async**: Some Cubed executors are async (`execute_dag` is a coroutine). Wrapping `remote_parallel_map` in `asyncio.to_thread` would handle this.

## Relationship to region writes

The region-write pattern (workers write directly to output zarr, return nothing) is essentially what Cubed tasks already do — the difference is Cubed manages the zarr layout and coordinates the multi-stage plan. Using braid standalone with region writes is a manual, single-stage version of what Cubed + braid gives you automatically.
