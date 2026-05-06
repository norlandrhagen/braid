from __future__ import annotations

import base64
import datetime
import json
import logging
import threading
import time
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

from braid.aws import batch_client, lambda_client, s3_client
from braid.config import Config, jobs_cache_path, load_config
from braid.errors import JobTimeoutError, PayloadTooLargeError, WorkerError
from braid.result_collector import collect_results, collect_results_iter
from braid.serialization import serialize_fn, serialize_input
from braid.sync import hash_uv_lock, sync_editables, sync_uv_lock

log = logging.getLogger(__name__)

# Lambda Event/async invocation payload limit is 256KB; use 250KB to leave margin
_INVOKE_PAYLOAD_LIMIT = 250 * 1024


def _upload_fn(fn: Callable, job_id: str, config: Config, s3) -> str:
    fn_data = serialize_fn(fn)
    key = config.s3_key(f"jobs/{job_id}/fn.pkl")
    s3_client.upload_bytes(s3, config.bucket, key, fn_data)
    return key


def _chunk(lst: list, size: int) -> list[list]:
    return [lst[i : i + size] for i in range(0, len(lst), size)]


def _invoke_batches(
    batches: list[list],
    fn_key: str,
    job_id: str,
    config: Config,
    lam,
    max_concurrency: int,
) -> None:
    import cloudpickle

    semaphore = threading.Semaphore(max_concurrency)
    errors: list[Exception] = []

    def _invoke_one(batch: list, start_idx: int) -> None:
        try:
            inputs_data = base64.b64encode(cloudpickle.dumps(batch)).decode()
            payload = {
                "job_id": job_id,
                "fn_key": fn_key,
                "inputs_b64": inputs_data,
                "start_idx": start_idx,
                "batch_size": len(batch),
            }
            # Check payload size before invoking (Lambda limit is 256KB, use 250KB as threshold)
            payload_bytes = json.dumps(payload).encode()
            if len(payload_bytes) > _INVOKE_PAYLOAD_LIMIT:
                raise PayloadTooLargeError(
                    len(payload_bytes), _INVOKE_PAYLOAD_LIMIT, what="batch inputs"
                )
            lambda_client.invoke_async(lam, config.worker_function_name, payload)
        except Exception as e:
            errors.append(e)
        finally:
            semaphore.release()

    threads = []
    start_idx = 0
    for batch in batches:
        semaphore.acquire()
        if errors:
            semaphore.release()
            break
        t = threading.Thread(target=_invoke_one, args=(batch, start_idx), daemon=True)
        t.start()
        threads.append(t)
        start_idx += len(batch)

    for t in threads:
        t.join()

    if errors:
        if len(errors) == 1:
            raise errors[0]
        raise ExceptionGroup("Lambda invoke failures", errors)


def _applied_state_path(config: Config) -> Path:
    from braid.config import state_dir

    return state_dir() / "cache" / "applied_state.json"


def _load_applied_state(config: Config) -> dict:
    p = _applied_state_path(config)
    if p.exists():
        return json.loads(p.read_text())
    return {}


def _save_applied_state(config: Config, state: dict) -> None:
    p = _applied_state_path(config)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(state))


_LAMBDA_PRICE_PER_GB_S = 0.0000166667
_LAMBDA_PRICE_PER_REQ = 0.0000002  # $0.20 per 1M


def _decide_retry(
    failures: list[dict],
    idx_map: list[int],
    attempt: int,
    retries: int,
) -> tuple[list[dict], list[int] | None]:
    """
    Returns (failures, orig_failed_indices). orig_failed_indices is None when the
    retry loop should stop (no failures, or retries exhausted) — in that case
    `failures` is remapped to original indices, ready to raise/report. Otherwise
    `failures` is returned unchanged and orig_failed_indices names the next round's
    inputs.
    """
    if not failures or attempt >= retries:
        remapped = [
            {"idx": idx_map[f["idx"]], **{k: v for k, v in f.items() if k != "idx"}}
            for f in failures
        ]
        return remapped, None

    orig_failed_indices = [idx_map[f["idx"]] for f in failures]
    log.warning(
        "retrying %d failed input(s) (attempt %d/%d)",
        len(orig_failed_indices),
        attempt + 1,
        retries,
    )
    return failures, orig_failed_indices


def fan(
    fn: Callable,
    inputs: list,
    *,
    batch_size: int | None = None,
    timeout_s: int | None = None,
    dry_run: bool = False,
    max_concurrency: int | None = None,
    max_retries: int | None = None,
    cleanup: bool = True,
    backend: str | None = None,
    exclude_packages: list[str] | None = None,
    config: Config | None = None,
) -> list[Any]:
    """
    Map fn over inputs in parallel using AWS Lambda or AWS Batch.

    To pass static kwargs to every worker call, use functools.partial:
    ``fan(functools.partial(fn, model_path=path), inputs)``.

    Parameters
    ----------
    fn:
        Callable to apply to each input. Serialized via cloudpickle.
    inputs:
        List of inputs to map over.
    batch_size:
        Number of inputs per Lambda invocation.
    timeout_s:
        Result collection timeout in seconds. Defaults to collect_timeout_s in
        config. Does not change the Lambda function's own timeout.
    dry_run:
        Serialize fn and first input, print sizes, return without invoking.
    max_concurrency:
        Maximum simultaneous invocations dispatched.
    max_retries:
        Number of times to retry a failed batch.
    cleanup:
        Delete S3 job artifacts after successful collection.
    backend:
        "lambda" or "batch". Overrides config for this call.
    exclude_packages:
        Package names to strip from the layer before uploading. Overrides config.
    config:
        Override config. Loaded from .braid/config.toml if not provided.
    """
    cfg = config or load_config()
    if exclude_packages is not None:
        cfg = cfg.model_copy(update={"exclude_packages": exclude_packages})
    effective_backend = backend or cfg.backend
    bs = min(batch_size or cfg.batch_size, max(len(inputs), 1))
    to = timeout_s or cfg.collect_timeout_s
    retries = max_retries if max_retries is not None else cfg.max_retries

    if dry_run:
        fn_data = serialize_fn(fn)
        sample_data = serialize_input(inputs[0]) if inputs else b""
        log.info(
            "dry_run: fn=%.1fKB  sample_input=%.1fKB",
            len(fn_data) / 1024,
            len(sample_data) / 1024,
        )
        log.info(
            "         inputs=%d  batch_size=%d  batches=%d",
            len(inputs),
            bs,
            len(_chunk(inputs, bs)),
        )
        return []

    if effective_backend == "batch":
        return _fan_batch(fn, inputs, cfg, to, retries, cleanup)

    # Assert uv.lock matches what was last deployed via `braid sync`
    current_hash = hash_uv_lock()
    state = _load_applied_state(cfg)
    if state.get("lock_hash") != current_hash:
        synced = state.get("lock_hash", "never")
        raise RuntimeError(
            f"uv.lock changed since last sync (deployed: {synced}, current: {current_hash}). "
            "Run `braid sync` first."
        )

    job_id = str(uuid.uuid4())
    s3 = s3_client.get_client(cfg.region)
    concurrency = max_concurrency or cfg.max_concurrency
    lam = lambda_client.get_client(
        cfg.region, max_pool_connections=min(concurrency, 200)
    )

    # Upload function
    fn_key = _upload_fn(fn, job_id, cfg, s3)

    # Invoke batches
    batches = _chunk(inputs, bs)
    t0 = time.monotonic()
    _invoke_batches(batches, fn_key, job_id, cfg, lam, max_concurrency=concurrency)

    # Collect with optional retry for failed inputs.
    # idx_map: position-in-current-batch → original index in `inputs`
    # Track all job_ids used (original + retries) for cleanup
    collected: dict[int, Any] = {}
    job_ids_to_cleanup = [job_id]  # Start with the original job_id
    attempt = 0
    current_job_id = job_id
    current_inputs = inputs
    current_batches = batches
    current_fn_key = fn_key
    idx_map = list(range(len(inputs)))  # identity for first round

    while True:
        raw, failures = collect_results(
            job_id=current_job_id,
            n=len(current_inputs),
            sqs_url=cfg.sqs_queue_url,
            s3_bucket=cfg.bucket,
            region=cfg.region,
            timeout_s=to,
            raise_on_error=False,
        )
        # Remap local indices back to original indices
        for local_idx, result in raw.items():
            collected[idx_map[local_idx]] = result

        failures, orig_failed_indices = _decide_retry(
            failures, idx_map, attempt, retries
        )
        if orig_failed_indices is None:
            break

        attempt += 1
        current_job_id = str(uuid.uuid4())
        job_ids_to_cleanup.append(current_job_id)  # Track new retry job_id for cleanup
        current_inputs = [inputs[i] for i in orig_failed_indices]
        idx_map = orig_failed_indices  # new local idx i → orig_failed_indices[i]
        current_batches = _chunk(current_inputs, bs)
        current_fn_key = _upload_fn(fn, current_job_id, cfg, s3)
        _invoke_batches(
            current_batches,
            current_fn_key,
            current_job_id,
            cfg,
            lam,
            max_concurrency=concurrency,
        )

    elapsed = time.monotonic() - t0
    log.info(
        "cost estimate (ceiling): %d invocations × %dMB × %.0fs ≤ $%.4f",
        len(batches),
        cfg.memory_mb,
        elapsed,
        (cfg.memory_mb / 1024) * elapsed * len(batches) * _LAMBDA_PRICE_PER_GB_S
        + len(batches) * _LAMBDA_PRICE_PER_REQ,
    )

    n_total = len(inputs)
    if failures:
        raise WorkerError(failures)
    n_collected = len(collected)
    if n_collected < n_total:
        raise JobTimeoutError(job_id, to, received=n_collected, expected=n_total)

    if cleanup:
        # Delete all job_ids used across attempts (original + retries)
        for jid in job_ids_to_cleanup:
            s3_client.delete_prefix(s3, cfg.bucket, cfg.s3_key(f"jobs/{jid}/"))

    return [collected[i] for i in range(n_total)]


def fan_iter(
    fn: Callable,
    inputs: list,
    *,
    batch_size: int | None = None,
    timeout_s: int | None = None,
    max_concurrency: int | None = None,
    exclude_packages: list[str] | None = None,
    config: Config | None = None,
):
    """
    Like fan, but yields (original_idx, result) as results arrive from Lambda.
    No retry support. Raises WorkerError on any failure.

    Lambda backend only. Batch backend does not support streaming.
    """
    cfg = config or load_config()
    if exclude_packages is not None:
        cfg = cfg.model_copy(update={"exclude_packages": exclude_packages})
    bs = min(batch_size or cfg.batch_size, max(len(inputs), 1))
    to = timeout_s or cfg.collect_timeout_s

    current_hash = hash_uv_lock()
    state = _load_applied_state(cfg)
    if state.get("lock_hash") != current_hash:
        synced = state.get("lock_hash", "never")
        raise RuntimeError(
            f"uv.lock changed since last sync (deployed: {synced}, current: {current_hash}). "
            "Run `braid sync` first."
        )

    job_id = str(uuid.uuid4())
    s3 = s3_client.get_client(cfg.region)
    concurrency = max_concurrency or cfg.max_concurrency
    lam = lambda_client.get_client(
        cfg.region, max_pool_connections=min(concurrency, 200)
    )

    fn_key = _upload_fn(fn, job_id, cfg, s3)
    batches = _chunk(inputs, bs)
    _invoke_batches(batches, fn_key, job_id, cfg, lam, max_concurrency=concurrency)

    yield from collect_results_iter(
        job_id=job_id,
        n=len(inputs),
        sqs_url=cfg.sqs_queue_url,
        s3_bucket=cfg.bucket,
        region=cfg.region,
        timeout_s=to,
    )


_BATCH_TIMEOUT_WARN = 600  # warn if the collection timeout is this low for Batch jobs


def _record_batch_job(job_id: str, batch_job_id: str, n: int, cfg: Config) -> None:
    path = jobs_cache_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    records = json.loads(path.read_text()) if path.exists() else []
    records.append(
        {
            "job_id": job_id,
            "batch_job_id": batch_job_id,
            "n_inputs": n,
            "submitted_at": datetime.datetime.now(datetime.UTC).isoformat(),
            "region": cfg.region,
            "queue": cfg.batch_job_queue,
        }
    )
    path.write_text(json.dumps(records[-50:], indent=2))


def _fan_batch(
    fn: Callable,
    inputs: list,
    cfg: Config,
    timeout_s: int,
    retries: int,
    cleanup: bool,
) -> list[Any]:
    from braid.dispatch_batch import submit_batch

    if timeout_s < _BATCH_TIMEOUT_WARN:
        log.warning(
            "collect_timeout_s=%d is low for a Batch job (cold start alone takes ~30-60s). "
            "Set it to the expected max job duration to avoid premature timeouts.",
            timeout_s,
        )

    s3 = s3_client.get_client(cfg.region)
    batch = batch_client.get_client(cfg.region)
    editable_manifest = sync_editables(cfg)
    requirements_key = sync_uv_lock(cfg)

    collected: dict[int, Any] = {}
    attempt = 0
    current_inputs = inputs
    idx_map = list(range(len(inputs)))

    while True:
        job_id, _, batch_job_id = submit_batch(
            fn, current_inputs, cfg, requirements_key, editable_manifest
        )
        _record_batch_job(job_id, batch_job_id, len(current_inputs), cfg)

        try:
            raw, failures = collect_results(
                job_id=job_id,
                n=len(current_inputs),
                sqs_url=cfg.sqs_queue_url,
                s3_bucket=cfg.bucket,
                region=cfg.region,
                timeout_s=timeout_s,
                raise_on_error=False,
            )
        except KeyboardInterrupt:
            log.info("Interrupted — terminating Batch job %s", batch_job_id)
            try:
                batch.terminate_job(jobId=batch_job_id, reason="client interrupted")
            except Exception as e:
                log.warning("Could not terminate Batch job %s: %s", batch_job_id, e)
            if cleanup:
                s3_client.delete_prefix(s3, cfg.bucket, cfg.s3_key(f"jobs/{job_id}/"))
            raise

        for local_idx, result in raw.items():
            collected[idx_map[local_idx]] = result

        # collect_results(raise_on_error=False) never raises on timeout — it just
        # returns whatever arrived before the deadline. Detect that case here so a
        # Batch job that's still running doesn't get silently mistaken for "done,
        # no failures" and left running unterminated. Union, not sum: an index can
        # report both a result and an error.
        settled = set(raw) | {f["idx"] for f in failures}
        if len(settled) < len(current_inputs):
            log.warning("Collection timed out — terminating Batch job %s", batch_job_id)
            try:
                batch.terminate_job(
                    jobId=batch_job_id, reason="braid collection timeout"
                )
            except Exception as e:
                log.warning("Failed to terminate Batch job %s: %s", batch_job_id, e)
            if cleanup:
                s3_client.delete_prefix(s3, cfg.bucket, cfg.s3_key(f"jobs/{job_id}/"))
            raise JobTimeoutError(
                job_id, timeout_s, received=len(collected), expected=len(inputs)
            )

        failures, orig_failed = _decide_retry(failures, idx_map, attempt, retries)
        if orig_failed is None:
            break

        attempt += 1
        current_inputs = [inputs[i] for i in orig_failed]
        idx_map = orig_failed

        if cleanup:
            s3_client.delete_prefix(s3, cfg.bucket, cfg.s3_key(f"jobs/{job_id}/"))

    n_total = len(inputs)
    if failures:
        raise WorkerError(failures)
    if len(collected) < n_total:
        raise JobTimeoutError(
            job_id, timeout_s, received=len(collected), expected=n_total
        )

    if cleanup:
        s3_client.delete_prefix(s3, cfg.bucket, cfg.s3_key(f"jobs/{job_id}/"))

    return [collected[i] for i in range(n_total)]
