from __future__ import annotations

import base64
import json
import logging
import queue
import threading
import time
from collections.abc import Callable
from typing import Any

import cloudpickle
from tqdm import tqdm

from braid.aws import s3_client, sqs_client
from braid.errors import JobTimeoutError, WorkerError

log = logging.getLogger(__name__)


def _process_message(msg: dict, s3, bucket: str) -> tuple[int, Any, dict | None]:
    """Returns (idx, result, failure) where failure is None on success."""
    body = sqs_client.parse_body(msg)
    idx = body["idx"]

    if not body["ok"]:
        if "inline_error" in body:
            failure = {
                "idx": idx,
                "error": body["inline_error"],
                "traceback": body.get("traceback", ""),
            }
        else:
            error_data = json.loads(
                s3_client.download_bytes(s3, bucket, body["s3_key"])
            )
            failure = {
                "idx": idx,
                "error": error_data["error"],
                "traceback": error_data["traceback"],
            }
        return idx, None, failure

    if body.get("inline"):
        result = cloudpickle.loads(base64.b64decode(body["data_b64"]))
    else:
        result = cloudpickle.loads(s3_client.download_bytes(s3, bucket, body["s3_key"]))

    return idx, result, None


def collect_results(
    job_id: str,
    n: int,
    sqs_url: str,
    s3_bucket: str,
    region: str,
    timeout_s: int = 300,
    show_progress: bool = True,
    poll_workers: int = 8,
    raise_on_error: bool = True,
) -> tuple[dict[int, Any], list[dict]]:
    """
    Blocking collection. Returns (results_by_idx, failures).
    If raise_on_error=True (default), raises JobTimeoutError or WorkerError instead of returning failures.
    """
    # Bug B: Early return for n <= 0
    if n <= 0:
        return {}, []

    s3 = s3_client.get_client(region)

    results: dict[int, Any] = {}
    failures: list[dict] = []
    failed_indices: set[int] = set()  # Bug D: Track failed indices for deduplication
    lock = threading.Lock()
    deadline = time.monotonic() + timeout_s
    done_event = threading.Event()

    bar = tqdm(total=n, disable=not show_progress, desc="collecting")

    def _poll_worker() -> None:
        sqs = sqs_client.get_client(region)
        while not done_event.is_set():
            if time.monotonic() > deadline:
                break
            remaining_s = deadline - time.monotonic()
            msgs = sqs_client.receive_messages(
                sqs, sqs_url, max_messages=10, wait_seconds=min(2, int(remaining_s))
            )
            for msg in msgs:
                body = sqs_client.parse_body(msg)
                # Bug A: Don't delete messages from other jobs; just skip them
                if body.get("job_id") != job_id:
                    continue
                # Bug C: Wrap message processing in try/except to avoid silent thread death
                try:
                    idx, result, failure = _process_message(msg, s3, s3_bucket)
                except Exception:
                    log.exception("error processing message in job %s", job_id)
                    continue
                sqs_client.delete_message(sqs, sqs_url, msg["ReceiptHandle"])
                with lock:
                    if failure is not None:
                        # Bug D: Deduplicate failures by idx
                        if idx not in failed_indices:
                            failed_indices.add(idx)
                            failures.append(failure)
                    else:
                        results[idx] = result
                    bar.update(1)
                    if len(results) + len(failed_indices) >= n:
                        done_event.set()

    threads = [
        threading.Thread(target=_poll_worker, daemon=True) for _ in range(poll_workers)
    ]
    for t in threads:
        t.start()

    while not done_event.is_set():
        if time.monotonic() > deadline:
            done_event.set()
            break
        time.sleep(0.05)

    for t in threads:
        t.join(timeout=1)

    bar.close()

    collected = len(results) + len(failures)
    if raise_on_error:
        if collected < n:
            log.warning(
                "job %s timed out: got %d/%d results in %ds",
                job_id,
                collected,
                n,
                timeout_s,
            )
            raise JobTimeoutError(job_id, timeout_s, received=collected, expected=n)
        if failures:
            log.warning("job %s had %d worker failure(s)", job_id, len(failures))
            raise WorkerError(failures)

    return results, failures


def _list_idx_keys(s3, bucket: str, prefix: str) -> dict[int, str]:
    """List `prefix/{idx}.*` keys, returning {idx: key}."""
    out: dict[int, str] = {}
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            name = key[len(prefix) :].split(".", 1)[0]
            try:
                out[int(name)] = key
            except ValueError:
                continue
    return out


def collect_results_s3(
    job_id: str,
    n: int,
    bucket: str,
    prefix: str,
    region: str,
    timeout_s: int | None,
    show_progress: bool = True,
    poll_interval_s: float = 2.0,
    on_poll: Callable[[int], None] | None = None,
) -> tuple[dict[int, Any], list[dict]]:
    """
    Poll S3 for detached-job results under `{prefix}/jobs/{job_id}/results/` and
    `.../errors/`. Non-destructive and idempotent — safe to call repeatedly or
    concurrently; unlike collect_results, it never deletes what it reads.

    Returns (results_by_idx, failures). Raises JobTimeoutError if timeout_s
    elapses before every index has settled. timeout_s=None waits indefinitely.

    `on_poll(settled_count)` runs after each incomplete poll; raise from it to
    abort early (used to fail fast when the underlying job has died).
    """
    if n <= 0:
        return {}, []

    s3 = s3_client.get_client(region)
    results_prefix = f"{prefix}results/"
    errors_prefix = f"{prefix}errors/"

    deadline = None if timeout_s is None else time.monotonic() + timeout_s
    bar = tqdm(total=n, disable=not show_progress, desc="collecting")

    try:
        while True:
            result_keys = _list_idx_keys(s3, bucket, results_prefix)
            error_keys = _list_idx_keys(s3, bucket, errors_prefix)
            # Union, not sum: an index can carry both a result and an error object.
            settled = len(set(result_keys) | set(error_keys))
            bar.n = settled
            bar.refresh()

            if settled >= n:
                break
            if deadline is not None and time.monotonic() > deadline:
                raise JobTimeoutError(job_id, timeout_s, received=settled, expected=n)
            if on_poll is not None:
                on_poll(settled)
            time.sleep(poll_interval_s)
    finally:
        bar.close()

    results: dict[int, Any] = {}
    for idx, key in result_keys.items():
        results[idx] = cloudpickle.loads(s3_client.download_bytes(s3, bucket, key))

    failures: list[dict] = []
    for idx, key in error_keys.items():
        error_data = json.loads(s3_client.download_bytes(s3, bucket, key))
        failures.append(
            {
                "idx": idx,
                "error": error_data["error"],
                "traceback": error_data["traceback"],
            }
        )

    return results, failures


def collect_results_iter(
    job_id: str,
    n: int,
    sqs_url: str,
    s3_bucket: str,
    region: str,
    timeout_s: int = 300,
    show_progress: bool = True,
    poll_workers: int = 8,
) -> Any:
    """
    Streaming collection. Yields (idx, result) as results arrive.
    Raises WorkerError on any failure, JobTimeoutError on timeout.
    """
    # Bug B: Early return for n <= 0
    if n <= 0:
        return

    s3 = s3_client.get_client(region)
    q: queue.Queue = queue.Queue()
    failures: list[dict] = []
    failed_indices: set[int] = set()  # Bug D: Track failed indices for deduplication
    lock = threading.Lock()
    deadline = time.monotonic() + timeout_s
    done_event = threading.Event()

    bar = tqdm(total=n, disable=not show_progress, desc="collecting")

    def _poll_worker() -> None:
        sqs = sqs_client.get_client(region)
        while not done_event.is_set():
            if time.monotonic() > deadline:
                break
            remaining_s = deadline - time.monotonic()
            msgs = sqs_client.receive_messages(
                sqs, sqs_url, max_messages=10, wait_seconds=min(2, int(remaining_s))
            )
            for msg in msgs:
                body = sqs_client.parse_body(msg)
                # Bug A: Don't delete messages from other jobs; just skip them
                if body.get("job_id") != job_id:
                    continue
                # Bug C: Wrap message processing in try/except to avoid silent thread death
                try:
                    idx, result, failure = _process_message(msg, s3, s3_bucket)
                except Exception:
                    log.exception("error processing message in job %s", job_id)
                    continue
                sqs_client.delete_message(sqs, sqs_url, msg["ReceiptHandle"])
                with lock:
                    bar.update(1)
                    if failure is not None:
                        # Bug D: Deduplicate failures by idx
                        if idx not in failed_indices:
                            failed_indices.add(idx)
                            failures.append(failure)
                            q.put(None)
                    else:
                        q.put((idx, result))

    threads = [
        threading.Thread(target=_poll_worker, daemon=True) for _ in range(poll_workers)
    ]
    for t in threads:
        t.start()

    received = 0
    while received < n:
        if time.monotonic() > deadline:
            done_event.set()
            bar.close()
            raise JobTimeoutError(job_id, timeout_s, received=received, expected=n)
        try:
            item = q.get(timeout=1.0)
        except queue.Empty:
            continue
        received += 1
        if item is None:
            if failures:
                done_event.set()
                bar.close()
                raise WorkerError(failures)
        else:
            yield item

    done_event.set()
    bar.close()
    for t in threads:
        t.join(timeout=1)

    if failures:
        raise WorkerError(failures)
