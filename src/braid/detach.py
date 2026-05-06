"""
Detached submission: submit a Batch job and return immediately, poll status
without consuming results, collect whenever convenient — from any process.

Batch backend only. `fan()`'s blocking, SQS-based path is unchanged; see
docs/superpowers/specs/2026-07-21-braid-detach-mode-design.md for the design.
"""

from __future__ import annotations

import dataclasses
import datetime
import json
import logging
import time
from collections.abc import Callable
from typing import Any

from braid.aws import batch_client, s3_client
from braid.config import Config, load_config
from braid.core import _record_batch_job
from braid.errors import ConfigError, JobFailedError, WorkerError
from braid.result_collector import _list_idx_keys, collect_results_s3
from braid.sync import sync_editables, sync_uv_lock

log = logging.getLogger(__name__)

_TERMINAL_BATCH_STATES = {"SUCCEEDED", "FAILED"}

# describe_jobs stops returning completed jobs after ~24h, so an UNKNOWN state is
# ambiguous: either the job aged out, or it is not visible yet just after submit.
# Require several consecutive UNKNOWN polls before treating it as terminal.
_UNKNOWN_GRACE_POLLS = 3


@dataclasses.dataclass
class JobStatus:
    """Non-destructive snapshot of a detached job: S3 result/error counts plus
    the AWS Batch job state. Never downloads results."""

    done: int
    failed: int
    pending: int
    batch_state: str
    complete: bool


def _s3_idx_sets(bucket: str, prefix: str, region: str) -> tuple[set[int], set[int]]:
    """Indices with a result object and with an error object. May overlap."""
    s3 = s3_client.get_client(region)
    done = set(_list_idx_keys(s3, bucket, f"{prefix}results/"))
    failed = set(_list_idx_keys(s3, bucket, f"{prefix}errors/"))
    return done, failed


def _batch_state(batch_job_id: str, cfg: Config) -> str:
    """Current AWS Batch state, or "UNKNOWN" once the job leaves describe_jobs."""
    batch = batch_client.get_client(cfg.region)
    jobs = batch_client.describe_jobs(batch, [batch_job_id])
    return jobs[0]["status"] if jobs else "UNKNOWN"


def _job_status(job_id: str, batch_job_id: str, n: int, cfg: Config) -> JobStatus:
    # Describe Batch BEFORE listing S3. A state read first is never newer than the
    # listing that follows it; the reverse order can pair a stale (short) listing
    # with a job that finished in between, reporting a complete job as failed.
    batch_state = _batch_state(batch_job_id, cfg)

    prefix = cfg.s3_key(f"jobs/{job_id}/")
    done, failed = _s3_idx_sets(cfg.bucket, prefix, cfg.region)
    settled = done | failed

    return JobStatus(
        done=len(done),
        failed=len(failed),
        pending=max(n - len(settled), 0),
        batch_state=batch_state,
        complete=len(settled) >= n,
    )


def _job_record_key(job_id: str, cfg: Config) -> str:
    return cfg.s3_key(f"jobs/{job_id}/job.json")


def _write_job_record(job_id: str, batch_job_id: str, n: int, cfg: Config) -> None:
    # Every field here is read back by attach(). Bucket and prefix are deliberately
    # absent: locating this object already requires them, so storing them proves
    # nothing. Region is not — the Batch client needs it and it can differ from the
    # region the reader's config names.
    record = {
        "job_id": job_id,
        "batch_job_id": batch_job_id,
        "n": n,
        "region": cfg.region,
        "submitted_at": datetime.datetime.now(datetime.UTC).isoformat(),
    }
    s3 = s3_client.get_client(cfg.region)
    s3_client.upload_bytes(
        s3, cfg.bucket, _job_record_key(job_id, cfg), json.dumps(record).encode()
    )


def _read_job_record(job_id: str, cfg: Config) -> dict:
    s3 = s3_client.get_client(cfg.region)
    key = _job_record_key(job_id, cfg)
    if not s3_client.key_exists(s3, cfg.bucket, key):
        raise ConfigError(
            f"No job record found for {job_id} at s3://{cfg.bucket}/{key}. "
            "Check the job_id and that `config` points at the same bucket it was submitted with."
        )
    return json.loads(s3_client.download_bytes(s3, cfg.bucket, key))


@dataclasses.dataclass
class JobHandle:
    """Handle to a job submitted via `submit()` or rebuilt via `attach()`.
    Never terminates or deletes the underlying job except via `.cancel()`/`.clean()`."""

    job_id: str
    batch_job_id: str
    n: int
    config: Config = dataclasses.field(repr=False)

    @property
    def region(self) -> str:
        return self.config.region

    def status(self) -> JobStatus:
        return _job_status(self.job_id, self.batch_job_id, self.n, self.config)

    def wait(self, timeout_s: int | None = None, poll_interval_s: int = 5) -> JobStatus:
        """Poll status() until complete or timeout_s elapses. Downloads nothing."""
        deadline = None if timeout_s is None else time.monotonic() + timeout_s
        while True:
            st = self.status()
            if st.complete or (deadline is not None and time.monotonic() > deadline):
                return st
            time.sleep(poll_interval_s)

    def result(
        self, timeout_s: int | None = None, poll_interval_s: float = 2.0
    ) -> list[Any]:
        """
        Block until every result has settled, then download them. Idempotent —
        safe to call repeatedly, concurrently, from any number of processes. On
        expiry raises JobTimeoutError and leaves the Batch job running; raises
        JobFailedError if the Batch job stopped without producing every result.
        """
        unknown_polls = 0

        def _fail_fast(settled: int) -> None:
            """Runs on every incomplete poll: stop waiting on a job that is gone."""
            nonlocal unknown_polls
            state = _batch_state(self.batch_job_id, self.config)
            unknown_polls = unknown_polls + 1 if state == "UNKNOWN" else 0
            # Died without writing all results (OOM, spot reclaim, crash), or aged
            # out of Batch's describe_jobs retention while still incomplete.
            if state in _TERMINAL_BATCH_STATES or unknown_polls >= _UNKNOWN_GRACE_POLLS:
                raise JobFailedError(
                    self.job_id, state, received=settled, expected=self.n
                )

        results, failures = collect_results_s3(
            job_id=self.job_id,
            n=self.n,
            bucket=self.config.bucket,
            prefix=self.config.s3_key(f"jobs/{self.job_id}/"),
            region=self.config.region,
            timeout_s=timeout_s,
            show_progress=False,
            poll_interval_s=poll_interval_s,
            on_poll=_fail_fast,
        )
        if failures:
            raise WorkerError(failures)
        # Completion counts settled indices, which can overlap if an index has both
        # a result and an error object; without this guard that would be a KeyError.
        if any(i not in results for i in range(self.n)):
            raise JobFailedError(
                self.job_id,
                _batch_state(self.batch_job_id, self.config),
                received=len(results),
                expected=self.n,
            )
        return [results[i] for i in range(self.n)]

    def cancel(self, reason: str = "cancelled by user") -> None:
        batch = batch_client.get_client(self.config.region)
        batch.terminate_job(jobId=self.batch_job_id, reason=reason)

    def clean(self) -> None:
        s3 = s3_client.get_client(self.config.region)
        s3_client.delete_prefix(
            s3, self.config.bucket, self.config.s3_key(f"jobs/{self.job_id}/")
        )


def submit(
    fn: Callable,
    inputs: list,
    *,
    config: Config | None = None,
    exclude_packages: list[str] | None = None,
) -> JobHandle:
    """
    Submit a Batch job and return immediately. Does not collect. Results go to
    S3, not SQS, so collection is non-destructive — call handle.result() from
    any process, any number of times.
    """
    cfg = config or load_config()
    if exclude_packages is not None:
        cfg = cfg.model_copy(update={"exclude_packages": exclude_packages})

    if cfg.backend != "batch":
        raise ConfigError(
            f"submit() requires backend='batch'; got '{cfg.backend}'. Use fan() instead."
        )

    from braid.dispatch_batch import submit_batch

    editable_manifest = sync_editables(cfg)
    requirements_key = sync_uv_lock(cfg)

    job_id, n, batch_job_id = submit_batch(
        fn, inputs, cfg, requirements_key, editable_manifest, detach=True
    )
    # Local record first: it is the only trace of the job if the S3 write fails.
    _record_batch_job(job_id, batch_job_id, n, cfg)
    try:
        _write_job_record(job_id, batch_job_id, n, cfg)
    except Exception:
        log.error(
            "Batch job %s (braid job_id=%s, n=%d) is RUNNING but its job record could "
            "not be written to S3, so attach(%r) will not find it. Cancel with: "
            "aws batch terminate-job --job-id %s --reason cleanup",
            batch_job_id,
            job_id,
            n,
            job_id,
            batch_job_id,
        )
        raise

    return JobHandle(job_id=job_id, batch_job_id=batch_job_id, n=n, config=cfg)


def attach(job_id: str, *, config: Config | None = None) -> JobHandle:
    """Rebuild a handle for a previously submitted job from its S3 record."""
    cfg = config or load_config()
    record = _read_job_record(job_id, cfg)
    # The job's own region wins: S3 reads work cross-region, but describe_jobs and
    # terminate_job would silently target the wrong Batch control plane.
    job_region = record.get("region", cfg.region)
    if job_region != cfg.region:
        log.info(
            "job %s was submitted in %s; using that region instead of %s",
            job_id,
            job_region,
            cfg.region,
        )
        cfg = cfg.model_copy(update={"region": job_region})
    return JobHandle(
        job_id=record["job_id"],
        batch_job_id=record["batch_job_id"],
        n=record["n"],
        config=cfg,
    )


def status(job_id: str, *, config: Config | None = None) -> JobStatus:
    """One-shot status poll without holding a handle."""
    handle = attach(job_id, config=config)
    return handle.status()
