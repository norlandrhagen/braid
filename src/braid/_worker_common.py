"""
Shared worker logic imported by both _worker_lambda.py and _worker_batch.py.

This file is deployed alongside each worker (included in the Lambda zip and
downloaded to /tmp/ for Batch). It must not import from braid or any
non-stdlib package at module level.
"""

from __future__ import annotations

import base64
import json
import logging
import os
import shutil
import sys
import threading
import time
import urllib.request
import zipfile

log = logging.getLogger("braid.worker")

_INLINE_LIMIT = 200 * 1024

METRICS_PREFIX = "BRAID_METRICS "


def _get_json(url: str) -> dict:
    with urllib.request.urlopen(url, timeout=2) as r:
        return json.loads(r.read())


def _ts(iso: str) -> float:
    from datetime import datetime

    return datetime.fromisoformat(iso).timestamp()


def metrics_sample(task: dict, stats: dict) -> dict:
    """One sample from ECS task metadata `/task` and `/task/stats` responses.

    cpu is percent of one vCPU (200 = two vCPUs busy), from each container's own
    cpu_stats/precpu_stats window: Fargate refreshes stats less often than we poll.
    """
    cpu = mem = rx = tx = 0
    for s in stats.values():
        if not s:
            continue
        window = _ts(s["read"]) - _ts(s["preread"])
        if 0 < window < 3600:  # preread is year 0001 on the first read
            used = (
                s["cpu_stats"]["cpu_usage"]["total_usage"]
                - s["precpu_stats"]["cpu_usage"]["total_usage"]
            )
            cpu += 100 * used / (window * 1e9)
        ms = s.get("memory_stats", {})
        # Page cache is reclaimable; subtract it like `docker stats` does.
        inner = ms.get("stats", {})
        mem += ms.get("usage", 0) - inner.get("inactive_file", inner.get("cache", 0))
        for n in (s.get("networks") or {}).values():
            rx += n["rx_bytes"]
            tx += n["tx_bytes"]
    eph = task.get("EphemeralStorageMetrics")
    if eph:
        disk, disk_limit = eph["Utilized"] * 2**20, eph["Reserved"] * 2**20
    else:  # EC2: no task-level storage metrics
        du = shutil.disk_usage("/")
        disk, disk_limit = du.used, du.total
    limits = task.get("Limits", {})
    return {
        "t": round(time.time(), 1),
        "cpu": round(cpu, 1),
        "cpu_limit": 100 * limits.get("CPU", 0),
        "mem": mem,
        "mem_limit": limits.get("Memory", 0) * 2**20,
        "disk": disk,
        "disk_limit": disk_limit,
        "rx": rx,
        "tx": tx,
    }


def start_metrics_sampler(interval: float = 5.0) -> None:
    """Print a METRICS_PREFIX JSON line to stderr (-> CloudWatch) every `interval` s."""
    base = os.environ.get("ECS_CONTAINER_METADATA_URI_V4")
    if not base:
        return

    def loop() -> None:
        while True:
            try:
                s = metrics_sample(
                    _get_json(f"{base}/task"), _get_json(f"{base}/task/stats")
                )
                sys.stderr.write(METRICS_PREFIX + json.dumps(s) + "\n")
                sys.stderr.flush()
            except Exception as e:  # never take down the job over metrics
                log.debug("metrics sample failed: %s", e)
            time.sleep(interval)

    threading.Thread(target=loop, daemon=True, name="braid-metrics").start()


def _detached() -> bool:
    """Detached jobs (braid.submit) store results in S3 and send no SQS message."""
    return os.environ.get("BRAID_DETACH") == "1"


def bootstrap_uv_sync() -> None:
    """Install the locked project env into the system Python via `uv sync --frozen`."""
    url = os.environ.get("BRAID_PROJECT_URL")
    if not url:
        return
    import importlib
    import site
    import subprocess

    project_dir = "/tmp/project"
    urllib.request.urlretrieve(url, "/tmp/project.zip")
    zipfile.ZipFile("/tmp/project.zip").extractall(project_dir)

    cmd = [
        "uv",
        "sync",
        "--frozen",
        "--all-groups",  # match the Lambda layer build
        "--no-dev",
        "--inexact",
        "--python",
        sys.executable,
    ]
    # Editable path deps outside the project aren't in the zip; bootstrap_editables adds them.
    for pkg in json.loads(os.environ.get("BRAID_EDITABLES", "[]")):
        cmd += ["--no-install-package", pkg["name"]]
    result = subprocess.run(
        cmd,
        cwd=project_dir,
        env={**os.environ, "UV_PROJECT_ENVIRONMENT": sys.prefix},
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"uv sync failed:\n{result.stderr}")

    # Make packages (and the project's editable .pth) importable in this process.
    importlib.invalidate_caches()
    for d in site.getsitepackages():
        site.addsitedir(d)
    log.info("uv sync complete")


def bootstrap_editables() -> None:
    """Download editable packages from S3 and insert into sys.path."""
    manifest_raw = os.environ.get("BRAID_EDITABLES")
    if not manifest_raw:
        return

    manifest = json.loads(manifest_raw)
    s3 = None

    for pkg in manifest:
        name = pkg["name"]
        dest = f"/tmp/editables/{name}"

        if os.path.isdir(dest):
            if dest not in sys.path:
                sys.path.insert(0, dest)
            continue

        zip_path = f"/tmp/{name}.zip"
        if "url" in pkg:
            # Batch: presigned, so the base image needs no boto3.
            urllib.request.urlretrieve(pkg["url"], zip_path)
        else:
            # Lambda: manifest is baked in at sync time, so presigned URLs would expire.
            import boto3

            s3 = s3 or boto3.client("s3")
            s3.download_file(os.environ["BRAID_BUCKET"], pkg["s3_key"], zip_path)
        os.makedirs(dest, exist_ok=True)
        zipfile.ZipFile(zip_path).extractall(dest)
        os.unlink(zip_path)
        sys.path.insert(0, dest)
        log.info("Editable bootstrapped: %s", name)


def send_result(
    s3,
    sqs,
    job_id: str,
    idx: int,
    result_data: bytes,
    queue_url: str,
    bucket: str,
    s3_key_fn,
) -> None:
    """Send a successful result via SQS (inline if small, S3 otherwise).

    Detached jobs write jobs/{job_id}/results/{idx}.pkl and send no SQS message,
    so that collection is non-destructive and repeatable.
    """
    if _detached():
        s3.put_object(
            Bucket=bucket,
            Key=s3_key_fn(f"jobs/{job_id}/results/{idx}.pkl"),
            Body=result_data,
        )
        return

    if len(result_data) <= _INLINE_LIMIT:
        msg = {
            "job_id": job_id,
            "idx": idx,
            "ok": True,
            "inline": True,
            "data_b64": base64.b64encode(result_data).decode(),
        }
    else:
        key = s3_key_fn(f"jobs/{job_id}/{idx}.pkl")
        s3.put_object(Bucket=bucket, Key=key, Body=result_data)
        msg = {
            "job_id": job_id,
            "idx": idx,
            "ok": True,
            "inline": False,
            "s3_key": key,
        }
    sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(msg))


def send_error(
    s3,
    sqs,
    job_id: str,
    idx: int,
    error: str,
    tb: str,
    queue_url: str,
    bucket: str,
    s3_key_fn,
) -> None:
    """Write error JSON to S3 and notify via SQS (no SQS message when detached)."""
    if _detached():
        try:
            s3.put_object(
                Bucket=bucket,
                Key=s3_key_fn(f"jobs/{job_id}/errors/{idx}.json"),
                Body=json.dumps({"error": error, "traceback": tb}).encode(),
            )
        except Exception:
            log.exception("Failed to write error to S3")
        return

    key = s3_key_fn(f"jobs/{job_id}/{idx}.error.json")
    try:
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps({"error": error, "traceback": tb}).encode(),
        )
        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(
                {"job_id": job_id, "idx": idx, "ok": False, "s3_key": key}
            ),
        )
    except Exception:
        log.exception("Failed to send error to S3/SQS")
