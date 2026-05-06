"""
AWS Batch worker for braid. Downloaded to /tmp/w.py and executed by the
container bootstrap command. _worker_common.py is downloaded alongside it.
"""

from __future__ import annotations

import base64
import json
import logging
import os
import subprocess
import sys
import traceback

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
log = logging.getLogger("braid.worker_batch")


def _bootstrap_uv_sync() -> None:
    req_key = os.environ.get("BRAID_REQUIREMENTS_KEY")
    if not req_key:
        return
    import boto3

    bucket = os.environ["BRAID_BUCKET"]
    s3 = boto3.client("s3")
    req_path = "/tmp/requirements.txt"
    log.info("Downloading requirements.txt from s3://%s/%s", bucket, req_key)
    s3.download_file(bucket, req_key, req_path)
    result = subprocess.run(
        ["uv", "pip", "install", "--system", "-r", req_path],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"uv pip install failed:\n{result.stderr}")
    log.info("uv sync complete")


def main() -> None:
    _bootstrap_uv_sync()

    # In production _worker_common.py is downloaded alongside this script.
    # In tests it lives in the same package directory.
    import os as _os

    _worker_dir = _os.path.dirname(_os.path.abspath(__file__))
    for _p in (_worker_dir, "/tmp"):
        if _p not in sys.path:
            sys.path.insert(0, _p)
    from _worker_common import bootstrap_editables, send_error, send_result

    bootstrap_editables()

    import boto3
    import cloudpickle

    # For single (non-array) jobs, AWS_BATCH_JOB_ARRAY_INDEX is not set; default to 0
    array_index = int(os.environ.get("AWS_BATCH_JOB_ARRAY_INDEX", "0"))
    job_id = os.environ["BRAID_JOB_ID"]
    bucket = os.environ["BRAID_BUCKET"]
    bucket_prefix = os.environ.get("BRAID_BUCKET_PREFIX", "").rstrip("/")
    sqs_queue_url = os.environ["BRAID_SQS_QUEUE_URL"]
    log.info("job=%s array_index=%d", job_id, array_index)

    def s3_key(suffix: str) -> str:
        return f"{bucket_prefix}/{suffix.lstrip('/')}" if bucket_prefix else suffix

    s3 = boto3.client("s3")
    sqs = boto3.client("sqs")

    try:
        manifest_key = s3_key(f"jobs/{job_id}/manifest.json")
        manifest = json.loads(
            s3.get_object(Bucket=bucket, Key=manifest_key)["Body"].read()
        )
    except Exception as e:
        send_error(
            s3,
            sqs,
            job_id,
            array_index,
            str(e),
            traceback.format_exc(),
            sqs_queue_url,
            bucket,
            s3_key,
        )
        raise

    try:
        input_val = cloudpickle.loads(
            base64.b64decode(manifest["inputs"][array_index]["data_b64"])
        )
    except Exception as e:
        send_error(
            s3,
            sqs,
            job_id,
            array_index,
            str(e),
            traceback.format_exc(),
            sqs_queue_url,
            bucket,
            s3_key,
        )
        raise

    try:
        fn = cloudpickle.loads(
            s3.get_object(Bucket=bucket, Key=manifest["fn_key"])["Body"].read()
        )
    except Exception as e:
        send_error(
            s3,
            sqs,
            job_id,
            array_index,
            str(e),
            traceback.format_exc(),
            sqs_queue_url,
            bucket,
            s3_key,
        )
        raise

    try:
        result = fn(input_val)
        result_data = cloudpickle.dumps(result)
    except Exception as e:
        send_error(
            s3,
            sqs,
            job_id,
            array_index,
            str(e),
            traceback.format_exc(),
            sqs_queue_url,
            bucket,
            s3_key,
        )
        log.error("job=%s idx=%d error: %s", job_id, array_index, e)
        return

    send_result(
        s3, sqs, job_id, array_index, result_data, sqs_queue_url, bucket, s3_key
    )
    log.info("job=%s idx=%d done", job_id, array_index)


if __name__ == "__main__":
    main()
