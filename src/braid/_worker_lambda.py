"""
AWS Lambda handler for braid workers.

This file is deployed as the Lambda function code. It must only import from
the Lambda layer, stdlib, or _worker_common (included in the same zip).
"""

from __future__ import annotations

import base64
import json
import logging
import os
import sys
import traceback

import cloudpickle

# _worker_common.py lives alongside this file (Lambda zip) or in /tmp (future).
# Ensure it's importable regardless of how this module is loaded.
_here = os.path.dirname(os.path.abspath(__file__))
if _here not in sys.path:
    sys.path.insert(0, _here)

from _worker_common import bootstrap_editables, send_error, send_result  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
log = logging.getLogger("braid.worker")


def handler(event: dict, context) -> dict:
    import boto3

    bucket = os.environ["BRAID_BUCKET"]
    bucket_prefix = os.environ.get("BRAID_BUCKET_PREFIX", "").rstrip("/")
    sqs_queue_url = os.environ["BRAID_SQS_QUEUE_URL"]
    job_id = event["job_id"]
    start_idx = event["start_idx"]
    log.info(
        "job=%s start_idx=%d batch_size=%d",
        job_id,
        start_idx,
        event.get("batch_size", 1),
    )

    def s3_key(suffix: str) -> str:
        return f"{bucket_prefix}/{suffix.lstrip('/')}" if bucket_prefix else suffix

    s3 = boto3.client("s3")
    sqs = boto3.client("sqs")

    def _send_batch_error(error: str, tb: str, input_count: int) -> None:
        """Send one error message per input so client gets the right count."""
        for i in range(input_count):
            global_idx = start_idx + i
            msg: dict = {
                "job_id": job_id,
                "idx": global_idx,
                "ok": False,
                "inline_error": error,
                "traceback": tb,
            }
            try:
                sqs.send_message(QueueUrl=sqs_queue_url, MessageBody=json.dumps(msg))
            except Exception:
                pass

    try:
        bootstrap_editables()
    except Exception as e:
        _send_batch_error(str(e), traceback.format_exc(), event.get("batch_size", 1))
        raise

    try:
        inputs_raw = event.get("inputs_b64")
        inputs = cloudpickle.loads(base64.b64decode(inputs_raw))
    except Exception as e:
        _send_batch_error(str(e), traceback.format_exc(), event.get("batch_size", 1))
        raise

    try:
        fn_key = event["fn_key"]
        fn_data = s3.get_object(Bucket=bucket, Key=fn_key)["Body"].read()
        fn = cloudpickle.loads(fn_data)
    except Exception as e:
        _send_batch_error(str(e), traceback.format_exc(), len(inputs))
        raise

    results = []
    for i, x in enumerate(inputs):
        global_idx = start_idx + i
        try:
            result = fn(x)
            result_data = cloudpickle.dumps(result)
            results.append({"idx": global_idx, "ok": True, "data": result_data})
        except Exception as e:
            tb = traceback.format_exc()
            log.error("job=%s idx=%d error: %s", job_id, global_idx, e)
            results.append(
                {
                    "idx": global_idx,
                    "ok": False,
                    "error": str(e),
                    "traceback": tb,
                }
            )

    for r in results:
        global_idx = r["idx"]
        if r["ok"]:
            send_result(
                s3, sqs, job_id, global_idx, r["data"], sqs_queue_url, bucket, s3_key
            )
        else:
            send_error(
                s3,
                sqs,
                job_id,
                global_idx,
                r["error"],
                r["traceback"],
                sqs_queue_url,
                bucket,
                s3_key,
            )

    log.info("job=%s done: %d results sent", job_id, len(results))
    return {"status": "ok", "count": len(results)}
