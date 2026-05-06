"""
AWS Batch worker for `braid batch run` script jobs. Downloaded to /tmp/w.py
and executed by the container bootstrap command; _worker_common.py is
downloaded alongside it.

Unlike _worker_batch.py, this worker runs an entire script rather than a
manifest of pickled inputs, and in PEP 723 mode never installs boto3 (the
base image ships none) — the script itself and the completion markers move
over plain presigned HTTP via urllib, not boto3.
"""

from __future__ import annotations

import json
import logging
import os
import pickle
import subprocess
import sys
import urllib.request

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
log = logging.getLogger("braid.worker_script")


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


def _put(url: str, body: bytes) -> None:
    req = urllib.request.Request(url, data=body, method="PUT")
    urllib.request.urlopen(req)


def main() -> None:
    mode = os.environ["BRAID_SCRIPT_MODE"]
    urllib.request.urlretrieve(os.environ["BRAID_SCRIPT_URL"], "/tmp/script.py")

    if mode == "project":
        _bootstrap_uv_sync()

        # In production _worker_common.py is downloaded alongside this script.
        # In tests it lives in the same package directory.
        worker_dir = os.path.dirname(os.path.abspath(__file__))
        for p in (worker_dir, "/tmp"):
            if p not in sys.path:
                sys.path.insert(0, p)
        from _worker_common import bootstrap_editables

        bootstrap_editables()
        cmd = [sys.executable, "/tmp/script.py"]
    else:
        python_version = os.environ["BRAID_PYTHON_VERSION"]
        cmd = [
            "uv",
            "run",
            "--no-project",
            f"--python={python_version}",
            "/tmp/script.py",
        ]

    code = subprocess.call(cmd)

    try:
        if code == 0:
            _put(os.environ["BRAID_RESULT_PUT_URL"], pickle.dumps({"exit_code": 0}))
        else:
            _put(
                os.environ["BRAID_ERROR_PUT_URL"],
                json.dumps(
                    {"error": f"script exited with code {code}", "traceback": ""}
                ).encode(),
            )
    except Exception:
        log.exception("Failed to write completion marker")

    log.info("job=%s script exit_code=%d", os.environ.get("BRAID_JOB_ID", "?"), code)
    sys.exit(code)


if __name__ == "__main__":
    main()
