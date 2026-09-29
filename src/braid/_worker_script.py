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
from collections import deque

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
log = logging.getLogger("braid.worker_script")


def _put(url: str, body: bytes) -> None:
    req = urllib.request.Request(url, data=body, method="PUT")
    urllib.request.urlopen(req)


def _run(cmd: list[str], tail_lines: int = 50) -> tuple[int, str]:
    """Run cmd, tee stderr to our stderr (CloudWatch), return (exit code, stderr tail)."""
    tail: deque[str] = deque(maxlen=tail_lines)
    proc = subprocess.Popen(cmd, stderr=subprocess.PIPE, text=True, errors="replace")
    for line in proc.stderr:
        sys.stderr.write(line)
        tail.append(line)
    return proc.wait(), "".join(tail)


def main() -> None:
    mode = os.environ["BRAID_SCRIPT_MODE"]
    urllib.request.urlretrieve(os.environ["BRAID_SCRIPT_URL"], "/tmp/script.py")

    # In production _worker_common.py is downloaded alongside this script.
    # In tests it lives in the same package directory.
    worker_dir = os.path.dirname(os.path.abspath(__file__))
    for p in (worker_dir, "/tmp"):
        if p not in sys.path:
            sys.path.insert(0, p)
    from _worker_common import start_metrics_sampler

    start_metrics_sampler()

    if mode == "project":
        from _worker_common import bootstrap_editables, bootstrap_uv_sync

        bootstrap_uv_sync()
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

    code, stderr_tail = _run(cmd)

    try:
        if code == 0:
            _put(os.environ["BRAID_RESULT_PUT_URL"], pickle.dumps({"exit_code": 0}))
        else:
            _put(
                os.environ["BRAID_ERROR_PUT_URL"],
                json.dumps(
                    {
                        "error": f"script exited with code {code}",
                        "traceback": stderr_tail,
                    }
                ).encode(),
            )
    except Exception:
        log.exception("Failed to write completion marker")

    log.info("job=%s script exit_code=%d", os.environ.get("BRAID_JOB_ID", "?"), code)
    sys.exit(code)


if __name__ == "__main__":
    main()
