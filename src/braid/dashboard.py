"""`braid dashboard`: local web UI for Batch jobs with live resource metrics.

Metrics come from BRAID_METRICS lines the worker sampler writes to each task's
CloudWatch log stream (see _worker_common.start_metrics_sampler).
"""

from __future__ import annotations

import json
import time
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from braid._worker_common import METRICS_PREFIX
from braid.config import jobs_cache_path, load_config

LOG_GROUP = "/aws/batch/job"
_HTML = Path(__file__).with_name("dashboard.html")


def parse_metrics(events: list[dict]) -> list[dict]:
    """Samples from CloudWatch log events; non-metric and malformed lines are skipped."""
    out = []
    for e in events:
        msg = e["message"]
        if msg.startswith(METRICS_PREFIX):
            try:
                out.append(json.loads(msg[len(METRICS_PREFIX) :]))
            except json.JSONDecodeError:
                pass
    return out


class _Api:
    def __init__(self) -> None:
        import boto3

        from braid.aws import batch_client

        self.config = load_config()
        self.batch = batch_client.get_client(self.config.region)
        self.logs = boto3.client("logs", region_name=self.config.region)

    def jobs(self) -> list[dict]:
        from braid.aws import batch_client

        path = jobs_cache_path()
        records = json.loads(path.read_text()) if path.exists() else []
        names = {r["batch_job_id"]: r.get("name") or "" for r in records}
        summaries = batch_client.list_braid_jobs(
            self.batch, self.config.batch_job_queue
        )
        return [
            {
                "id": j["jobName"].removeprefix("braid-"),
                "batch_job_id": j["jobId"],
                "name": names.get(j["jobId"], ""),
                "status": j["status"],
                "created": j["createdAt"],
                "stopped": j.get("stoppedAt"),
                "size": j.get("arrayProperties", {}).get("size", 1),
            }
            for j in summaries[:100]
        ]

    def job(self, batch_job_id: str, since_ms: int) -> dict:
        from braid.aws import batch_client
        from braid.cli.commands import _fargate_cost_estimate

        jobs = batch_client.describe_jobs(self.batch, [batch_job_id])
        if not jobs:
            raise LookupError(batch_job_id)
        j = jobs[0]
        c = j.get("container", {})
        req = {r["type"]: r["value"] for r in c.get("resourceRequirements", [])}
        vcpu, mem_mb = float(req.get("VCPU", 0)), int(req.get("MEMORY", 0))
        started, stopped = j.get("startedAt"), j.get("stoppedAt")
        elapsed_s = ((stopped or time.time() * 1000) - started) / 1000 if started else 0
        cost = (
            "EC2/varies"
            if self.config.batch_instance_types
            else _fargate_cost_estimate(vcpu, mem_mb, elapsed_s)
        )
        samples = []
        stream = c.get("logStreamName")
        if stream:
            kwargs = {
                "logGroupName": LOG_GROUP,
                "logStreamNames": [stream],
                "filterPattern": f'"{METRICS_PREFIX.strip()}"',
                "startTime": since_ms,
            }
            try:
                for page in self.logs.get_paginator("filter_log_events").paginate(
                    **kwargs
                ):
                    samples += parse_metrics(page["events"])
            except self.logs.exceptions.ResourceNotFoundException:
                pass
        return {
            "job": {
                "batch_job_id": j["jobId"],
                "name": j["jobName"],
                "status": j["status"],
                "reason": j.get("statusReason", ""),
                "created": j["createdAt"],
                "started": started,
                "stopped": stopped,
                "elapsed_s": elapsed_s,
                "vcpu": vcpu,
                "mem_mb": mem_mb,
                "cost": cost,
                "size": j.get("arrayProperties", {}).get("size", 1),
                "array_summary": j.get("arrayProperties", {}).get("statusSummary", {}),
                "has_stream": bool(stream),
            },
            "samples": samples,
        }

    def logs_tail(self, batch_job_id: str) -> list[dict]:
        from braid.aws import batch_client

        jobs = batch_client.describe_jobs(self.batch, [batch_job_id])
        stream = jobs[0].get("container", {}).get("logStreamName") if jobs else None
        if not stream:
            return []
        try:
            resp = self.logs.get_log_events(
                logGroupName=LOG_GROUP,
                logStreamName=stream,
                limit=500,
                startFromHead=False,
            )
        except self.logs.exceptions.ResourceNotFoundException:
            return []
        return [
            {"t": e["timestamp"], "message": e["message"]}
            for e in resp["events"]
            if not e["message"].startswith(METRICS_PREFIX)
        ]


def _handler(api: _Api) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args) -> None:  # keep the terminal quiet
            pass

        def _send(self, code: int, body: bytes, ctype: str) -> None:
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:
            url = urlparse(self.path)
            q = {k: v[0] for k, v in parse_qs(url.query).items()}
            try:
                if url.path == "/":
                    return self._send(
                        200, _HTML.read_bytes(), "text/html; charset=utf-8"
                    )
                if url.path == "/api/jobs":
                    data = api.jobs()
                elif url.path == "/api/job":
                    data = api.job(q["id"], int(q.get("since", 0)))
                elif url.path == "/api/logs":
                    data = api.logs_tail(q["id"])
                else:
                    return self._send(404, b"not found", "text/plain")
            except (KeyError, LookupError) as e:
                return self._send(
                    404, json.dumps({"error": str(e)}).encode(), "application/json"
                )
            except Exception as e:
                return self._send(
                    500, json.dumps({"error": str(e)}).encode(), "application/json"
                )
            self._send(200, json.dumps(data).encode(), "application/json")

    return Handler


def serve(port: int = 8787, open_browser: bool = True) -> None:
    server = ThreadingHTTPServer(("127.0.0.1", port), _handler(_Api()))
    url = f"http://127.0.0.1:{port}/"
    print(f"braid dashboard at {url} (Ctrl-C to stop)")
    if open_browser:
        webbrowser.open(url)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
