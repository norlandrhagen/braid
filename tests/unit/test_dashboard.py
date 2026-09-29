from __future__ import annotations

import json

from braid._worker_common import METRICS_PREFIX, metrics_sample
from braid.dashboard import parse_metrics

TASK = {
    "Limits": {"CPU": 2, "Memory": 4096},
    "EphemeralStorageMetrics": {"Utilized": 100, "Reserved": 21504},
}


def _stats(cpu_ns: int, prev_ns: int) -> dict:
    return {
        "abc": {
            "read": "2026-09-29T12:00:02.000000000Z",
            "preread": "2026-09-29T12:00:00.000000000Z",
            "cpu_stats": {"cpu_usage": {"total_usage": cpu_ns}},
            "precpu_stats": {"cpu_usage": {"total_usage": prev_ns}},
            "memory_stats": {"usage": 3 * 2**20, "stats": {"inactive_file": 2**20}},
            "networks": {"eth1": {"rx_bytes": 10, "tx_bytes": 20}},
        },
        "pending": None,  # containers not yet started report null
    }


def test_metrics_sample():
    s = metrics_sample(TASK, _stats(4 * 10**9, 10**9))  # 3 cpu-seconds in 2s
    assert s["cpu"] == 150
    assert s["cpu_limit"] == 200
    assert s["mem"] == 2 * 2**20
    assert s["mem_limit"] == 4096 * 2**20
    assert (s["disk"], s["disk_limit"]) == (100 * 2**20, 21504 * 2**20)
    assert (s["rx"], s["tx"]) == (10, 20)


def test_metrics_sample_first_read():
    stats = _stats(10**9, 0)
    stats["abc"]["preread"] = "0001-01-01T00:00:00Z"
    assert metrics_sample(TASK, stats)["cpu"] == 0


def test_parse_metrics_skips_other_lines():
    events = [
        {"message": "INFO hello"},
        {"message": METRICS_PREFIX + json.dumps({"t": 1, "cpu": 50})},
        {"message": METRICS_PREFIX + "{bad"},
    ]
    assert parse_metrics(events) == [{"t": 1, "cpu": 50}]
