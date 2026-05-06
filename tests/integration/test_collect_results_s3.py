"""
Integration tests for result_collector.collect_results_s3 (the detached-job
collector). Mirrors tests/integration/test_result_collector.py for the SQS path.
"""

from __future__ import annotations

import json
import time

import boto3
import cloudpickle
import pytest
from braid.errors import JobTimeoutError
from braid.result_collector import collect_results_s3
from moto import mock_aws

REGION = "us-east-1"
BUCKET = "test-bucket"
PREFIX = "jobs/job1/"


@pytest.fixture
def s3():
    with mock_aws():
        client = boto3.client("s3", region_name=REGION)
        client.create_bucket(Bucket=BUCKET)
        yield client


def _put_result(s3, idx: int, value) -> None:
    s3.put_object(
        Bucket=BUCKET, Key=f"{PREFIX}results/{idx}.pkl", Body=cloudpickle.dumps(value)
    )


def _put_error(s3, idx: int, error: str, tb: str = "") -> None:
    s3.put_object(
        Bucket=BUCKET,
        Key=f"{PREFIX}errors/{idx}.json",
        Body=json.dumps({"error": error, "traceback": tb}).encode(),
    )


class TestCollectResultsS3Ordering:
    def test_results_ordered_by_idx(self, s3):
        _put_result(s3, 2, "c")
        _put_result(s3, 0, "a")
        _put_result(s3, 1, "b")

        results, failures = collect_results_s3(
            job_id="job1",
            n=3,
            bucket=BUCKET,
            prefix=PREFIX,
            region=REGION,
            timeout_s=5,
            show_progress=False,
        )
        assert failures == []
        assert [results[i] for i in range(3)] == ["a", "b", "c"]


class TestCollectResultsS3Errors:
    def test_error_objects_returned_as_failures(self, s3):
        _put_result(s3, 0, "ok")
        _put_error(s3, 1, "boom", "trace")

        results, failures = collect_results_s3(
            job_id="job1",
            n=2,
            bucket=BUCKET,
            prefix=PREFIX,
            region=REGION,
            timeout_s=5,
            show_progress=False,
        )
        assert results == {0: "ok"}
        assert len(failures) == 1
        assert failures[0]["idx"] == 1
        assert failures[0]["error"] == "boom"


class TestCollectResultsS3Timeout:
    def test_missing_indices_raise_timeout(self, s3):
        _put_result(s3, 0, "only one")

        with pytest.raises(JobTimeoutError) as exc_info:
            collect_results_s3(
                job_id="job1",
                n=3,
                bucket=BUCKET,
                prefix=PREFIX,
                region=REGION,
                timeout_s=1,
                show_progress=False,
                poll_interval_s=0.1,
            )
        assert exc_info.value.received == 1
        assert exc_info.value.expected == 3


class TestCollectResultsS3Idempotent:
    def test_two_sequential_calls_each_return_full_results(self, s3):
        """Regression guard for the SQS drain hazard: S3 reads must not delete."""
        _put_result(s3, 0, "a")
        _put_result(s3, 1, "b")

        first, _ = collect_results_s3(
            job_id="job1",
            n=2,
            bucket=BUCKET,
            prefix=PREFIX,
            region=REGION,
            timeout_s=5,
            show_progress=False,
        )
        second, _ = collect_results_s3(
            job_id="job1",
            n=2,
            bucket=BUCKET,
            prefix=PREFIX,
            region=REGION,
            timeout_s=5,
            show_progress=False,
        )
        assert first == second == {0: "a", 1: "b"}


class TestCollectResultsS3EmptyN:
    def test_n_zero_returns_immediately(self, s3):
        start = time.monotonic()
        results, failures = collect_results_s3(
            job_id="job1",
            n=0,
            bucket=BUCKET,
            prefix=PREFIX,
            region=REGION,
            timeout_s=5,
            show_progress=False,
        )
        assert results == {} and failures == []
        assert time.monotonic() - start < 1.0
