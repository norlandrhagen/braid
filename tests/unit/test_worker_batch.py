"""
Unit tests for _worker_batch.main(). Calls the worker in-process with moto
S3/SQS, mirroring the pattern in tests/integration/test_worker.py for Lambda.

Bootstrap steps (_bootstrap_uv_sync, _bootstrap_editables) are skipped by not
setting BRAID_REQUIREMENTS_KEY or BRAID_EDITABLES in the environment.
"""

from __future__ import annotations

import base64
import json
import os

import boto3
import cloudpickle
import pytest
from braid import _worker_batch
from moto import mock_aws

REGION = "us-east-1"
BUCKET = "test-bucket"
JOB_ID = "test-job"


@pytest.fixture
def aws_env():
    with mock_aws():
        s3 = boto3.client("s3", region_name=REGION)
        sqs = boto3.client("sqs", region_name=REGION)

        s3.create_bucket(Bucket=BUCKET)
        resp = sqs.create_queue(QueueName="test-queue")
        queue_url = resp["QueueUrl"]

        env_keys = [
            "BRAID_BUCKET",
            "BRAID_BUCKET_PREFIX",
            "BRAID_SQS_QUEUE_URL",
            "BRAID_JOB_ID",
            "AWS_BATCH_JOB_ARRAY_INDEX",
            "BRAID_REQUIREMENTS_KEY",
            "BRAID_EDITABLES",
        ]
        old = {k: os.environ.get(k) for k in env_keys}

        os.environ["BRAID_BUCKET"] = BUCKET
        os.environ["BRAID_BUCKET_PREFIX"] = ""
        os.environ["BRAID_SQS_QUEUE_URL"] = queue_url
        os.environ["BRAID_JOB_ID"] = JOB_ID
        os.environ.pop("BRAID_REQUIREMENTS_KEY", None)
        os.environ.pop("BRAID_EDITABLES", None)

        yield {"s3": s3, "sqs": sqs, "queue_url": queue_url}

        for k, v in old.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def _write_manifest(s3, inputs: list, fn, prefix: str = "") -> None:
    fn_key = f"{prefix}jobs/{JOB_ID}/fn.pkl".lstrip("/")
    s3.put_object(Bucket=BUCKET, Key=fn_key, Body=cloudpickle.dumps(fn))

    manifest = {
        "fn_key": fn_key,
        "inputs": [
            {"idx": i, "data_b64": base64.b64encode(cloudpickle.dumps(x)).decode()}
            for i, x in enumerate(inputs)
        ],
    }
    manifest_key = f"{prefix}jobs/{JOB_ID}/manifest.json".lstrip("/")
    s3.put_object(Bucket=BUCKET, Key=manifest_key, Body=json.dumps(manifest).encode())


class TestWorkerBatchHappyPath:
    def test_happy_path(self, aws_env):
        s3 = aws_env["s3"]
        sqs = aws_env["sqs"]
        queue_url = aws_env["queue_url"]

        _write_manifest(s3, [4], lambda x: x**2)
        os.environ["AWS_BATCH_JOB_ARRAY_INDEX"] = "0"

        _worker_batch.main()

        msgs = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10).get(
            "Messages", []
        )
        assert len(msgs) == 1
        body = json.loads(msgs[0]["Body"])
        assert body["ok"] is True
        assert body["idx"] == 0
        assert body["inline"] is True
        result = cloudpickle.loads(base64.b64decode(body["data_b64"]))
        assert result == 16

    def test_array_index_selects_correct_input(self, aws_env):
        s3 = aws_env["s3"]
        sqs = aws_env["sqs"]
        queue_url = aws_env["queue_url"]

        _write_manifest(s3, [10, 20, 30], lambda x: x + 1)
        os.environ["AWS_BATCH_JOB_ARRAY_INDEX"] = "1"

        _worker_batch.main()

        msgs = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10).get(
            "Messages", []
        )
        assert len(msgs) == 1
        body = json.loads(msgs[0]["Body"])
        assert body["idx"] == 1
        assert body["ok"] is True
        result = cloudpickle.loads(base64.b64decode(body["data_b64"]))
        assert result == 21


class TestWorkerBatchErrorPath:
    def test_error_writes_sqs_and_s3(self, aws_env):
        s3 = aws_env["s3"]
        sqs = aws_env["sqs"]
        queue_url = aws_env["queue_url"]

        _write_manifest(s3, [0], lambda x: 1 / x)
        os.environ["AWS_BATCH_JOB_ARRAY_INDEX"] = "0"

        _worker_batch.main()

        msgs = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10).get(
            "Messages", []
        )
        assert len(msgs) == 1
        body = json.loads(msgs[0]["Body"])
        assert body["ok"] is False
        assert body["idx"] == 0
        assert "s3_key" in body

        error_obj = s3.get_object(Bucket=BUCKET, Key=body["s3_key"])
        error_data = json.loads(error_obj["Body"].read())
        assert (
            "division by zero" in error_data["error"].lower()
            or "zerodivisionerror" in error_data["error"].lower()
        )
        assert "traceback" in error_data


class TestWorkerBatchLargeResult:
    def test_large_result_written_to_s3(self, aws_env):
        s3 = aws_env["s3"]
        sqs = aws_env["sqs"]
        queue_url = aws_env["queue_url"]

        _write_manifest(s3, ["x"], lambda _: b"z" * (300 * 1024))
        os.environ["AWS_BATCH_JOB_ARRAY_INDEX"] = "0"

        _worker_batch.main()

        msgs = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10).get(
            "Messages", []
        )
        assert len(msgs) == 1
        body = json.loads(msgs[0]["Body"])
        assert body["ok"] is True
        assert body["inline"] is False
        assert "s3_key" in body

        result_obj = s3.get_object(Bucket=BUCKET, Key=body["s3_key"])
        result = cloudpickle.loads(result_obj["Body"].read())
        assert result == b"z" * (300 * 1024)


class TestWorkerBatchBucketPrefix:
    def test_bucket_prefix(self, aws_env):
        s3 = aws_env["s3"]
        sqs = aws_env["sqs"]
        queue_url = aws_env["queue_url"]

        os.environ["BRAID_BUCKET_PREFIX"] = "my-prefix"
        _write_manifest(s3, [7], lambda x: x * 3, prefix="my-prefix/")
        os.environ["AWS_BATCH_JOB_ARRAY_INDEX"] = "0"

        _worker_batch.main()

        msgs = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10).get(
            "Messages", []
        )
        assert len(msgs) == 1
        body = json.loads(msgs[0]["Body"])
        assert body["ok"] is True
        result = cloudpickle.loads(base64.b64decode(body["data_b64"]))
        assert result == 21

        os.environ["BRAID_BUCKET_PREFIX"] = ""


class TestWorkerBatchSingleJob:
    def test_missing_array_index_defaults_to_zero(self, aws_env):
        """Single (non-array) jobs have no AWS_BATCH_JOB_ARRAY_INDEX env var; should default to 0."""
        s3 = aws_env["s3"]
        sqs = aws_env["sqs"]
        queue_url = aws_env["queue_url"]

        _write_manifest(s3, [99], lambda x: x + 1)
        # Don't set AWS_BATCH_JOB_ARRAY_INDEX — it won't be present for non-array jobs
        os.environ.pop("AWS_BATCH_JOB_ARRAY_INDEX", None)

        _worker_batch.main()

        msgs = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10).get(
            "Messages", []
        )
        assert len(msgs) == 1
        body = json.loads(msgs[0]["Body"])
        assert body["ok"] is True
        assert body["idx"] == 0
        result = cloudpickle.loads(base64.b64decode(body["data_b64"]))
        assert result == 100
