"""
Integration test for fan.

Strategy: mock lambda_client.invoke_async to call the worker handler directly
(in-process), while S3 and SQS run against real moto mocks. This tests the
full dispatch → collect round-trip without needing actual Lambda execution.
"""

from __future__ import annotations

import io
import json
import os
import zipfile

import boto3
import pytest
from braid._worker_lambda import handler as worker_handler
from braid.config import Config
from braid.core import fan
from moto import mock_aws

REGION = "us-east-1"
BUCKET = "test-bucket"
WORKER_NAME = "braid-worker"


@pytest.fixture
def aws_resources():
    with mock_aws():
        s3 = boto3.client("s3", region_name=REGION)
        sqs = boto3.client("sqs", region_name=REGION)
        lam = boto3.client("lambda", region_name=REGION)
        iam = boto3.client("iam", region_name=REGION)

        s3.create_bucket(Bucket=BUCKET)
        resp = sqs.create_queue(QueueName="braid-results")
        queue_url = resp["QueueUrl"]

        # Create a minimal IAM role so Lambda function creation works
        trust = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"Service": "lambda.amazonaws.com"},
                        "Action": "sts:AssumeRole",
                    }
                ],
            }
        )
        role_arn = iam.create_role(
            RoleName="braid-worker-role", AssumeRolePolicyDocument=trust
        )["Role"]["Arn"]

        # Create a stub Lambda function (moto won't execute it, we'll intercept invokes)
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("handler.py", "def handler(event, context): pass")
        lam.create_function(
            FunctionName=WORKER_NAME,
            Runtime="python3.13",
            Role=role_arn,
            Handler="handler.handler",
            Code={"ZipFile": buf.getvalue()},
            Timeout=300,
            MemorySize=512,
        )

        # Set env vars so worker handler can find S3/SQS
        old_env = {
            k: os.environ.get(k)
            for k in ["BRAID_BUCKET", "BRAID_BUCKET_PREFIX", "BRAID_SQS_QUEUE_URL"]
        }
        os.environ["BRAID_BUCKET"] = BUCKET
        os.environ["BRAID_BUCKET_PREFIX"] = ""
        os.environ["BRAID_SQS_QUEUE_URL"] = queue_url

        yield {
            "s3": s3,
            "sqs": sqs,
            "lam": lam,
            "queue_url": queue_url,
            "bucket": BUCKET,
            "region": REGION,
        }

        for k, v in old_env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def _make_config(queue_url: str) -> Config:
    return Config(
        region=REGION,
        bucket=BUCKET,
        bucket_prefix="",
        sqs_queue_url=queue_url,
        worker_function_name=WORKER_NAME,
    )


def _make_invoke_side_effect(mocker):
    """Returns a side_effect that calls the worker handler synchronously."""

    def _invoke(lam_client, function_name: str, payload: dict) -> None:
        inputs_b64 = payload["inputs_b64"]
        event = {
            "job_id": payload["job_id"],
            "fn_key": payload["fn_key"],
            "inputs_b64": inputs_b64,
            "start_idx": payload["start_idx"],
            "batch_size": payload["batch_size"],
        }
        worker_handler(event, None)

    return _invoke


_SYNCED_HASH = "abc123def456abcd"


@pytest.fixture
def synced(mocker):
    """Mock the staleness check so fan() believes braid sync was run."""
    mocker.patch("braid.core.hash_uv_lock", return_value=_SYNCED_HASH)
    mocker.patch(
        "braid.core._load_applied_state", return_value={"lock_hash": _SYNCED_HASH}
    )


class TestRemoteParallelMapHappyPath:
    def test_squares(self, aws_resources, mocker, synced):
        mocker.patch(
            "braid.aws.lambda_client.invoke_async",
            side_effect=_make_invoke_side_effect(mocker),
        )

        config = _make_config(aws_resources["queue_url"])
        results = fan(
            lambda x: x**2,
            list(range(10)),
            batch_size=5,
            config=config,
            cleanup=False,
            timeout_s=30,
        )

        assert results == [x**2 for x in range(10)]

    def test_single_input(self, aws_resources, mocker, synced):
        mocker.patch(
            "braid.aws.lambda_client.invoke_async",
            side_effect=_make_invoke_side_effect(mocker),
        )

        config = _make_config(aws_resources["queue_url"])
        results = fan(
            lambda x: x * 2,
            [21],
            batch_size=1,
            config=config,
            cleanup=False,
            timeout_s=30,
        )

        assert results == [42]

    def test_returns_in_order(self, aws_resources, mocker, synced):
        """Results must be ordered by input index even if workers complete out of order."""
        mocker.patch(
            "braid.aws.lambda_client.invoke_async",
            side_effect=_make_invoke_side_effect(mocker),
        )

        config = _make_config(aws_resources["queue_url"])
        inputs = list(range(20))
        results = fan(
            lambda x: x,
            inputs,
            batch_size=3,
            config=config,
            cleanup=False,
            timeout_s=30,
        )

        assert results == inputs


class TestRemoteParallelMapWorkerError:
    def test_worker_error_propagates(self, aws_resources, mocker, synced):
        from braid.errors import WorkerError

        mocker.patch(
            "braid.aws.lambda_client.invoke_async",
            side_effect=_make_invoke_side_effect(mocker),
        )

        config = _make_config(aws_resources["queue_url"])

        with pytest.raises(WorkerError) as exc_info:
            fan(
                lambda x: 1 / x,
                [1, 0, 2],
                batch_size=3,
                max_retries=0,
                config=config,
                cleanup=False,
                timeout_s=30,
            )

        err = exc_info.value
        assert err.idx == 1
        assert (
            "division by zero" in err.original.lower()
            or "zerodivisionerror" in err.original.lower()
        )
        assert len(err.failures) == 1


class TestRemoteParallelMapRetry:
    def test_retry_recovers_transient_failure(self, aws_resources, mocker, synced):
        """First batch for job_id_0 injects a failure for idx=1; retry job succeeds."""
        first_job_done = [False]
        original_invoke = _make_invoke_side_effect(mocker)

        def inject_one_failure(lam_client, function_name, payload):
            if not first_job_done[0] and payload["start_idx"] == 0:
                # Run the normal worker for inputs 0 and 2, inject error for idx=1
                import base64 as _b64
                import json as _json

                import boto3 as _boto3
                import cloudpickle as _cp

                sqs = _boto3.client("sqs", region_name=REGION)
                s3 = _boto3.client("s3", region_name=REGION)
                job_id = payload["job_id"]
                inputs = _cp.loads(_b64.b64decode(payload["inputs_b64"]))
                fn_data = s3.get_object(Bucket=BUCKET, Key=payload["fn_key"])[
                    "Body"
                ].read()
                fn = _cp.loads(fn_data)
                for i, x in enumerate(inputs):
                    if x == 1:
                        error_key = f"jobs/{job_id}/{i}.error.json"
                        s3.put_object(
                            Bucket=BUCKET,
                            Key=error_key,
                            Body=_json.dumps(
                                {"error": "transient", "traceback": ""}
                            ).encode(),
                        )
                        sqs.send_message(
                            QueueUrl=aws_resources["queue_url"],
                            MessageBody=_json.dumps(
                                {
                                    "job_id": job_id,
                                    "idx": i,
                                    "ok": False,
                                    "s3_key": error_key,
                                }
                            ),
                        )
                    else:
                        result = _cp.dumps(fn(x))
                        sqs.send_message(
                            QueueUrl=aws_resources["queue_url"],
                            MessageBody=_json.dumps(
                                {
                                    "job_id": job_id,
                                    "idx": i,
                                    "ok": True,
                                    "inline": True,
                                    "data_b64": _b64.b64encode(result).decode(),
                                }
                            ),
                        )
                first_job_done[0] = True
            else:
                original_invoke(lam_client, function_name, payload)

        mocker.patch(
            "braid.aws.lambda_client.invoke_async", side_effect=inject_one_failure
        )

        config = _make_config(aws_resources["queue_url"])
        results = fan(
            lambda x: x * 10,
            [0, 1, 2],
            batch_size=3,
            max_retries=1,
            config=config,
            cleanup=False,
            timeout_s=30,
        )

        assert results == [0, 10, 20]

    def test_retry_exhausted_raises_worker_error(self, aws_resources, mocker, synced):
        from braid.errors import WorkerError

        mocker.patch(
            "braid.aws.lambda_client.invoke_async",
            side_effect=_make_invoke_side_effect(mocker),
        )

        config = _make_config(aws_resources["queue_url"])

        with pytest.raises(WorkerError) as exc_info:
            fan(
                lambda x: 1 / x,  # always fails for x=0
                [1, 0, 2],
                batch_size=3,
                max_retries=1,
                config=config,
                cleanup=False,
                timeout_s=30,
            )

        err = exc_info.value
        assert err.idx == 1  # original index preserved through retry
        assert len(err.failures) == 1


class TestRemoteParallelMapDryRun:
    def test_dry_run_returns_empty(self, aws_resources, mocker):
        config = _make_config(aws_resources["queue_url"])
        results = fan(
            lambda x: x,
            list(range(5)),
            dry_run=True,
            config=config,
        )

        assert results == []


class TestPayloadSizeCheck:
    """Test Lambda invoke payload size validation."""

    def test_oversized_batch_raises_payload_too_large_error(
        self, aws_resources, mocker, synced
    ):
        """A batch exceeding 250KB should raise PayloadTooLargeError before invoking."""
        from braid.errors import PayloadTooLargeError

        mocker.patch(
            "braid.aws.lambda_client.invoke_async",
            side_effect=_make_invoke_side_effect(mocker),
        )

        config = _make_config(aws_resources["queue_url"])

        # Create inputs that when serialized will exceed 250KB
        # Each string of ~10KB * 30 items = ~300KB when pickled+b64 encoded
        large_inputs = ["x" * (10 * 1024) for _ in range(30)]

        with pytest.raises(PayloadTooLargeError) as exc_info:
            fan(
                lambda x: x,
                large_inputs,
                batch_size=30,  # All in one batch
                config=config,
                cleanup=False,
                timeout_s=30,
            )

        err = exc_info.value
        assert "batch inputs" in str(err)
        assert "reducing batch_size" in str(err) or "batch" in str(err).lower()

    def test_normal_batch_size_works(self, aws_resources, mocker, synced):
        """Normal-sized batches should work without hitting the size limit."""
        mocker.patch(
            "braid.aws.lambda_client.invoke_async",
            side_effect=_make_invoke_side_effect(mocker),
        )

        config = _make_config(aws_resources["queue_url"])
        # Small inputs that won't exceed the limit
        small_inputs = list(range(100))

        results = fan(
            lambda x: x * 2,
            small_inputs,
            batch_size=50,
            config=config,
            cleanup=False,
            timeout_s=30,
        )

        assert results == [x * 2 for x in small_inputs]


class TestRetryArtifactCleanup:
    """Test that all retry job_ids get cleaned up, not just the original."""

    def test_retry_cleanup_removes_all_job_ids(self, aws_resources, mocker, synced):
        """Verify that cleanup removes S3 artifacts for all retry job_ids."""
        import uuid as uuid_module

        mocker.patch(
            "braid.aws.lambda_client.invoke_async",
            side_effect=_make_invoke_side_effect(mocker),
        )

        config = _make_config(aws_resources["queue_url"])

        # Mock uuid.uuid4 to return predictable values so we know the job_ids
        # Keep a counter to track which calls are for job_ids (0, 1) vs other uses
        job_id_original = uuid_module.UUID("12345678-1234-1234-1234-123456789012")
        job_id_retry = uuid_module.UUID("87654321-4321-4321-4321-210987654321")
        job_id_values = [job_id_original, job_id_retry]
        call_count = [0]
        real_uuid4 = uuid_module.uuid4

        def mock_uuid4():
            # Use our tracked values for the first two calls (job_ids), then fall back
            if call_count[0] < 2:
                result = job_id_values[call_count[0]]
                call_count[0] += 1
                return result
            else:
                # For other calls (boto3 internal), use real uuid4
                return real_uuid4()

        mocker.patch(
            "braid.core.uuid.uuid4",
            side_effect=mock_uuid4,
        )

        # Run fan with a retry, without mocking delete_prefix so it runs for real
        first_job_done = [False]
        original_invoke = _make_invoke_side_effect(mocker)

        def inject_one_failure(lam_client, function_name, payload):
            if not first_job_done[0] and payload["start_idx"] == 0:
                # Run the normal worker for inputs 0 and 2, inject error for idx=1
                import base64 as _b64
                import json as _json

                import boto3 as _boto3
                import cloudpickle as _cp

                sqs = _boto3.client("sqs", region_name=REGION)
                s3 = _boto3.client("s3", region_name=REGION)
                job_id = payload["job_id"]
                inputs = _cp.loads(_b64.b64decode(payload["inputs_b64"]))
                fn_data = s3.get_object(Bucket=BUCKET, Key=payload["fn_key"])[
                    "Body"
                ].read()
                fn = _cp.loads(fn_data)
                for i, x in enumerate(inputs):
                    if x == 1:
                        error_key = f"jobs/{job_id}/{i}.error.json"
                        s3.put_object(
                            Bucket=BUCKET,
                            Key=error_key,
                            Body=_json.dumps(
                                {"error": "transient", "traceback": ""}
                            ).encode(),
                        )
                        sqs.send_message(
                            QueueUrl=aws_resources["queue_url"],
                            MessageBody=_json.dumps(
                                {
                                    "job_id": job_id,
                                    "idx": i,
                                    "ok": False,
                                    "s3_key": error_key,
                                }
                            ),
                        )
                    else:
                        result = _cp.dumps(fn(x))
                        sqs.send_message(
                            QueueUrl=aws_resources["queue_url"],
                            MessageBody=_json.dumps(
                                {
                                    "job_id": job_id,
                                    "idx": i,
                                    "ok": True,
                                    "inline": True,
                                    "data_b64": _b64.b64encode(result).decode(),
                                }
                            ),
                        )
                first_job_done[0] = True
            else:
                original_invoke(lam_client, function_name, payload)

        mocker.patch(
            "braid.aws.lambda_client.invoke_async", side_effect=inject_one_failure
        )

        results = fan(
            lambda x: x * 10,
            [0, 1, 2],
            batch_size=3,
            max_retries=1,
            config=config,
            cleanup=True,
            timeout_s=30,
        )

        assert results == [0, 10, 20]

        # Verify that S3 prefixes for both original and retry job_ids are actually empty
        s3 = aws_resources["s3"]
        job_id_original_str = str(job_id_original)
        job_id_retry_str = str(job_id_retry)

        # Check original job_id prefix
        response_original = s3.list_objects_v2(
            Bucket=BUCKET, Prefix=f"jobs/{job_id_original_str}/"
        )
        assert (
            "Contents" not in response_original
        ), f"Expected no objects under jobs/{job_id_original_str}/ but found some"

        # Check retry job_id prefix
        response_retry = s3.list_objects_v2(
            Bucket=BUCKET, Prefix=f"jobs/{job_id_retry_str}/"
        )
        assert (
            "Contents" not in response_retry
        ), f"Expected no objects under jobs/{job_id_retry_str}/ but found some"
