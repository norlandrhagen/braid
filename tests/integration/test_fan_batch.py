"""
Integration tests for fan(..., backend="batch"). Mirrors test_fan.py for Lambda.

Strategy: mock batch_client.submit_array_job with a side-effect that reads the
submitted env overrides, then calls _worker_batch.main() in-process for each
array index. S3 and SQS run against real moto mocks.
"""

from __future__ import annotations

import os
import uuid

import boto3
import pytest
from braid import _worker_batch
from braid.config import Config
from braid.core import fan
from braid.errors import WorkerError
from moto import mock_aws

REGION = "us-east-1"
BUCKET = "test-bucket"


@pytest.fixture
def aws_resources():
    with mock_aws():
        s3 = boto3.client("s3", region_name=REGION)
        sqs = boto3.client("sqs", region_name=REGION)
        batch = boto3.client("batch", region_name=REGION)

        s3.create_bucket(Bucket=BUCKET)
        resp = sqs.create_queue(QueueName="braid-results")
        queue_url = resp["QueueUrl"]

        env_keys = [
            "BRAID_BUCKET",
            "BRAID_BUCKET_PREFIX",
            "BRAID_SQS_QUEUE_URL",
            "BRAID_JOB_ID",
            "BRAID_REQUIREMENTS_KEY",
            "BRAID_EDITABLES",
            "AWS_BATCH_JOB_ARRAY_INDEX",
            "BRAID_WORKER_URL",
        ]
        old = {k: os.environ.get(k) for k in env_keys}

        yield {
            "s3": s3,
            "sqs": sqs,
            "batch": batch,
            "queue_url": queue_url,
            "region": REGION,
            "bucket": BUCKET,
        }

        for k, v in old.items():
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
        backend="batch",
        batch_job_queue="braid-queue",
        batch_job_definition="braid-job",
        batch_execution_role_arn="arn:aws:iam::123456789012:role/test",
    )


def _make_submit_side_effect():
    """
    Returns a submit_array_job side-effect that runs _worker_batch.main()
    in-process for each array index, simulating Batch task execution.
    """
    _ENV_PASSTHROUGH = [
        "BRAID_JOB_ID",
        "BRAID_BUCKET",
        "BRAID_BUCKET_PREFIX",
        "BRAID_SQS_QUEUE_URL",
        "BRAID_REQUIREMENTS_KEY",
        "BRAID_WORKER_URL",
    ]

    def _submit(client, job_def_name, queue_name, job_id, array_size, env_overrides):
        old = {
            k: os.environ.get(k)
            for k in list(env_overrides) + ["AWS_BATCH_JOB_ARRAY_INDEX"]
        }
        for k, v in env_overrides.items():
            os.environ[k] = v
        # Skip bootstrap: no BRAID_REQUIREMENTS_KEY unless explicitly set
        os.environ.pop("BRAID_REQUIREMENTS_KEY", None)
        os.environ.pop("BRAID_EDITABLES", None)
        try:
            for i in range(array_size):
                os.environ["AWS_BATCH_JOB_ARRAY_INDEX"] = str(i)
                _worker_batch.main()
        finally:
            for k, v in old.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v
        return str(uuid.uuid4())

    return _submit


def _make_submit_single_side_effect():
    """
    Returns a submit_single_job side-effect that runs _worker_batch.main()
    in-process once (for index 0), simulating single Batch task execution.
    For single jobs, AWS_BATCH_JOB_ARRAY_INDEX is not set; worker defaults to 0.
    """

    def _submit(client, job_def_name, queue_name, job_id, env_overrides):
        old = {
            k: os.environ.get(k)
            for k in list(env_overrides) + ["AWS_BATCH_JOB_ARRAY_INDEX"]
        }
        for k, v in env_overrides.items():
            os.environ[k] = v
        # Skip bootstrap: no BRAID_REQUIREMENTS_KEY unless explicitly set
        os.environ.pop("BRAID_REQUIREMENTS_KEY", None)
        os.environ.pop("BRAID_EDITABLES", None)
        # For single jobs, AWS_BATCH_JOB_ARRAY_INDEX is not set by AWS;
        # the worker defaults to 0 when it's absent.
        os.environ.pop("AWS_BATCH_JOB_ARRAY_INDEX", None)
        try:
            _worker_batch.main()
        finally:
            for k, v in old.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v
        return str(uuid.uuid4())

    return _submit


@pytest.fixture
def mocked_batch(mocker):
    mocker.patch("braid.dispatch_batch._ensure_job_definition")
    mocker.patch("braid.core.sync_uv_lock", return_value="test/requirements.txt")
    mocker.patch("braid.core.sync_editables", return_value=[])
    mocker.patch(
        "braid.aws.batch_client.submit_array_job",
        side_effect=_make_submit_side_effect(),
    )
    mocker.patch(
        "braid.aws.batch_client.submit_single_job",
        side_effect=_make_submit_single_side_effect(),
    )


class TestFanBatchHappyPath:
    def test_squares(self, aws_resources, mocked_batch):
        config = _make_config(aws_resources["queue_url"])
        results = fan(
            lambda x: x**2,
            list(range(10)),
            config=config,
            cleanup=False,
            timeout_s=30,
        )
        assert results == [x**2 for x in range(10)]

    def test_single_input(self, aws_resources, mocked_batch):
        config = _make_config(aws_resources["queue_url"])
        results = fan(
            lambda x: x * 3,
            [7],
            config=config,
            cleanup=False,
            timeout_s=30,
        )
        assert results == [21]

    def test_results_in_order(self, aws_resources, mocked_batch):
        config = _make_config(aws_resources["queue_url"])
        inputs = list(range(20))
        results = fan(
            lambda x: x,
            inputs,
            config=config,
            cleanup=False,
            timeout_s=30,
        )
        assert results == inputs


class TestFanBatchWorkerError:
    def test_worker_error_propagates(self, aws_resources, mocked_batch):
        config = _make_config(aws_resources["queue_url"])

        with pytest.raises(WorkerError) as exc_info:
            fan(
                lambda x: 1 / x,
                [1, 0, 2],
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


class TestFanBatchRetry:
    def test_retry_recovers_transient_failure(self, aws_resources, mocker):
        mocker.patch("braid.dispatch_batch._ensure_job_definition")
        mocker.patch("braid.core.sync_uv_lock", return_value="test/requirements.txt")
        mocker.patch("braid.core.sync_editables", return_value=[])

        first_call_done = [False]
        real_side_effect = _make_submit_side_effect()
        real_single_side_effect = _make_submit_single_side_effect()

        def _inject_failure(
            client, job_def_name, queue_name, job_id, array_size, env_overrides
        ):
            if not first_call_done[0]:
                # Run real worker for all but index 1 — inject a fake error for index 1
                import base64 as _b64
                import json as _json

                import boto3 as _boto3
                import cloudpickle as _cp

                s3 = _boto3.client("s3", region_name=REGION)
                sqs = _boto3.client("sqs", region_name=REGION)
                queue_url = aws_resources["queue_url"]

                old = {
                    k: os.environ.get(k)
                    for k in list(env_overrides) + ["AWS_BATCH_JOB_ARRAY_INDEX"]
                }
                for k, v in env_overrides.items():
                    os.environ[k] = v
                os.environ.pop("BRAID_REQUIREMENTS_KEY", None)
                os.environ.pop("BRAID_EDITABLES", None)

                try:
                    manifest_key = f"jobs/{job_id}/manifest.json"
                    manifest = _json.loads(
                        s3.get_object(Bucket=BUCKET, Key=manifest_key)["Body"].read()
                    )
                    fn = _cp.loads(
                        s3.get_object(Bucket=BUCKET, Key=manifest["fn_key"])[
                            "Body"
                        ].read()
                    )
                    for i in range(array_size):
                        input_val = _cp.loads(
                            _b64.b64decode(manifest["inputs"][i]["data_b64"])
                        )
                        if i == 1:
                            error_key = f"jobs/{job_id}/{i}.error.json"
                            s3.put_object(
                                Bucket=BUCKET,
                                Key=error_key,
                                Body=_json.dumps(
                                    {"error": "transient", "traceback": ""}
                                ).encode(),
                            )
                            sqs.send_message(
                                QueueUrl=queue_url,
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
                            result = _cp.dumps(fn(input_val))
                            sqs.send_message(
                                QueueUrl=queue_url,
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
                finally:
                    for k, v in old.items():
                        if v is None:
                            os.environ.pop(k, None)
                        else:
                            os.environ[k] = v

                first_call_done[0] = True
                return str(uuid.uuid4())
            else:
                return real_side_effect(
                    client, job_def_name, queue_name, job_id, array_size, env_overrides
                )

        mocker.patch(
            "braid.aws.batch_client.submit_array_job",
            side_effect=_inject_failure,
        )
        mocker.patch(
            "braid.aws.batch_client.submit_single_job",
            side_effect=real_single_side_effect,
        )

        config = _make_config(aws_resources["queue_url"])
        results = fan(
            lambda x: x * 10,
            [0, 1, 2],
            max_retries=1,
            config=config,
            cleanup=False,
            timeout_s=30,
        )
        assert results == [0, 10, 20]

    def test_retry_exhausted_raises_worker_error(self, aws_resources, mocked_batch):
        config = _make_config(aws_resources["queue_url"])

        with pytest.raises(WorkerError) as exc_info:
            fan(
                lambda x: 1 / x,
                [1, 0, 2],
                max_retries=1,
                config=config,
                cleanup=False,
                timeout_s=30,
            )

        err = exc_info.value
        assert err.idx == 1
        assert len(err.failures) == 1
