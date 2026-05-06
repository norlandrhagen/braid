"""
Integration test for the detached submission path: submit() -> status() ->
result() -> attach() -> result() again. Reuses the in-process worker harness
from tests/integration/test_fan_batch.py (moto S3/SQS, submit_array_job mocked
with a side effect that runs _worker_batch.main() per array index).
"""

from __future__ import annotations

import os
import uuid

import boto3
import pytest
from braid import _worker_batch
from braid.config import Config
from braid.detach import attach, status, submit
from braid.errors import WorkerError
from moto import mock_aws

REGION = "us-east-1"
BUCKET = "test-bucket"


@pytest.fixture
def aws_resources():
    with mock_aws():
        s3 = boto3.client("s3", region_name=REGION)
        sqs = boto3.client("sqs", region_name=REGION)

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
            "BRAID_COMMON_URL",
            "BRAID_DETACH",
        ]
        old = {k: os.environ.get(k) for k in env_keys}

        yield {"s3": s3, "sqs": sqs, "queue_url": queue_url}

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
    def _submit(client, job_def_name, queue_name, job_id, array_size, env_overrides):
        old = {
            k: os.environ.get(k)
            for k in list(env_overrides) + ["AWS_BATCH_JOB_ARRAY_INDEX"]
        }
        for k, v in env_overrides.items():
            os.environ[k] = v
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


@pytest.fixture
def mocked_batch(mocker):
    mocker.patch("braid.dispatch_batch._ensure_job_definition")
    mocker.patch("braid.detach.sync_uv_lock", return_value="test/requirements.txt")
    mocker.patch("braid.detach.sync_editables", return_value=[])
    mocker.patch(
        "braid.aws.batch_client.submit_array_job",
        side_effect=_make_submit_side_effect(),
    )
    # The worker runs synchronously inside submit_array_job above, so by the
    # time submit() returns, results are already in S3 — describe_jobs only
    # needs to report a terminal state, not drive completion.
    mocker.patch(
        "braid.aws.batch_client.describe_jobs",
        side_effect=lambda client, job_ids: [
            {"jobId": jid, "status": "SUCCEEDED"} for jid in job_ids
        ],
    )


class TestDetachBatchRoundTrip:
    def test_submit_status_result_attach_result(self, aws_resources, mocked_batch):
        cfg = _make_config(aws_resources["queue_url"])

        handle = submit(lambda x: x * 2, list(range(5)), config=cfg)

        st = handle.status()
        assert st.complete is True
        assert st.done == 5
        assert st.failed == 0

        assert status(handle.job_id, config=cfg).complete is True

        results = handle.result(timeout_s=10)
        assert results == [x * 2 for x in range(5)]

        reattached = attach(handle.job_id, config=cfg)
        results_again = reattached.result(timeout_s=10)
        assert results_again == [x * 2 for x in range(5)]

    def test_result_raises_worker_error_on_failures(self, aws_resources, mocked_batch):
        cfg = _make_config(aws_resources["queue_url"])

        handle = submit(lambda x: 1 / x, [1, 0, 2], config=cfg)

        with pytest.raises(WorkerError) as exc_info:
            handle.result(timeout_s=10)
        assert exc_info.value.idx == 1


class TestDetachDoesNotSendSqs:
    def test_no_sqs_messages_for_detached_job(self, aws_resources, mocked_batch):
        cfg = _make_config(aws_resources["queue_url"])
        submit(lambda x: x, [1, 2], config=cfg)

        msgs = (
            aws_resources["sqs"]
            .receive_message(
                QueueUrl=aws_resources["queue_url"], MaxNumberOfMessages=10
            )
            .get("Messages", [])
        )
        assert msgs == []
