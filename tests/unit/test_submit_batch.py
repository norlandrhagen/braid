"""
Unit tests for dispatch_batch.submit_batch(). Uses moto S3 for artifact
verification; batch_client.submit_array_job is mocked to avoid setting up
full Batch infrastructure (VPC/SG/IAM) in moto.
"""

from __future__ import annotations

import base64
import json
import uuid

import boto3
import cloudpickle
import pytest
from braid.config import Config
from braid.dispatch_batch import submit_batch
from moto import mock_aws

REGION = "us-east-1"
BUCKET = "test-bucket"


def _make_config(
    queue_url: str = "https://sqs.us-east-1.amazonaws.com/123/q",
) -> Config:
    return Config(
        region=REGION,
        bucket=BUCKET,
        bucket_prefix="",
        sqs_queue_url=queue_url,
        batch_job_queue="braid-queue",
        batch_job_definition="braid-job",
        batch_execution_role_arn="arn:aws:iam::123456789012:role/test",
    )


@pytest.fixture
def aws_s3(mocker):
    """S3-only fixture. submit_array_job and submit_single_job are mocked to capture call args."""
    with mock_aws():
        s3 = boto3.client("s3", region_name=REGION)
        s3.create_bucket(Bucket=BUCKET)

        submitted: list[dict] = []

        def _capture_array_submit(
            client, job_def_name, queue_name, job_id, array_size, env_overrides
        ):
            fake_id = str(uuid.uuid4())
            submitted.append(
                {
                    "type": "array",
                    "job_def_name": job_def_name,
                    "queue_name": queue_name,
                    "job_id": job_id,
                    "array_size": array_size,
                    "env_overrides": env_overrides,
                    "batch_job_id": fake_id,
                }
            )
            return fake_id

        def _capture_single_submit(
            client, job_def_name, queue_name, job_id, env_overrides
        ):
            fake_id = str(uuid.uuid4())
            submitted.append(
                {
                    "type": "single",
                    "job_def_name": job_def_name,
                    "queue_name": queue_name,
                    "job_id": job_id,
                    "env_overrides": env_overrides,
                    "batch_job_id": fake_id,
                }
            )
            return fake_id

        mocker.patch("braid.dispatch_batch._ensure_job_definition")
        mocker.patch(
            "braid.aws.batch_client.submit_array_job", side_effect=_capture_array_submit
        )
        mocker.patch(
            "braid.aws.batch_client.submit_single_job",
            side_effect=_capture_single_submit,
        )

        yield {"s3": s3, "submitted": submitted}


class TestSubmitBatchS3Artifacts:
    def test_fn_and_manifest_uploaded(self, aws_s3):
        s3 = aws_s3["s3"]
        cfg = _make_config()

        def fn(x):
            return x + 1

        inputs = [1, 2, 3]
        job_id, n, _ = submit_batch(fn, inputs, cfg, "", [])

        keys = [
            obj["Key"] for obj in s3.list_objects_v2(Bucket=BUCKET).get("Contents", [])
        ]
        fn_keys = [k for k in keys if k.endswith("fn.pkl")]
        manifest_keys = [k for k in keys if k.endswith("manifest.json")]

        assert fn_keys == [f"jobs/{job_id}/fn.pkl"]
        assert manifest_keys == [f"jobs/{job_id}/manifest.json"]

    def test_manifest_structure(self, aws_s3):
        s3 = aws_s3["s3"]
        cfg = _make_config()

        inputs = ["a", "b", "c"]
        job_id, n, _ = submit_batch(lambda x: x, inputs, cfg, "", [])

        manifest = json.loads(
            s3.get_object(Bucket=BUCKET, Key=f"jobs/{job_id}/manifest.json")[
                "Body"
            ].read()
        )

        assert "fn_key" in manifest
        assert len(manifest["inputs"]) == 3
        for i, entry in enumerate(manifest["inputs"]):
            assert entry["idx"] == i
            assert cloudpickle.loads(base64.b64decode(entry["data_b64"])) == inputs[i]

    def test_worker_scripts_uploaded(self, aws_s3):
        s3 = aws_s3["s3"]
        cfg = _make_config()

        submit_batch(lambda x: x, [1], cfg, "", [])

        keys = [
            obj["Key"] for obj in s3.list_objects_v2(Bucket=BUCKET).get("Contents", [])
        ]
        worker_keys = [k for k in keys if "braid-system/worker_batch/" in k]
        common_keys = [k for k in keys if "braid-system/worker_common/" in k]
        assert len(worker_keys) == 1 and worker_keys[0].endswith(".py")
        assert len(common_keys) == 1 and common_keys[0].endswith(".py")

    def test_worker_script_dedup(self, aws_s3):
        s3 = aws_s3["s3"]
        cfg = _make_config()

        submit_batch(lambda x: x, [1], cfg, "", [])
        submit_batch(lambda x: x, [2], cfg, "", [])

        keys = [
            obj["Key"] for obj in s3.list_objects_v2(Bucket=BUCKET).get("Contents", [])
        ]
        worker_keys = [k for k in keys if "braid-system/worker_batch/" in k]
        common_keys = [k for k in keys if "braid-system/worker_common/" in k]
        assert len(worker_keys) == 1
        assert len(common_keys) == 1


class TestSubmitBatchJobSubmission:
    def test_array_size_matches_inputs(self, aws_s3):
        submitted = aws_s3["submitted"]
        cfg = _make_config()

        inputs = list(range(7))
        job_id, n, batch_job_id = submit_batch(lambda x: x, inputs, cfg, "", [])

        assert n == 7
        assert len(submitted) == 1
        assert submitted[0]["array_size"] == 7
        assert submitted[0]["batch_job_id"] == batch_job_id

    def test_env_vars_present(self, aws_s3):
        submitted = aws_s3["submitted"]
        queue_url = "https://sqs.us-east-1.amazonaws.com/123/braid-results"
        cfg = _make_config(queue_url)

        job_id, _, _ = submit_batch(lambda x: x, [1, 2], cfg, "req-key", [])

        env = submitted[0]["env_overrides"]
        assert env["BRAID_JOB_ID"] == job_id
        assert env["BRAID_BUCKET"] == BUCKET
        assert env["BRAID_SQS_QUEUE_URL"] == queue_url
        assert env["BRAID_REQUIREMENTS_KEY"] == "req-key"
        assert "BRAID_WORKER_URL" in env
        assert "BRAID_COMMON_URL" in env

    def test_returns_correct_tuple(self, aws_s3):
        cfg = _make_config()

        result = submit_batch(lambda x: x, [10, 20], cfg, "", [])

        assert len(result) == 3
        job_id, n, batch_job_id = result
        assert isinstance(job_id, str) and len(job_id) == 36
        assert n == 2
        assert isinstance(batch_job_id, str)

    def test_single_input_calls_submit_single_job(self, aws_s3):
        """Single input should use submit_single_job, not submit_array_job."""
        submitted = aws_s3["submitted"]
        cfg = _make_config()

        job_id, n, batch_job_id = submit_batch(lambda x: x * 2, [42], cfg, "", [])

        assert n == 1
        assert len(submitted) == 1
        assert submitted[0]["type"] == "single"
        assert submitted[0]["job_id"] == job_id
        assert "array_size" not in submitted[0]

    def test_zero_inputs_raises_value_error(self, aws_s3):
        """Empty inputs should raise ValueError."""
        cfg = _make_config()

        with pytest.raises(ValueError, match="at least one input"):
            submit_batch(lambda x: x, [], cfg, "", [])

    def test_over_10000_inputs_raises_value_error(self, aws_s3):
        """Inputs > 10000 should raise ValueError mentioning the limit."""
        cfg = _make_config()

        with pytest.raises(ValueError, match="10000"):
            submit_batch(lambda x: x, list(range(10001)), cfg, "", [])

    def test_two_inputs_calls_submit_array_job(self, aws_s3):
        """2-10000 inputs should still use submit_array_job."""
        submitted = aws_s3["submitted"]
        cfg = _make_config()

        job_id, n, batch_job_id = submit_batch(lambda x: x, [1, 2], cfg, "", [])

        assert n == 2
        assert len(submitted) == 1
        assert submitted[0]["type"] == "array"
        assert submitted[0]["array_size"] == 2

    def test_exactly_10000_inputs_calls_submit_array_job(self, aws_s3):
        """Exactly 10000 inputs (upper boundary) should use submit_array_job."""
        submitted = aws_s3["submitted"]
        cfg = _make_config()

        inputs = list(range(10000))
        job_id, n, batch_job_id = submit_batch(lambda x: x, inputs, cfg, "", [])

        assert n == 10000
        assert len(submitted) == 1
        assert submitted[0]["type"] == "array"
        assert submitted[0]["array_size"] == 10000


class TestSubmitBatchDetach:
    def test_detach_sets_env_flag_on_array_job(self, aws_s3):
        cfg = _make_config()
        submit_batch(lambda x: x, [1, 2, 3], cfg, "", [], detach=True)

        env = aws_s3["submitted"][0]["env_overrides"]
        assert env["BRAID_DETACH"] == "1"

    def test_detach_sets_env_flag_on_single_job(self, aws_s3):
        cfg = _make_config()
        submit_batch(lambda x: x, [1], cfg, "", [], detach=True)

        assert aws_s3["submitted"][0]["type"] == "single"
        assert aws_s3["submitted"][0]["env_overrides"]["BRAID_DETACH"] == "1"

    def test_default_has_no_detach_flag(self, aws_s3):
        """Regression guard: fan()'s submissions must not be marked detached."""
        cfg = _make_config()
        submit_batch(lambda x: x, [1, 2, 3], cfg, "", [])

        assert "BRAID_DETACH" not in aws_s3["submitted"][0]["env_overrides"]
