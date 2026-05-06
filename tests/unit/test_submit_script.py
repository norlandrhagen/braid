"""
Unit tests for dispatch_batch.submit_script() and _has_pep723_header().
Follows the moto S3 + mocked Batch-submission pattern used in
tests/unit/test_submit_batch.py. Local/S3 job-record writers are mocked so
tests don't touch the real .braid/cache/ state.
"""

from __future__ import annotations

import textwrap
import uuid

import boto3
import pytest
from braid.config import Config
from braid.dispatch_batch import _has_pep723_header, submit_script
from moto import mock_aws

REGION = "us-east-1"
BUCKET = "test-bucket"

PEP723_SCRIPT = textwrap.dedent(
    """\
    # /// script
    # requires-python = ">=3.11"
    # dependencies = ["requests"]
    # ///
    print("hello")
    """
)

PLAIN_SCRIPT = 'print("hello")\n'

# Mentions "script" header syntax in prose but isn't a `#`-prefixed comment
# block, so it must not match (the PEP 723 regex only looks at comment lines).
NOT_A_HEADER_MENTION = textwrap.dedent(
    '''\
    """
    This docstring mentions /// script but has no leading '#' on each line,
    so it isn't a real PEP 723 metadata block.
    /// script
    dependencies = ["requests"]
    ///
    """
    print("hello")
    '''
)

MID_FILE_HEADER = textwrap.dedent(
    """\
    print("before")
    # /// script
    # dependencies = []
    # ///
    print("after")
    """
)


def _make_config() -> Config:
    return Config(
        region=REGION,
        bucket=BUCKET,
        bucket_prefix="",
        sqs_queue_url="https://sqs.us-east-1.amazonaws.com/123/q",
        batch_job_queue="braid-queue",
        batch_job_definition="braid-job",
        batch_execution_role_arn="arn:aws:iam::123456789012:role/test",
    )


class TestPep723Detection:
    def test_header_present(self, tmp_path):
        p = tmp_path / "script.py"
        p.write_text(PEP723_SCRIPT)
        assert _has_pep723_header(p) is True

    def test_header_absent(self, tmp_path):
        p = tmp_path / "script.py"
        p.write_text(PLAIN_SCRIPT)
        assert _has_pep723_header(p) is False

    def test_prose_mention_is_not_a_header(self, tmp_path):
        p = tmp_path / "script.py"
        p.write_text(NOT_A_HEADER_MENTION)
        assert _has_pep723_header(p) is False

    def test_mid_file_header(self, tmp_path):
        p = tmp_path / "script.py"
        p.write_text(MID_FILE_HEADER)
        assert _has_pep723_header(p) is True


@pytest.fixture
def aws_s3(mocker):
    """S3-only fixture. submit_single_job mocked; job-record writers mocked."""
    with mock_aws():
        s3 = boto3.client("s3", region_name=REGION)
        s3.create_bucket(Bucket=BUCKET)

        submitted: list[dict] = []

        def _capture_single_submit(
            client, job_def_name, queue_name, job_id, env_overrides
        ):
            fake_id = str(uuid.uuid4())
            submitted.append(
                {
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
            "braid.aws.batch_client.submit_single_job",
            side_effect=_capture_single_submit,
        )
        mocker.patch("braid.core._record_batch_job")
        mocker.patch("braid.detach._write_job_record")

        yield {"s3": s3, "submitted": submitted}


class TestSubmitScriptS3Artifacts:
    def test_script_uploaded(self, aws_s3, tmp_path):
        s3 = aws_s3["s3"]
        cfg = _make_config()
        script = tmp_path / "script.py"
        script.write_text(PLAIN_SCRIPT)

        job_id, _ = submit_script(script, cfg)

        body = s3.get_object(Bucket=BUCKET, Key=f"jobs/{job_id}/script.py")[
            "Body"
        ].read()
        assert body.decode() == PLAIN_SCRIPT

    def test_worker_scripts_uploaded(self, aws_s3, tmp_path):
        s3 = aws_s3["s3"]
        cfg = _make_config()
        script = tmp_path / "script.py"
        script.write_text(PLAIN_SCRIPT)

        submit_script(script, cfg)

        keys = [
            obj["Key"] for obj in s3.list_objects_v2(Bucket=BUCKET).get("Contents", [])
        ]
        assert any("braid-system/worker_script/" in k for k in keys)
        assert any("braid-system/worker_common/" in k for k in keys)


class TestSubmitScriptEnvVars:
    def test_pep723_mode_env(self, aws_s3, tmp_path):
        submitted = aws_s3["submitted"]
        cfg = _make_config()
        script = tmp_path / "script.py"
        script.write_text(PEP723_SCRIPT)

        submit_script(script, cfg)

        env = submitted[0]["env_overrides"]
        assert env["BRAID_SCRIPT_MODE"] == "pep723"
        assert "BRAID_SCRIPT_URL" in env
        assert "BRAID_RESULT_PUT_URL" in env
        assert "BRAID_ERROR_PUT_URL" in env
        assert env["BRAID_PYTHON_VERSION"] == cfg.python_version
        assert "BRAID_REQUIREMENTS_KEY" not in env
        assert "BRAID_EDITABLES" not in env
        assert "BRAID_SQS_QUEUE_URL" not in env

    def test_project_mode_env(self, aws_s3, tmp_path):
        submitted = aws_s3["submitted"]
        cfg = _make_config()
        script = tmp_path / "script.py"
        script.write_text(PLAIN_SCRIPT)

        submit_script(
            script,
            cfg,
            requirements_s3_key="req-key",
            editable_manifest=[{"name": "pkg", "s3_key": "k", "src_hash": "h"}],
        )

        env = submitted[0]["env_overrides"]
        assert env["BRAID_SCRIPT_MODE"] == "project"
        assert env["BRAID_REQUIREMENTS_KEY"] == "req-key"
        assert "BRAID_EDITABLES" in env
        assert env["BRAID_BUCKET"] == BUCKET
        assert "BRAID_SQS_QUEUE_URL" not in env

    def test_no_sqs_queue_url_ever(self, aws_s3, tmp_path):
        """Script workers never touch SQS — no queue URL in either mode."""
        submitted = aws_s3["submitted"]
        cfg = _make_config()
        script = tmp_path / "script.py"
        script.write_text(PEP723_SCRIPT)
        submit_script(script, cfg)
        assert "BRAID_SQS_QUEUE_URL" not in submitted[0]["env_overrides"]

    def test_env_passthrough(self, aws_s3, tmp_path):
        submitted = aws_s3["submitted"]
        cfg = _make_config()
        script = tmp_path / "script.py"
        script.write_text(PEP723_SCRIPT)

        submit_script(script, cfg, env_passthrough={"FOO": "bar"})

        assert submitted[0]["env_overrides"]["FOO"] == "bar"


class TestSubmitScriptJobSubmission:
    def test_single_non_array_job(self, aws_s3, tmp_path):
        submitted = aws_s3["submitted"]
        cfg = _make_config()
        script = tmp_path / "script.py"
        script.write_text(PEP723_SCRIPT)

        job_id, batch_job_id = submit_script(script, cfg)

        assert len(submitted) == 1
        assert submitted[0]["job_id"] == job_id
        assert submitted[0]["batch_job_id"] == batch_job_id
        assert "array_size" not in submitted[0]

    def test_returns_correct_tuple(self, aws_s3, tmp_path):
        cfg = _make_config()
        script = tmp_path / "script.py"
        script.write_text(PEP723_SCRIPT)

        result = submit_script(script, cfg)

        assert len(result) == 2
        job_id, batch_job_id = result
        assert isinstance(job_id, str) and len(job_id) == 36
        assert isinstance(batch_job_id, str)

    def test_job_records_written(self, aws_s3, tmp_path, mocker):
        record_local = mocker.patch("braid.core._record_batch_job")
        record_s3 = mocker.patch("braid.detach._write_job_record")
        cfg = _make_config()
        script = tmp_path / "script.py"
        script.write_text(PEP723_SCRIPT)

        job_id, batch_job_id = submit_script(script, cfg)

        record_local.assert_called_once_with(job_id, batch_job_id, 1, cfg)
        record_s3.assert_called_once_with(job_id, batch_job_id, 1, cfg)
