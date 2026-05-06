"""
Unit tests for braid.detach: submit/attach/status/JobHandle.

Follows the moto S3 + mocked Batch-submission pattern used in
tests/unit/test_submit_batch.py.
"""

from __future__ import annotations

import json

import boto3
import cloudpickle
import pytest
from braid.config import Config
from braid.detach import JobHandle, attach, status, submit
from braid.errors import ConfigError, JobFailedError, JobTimeoutError
from moto import mock_aws

REGION = "us-east-1"
BUCKET = "test-bucket"


def _make_config(backend: str = "batch") -> Config:
    return Config(
        region=REGION,
        bucket=BUCKET,
        bucket_prefix="",
        sqs_queue_url="https://sqs.us-east-1.amazonaws.com/123/q",
        backend=backend,
        batch_job_queue="braid-queue",
        batch_job_definition="braid-job",
        batch_execution_role_arn="arn:aws:iam::123456789012:role/test",
    )


@pytest.fixture
def aws_s3(mocker):
    with mock_aws():
        s3 = boto3.client("s3", region_name=REGION)
        s3.create_bucket(Bucket=BUCKET)

        submitted: list[dict] = []

        def _capture_array_submit(
            client, job_def_name, queue_name, job_id, array_size, env_overrides
        ):
            submitted.append({"job_id": job_id, "env_overrides": env_overrides})
            return "aws-batch-job-id"

        mocker.patch("braid.dispatch_batch._ensure_job_definition")
        mocker.patch("braid.detach.sync_editables", return_value=[])
        mocker.patch("braid.detach.sync_uv_lock", return_value="req-key")
        mocker.patch(
            "braid.aws.batch_client.submit_array_job", side_effect=_capture_array_submit
        )

        yield {"s3": s3, "submitted": submitted}


class TestSubmitDetachFlag:
    def test_submit_sets_detach_env_flag(self, aws_s3):
        cfg = _make_config()
        submit(lambda x: x, [1, 2, 3], config=cfg)

        env = aws_s3["submitted"][0]["env_overrides"]
        assert env["BRAID_DETACH"] == "1"

    def test_submit_on_lambda_backend_raises(self, aws_s3):
        cfg = _make_config(backend="lambda")
        with pytest.raises(ConfigError, match="requires backend='batch'"):
            submit(lambda x: x, [1], config=cfg)


class TestJobRecordRoundTrip:
    def test_job_json_written_and_round_trips_through_attach(self, aws_s3):
        cfg = _make_config()
        handle = submit(lambda x: x, [1, 2, 3], config=cfg)

        key = cfg.s3_key(f"jobs/{handle.job_id}/job.json")
        record = json.loads(
            aws_s3["s3"].get_object(Bucket=BUCKET, Key=key)["Body"].read()
        )
        assert record["job_id"] == handle.job_id
        assert record["batch_job_id"] == handle.batch_job_id
        assert record["n"] == 3

        reattached = attach(handle.job_id, config=cfg)
        assert reattached.job_id == handle.job_id
        assert reattached.batch_job_id == handle.batch_job_id
        assert reattached.n == 3

    def test_attach_missing_job_raises_config_error(self, aws_s3):
        cfg = _make_config()
        with pytest.raises(ConfigError, match="No job record found"):
            attach("does-not-exist", config=cfg)


class TestStatusCounts:
    def _write_job(self, s3, job_id: str, n: int, batch_job_id: str = "b-1") -> None:
        record = {
            "job_id": job_id,
            "batch_job_id": batch_job_id,
            "n": n,
            "region": REGION,
            "submitted_at": "2026-01-01T00:00:00Z",
        }
        s3.put_object(
            Bucket=BUCKET,
            Key=f"jobs/{job_id}/job.json",
            Body=json.dumps(record).encode(),
        )

    def _mock_describe(self, mocker, jobs: list[dict]) -> None:
        mocker.patch("braid.aws.batch_client.describe_jobs", return_value=jobs)

    def test_status_empty(self, aws_s3, mocker):
        s3 = aws_s3["s3"]
        self._write_job(s3, "job-empty", n=3)
        self._mock_describe(mocker, [{"jobId": "b-1", "status": "RUNNING"}])

        cfg = _make_config()
        st = status("job-empty", config=cfg)
        assert st.done == 0
        assert st.failed == 0
        assert st.pending == 3
        assert st.batch_state == "RUNNING"
        assert st.complete is False

    def test_status_partial(self, aws_s3, mocker):
        s3 = aws_s3["s3"]
        self._write_job(s3, "job-partial", n=3)
        s3.put_object(Bucket=BUCKET, Key="jobs/job-partial/results/0.pkl", Body=b"x")
        self._mock_describe(mocker, [{"jobId": "b-1", "status": "RUNNING"}])

        st = status("job-partial", config=_make_config())
        assert st.done == 1
        assert st.pending == 2
        assert st.complete is False

    def test_status_all_done(self, aws_s3, mocker):
        s3 = aws_s3["s3"]
        self._write_job(s3, "job-done", n=2)
        s3.put_object(Bucket=BUCKET, Key="jobs/job-done/results/0.pkl", Body=b"x")
        s3.put_object(Bucket=BUCKET, Key="jobs/job-done/results/1.pkl", Body=b"x")
        self._mock_describe(mocker, [{"jobId": "b-1", "status": "SUCCEEDED"}])

        st = status("job-done", config=_make_config())
        assert st.done == 2
        assert st.failed == 0
        assert st.pending == 0
        assert st.complete is True

    def test_status_all_failed(self, aws_s3, mocker):
        s3 = aws_s3["s3"]
        self._write_job(s3, "job-failed", n=2)
        s3.put_object(Bucket=BUCKET, Key="jobs/job-failed/errors/0.json", Body=b"{}")
        s3.put_object(Bucket=BUCKET, Key="jobs/job-failed/errors/1.json", Body=b"{}")
        self._mock_describe(mocker, [{"jobId": "b-1", "status": "SUCCEEDED"}])

        st = status("job-failed", config=_make_config())
        assert st.failed == 2
        assert st.done == 0
        assert st.complete is True

    def test_status_mixed(self, aws_s3, mocker):
        s3 = aws_s3["s3"]
        self._write_job(s3, "job-mixed", n=3)
        s3.put_object(Bucket=BUCKET, Key="jobs/job-mixed/results/0.pkl", Body=b"x")
        s3.put_object(Bucket=BUCKET, Key="jobs/job-mixed/errors/1.json", Body=b"{}")
        self._mock_describe(mocker, [{"jobId": "b-1", "status": "RUNNING"}])

        st = status("job-mixed", config=_make_config())
        assert st.done == 1
        assert st.failed == 1
        assert st.pending == 1
        assert st.complete is False

    def test_status_unknown_when_describe_jobs_empty(self, aws_s3, mocker):
        s3 = aws_s3["s3"]
        self._write_job(s3, "job-gone", n=1)
        self._mock_describe(mocker, [])

        st = status("job-gone", config=_make_config())
        assert st.batch_state == "UNKNOWN"
        assert st.done == 0


class TestJobHandleResultRegression:
    def test_result_timeout_does_not_terminate_job(self, aws_s3, mocker):
        """Direct guard against fan()'s terminate-on-timeout behavior being copied in."""
        s3 = aws_s3["s3"]
        s3.put_object(
            Bucket=BUCKET,
            Key="jobs/job-x/job.json",
            Body=json.dumps(
                {
                    "job_id": "job-x",
                    "batch_job_id": "b-1",
                    "n": 2,
                    "region": REGION,
                    "submitted_at": "2026-01-01T00:00:00Z",
                }
            ).encode(),
        )
        mocker.patch(
            "braid.aws.batch_client.describe_jobs",
            return_value=[{"jobId": "b-1", "status": "RUNNING"}],
        )
        mock_batch_client = mocker.MagicMock()
        mocker.patch(
            "braid.aws.batch_client.get_client", return_value=mock_batch_client
        )
        handle = attach("job-x", config=_make_config())

        with pytest.raises(JobTimeoutError):
            handle.result(timeout_s=1, poll_interval_s=0.01)

        mock_batch_client.terminate_job.assert_not_called()


class TestJobHandleFields:
    def test_handle_has_expected_fields(self, aws_s3):
        cfg = _make_config()
        handle = submit(lambda x: x, [1, 2], config=cfg)
        assert isinstance(handle, JobHandle)
        assert handle.n == 2
        assert handle.region == REGION
        assert handle.batch_job_id == "aws-batch-job-id"


class TestJobFailure:
    """Covers the paths added when the terminal-state / ordering bugs were fixed."""

    def _write_job(self, s3, job_id: str, n: int, batch_job_id: str = "b-1") -> None:
        s3.put_object(
            Bucket=BUCKET,
            Key=f"jobs/{job_id}/job.json",
            Body=json.dumps(
                {
                    "job_id": job_id,
                    "batch_job_id": batch_job_id,
                    "n": n,
                    "region": REGION,
                    "submitted_at": "2026-01-01T00:00:00Z",
                }
            ).encode(),
        )

    def test_status_describes_batch_before_listing_s3(self, aws_s3, mocker):
        """A job that finishes between the two reads must not look incomplete.

        describe_jobs runs first, so the listing that follows is at least as new as
        the state. Simulated by having the last result appear during describe_jobs.
        """
        s3 = aws_s3["s3"]
        self._write_job(s3, "job-race", n=2)
        s3.put_object(Bucket=BUCKET, Key="jobs/job-race/results/0.pkl", Body=b"x")

        def _describe(client, job_ids):
            s3.put_object(Bucket=BUCKET, Key="jobs/job-race/results/1.pkl", Body=b"y")
            return [{"jobId": "b-1", "status": "SUCCEEDED"}]

        mocker.patch("braid.aws.batch_client.describe_jobs", side_effect=_describe)

        st = status("job-race", config=_make_config())
        assert st.batch_state == "SUCCEEDED"
        assert st.complete is True
        assert st.done == 2

    def test_result_raises_job_failed_on_terminal_incomplete(self, aws_s3, mocker):
        s3 = aws_s3["s3"]
        self._write_job(s3, "job-dead", n=3)
        s3.put_object(Bucket=BUCKET, Key="jobs/job-dead/results/0.pkl", Body=b"x")
        mocker.patch(
            "braid.aws.batch_client.describe_jobs",
            return_value=[{"jobId": "b-1", "status": "FAILED"}],
        )

        handle = attach("job-dead", config=_make_config())
        with pytest.raises(JobFailedError) as exc_info:
            handle.result(timeout_s=30)

        assert exc_info.value.batch_state == "FAILED"
        assert exc_info.value.received == 1
        assert exc_info.value.expected == 3

    def test_result_raises_job_failed_after_unknown_grace_polls(
        self, aws_s3, mocker
    ) -> None:
        """An incomplete job aged out of describe_jobs fails fast, not on timeout."""
        s3 = aws_s3["s3"]
        self._write_job(s3, "job-aged", n=2)
        mocker.patch("braid.aws.batch_client.describe_jobs", return_value=[])
        mocker.patch("braid.result_collector.time.sleep")

        handle = attach("job-aged", config=_make_config())
        with pytest.raises(JobFailedError) as exc_info:
            handle.result()  # no timeout: only the UNKNOWN grace can end this

        assert exc_info.value.batch_state == "UNKNOWN"

    def test_unknown_grace_resets_when_state_reappears(self, aws_s3, mocker):
        """A real state between UNKNOWN polls restarts the grace count, so a job
        not yet visible just after submit is not mistaken for one that aged out."""
        s3 = aws_s3["s3"]
        self._write_job(s3, "job-flap", n=1)
        # UNKNOWN, UNKNOWN, RUNNING, UNKNOWN, UNKNOWN, UNKNOWN: fails on poll 6.
        # Without the reset it would fail on poll 4.
        states = [[], [], [{"jobId": "b-1", "status": "RUNNING"}], [], [], []]
        polls = []

        def _describe(client, job_ids):
            polls.append(1)
            return states.pop(0)

        mocker.patch("braid.aws.batch_client.describe_jobs", side_effect=_describe)
        mocker.patch("braid.result_collector.time.sleep")

        handle = attach("job-flap", config=_make_config())
        with pytest.raises(JobFailedError):
            handle.result()

        assert len(polls) == 6

    def test_result_raises_job_failed_on_index_gap(self, aws_s3, mocker):
        """Overlapping result+error objects satisfy `complete` with a hole; the
        handle must report it rather than raising a bare KeyError."""
        s3 = aws_s3["s3"]
        self._write_job(s3, "job-gap", n=2)
        s3.put_object(
            Bucket=BUCKET, Key="jobs/job-gap/results/0.pkl", Body=cloudpickle.dumps(1)
        )
        mocker.patch(
            "braid.aws.batch_client.describe_jobs",
            return_value=[{"jobId": "b-1", "status": "SUCCEEDED"}],
        )
        mocker.patch("braid.detach.collect_results_s3", return_value=({0: 1}, []))

        handle = attach("job-gap", config=_make_config())
        with pytest.raises(JobFailedError):
            handle.result(timeout_s=5)


class TestSubmitRecordDurability:
    def test_job_record_write_failure_logs_ids(self, aws_s3, mocker, caplog):
        mocker.patch("braid.detach._record_batch_job")
        mocker.patch(
            "braid.detach._write_job_record", side_effect=RuntimeError("s3 down")
        )

        with caplog.at_level("ERROR"), pytest.raises(RuntimeError):
            submit(lambda x: x, [1, 2], config=_make_config())

        assert "aws-batch-job-id" in caplog.text
        assert "terminate-job" in caplog.text


class TestAttachRegion:
    def test_attach_uses_the_region_the_job_was_submitted_in(self, aws_s3, mocker):
        """describe_jobs/terminate_job must target the job's own Batch control
        plane, not whatever region the reader's config happens to name."""
        s3 = aws_s3["s3"]
        s3.put_object(
            Bucket=BUCKET,
            Key="jobs/job-west/job.json",
            Body=json.dumps(
                {
                    "job_id": "job-west",
                    "batch_job_id": "b-west",
                    "n": 1,
                    "region": "us-west-2",
                    "submitted_at": "2026-01-01T00:00:00Z",
                }
            ).encode(),
        )
        get_client = mocker.patch(
            "braid.aws.batch_client.get_client", return_value=mocker.MagicMock()
        )
        mocker.patch(
            "braid.aws.batch_client.describe_jobs",
            return_value=[{"jobId": "b-west", "status": "RUNNING"}],
        )

        handle = attach("job-west", config=_make_config())  # config says us-east-1
        assert handle.region == "us-west-2"

        handle.status()
        assert get_client.call_args.args == ("us-west-2",)
