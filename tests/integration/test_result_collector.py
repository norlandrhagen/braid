import base64
import json
import time
import unittest.mock

import boto3
import cloudpickle
import pytest
from braid.aws import sqs_client
from braid.errors import JobTimeoutError, WorkerError
from braid.result_collector import collect_results, collect_results_iter
from moto import mock_aws


@pytest.fixture
def aws_setup():
    with mock_aws():
        sqs = boto3.client("sqs", region_name="us-east-1")
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="test-bucket")
        resp = sqs.create_queue(QueueName="test-results")
        queue_url = resp["QueueUrl"]
        yield {
            "sqs": sqs,
            "s3": s3,
            "queue_url": queue_url,
            "bucket": "test-bucket",
            "region": "us-east-1",
        }


class TestCollectResultsInline:
    def test_collect_inline_results(self, aws_setup):
        sqs = aws_setup["sqs"]
        queue_url = aws_setup["queue_url"]

        result1 = cloudpickle.dumps(42)
        result2 = cloudpickle.dumps(43)

        msg1 = {
            "job_id": "job1",
            "idx": 0,
            "ok": True,
            "inline": True,
            "data_b64": base64.b64encode(result1).decode(),
        }
        msg2 = {
            "job_id": "job1",
            "idx": 1,
            "ok": True,
            "inline": True,
            "data_b64": base64.b64encode(result2).decode(),
        }

        sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(msg1))
        sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(msg2))

        results, _ = collect_results(
            job_id="job1",
            n=2,
            sqs_url=queue_url,
            s3_bucket="test-bucket",
            region="us-east-1",
            timeout_s=5,
            show_progress=False,
        )

        assert [results[i] for i in range(2)] == [42, 43]


class TestCollectResultsS3:
    def test_collect_s3_results(self, aws_setup):
        sqs = aws_setup["sqs"]
        s3 = aws_setup["s3"]
        queue_url = aws_setup["queue_url"]
        bucket = aws_setup["bucket"]

        result_data = cloudpickle.dumps({"data": "large"})
        s3.put_object(Bucket=bucket, Key="jobs/job1/0.pkl", Body=result_data)

        msg = {
            "job_id": "job1",
            "idx": 0,
            "ok": True,
            "inline": False,
            "s3_key": "jobs/job1/0.pkl",
        }

        sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(msg))

        results, _ = collect_results(
            job_id="job1",
            n=1,
            sqs_url=queue_url,
            s3_bucket=bucket,
            region="us-east-1",
            timeout_s=5,
            show_progress=False,
        )

        assert results[0] == {"data": "large"}


class TestCollectResultsJobFiltering:
    def test_collect_filters_by_job_id(self, aws_setup):
        sqs = aws_setup["sqs"]
        queue_url = aws_setup["queue_url"]

        other_msg = {
            "job_id": "other-job",
            "idx": 0,
            "ok": True,
            "inline": True,
            "data_b64": base64.b64encode(cloudpickle.dumps(99)).decode(),
        }

        target_msg = {
            "job_id": "target-job",
            "idx": 0,
            "ok": True,
            "inline": True,
            "data_b64": base64.b64encode(cloudpickle.dumps(42)).decode(),
        }

        sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(other_msg))
        sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(target_msg))

        results, _ = collect_results(
            job_id="target-job",
            n=1,
            sqs_url=queue_url,
            s3_bucket="test-bucket",
            region="us-east-1",
            timeout_s=5,
            show_progress=False,
        )

        assert results[0] == 42


class TestCollectResultsWorkerError:
    def test_collect_raises_worker_error(self, aws_setup):
        sqs = aws_setup["sqs"]
        s3 = aws_setup["s3"]
        queue_url = aws_setup["queue_url"]
        bucket = aws_setup["bucket"]

        error_data = {
            "error": "ZeroDivisionError: division by zero",
            "traceback": "Traceback (most recent call last):\n  ...",
        }
        s3.put_object(
            Bucket=bucket,
            Key="jobs/job1/0.error.json",
            Body=json.dumps(error_data).encode(),
        )

        msg = {
            "job_id": "job1",
            "idx": 0,
            "ok": False,
            "s3_key": "jobs/job1/0.error.json",
        }

        sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(msg))

        with pytest.raises(WorkerError) as exc_info:
            collect_results(
                job_id="job1",
                n=1,
                sqs_url=queue_url,
                s3_bucket=bucket,
                region="us-east-1",
                timeout_s=5,
                show_progress=False,
            )

        assert exc_info.value.idx == 0
        assert "division by zero" in exc_info.value.original


class TestCollectResultsTimeout:
    def test_collect_timeout_raises_error(self, aws_setup):
        sqs = aws_setup["sqs"]
        queue_url = aws_setup["queue_url"]

        msg = {
            "job_id": "job1",
            "idx": 0,
            "ok": True,
            "inline": True,
            "data_b64": base64.b64encode(cloudpickle.dumps(42)).decode(),
        }

        sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(msg))

        with pytest.raises(JobTimeoutError) as exc_info:
            collect_results(
                job_id="job1",
                n=5,
                sqs_url=queue_url,
                s3_bucket="test-bucket",
                region="us-east-1",
                timeout_s=1,
                show_progress=False,
            )

        assert exc_info.value.job_id == "job1"
        assert exc_info.value.expected == 5
        assert exc_info.value.received < 5


class TestBugACrossJobDelete:
    """Bug A: messages from other jobs should not be deleted; they should be skipped."""

    def test_cross_job_messages_not_deleted(self, aws_setup):
        sqs = aws_setup["sqs"]

        # Create a queue with short visibility timeout for this test
        resp = sqs.create_queue(
            QueueName="test-cross-job-delete", Attributes={"VisibilityTimeout": "1"}
        )
        queue_url = resp["QueueUrl"]

        msg_job_a = {
            "job_id": "job-a",
            "idx": 0,
            "ok": True,
            "inline": True,
            "data_b64": base64.b64encode(cloudpickle.dumps(42)).decode(),
        }
        msg_job_b = {
            "job_id": "job-b",
            "idx": 0,
            "ok": True,
            "inline": True,
            "data_b64": base64.b64encode(cloudpickle.dumps(99)).decode(),
        }

        sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(msg_job_a))
        sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(msg_job_b))

        results_a, _ = collect_results(
            job_id="job-a",
            n=1,
            sqs_url=queue_url,
            s3_bucket="test-bucket",
            region="us-east-1",
            timeout_s=5,
            show_progress=False,
        )

        assert results_a[0] == 42

        # Wait a bit for visibility timeout to expire
        time.sleep(1.5)

        results_b, _ = collect_results(
            job_id="job-b",
            n=1,
            sqs_url=queue_url,
            s3_bucket="test-bucket",
            region="us-east-1",
            timeout_s=5,
            show_progress=False,
        )

        assert results_b[0] == 99


class TestBugBN0Hang:
    """Bug B: collect_results with n=0 should return immediately, not hang for timeout."""

    def test_n_zero_returns_immediately(self, aws_setup):
        queue_url = aws_setup["queue_url"]

        start = time.monotonic()
        results, failures = collect_results(
            job_id="job1",
            n=0,
            sqs_url=queue_url,
            s3_bucket="test-bucket",
            region="us-east-1",
            timeout_s=5,
            show_progress=False,
        )
        elapsed = time.monotonic() - start

        assert results == {}
        assert failures == []
        assert elapsed < 1.0, f"Expected immediate return, but took {elapsed}s"


class TestBugCSilentThreadDeath:
    """Bug C: exceptions in _process_message should not kill the poll thread."""

    def test_message_processing_exception_does_not_kill_thread(self, aws_setup):
        sqs = aws_setup["sqs"]
        queue_url = aws_setup["queue_url"]

        msg_bad = {
            "job_id": "job1",
            "idx": 0,
            "ok": True,
            "inline": True,
            "data_b64": base64.b64encode(cloudpickle.dumps(42)).decode(),
        }
        msg_good = {
            "job_id": "job1",
            "idx": 1,
            "ok": True,
            "inline": True,
            "data_b64": base64.b64encode(cloudpickle.dumps(99)).decode(),
        }

        sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(msg_bad))
        sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(msg_good))

        call_count = [0]

        def mock_process_impl(msg, s3, bucket):
            call_count[0] += 1
            body = sqs_client.parse_body(msg)
            idx = body["idx"]
            # Fail on first message (idx=0), succeed on second (idx=1)
            if idx == 0:
                raise Exception("Simulated processing error")
            return idx, 99, None

        with unittest.mock.patch(
            "braid.result_collector._process_message", side_effect=mock_process_impl
        ):
            results, failures = collect_results(
                job_id="job1",
                n=1,
                sqs_url=queue_url,
                s3_bucket="test-bucket",
                region="us-east-1",
                timeout_s=5,
                show_progress=False,
            )

            # Should get the good message despite the exception on the bad message
            assert results[1] == 99
            assert len(failures) == 0


class TestBugDFailureDedup:
    """Bug D: duplicate failure messages should be deduplicated."""

    def test_duplicate_failure_deduplication(self, aws_setup):
        sqs = aws_setup["sqs"]
        queue_url = aws_setup["queue_url"]

        failure_msg = {
            "job_id": "job1",
            "idx": 0,
            "ok": False,
            "inline_error": "ValueError: bad value",
            "traceback": "Traceback...",
        }
        success_msg = {
            "job_id": "job1",
            "idx": 1,
            "ok": True,
            "inline": True,
            "data_b64": base64.b64encode(cloudpickle.dumps(42)).decode(),
        }

        sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(failure_msg))
        sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(failure_msg))
        sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(success_msg))

        with pytest.raises(WorkerError) as exc_info:
            collect_results(
                job_id="job1",
                n=2,
                sqs_url=queue_url,
                s3_bucket="test-bucket",
                region="us-east-1",
                timeout_s=5,
                show_progress=False,
            )

        # Should have only 1 unique failure (idx=0), not 2
        assert len(exc_info.value.failures) == 1
        assert exc_info.value.failures[0]["idx"] == 0


class TestBugDFailureDedupIter:
    """Bug D: duplicate failures in collect_results_iter should also be deduped."""

    def test_duplicate_failure_deduplication_iter(self, aws_setup):
        sqs = aws_setup["sqs"]
        queue_url = aws_setup["queue_url"]

        failure_msg = {
            "job_id": "job1",
            "idx": 0,
            "ok": False,
            "inline_error": "ValueError: bad value",
            "traceback": "Traceback...",
        }

        sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(failure_msg))
        sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(failure_msg))

        with pytest.raises(WorkerError) as exc_info:
            list(
                collect_results_iter(
                    job_id="job1",
                    n=2,
                    sqs_url=queue_url,
                    s3_bucket="test-bucket",
                    region="us-east-1",
                    timeout_s=5,
                    show_progress=False,
                )
            )

        assert len(exc_info.value.failures) == 1
        assert exc_info.value.failures[0]["idx"] == 0
