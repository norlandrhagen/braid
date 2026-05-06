import base64
import inspect
import json
import os

import boto3
import cloudpickle
import pytest
from braid._worker_lambda import handler
from moto import mock_aws


@pytest.fixture
def aws_env():
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        sqs = boto3.client("sqs", region_name="us-east-1")

        s3.create_bucket(Bucket="test-bucket")
        resp = sqs.create_queue(QueueName="test-queue")
        queue_url = resp["QueueUrl"]

        old_bucket = os.environ.get("BRAID_BUCKET")
        old_prefix = os.environ.get("BRAID_BUCKET_PREFIX")
        old_queue = os.environ.get("BRAID_SQS_QUEUE_URL")

        os.environ["BRAID_BUCKET"] = "test-bucket"
        os.environ["BRAID_BUCKET_PREFIX"] = ""
        os.environ["BRAID_SQS_QUEUE_URL"] = queue_url

        yield {"s3": s3, "sqs": sqs, "queue_url": queue_url, "bucket": "test-bucket"}

        if old_bucket is None:
            del os.environ["BRAID_BUCKET"]
        else:
            os.environ["BRAID_BUCKET"] = old_bucket

        if old_prefix is None:
            os.environ.pop("BRAID_BUCKET_PREFIX", None)
        else:
            os.environ["BRAID_BUCKET_PREFIX"] = old_prefix

        if old_queue is None:
            del os.environ["BRAID_SQS_QUEUE_URL"]
        else:
            os.environ["BRAID_SQS_QUEUE_URL"] = old_queue


class TestWorkerHandlerHappyPath:
    def test_worker_happy_path(self, aws_env):
        s3 = aws_env["s3"]
        sqs = aws_env["sqs"]
        queue_url = aws_env["queue_url"]

        def fn(x):
            return x**2

        fn_data = cloudpickle.dumps(fn)
        s3.put_object(Bucket="test-bucket", Key="jobs/test-job/fn.pkl", Body=fn_data)

        inputs = [3, 7]
        inputs_b64 = base64.b64encode(cloudpickle.dumps(inputs)).decode()

        event = {
            "job_id": "test-job",
            "fn_key": "jobs/test-job/fn.pkl",
            "inputs_b64": inputs_b64,
            "start_idx": 0,
        }

        response = handler(event, None)

        assert response["status"] == "ok"
        assert response["count"] == 2

        messages = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10).get(
            "Messages", []
        )

        assert len(messages) == 2

        msg_bodies = [json.loads(m["Body"]) for m in messages]
        msg_bodies.sort(key=lambda m: m["idx"])

        assert msg_bodies[0]["idx"] == 0
        assert msg_bodies[0]["ok"] is True
        assert msg_bodies[0]["inline"] is True
        result0 = cloudpickle.loads(base64.b64decode(msg_bodies[0]["data_b64"]))
        assert result0 == 9

        assert msg_bodies[1]["idx"] == 1
        assert msg_bodies[1]["ok"] is True
        assert msg_bodies[1]["inline"] is True
        result1 = cloudpickle.loads(base64.b64decode(msg_bodies[1]["data_b64"]))
        assert result1 == 49


class TestWorkerHandlerError:
    def test_worker_error(self, aws_env):
        s3 = aws_env["s3"]
        sqs = aws_env["sqs"]
        queue_url = aws_env["queue_url"]

        def fn(x):
            return 1 / x

        fn_data = cloudpickle.dumps(fn)
        s3.put_object(Bucket="test-bucket", Key="jobs/test-job/fn.pkl", Body=fn_data)

        inputs = [0]
        inputs_b64 = base64.b64encode(cloudpickle.dumps(inputs)).decode()

        event = {
            "job_id": "test-job",
            "fn_key": "jobs/test-job/fn.pkl",
            "inputs_b64": inputs_b64,
            "start_idx": 0,
        }

        response = handler(event, None)

        assert response["status"] == "ok"
        assert response["count"] == 1

        messages = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10).get(
            "Messages", []
        )

        assert len(messages) == 1

        msg_body = json.loads(messages[0]["Body"])
        assert msg_body["idx"] == 0
        assert msg_body["ok"] is False
        assert "s3_key" in msg_body

        error_key = msg_body["s3_key"]
        error_obj = s3.get_object(Bucket="test-bucket", Key=error_key)
        error_data = json.loads(error_obj["Body"].read().decode())

        assert (
            "division by zero" in error_data["error"]
            or "ZeroDivisionError" in error_data["error"]
        )
        assert "traceback" in error_data


class TestWorkerHandlerLargeResult:
    def test_worker_large_result_to_s3(self, aws_env):
        s3 = aws_env["s3"]
        sqs = aws_env["sqs"]
        queue_url = aws_env["queue_url"]

        def fn(x):
            return b"x" * (300 * 1024)

        fn_data = cloudpickle.dumps(fn)
        s3.put_object(Bucket="test-bucket", Key="jobs/test-job/fn.pkl", Body=fn_data)

        inputs = ["dummy"]
        inputs_b64 = base64.b64encode(cloudpickle.dumps(inputs)).decode()

        event = {
            "job_id": "test-job",
            "fn_key": "jobs/test-job/fn.pkl",
            "inputs_b64": inputs_b64,
            "start_idx": 0,
        }

        response = handler(event, None)

        assert response["status"] == "ok"
        assert response["count"] == 1

        messages = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10).get(
            "Messages", []
        )

        assert len(messages) == 1

        msg_body = json.loads(messages[0]["Body"])
        assert msg_body["idx"] == 0
        assert msg_body["ok"] is True
        assert msg_body["inline"] is False
        assert "s3_key" in msg_body

        result_key = msg_body["s3_key"]
        result_obj = s3.get_object(Bucket="test-bucket", Key=result_key)
        result_data = cloudpickle.loads(result_obj["Body"].read())

        assert result_data == b"x" * (300 * 1024)


class TestWorkerHandlerWithPrefix:
    def test_worker_with_bucket_prefix(self, aws_env):
        s3 = aws_env["s3"]
        sqs = aws_env["sqs"]
        queue_url = aws_env["queue_url"]

        os.environ["BRAID_BUCKET_PREFIX"] = "my-prefix"

        def fn(x):
            return x + 1

        fn_data = cloudpickle.dumps(fn)
        s3.put_object(
            Bucket="test-bucket", Key="my-prefix/jobs/test-job/fn.pkl", Body=fn_data
        )

        inputs = [5]
        inputs_b64 = base64.b64encode(cloudpickle.dumps(inputs)).decode()

        event = {
            "job_id": "test-job",
            "fn_key": "my-prefix/jobs/test-job/fn.pkl",
            "inputs_b64": inputs_b64,
            "start_idx": 0,
        }

        response = handler(event, None)

        assert response["status"] == "ok"
        assert response["count"] == 1

        messages = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10).get(
            "Messages", []
        )

        assert len(messages) == 1

        msg_body = json.loads(messages[0]["Body"])
        assert msg_body["idx"] == 0
        assert msg_body["ok"] is True


class TestWorkerCodeQuality:
    def test_sigterm_handler_removed(self):
        """Verify that the dead SIGTERM handler has been removed from the module."""
        import braid._worker_lambda

        source = inspect.getsource(braid._worker_lambda)
        assert (
            "_sigterm_handler" not in source
        ), "SIGTERM handler should be removed from _worker_lambda.py"
