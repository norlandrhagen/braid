"""
Unit tests for _worker_common.py functions.

Tests send_error and bootstrap_editables from the shared worker logic.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import sys
import tempfile
import uuid
import zipfile
from unittest import mock

import boto3
from braid import _worker_common
from moto import mock_aws

REGION = "us-east-1"
BUCKET = "test-bucket"


class TestSendError:
    """Tests for the send_error function."""

    def test_send_error_logs_exception_on_s3_failure(self, caplog):
        """send_error logs exception via log.exception when S3 put fails."""
        s3 = mock.MagicMock()
        sqs = mock.MagicMock()
        queue_url = "http://queue"

        # Make S3 put_object raise an exception
        s3.put_object.side_effect = Exception("S3 put failed")

        with caplog.at_level(logging.ERROR):
            # Should not raise, should swallow the exception
            _worker_common.send_error(
                s3=s3,
                sqs=sqs,
                job_id="test-job",
                idx=0,
                error="Test error",
                tb="Test traceback",
                queue_url=queue_url,
                bucket=BUCKET,
                s3_key_fn=lambda x: x,
            )

        # Verify exception was logged (before the fix, this would fail)
        assert any(
            "Failed to send error to S3/SQS" in record.message
            for record in caplog.records
        )

    def test_send_error_logs_exception_on_sqs_failure(self, caplog):
        """send_error logs exception via log.exception when SQS send fails."""
        s3 = mock.MagicMock()
        sqs = mock.MagicMock()
        queue_url = "http://queue"

        # S3 succeeds but SQS send fails
        sqs.send_message.side_effect = Exception("SQS send failed")

        with caplog.at_level(logging.ERROR):
            # Should not raise, should swallow the exception
            _worker_common.send_error(
                s3=s3,
                sqs=sqs,
                job_id="test-job",
                idx=0,
                error="Test error",
                tb="Test traceback",
                queue_url=queue_url,
                bucket=BUCKET,
                s3_key_fn=lambda x: x,
            )

        # Verify exception was logged
        assert any(
            "Failed to send error to S3/SQS" in record.message
            for record in caplog.records
        )

    def test_send_error_succeeds_normally(self):
        """send_error sends error message to S3 and SQS successfully."""
        with mock_aws():
            s3 = boto3.client("s3", region_name=REGION)
            sqs = boto3.client("sqs", region_name=REGION)

            s3.create_bucket(Bucket=BUCKET)
            resp = sqs.create_queue(QueueName="test-queue")
            queue_url = resp["QueueUrl"]

            # Call send_error
            _worker_common.send_error(
                s3=s3,
                sqs=sqs,
                job_id="test-job",
                idx=0,
                error="Test error message",
                tb="Test traceback",
                queue_url=queue_url,
                bucket=BUCKET,
                s3_key_fn=lambda x: x,
            )

            # Verify S3 object was written
            obj = s3.get_object(Bucket=BUCKET, Key="jobs/test-job/0.error.json")
            error_data = json.loads(obj["Body"].read())
            assert error_data["error"] == "Test error message"
            assert error_data["traceback"] == "Test traceback"

            # Verify SQS message was sent
            msgs = sqs.receive_message(QueueUrl=queue_url).get("Messages", [])
            assert len(msgs) == 1
            body = json.loads(msgs[0]["Body"])
            assert body["ok"] is False
            assert body["job_id"] == "test-job"
            assert body["idx"] == 0


class TestBootstrapEditables:
    """Tests for the bootstrap_editables function."""

    def test_bootstrap_editables_returns_early_without_env(self):
        """bootstrap_editables returns early if BRAID_EDITABLES not set."""
        os.environ.pop("BRAID_EDITABLES", None)
        # Should just return without error
        _worker_common.bootstrap_editables()

    def test_bootstrap_editables_uses_zipfile_not_subprocess(self):
        """Verify subprocess is not imported at module level in _worker_common."""
        # The fix replaced subprocess.run() with zipfile.ZipFile().extractall()
        # Verify that subprocess is not imported at module level
        import inspect

        source = inspect.getsource(_worker_common)

        # Check that subprocess is not imported at module level
        # (the only place it was used was in the subprocess.run call we replaced)
        lines = source.split("\n")
        import_lines = [
            line
            for line in lines
            if line.startswith("import ") or line.startswith("from ")
        ]

        # subprocess should NOT be in the imports anymore
        assert not any("subprocess" in line for line in import_lines), (
            "subprocess should not be imported at module level "
            "(it was only used for unzip which is now handled by zipfile)"
        )

        # Verify zipfile IS imported
        assert any(
            "zipfile" in line for line in import_lines
        ), "zipfile should be imported at module level"

    def test_bootstrap_editables_functional_extracts_real_function(self, monkeypatch):
        """Functional test: Call real bootstrap_editables() and verify it extracts zip with zipfile."""
        # Build a test package name with UUID to avoid collisions
        test_pkg_name = f"testpkg-{uuid.uuid4().hex[:8]}"
        dest = f"/tmp/editables/{test_pkg_name}"

        try:
            # Build a real zip file with identifiable content
            with tempfile.TemporaryDirectory() as tmpdir:
                test_zip_path = os.path.join(tmpdir, "test.zip")
                marker_content = "test-marker-content-12345"
                with zipfile.ZipFile(test_zip_path, "w") as zf:
                    zf.writestr("marker.txt", marker_content)
                    zf.writestr("__init__.py", "# test editable package")

                # Mock S3 download: copy our test zip to the expected location
                def mock_download_file(bucket, key, local_path):
                    shutil.copy(test_zip_path, local_path)

                # Create manifest pointing to the test package
                manifest = [{"name": test_pkg_name, "s3_key": "s3://bucket/test.zip"}]

                # Setup environment variables
                monkeypatch.setenv("BRAID_EDITABLES", json.dumps(manifest))
                monkeypatch.setenv("BRAID_BUCKET", "test-bucket")

                # Mock boto3.client to return our mock S3
                mock_s3 = mock.MagicMock()
                mock_s3.download_file.side_effect = mock_download_file

                # Mock subprocess.run to fail loudly if called
                with mock.patch("boto3.client", return_value=mock_s3):
                    with mock.patch(
                        "subprocess.run",
                        side_effect=AssertionError(
                            "subprocess.run should not be called; zipfile should be used instead"
                        ),
                    ):
                        # Call the REAL bootstrap_editables function
                        _worker_common.bootstrap_editables()

            # Verify extraction succeeded
            marker_file = os.path.join(dest, "marker.txt")
            assert os.path.exists(
                marker_file
            ), f"Marker file {marker_file} should exist after extraction"
            with open(marker_file) as f:
                actual_content = f.read()
            assert (
                actual_content == marker_content
            ), f"Marker file content mismatch: expected '{marker_content}', got '{actual_content}'"

            # Verify __init__.py was extracted
            init_file = os.path.join(dest, "__init__.py")
            assert os.path.exists(init_file), f"__init__.py should exist at {init_file}"

            # Verify destination was added to sys.path
            assert (
                dest in sys.path
            ), f"Destination {dest} should have been added to sys.path"

        finally:
            # Clean up: remove from sys.path and delete the directory
            if dest in sys.path:
                sys.path.remove(dest)
            shutil.rmtree(dest, ignore_errors=True)


class TestDetachedResultStore:
    """BRAID_DETACH=1 routes results and errors to S3 with no SQS message."""

    def _s3(self):
        s3 = boto3.client("s3", region_name=REGION)
        s3.create_bucket(Bucket=BUCKET)
        return s3

    @mock_aws
    def test_detached_result_written_to_results_prefix(self):
        s3 = self._s3()
        sqs = mock.MagicMock()

        with mock.patch.dict(os.environ, {"BRAID_DETACH": "1"}):
            _worker_common.send_result(
                s3, sqs, "job-1", 7, b"payload", "http://queue", BUCKET, lambda k: k
            )

        body = s3.get_object(Bucket=BUCKET, Key="jobs/job-1/results/7.pkl")[
            "Body"
        ].read()
        assert body == b"payload"
        sqs.send_message.assert_not_called()

    @mock_aws
    def test_detached_result_honors_key_prefix_fn(self):
        s3 = self._s3()
        sqs = mock.MagicMock()

        with mock.patch.dict(os.environ, {"BRAID_DETACH": "1"}):
            _worker_common.send_result(
                s3, sqs, "job-1", 0, b"x", "http://queue", BUCKET, lambda k: f"pfx/{k}"
            )

        assert s3.get_object(Bucket=BUCKET, Key="pfx/jobs/job-1/results/0.pkl")

    @mock_aws
    def test_detached_large_result_written_to_results_prefix(self):
        """Detached mode ignores the inline size limit entirely."""
        s3 = self._s3()
        sqs = mock.MagicMock()
        big = b"z" * (300 * 1024)

        with mock.patch.dict(os.environ, {"BRAID_DETACH": "1"}):
            _worker_common.send_result(
                s3, sqs, "job-1", 1, big, "http://queue", BUCKET, lambda k: k
            )

        body = s3.get_object(Bucket=BUCKET, Key="jobs/job-1/results/1.pkl")[
            "Body"
        ].read()
        assert body == big
        sqs.send_message.assert_not_called()

    @mock_aws
    def test_detached_error_written_to_errors_prefix(self):
        s3 = self._s3()
        sqs = mock.MagicMock()

        with mock.patch.dict(os.environ, {"BRAID_DETACH": "1"}):
            _worker_common.send_error(
                s3,
                sqs,
                "job-1",
                3,
                "boom",
                "tb-text",
                "http://queue",
                BUCKET,
                lambda k: k,
            )

        payload = json.loads(
            s3.get_object(Bucket=BUCKET, Key="jobs/job-1/errors/3.json")["Body"].read()
        )
        assert payload == {"error": "boom", "traceback": "tb-text"}
        sqs.send_message.assert_not_called()

    @mock_aws
    def test_non_detached_result_still_inline_via_sqs(self):
        """Regression guard: fan()'s path is unchanged when BRAID_DETACH is unset."""
        s3 = self._s3()
        sqs = mock.MagicMock()

        env = {k: v for k, v in os.environ.items() if k != "BRAID_DETACH"}
        with mock.patch.dict(os.environ, env, clear=True):
            _worker_common.send_result(
                s3, sqs, "job-1", 2, b"small", "http://queue", BUCKET, lambda k: k
            )

        sqs.send_message.assert_called_once()
        msg = json.loads(sqs.send_message.call_args.kwargs["MessageBody"])
        assert msg["inline"] is True
        assert msg["idx"] == 2
        assert not s3.list_objects_v2(Bucket=BUCKET).get("Contents")

    @mock_aws
    def test_detached_error_swallows_s3_failure(self, caplog):
        sqs = mock.MagicMock()
        s3 = mock.MagicMock()
        s3.put_object.side_effect = Exception("S3 put failed")

        with mock.patch.dict(os.environ, {"BRAID_DETACH": "1"}):
            with caplog.at_level(logging.ERROR):
                _worker_common.send_error(
                    s3,
                    sqs,
                    "job-1",
                    0,
                    "boom",
                    "tb",
                    "http://queue",
                    BUCKET,
                    lambda k: k,
                )

        assert "Failed to write error to S3" in caplog.text
