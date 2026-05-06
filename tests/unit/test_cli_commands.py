from unittest.mock import MagicMock

import pytest
from braid.cli.commands import cli
from braid.config import Config
from click.testing import CliRunner


class TestCleanCommand:
    """Test the 'clean' command with bucket_prefix handling."""

    @pytest.fixture
    def runner(self):
        return CliRunner()

    def test_clean_with_bucket_prefix(self, runner, mocker):
        """Test that 'clean' uses config.s3_key when bucket_prefix is set."""
        config = Config(
            bucket="test-bucket",
            bucket_prefix="lambda-test",
            sqs_queue_url="http://example.com",
            region="us-east-1",
        )

        # Mock load_config to return our test config
        mocker.patch("braid.cli.commands.load_config", return_value=config)

        # Mock s3_client functions
        mock_get_client = mocker.patch("braid.cli.commands.s3_client.get_client")
        mock_delete_prefix = mocker.patch("braid.cli.commands.s3_client.delete_prefix")
        mock_s3_client = MagicMock()
        mock_get_client.return_value = mock_s3_client

        # Call the clean command
        result = runner.invoke(cli, ["clean", "job123"])

        # Verify the command succeeded
        assert result.exit_code == 0, f"Command failed: {result.output}"

        # Verify get_client was called with the correct region
        mock_get_client.assert_called_once_with("us-east-1")

        # Verify delete_prefix was called with the prefixed key
        # Expected key: "lambda-test/jobs/job123/" (using s3_key method)
        expected_key = config.s3_key("jobs/job123/")
        mock_delete_prefix.assert_called_once_with(
            mock_s3_client, "test-bucket", expected_key
        )
        assert expected_key == "lambda-test/jobs/job123/"

        # Verify the output shows the prefixed key
        assert expected_key in result.output

    def test_clean_without_bucket_prefix(self, runner, mocker):
        """Test that 'clean' works without bucket_prefix (no regression)."""
        config = Config(
            bucket="test-bucket",
            bucket_prefix="",
            sqs_queue_url="http://example.com",
            region="us-east-1",
        )

        # Mock load_config to return our test config
        mocker.patch("braid.cli.commands.load_config", return_value=config)

        # Mock s3_client functions
        mock_get_client = mocker.patch("braid.cli.commands.s3_client.get_client")
        mock_delete_prefix = mocker.patch("braid.cli.commands.s3_client.delete_prefix")
        mock_s3_client = MagicMock()
        mock_get_client.return_value = mock_s3_client

        # Call the clean command
        result = runner.invoke(cli, ["clean", "job456"])

        # Verify the command succeeded
        assert result.exit_code == 0, f"Command failed: {result.output}"

        # Verify get_client was called with the correct region
        mock_get_client.assert_called_once_with("us-east-1")

        # Verify delete_prefix was called with the non-prefixed key
        # Expected key: "jobs/job456/" (no prefix)
        expected_key = config.s3_key("jobs/job456/")
        mock_delete_prefix.assert_called_once_with(
            mock_s3_client, "test-bucket", expected_key
        )
        assert expected_key == "jobs/job456/"

        # Verify the output shows the key without prefix
        assert expected_key in result.output


class TestBatchRunCommand:
    """Test the 'batch run' command: config overrides, guards, output."""

    @pytest.fixture
    def runner(self):
        return CliRunner()

    def _config(self, **overrides):
        base = dict(
            bucket="test-bucket",
            bucket_prefix="",
            sqs_queue_url="http://example.com",
            region="us-east-1",
            batch_execution_role_arn="arn:aws:iam::123456789012:role/test",
        )
        base.update(overrides)
        return Config(**base)

    def test_missing_role_guard(self, runner, mocker, tmp_path):
        config = self._config(batch_execution_role_arn="")
        mocker.patch("braid.cli.commands.load_config", return_value=config)
        script = tmp_path / "script.py"
        script.write_text("print('hi')\n")

        result = runner.invoke(cli, ["batch", "run", str(script)])

        assert result.exit_code != 0
        assert "braid init" in result.output

    def test_malformed_env_pair(self, runner, mocker, tmp_path):
        config = self._config()
        mocker.patch("braid.cli.commands.load_config", return_value=config)
        mocker.patch("braid.dispatch_batch._has_pep723_header", return_value=True)
        script = tmp_path / "script.py"
        script.write_text("print('hi')\n")

        result = runner.invoke(cli, ["batch", "run", str(script), "--env", "NOVALUE"])

        assert result.exit_code != 0
        assert "KEY=VALUE" in result.output

    def test_success_output_and_overrides(self, runner, mocker, tmp_path):
        config = self._config()
        mocker.patch("braid.cli.commands.load_config", return_value=config)
        mocker.patch("braid.dispatch_batch._has_pep723_header", return_value=True)
        mock_submit = mocker.patch(
            "braid.dispatch_batch.submit_script",
            return_value=("job-123", "batch-job-456"),
        )
        script = tmp_path / "script.py"
        script.write_text("print('hi')\n")

        result = runner.invoke(
            cli,
            [
                "batch",
                "run",
                str(script),
                "--vcpu",
                "4",
                "--memory",
                "8192",
                "--env",
                "FOO=bar",
            ],
        )

        assert result.exit_code == 0, result.output
        assert "job-123" in result.output
        assert "batch-job-456" in result.output

        used_config = mock_submit.call_args[0][1]
        assert used_config.batch_vcpu == 4
        assert used_config.batch_memory_mb == 8192
        assert mock_submit.call_args.kwargs["env_passthrough"] == {"FOO": "bar"}

    def test_project_mode_syncs_deps(self, runner, mocker, tmp_path):
        config = self._config()
        mocker.patch("braid.cli.commands.load_config", return_value=config)
        mocker.patch("braid.dispatch_batch._has_pep723_header", return_value=False)
        mock_sync_lock = mocker.patch("braid.sync.sync_uv_lock", return_value="req-key")
        mock_sync_editables = mocker.patch("braid.sync.sync_editables", return_value=[])
        mocker.patch(
            "braid.dispatch_batch.submit_script",
            return_value=("job-123", "batch-job-456"),
        )
        script = tmp_path / "script.py"
        script.write_text("print('hi')\n")

        result = runner.invoke(cli, ["batch", "run", str(script)])

        assert result.exit_code == 0, result.output
        mock_sync_lock.assert_called_once()
        mock_sync_editables.assert_called_once()
