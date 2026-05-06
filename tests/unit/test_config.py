import pytest
from braid.config import Config, load_config
from braid.errors import ConfigError
from braid.sync import _cache_key, hash_uv_lock


class TestConfigS3Key:
    def test_s3_key_without_prefix(self):
        config = Config(bucket="test-bucket", sqs_queue_url="http://example.com")
        assert config.s3_key("jobs/job1/fn.pkl") == "jobs/job1/fn.pkl"

    def test_s3_key_with_prefix(self):
        config = Config(
            bucket="test-bucket",
            bucket_prefix="lambda-test",
            sqs_queue_url="http://example.com",
        )
        assert config.s3_key("jobs/job1/fn.pkl") == "lambda-test/jobs/job1/fn.pkl"

    def test_s3_key_with_prefix_trailing_slash(self):
        config = Config(
            bucket="test-bucket",
            bucket_prefix="lambda-test/",
            sqs_queue_url="http://example.com",
        )
        assert config.s3_key("/jobs/job1/fn.pkl") == "lambda-test/jobs/job1/fn.pkl"


class TestLoadConfig:
    def test_load_config_missing_file(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        with pytest.raises(ConfigError, match="No braid config found"):
            load_config()


class TestHashUVLock:
    def test_hash_uv_lock_missing_file(self, tmp_path):
        lock_path = tmp_path / "uv.lock"
        with pytest.raises(FileNotFoundError, match="uv.lock not found"):
            hash_uv_lock(lock_path)

    def test_hash_uv_lock_returns_hex_string(self, tmp_path):
        lock_path = tmp_path / "uv.lock"
        lock_path.write_text("test content")
        hash_val = hash_uv_lock(lock_path)
        assert isinstance(hash_val, str)
        assert len(hash_val) == 16
        assert all(c in "0123456789abcdef" for c in hash_val)

    def test_hash_uv_lock_consistent(self, tmp_path):
        lock_path = tmp_path / "uv.lock"
        lock_path.write_text("test content")
        hash1 = hash_uv_lock(lock_path)
        hash2 = hash_uv_lock(lock_path)
        assert hash1 == hash2

    def test_hash_uv_lock_different_content(self, tmp_path):
        lock_path = tmp_path / "uv.lock"
        lock_path.write_text("content1")
        hash1 = hash_uv_lock(lock_path)
        lock_path.write_text("content2")
        hash2 = hash_uv_lock(lock_path)
        assert hash1 != hash2


class TestCacheKey:
    def test_cache_key_format(self):
        config = Config(
            bucket="test-bucket",
            sqs_queue_url="http://example.com",
            arch="x86_64",
            python_version="3.13",
        )
        key = _cache_key("abc123def456", config)
        assert key == "abc123def456-x86_64-py313"

    def test_cache_key_arm64(self):
        config = Config(
            bucket="test-bucket",
            sqs_queue_url="http://example.com",
            arch="arm64",
            python_version="3.12",
        )
        key = _cache_key("abc123def456", config)
        assert key == "abc123def456-arm64-py312"
