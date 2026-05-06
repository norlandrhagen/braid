import tempfile
from pathlib import Path

from braid.config import Config
from braid.errors import LayerBuildError
from braid.sync import _platform_tag, _strip_layer, sync_editables
from moto import mock_aws


class TestPlatformTag:
    def test_platform_tag_arm64(self):
        config = Config(
            bucket="test-bucket",
            sqs_queue_url="http://example.com",
            arch="arm64",
        )
        assert _platform_tag(config) == "aarch64-manylinux_2_28"

    def test_platform_tag_x86_64(self):
        config = Config(
            bucket="test-bucket",
            sqs_queue_url="http://example.com",
            arch="x86_64",
        )
        assert _platform_tag(config) == "x86_64-manylinux_2_28"


class TestLayerBuildError:
    def test_layer_build_error_message_format(self):
        error = LayerBuildError("Something went wrong")
        assert str(error) == "Something went wrong"

    def test_layer_build_error_large_deps(self):
        message = (
            "Dependencies total 260MB unzipped, "
            "exceeding Lambda's 250MB limit. "
            "Use a container image for large dependency sets (no size limit)."
        )
        error = LayerBuildError(message)
        assert "260MB" in str(error)
        assert "250MB" in str(error)


class TestStripLayer:
    def _make_tree(self, root: Path) -> None:
        (root / "numpy").mkdir()
        (root / "numpy" / "core.py").write_text("x")
        (root / "numpy" / "tests").mkdir()
        (root / "numpy" / "tests" / "test_core.py").write_text("x")
        (root / "numpy-2.0.dist-info").mkdir()
        (root / "numpy-2.0.dist-info" / "RECORD").write_text("x")
        (root / "numpy-2.0.dist-info" / "METADATA").write_text(
            "Name: numpy\nVersion: 2.0.0\n"
        )
        # botocore.docs and boto3.docs are importable subpackages — must survive stripping
        for pkg in ("botocore", "boto3"):
            (root / pkg).mkdir()
            (root / pkg / "docs").mkdir()
            (root / pkg / "docs" / "__init__.py").write_text("x")
            (root / pkg / "docs" / "docstring.py").write_text("x")
        # A docs dir without __init__.py is plain documentation — safe to strip
        (root / "somelib").mkdir()
        (root / "somelib" / "docs").mkdir()
        (root / "somelib" / "docs" / "index.rst").write_text("x")
        (root / "numpy" / "fft.pyi").write_text("x")
        (root / "numpy" / "_core.h").write_text("x")
        (root / "numpy" / "_core.pyx").write_text("x")

    def test_strips_dist_info_record_keeps_metadata(self, tmp_path):
        self._make_tree(tmp_path)
        _strip_layer(tmp_path)
        # .dist-info dir survives (importlib.metadata needs it)
        assert (tmp_path / "numpy-2.0.dist-info").exists()
        # RECORD deleted (large manifest, not needed at runtime)
        assert not (tmp_path / "numpy-2.0.dist-info" / "RECORD").exists()
        # METADATA kept (version lookups need this)
        assert (tmp_path / "numpy-2.0.dist-info" / "METADATA").exists()

    def test_removes_tests_dir(self, tmp_path):
        self._make_tree(tmp_path)
        _strip_layer(tmp_path)
        assert not (tmp_path / "numpy" / "tests").exists()

    def test_removes_source_extensions(self, tmp_path):
        self._make_tree(tmp_path)
        _strip_layer(tmp_path)
        assert not (tmp_path / "numpy" / "fft.pyi").exists()
        assert not (tmp_path / "numpy" / "_core.h").exists()
        assert not (tmp_path / "numpy" / "_core.pyx").exists()

    def test_preserves_importable_docs_subpackages(self, tmp_path):
        self._make_tree(tmp_path)
        _strip_layer(tmp_path)
        # Both botocore.docs and boto3.docs are importable — must survive
        assert (tmp_path / "botocore" / "docs" / "docstring.py").exists()
        assert (tmp_path / "boto3" / "docs" / "docstring.py").exists()

    def test_strips_plain_docs_dir(self, tmp_path):
        self._make_tree(tmp_path)
        _strip_layer(tmp_path)
        assert not (tmp_path / "somelib" / "docs").exists()

    def test_preserves_normal_py_files(self, tmp_path):
        self._make_tree(tmp_path)
        _strip_layer(tmp_path)
        assert (tmp_path / "numpy" / "core.py").exists()


class TestSyncEditables:
    def test_sync_editables_no_lock_file(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        config = Config(
            bucket="test-bucket",
            sqs_queue_url="http://example.com",
        )
        result = sync_editables(config)
        assert result == []

    @mock_aws
    def test_sync_editables_hash_includes_non_py_files(self, tmp_path, monkeypatch):
        """
        Test that hash changes when a non-.py file is modified.
        This proves the fix includes non-.py files in the hash.
        """
        # Create a temporary editable package directory
        pkg_dir = tmp_path / "my_editable_pkg"
        pkg_dir.mkdir()

        # Create both .py and non-.py files
        (pkg_dir / "module.py").write_text("print('hello')")
        (pkg_dir / "data.json").write_text('{"key": "value1"}')

        # Create project root with uv.lock containing editable entry
        project_root = tmp_path / "project"
        project_root.mkdir()
        monkeypatch.chdir(project_root)

        # Create uv.lock with editable package
        lock_data = {
            "package": [
                {"name": "my-editable-pkg", "source": {"editable": str(pkg_dir)}}
            ]
        }
        import tomli_w

        (project_root / "uv.lock").write_text(tomli_w.dumps(lock_data))

        # Set up S3 mock
        import boto3

        boto3.client("s3").create_bucket(Bucket="test-bucket")

        config = Config(
            bucket="test-bucket",
            sqs_queue_url="http://example.com",
            region="us-east-1",
        )

        # First call to sync_editables - should upload the zip
        result1 = sync_editables(config)
        assert len(result1) == 1
        assert result1[0]["name"] == "my-editable-pkg"
        hash1 = result1[0]["src_hash"]

        # Modify only the non-.py file
        (pkg_dir / "data.json").write_text('{"key": "value2"}')

        # Second call to sync_editables - hash should change
        result2 = sync_editables(config)
        assert len(result2) == 1
        hash2 = result2[0]["src_hash"]

        # Hashes should be different because non-.py file changed
        assert hash1 != hash2, "Hash should change when non-.py file is modified"

    @mock_aws
    def test_sync_editables_hash_changes_on_py_file_modification(
        self, tmp_path, monkeypatch
    ):
        """
        Regression test: verify that modifying .py files still triggers hash change.
        """
        # Create a temporary editable package directory
        pkg_dir = tmp_path / "my_editable_pkg"
        pkg_dir.mkdir()

        # Create both .py and non-.py files
        (pkg_dir / "module.py").write_text("print('hello')")
        (pkg_dir / "data.json").write_text('{"key": "value1"}')

        # Create project root with uv.lock containing editable entry
        project_root = tmp_path / "project"
        project_root.mkdir()
        monkeypatch.chdir(project_root)

        # Create uv.lock with editable package
        lock_data = {
            "package": [
                {"name": "my-editable-pkg", "source": {"editable": str(pkg_dir)}}
            ]
        }
        import tomli_w

        (project_root / "uv.lock").write_text(tomli_w.dumps(lock_data))

        # Set up S3 mock
        import boto3

        boto3.client("s3").create_bucket(Bucket="test-bucket")

        config = Config(
            bucket="test-bucket",
            sqs_queue_url="http://example.com",
            region="us-east-1",
        )

        # First call to sync_editables
        result1 = sync_editables(config)
        assert len(result1) == 1
        hash1 = result1[0]["src_hash"]

        # Modify the .py file
        (pkg_dir / "module.py").write_text("print('goodbye')")

        # Second call to sync_editables - hash should change
        result2 = sync_editables(config)
        assert len(result2) == 1
        hash2 = result2[0]["src_hash"]

        # Hashes should be different because .py file changed
        assert hash1 != hash2, "Hash should change when .py file is modified"

    @mock_aws
    def test_sync_editables_excludes_git_files(self, tmp_path, monkeypatch):
        """
        Test that .git directories are excluded from both hash and zip.
        """
        # Create a temporary editable package directory
        pkg_dir = tmp_path / "my_editable_pkg"
        pkg_dir.mkdir()

        # Create files including .git directory
        (pkg_dir / "module.py").write_text("print('hello')")
        (pkg_dir / ".git").mkdir()
        (pkg_dir / ".git" / "config").write_text("git config")

        # Create project root with uv.lock containing editable entry
        project_root = tmp_path / "project"
        project_root.mkdir()
        monkeypatch.chdir(project_root)

        # Create uv.lock with editable package
        lock_data = {
            "package": [
                {"name": "my-editable-pkg", "source": {"editable": str(pkg_dir)}}
            ]
        }
        import tomli_w

        (project_root / "uv.lock").write_text(tomli_w.dumps(lock_data))

        # Set up S3 mock
        import boto3

        boto3.client("s3").create_bucket(Bucket="test-bucket")

        config = Config(
            bucket="test-bucket",
            sqs_queue_url="http://example.com",
            region="us-east-1",
        )

        # Call sync_editables
        result = sync_editables(config)
        assert len(result) == 1

        # Verify .git files are not included in the uploaded zip
        s3 = boto3.client("s3", region_name="us-east-1")
        s3_key = result[0]["s3_key"]

        # Download the zip and verify contents
        import zipfile

        with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
            tmp_path_zip = Path(tmp.name)

        try:
            resp = s3.get_object(Bucket="test-bucket", Key=s3_key)
            tmp_path_zip.write_bytes(resp["Body"].read())

            with zipfile.ZipFile(tmp_path_zip, "r") as zf:
                names = zf.namelist()
                # .git files should not be in the zip
                assert not any(".git" in name for name in names)
                # module.py should be in the zip
                assert "module.py" in names
        finally:
            tmp_path_zip.unlink()
