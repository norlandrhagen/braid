"""
Unit tests for _worker_script.main(). subprocess.call and urllib are mocked
so no real script executes and no real network calls happen, mirroring the
mocking style used elsewhere for worker entrypoints.
"""

from __future__ import annotations

import json
import os
import pickle

import pytest
from braid import _worker_script


@pytest.fixture
def script_env():
    env_keys = [
        "BRAID_JOB_ID",
        "BRAID_SCRIPT_URL",
        "BRAID_SCRIPT_MODE",
        "BRAID_PYTHON_VERSION",
        "BRAID_RESULT_PUT_URL",
        "BRAID_ERROR_PUT_URL",
        "BRAID_REQUIREMENTS_KEY",
        "BRAID_BUCKET",
        "BRAID_EDITABLES",
    ]
    old = {k: os.environ.get(k) for k in env_keys}

    os.environ["BRAID_JOB_ID"] = "test-job"
    os.environ["BRAID_SCRIPT_URL"] = "https://example.com/script.py"
    os.environ["BRAID_RESULT_PUT_URL"] = "https://example.com/put-result"
    os.environ["BRAID_ERROR_PUT_URL"] = "https://example.com/put-error"
    os.environ.pop("BRAID_REQUIREMENTS_KEY", None)
    os.environ.pop("BRAID_EDITABLES", None)

    yield

    for k, v in old.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v


class TestWorkerScriptModeDispatch:
    def test_pep723_mode_uses_uv_run(self, script_env, mocker):
        os.environ["BRAID_SCRIPT_MODE"] = "pep723"
        os.environ["BRAID_PYTHON_VERSION"] = "3.13"
        mocker.patch("urllib.request.urlretrieve")
        mock_put = mocker.patch.object(_worker_script, "_put")
        mock_call = mocker.patch("subprocess.call", return_value=0)

        with pytest.raises(SystemExit) as exc:
            _worker_script.main()

        assert exc.value.code == 0
        cmd = mock_call.call_args[0][0]
        assert cmd[:3] == ["uv", "run", "--no-project"]
        assert "--python=3.13" in cmd
        assert cmd[-1] == "/tmp/script.py"
        mock_put.assert_called_once()
        put_url, body = mock_put.call_args[0]
        assert put_url == "https://example.com/put-result"
        assert pickle.loads(body) == {"exit_code": 0}

    def test_project_mode_uses_sys_executable(self, script_env, mocker):
        os.environ["BRAID_SCRIPT_MODE"] = "project"
        mocker.patch("urllib.request.urlretrieve")
        mocker.patch.object(_worker_script, "_bootstrap_uv_sync")
        mocker.patch("sys.path", [])

        import types

        fake_common = types.ModuleType("_worker_common")
        fake_common.bootstrap_editables = mocker.Mock()
        mocker.patch.dict("sys.modules", {"_worker_common": fake_common})

        mock_put = mocker.patch.object(_worker_script, "_put")
        mock_call = mocker.patch("subprocess.call", return_value=0)

        with pytest.raises(SystemExit) as exc:
            _worker_script.main()

        assert exc.value.code == 0
        import sys

        assert mock_call.call_args[0][0] == [sys.executable, "/tmp/script.py"]
        fake_common.bootstrap_editables.assert_called_once()
        mock_put.assert_called_once()


class TestWorkerScriptExitCode:
    def test_nonzero_exit_writes_error_marker(self, script_env, mocker):
        os.environ["BRAID_SCRIPT_MODE"] = "pep723"
        os.environ["BRAID_PYTHON_VERSION"] = "3.13"
        mocker.patch("urllib.request.urlretrieve")
        mock_put = mocker.patch.object(_worker_script, "_put")
        mocker.patch("subprocess.call", return_value=1)

        with pytest.raises(SystemExit) as exc:
            _worker_script.main()

        assert exc.value.code == 1
        put_url, body = mock_put.call_args[0]
        assert put_url == "https://example.com/put-error"
        payload = json.loads(body)
        assert "exited with code 1" in payload["error"]

    def test_marker_put_failure_does_not_mask_exit_code(self, script_env, mocker):
        """Marker PUT failures must not prevent the script exit code from propagating."""
        os.environ["BRAID_SCRIPT_MODE"] = "pep723"
        os.environ["BRAID_PYTHON_VERSION"] = "3.13"
        mocker.patch("urllib.request.urlretrieve")
        mocker.patch.object(_worker_script, "_put", side_effect=OSError("network down"))
        mocker.patch("subprocess.call", return_value=0)

        with pytest.raises(SystemExit) as exc:
            _worker_script.main()

        assert exc.value.code == 0
