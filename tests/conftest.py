import os

import pytest


@pytest.fixture(autouse=True)
def aws_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture(autouse=True)
def isolated_project_root(tmp_path, monkeypatch):
    """braid.config._find_project_root walks up from cwd looking for pyproject.toml
    or .braid/, so any test that writes state (jobs cache, jobdef cache, etc.)
    without an explicit project_root would otherwise land in this repo's own
    .braid/cache/ directory. Force cwd to an empty tmp dir for every test.
    """
    monkeypatch.chdir(tmp_path)
