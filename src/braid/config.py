from __future__ import annotations

import os
import tomllib
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator

from braid.errors import ConfigError

_STATE_DIR = ".braid"
_CONFIG_FILE = "config.toml"
_LAYER_CACHE_FILE = "cache/layers.json"
_BATCH_BASE_IMAGE_TEMPLATE = "ghcr.io/astral-sh/uv:python{version}-bookworm-slim"


def _default_region() -> str:
    try:
        import boto3

        return boto3.session.Session().region_name or "us-west-2"
    except Exception:
        return "us-west-2"


class Config(BaseModel):
    region: str = Field(default_factory=_default_region)
    bucket: str
    bucket_prefix: str = Field(
        default=""
    )  # e.g. "lambda-test" → keys like "lambda-test/jobs/..."
    sqs_queue_url: str
    worker_function_name: str = Field(default="braid-worker")
    arch: Literal["x86_64", "arm64"] = Field(default="x86_64")
    python_version: str = Field(default="3.13")
    # runtime defaults (can be overridden per call)
    batch_size: int = Field(default=10, ge=1)
    memory_mb: int = Field(default=512, ge=128, le=10240)
    collect_timeout_s: int = Field(default=300, ge=1)  # how long fan waits for results
    lambda_timeout_s: int = Field(default=300, ge=1, le=900)  # Lambda's own timeout
    max_concurrency: int = Field(default=200, ge=1)
    max_retries: int = Field(default=2, ge=0)
    exclude_packages: list[str] = Field(default_factory=list)

    # Batch backend
    backend: Literal["lambda", "batch"] = Field(default="lambda")
    batch_job_queue: str = Field(default="braid-queue")
    batch_job_definition: str = Field(default="braid-job")
    batch_vcpu: float = Field(default=2.0)
    batch_memory_mb: int = Field(default=4096)
    batch_execution_role_arn: str = Field(default="")
    batch_subnet_ids: list[str] = Field(default_factory=list)
    batch_security_group_ids: list[str] = Field(default_factory=list)
    batch_instance_types: list[str] = Field(default_factory=list)
    batch_image: str = Field(
        default=""
    )  # custom base image; defaults to ghcr.io/astral-sh/uv:python{version}-bookworm-slim
    batch_ephemeral_storage_gib: int = Field(default=21, ge=21, le=200)

    def s3_key(self, suffix: str) -> str:
        """Prepend bucket_prefix to an S3 key suffix."""
        if self.bucket_prefix:
            return f"{self.bucket_prefix.rstrip('/')}/{suffix.lstrip('/')}"
        return suffix

    @model_validator(mode="before")
    @classmethod
    def reject_legacy_keys(cls, data: Any) -> Any:
        # pydantic ignores extra keys, so a stale timeout_s would silently
        # revert collect_timeout_s to its default.
        if isinstance(data, dict) and "timeout_s" in data:
            raise ValueError(
                "timeout_s was renamed to collect_timeout_s. Update .braid/config.toml."
            )
        return data

    @model_validator(mode="after")
    def apply_env_overrides(self) -> Config:
        if region := os.environ.get("BRAID_REGION"):
            self.region = region
        if bucket := os.environ.get("BRAID_BUCKET"):
            self.bucket = bucket
        if queue := os.environ.get("BRAID_SQS_QUEUE_URL"):
            self.sqs_queue_url = queue
        return self


def _find_project_root(start: Path | None = None) -> Path:
    here = start or Path.cwd()
    for parent in [here, *here.parents]:
        if (parent / "pyproject.toml").exists() or (parent / _STATE_DIR).exists():
            return parent
    return here


def state_dir(project_root: Path | None = None) -> Path:
    return _find_project_root(project_root) / _STATE_DIR


def layer_cache_path(project_root: Path | None = None) -> Path:
    return state_dir(project_root) / "cache" / "layers.json"


def batch_jobdef_cache_path(project_root: Path | None = None) -> Path:
    return state_dir(project_root) / "cache" / "jobdefs.json"


def jobs_cache_path(project_root: Path | None = None) -> Path:
    return state_dir(project_root) / "cache" / "jobs.json"


def batch_image(python_version: str) -> str:
    return _BATCH_BASE_IMAGE_TEMPLATE.format(version=python_version)


def load_config(project_root: Path | None = None) -> Config:
    """Load .braid/config.toml. Raises ConfigError if missing or invalid."""
    root = _find_project_root(project_root)
    config_path = root / _STATE_DIR / _CONFIG_FILE

    if not config_path.exists():
        raise ConfigError(
            f"No braid config found at {config_path}. Run `braid init` first."
        )

    with config_path.open("rb") as f:
        raw = tomllib.load(f)

    try:
        return Config(**raw)
    except Exception as e:
        raise ConfigError(f"Invalid config at {config_path}: {e}") from e


def write_config(config: Config, project_root: Path | None = None) -> Path:
    import tomli_w  # optional write dep

    root = _find_project_root(project_root)
    config_path = root / _STATE_DIR / _CONFIG_FILE
    config_path.parent.mkdir(parents=True, exist_ok=True)

    with config_path.open("wb") as f:
        tomli_w.dump(config.model_dump(), f)

    return config_path
