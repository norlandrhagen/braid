from __future__ import annotations

import base64
import hashlib
import json
import logging
import re
import uuid
from pathlib import Path

import cloudpickle

from braid.aws import batch_client, s3_client
from braid.config import Config, batch_image, batch_jobdef_cache_path

log = logging.getLogger(__name__)

# PEP 723 inline script metadata block: a `# /// script` ... `# ///` comment fence.
# Regex from the PEP 723 reference implementation.
_PEP723_RE = re.compile(
    r"(?m)^# /// (?P<type>[A-Za-z0-9-]+)$\s(?P<content>(^#(| .*)$\s)+)^# ///$"
)


def _bootstrap_cmd(python_version: str) -> list[str]:
    # Use `uv run` so any base image works regardless of whether `python` is on PATH.
    # --no-project avoids requiring a pyproject.toml in the container.
    return [
        "uv",
        "run",
        "--no-project",
        f"--python={python_version}",
        "python",
        "-c",
        (
            "import urllib.request as r,os,subprocess,sys;"
            " r.urlretrieve(os.environ['BRAID_WORKER_URL'],'/tmp/w.py');"
            " r.urlretrieve(os.environ['BRAID_COMMON_URL'],'/tmp/_worker_common.py');"
            " sys.exit(subprocess.call([sys.executable,'/tmp/w.py']))"
        ),
    ]


def _upload_named_script(s3_obj, config: Config, filename: str, prefix: str) -> str:
    """Upload a package-local script under braid-system/{prefix}/{hash}.py, dedup by hash."""
    content = (Path(__file__).parent / filename).read_bytes()
    content_hash = hashlib.sha256(content).hexdigest()[:12]
    key = config.s3_key(f"braid-system/{prefix}/{content_hash}.py")
    if not s3_client.key_exists(s3_obj, config.bucket, key):
        s3_client.upload_bytes(s3_obj, config.bucket, key, content)
        log.debug("Uploaded %s: %s", filename, key)
    return key


def _upload_worker_script(s3_obj, config: Config) -> tuple[str, str]:
    """Upload _worker_batch.py and _worker_common.py. Return (worker_key, common_key)."""
    worker_key = _upload_named_script(
        s3_obj, config, "_worker_batch.py", "worker_batch"
    )
    common_key = _upload_named_script(
        s3_obj, config, "_worker_common.py", "worker_common"
    )
    return worker_key, common_key


def _presign(s3_client_obj, bucket: str, key: str, expires: int = 86400) -> str:
    return s3_client_obj.generate_presigned_url(
        "get_object", Params={"Bucket": bucket, "Key": key}, ExpiresIn=expires
    )


def _presign_put(s3_client_obj, bucket: str, key: str, expires: int = 86400) -> str:
    return s3_client_obj.generate_presigned_url(
        "put_object", Params={"Bucket": bucket, "Key": key}, ExpiresIn=expires
    )


def _has_pep723_header(script_path: Path) -> bool:
    """True if script has a `# /// script` ... `# ///` PEP 723 metadata block."""
    text = script_path.read_text()
    return any(m.group("type") == "script" for m in _PEP723_RE.finditer(text))


def _resolved_image(config: Config) -> str:
    return config.batch_image or batch_image(config.python_version)


def _jobdef_cache_key(config: Config) -> str:
    image = _resolved_image(config)
    mode = "ec2" if config.batch_instance_types else "fargate"
    raw = f"{image}:{config.batch_execution_role_arn}:{config.batch_vcpu}:{config.batch_memory_mb}:{mode}:{config.batch_ephemeral_storage_gib}:{config.python_version}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def _ensure_job_definition(config: Config) -> None:
    cache_key = _jobdef_cache_key(config)
    cache_path = batch_jobdef_cache_path()
    cache = json.loads(cache_path.read_text()) if cache_path.exists() else {}
    if cache.get("key") == cache_key:
        return

    image = _resolved_image(config)
    batch = batch_client.get_client(config.region)
    batch_client.register_job_definition(
        batch,
        name=config.batch_job_definition,
        image=image,
        command=_bootstrap_cmd(config.python_version),
        execution_role_arn=config.batch_execution_role_arn,
        vcpu=config.batch_vcpu,
        memory_mb=config.batch_memory_mb,
        ec2=bool(config.batch_instance_types),
        ephemeral_storage_gib=config.batch_ephemeral_storage_gib,
    )
    log.info("Registered Batch job definition: %s", config.batch_job_definition)

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps({"key": cache_key}))


def submit_batch(
    fn,
    inputs: list,
    config: Config,
    requirements_s3_key: str,
    editable_manifest: list[dict],
    detach: bool = False,
) -> tuple[str, int, str]:
    """Serialize fn + inputs, write S3 manifest, submit Batch array job.

    Returns (braid_job_id, n_inputs, batch_job_id).
    batch_job_id is the AWS Batch array job ID, used to terminate stuck jobs.
    detach makes workers store results in S3 rather than SQS.
    """
    job_id = str(uuid.uuid4())
    s3 = s3_client.get_client(config.region)

    fn_key = config.s3_key(f"jobs/{job_id}/fn.pkl")
    s3.put_object(Bucket=config.bucket, Key=fn_key, Body=cloudpickle.dumps(fn))

    manifest = {
        "fn_key": fn_key,
        "inputs": [
            {"idx": i, "data_b64": base64.b64encode(cloudpickle.dumps(x)).decode()}
            for i, x in enumerate(inputs)
        ],
    }
    manifest_key = config.s3_key(f"jobs/{job_id}/manifest.json")
    s3.put_object(
        Bucket=config.bucket, Key=manifest_key, Body=json.dumps(manifest).encode()
    )

    worker_key, common_key = _upload_worker_script(s3, config)
    worker_url = _presign(s3, config.bucket, worker_key)
    common_url = _presign(s3, config.bucket, common_key)

    _ensure_job_definition(config)

    # Validate input size: AWS Batch array jobs require size 2-10000; size 1 must use non-array job
    if len(inputs) == 0:
        raise ValueError("submit_batch requires at least one input")
    if len(inputs) > 10000:
        raise ValueError(
            f"AWS Batch array jobs are limited to 10000 inputs; got {len(inputs)}. "
            "Consider splitting your input list across multiple fan() calls."
        )

    env_overrides: dict[str, str] = {
        "BRAID_JOB_ID": job_id,
        "BRAID_BUCKET": config.bucket,
        "BRAID_BUCKET_PREFIX": config.bucket_prefix,
        "BRAID_SQS_QUEUE_URL": config.sqs_queue_url,
        "BRAID_REQUIREMENTS_KEY": requirements_s3_key,
        "BRAID_WORKER_URL": worker_url,
        "BRAID_COMMON_URL": common_url,
    }
    if detach:
        env_overrides["BRAID_DETACH"] = "1"
    if editable_manifest:
        env_overrides["BRAID_EDITABLES"] = json.dumps(editable_manifest)

    batch = batch_client.get_client(config.region)

    if len(inputs) == 1:
        # Single input uses non-array job submission
        batch_job_id = batch_client.submit_single_job(
            batch,
            job_def_name=config.batch_job_definition,
            queue_name=config.batch_job_queue,
            job_id=job_id,
            env_overrides=env_overrides,
        )
        log.info(
            "Submitted Batch single job %s (braid job_id=%s, n=1)",
            batch_job_id,
            job_id,
        )
    else:
        # 2-10000 inputs use array job submission
        batch_job_id = batch_client.submit_array_job(
            batch,
            job_def_name=config.batch_job_definition,
            queue_name=config.batch_job_queue,
            job_id=job_id,
            array_size=len(inputs),
            env_overrides=env_overrides,
        )
        log.info(
            "Submitted Batch array job %s (braid job_id=%s, n=%d)",
            batch_job_id,
            job_id,
            len(inputs),
        )
    return job_id, len(inputs), batch_job_id


def submit_script(
    script_path: Path,
    config: Config,
    requirements_s3_key: str = "",
    editable_manifest: list[dict] | None = None,
    env_passthrough: dict[str, str] | None = None,
) -> tuple[str, str]:
    """Upload a script, submit a single non-array Batch job to run it.

    Fire-and-forget: no manifest, no result collection. The worker writes a
    completion marker (results/0.pkl on success, errors/0.json on failure) so
    the existing detach status machinery (braid status/jobs) can see it.

    Returns (braid_job_id, batch_job_id).
    """
    job_id = str(uuid.uuid4())
    s3 = s3_client.get_client(config.region)

    script_key = config.s3_key(f"jobs/{job_id}/script.py")
    s3_client.upload_bytes(s3, config.bucket, script_key, script_path.read_bytes())

    script_worker_key = _upload_named_script(
        s3, config, "_worker_script.py", "worker_script"
    )
    common_key = _upload_named_script(s3, config, "_worker_common.py", "worker_common")

    script_url = _presign(s3, config.bucket, script_key)
    worker_url = _presign(s3, config.bucket, script_worker_key)
    common_url = _presign(s3, config.bucket, common_key)

    result_put_url = _presign_put(
        s3, config.bucket, config.s3_key(f"jobs/{job_id}/results/0.pkl")
    )
    error_put_url = _presign_put(
        s3, config.bucket, config.s3_key(f"jobs/{job_id}/errors/0.json")
    )

    _ensure_job_definition(config)

    mode = "pep723" if _has_pep723_header(script_path) else "project"

    env_overrides: dict[str, str] = {
        "BRAID_JOB_ID": job_id,
        "BRAID_WORKER_URL": worker_url,
        "BRAID_COMMON_URL": common_url,
        "BRAID_SCRIPT_URL": script_url,
        "BRAID_SCRIPT_MODE": mode,
        "BRAID_PYTHON_VERSION": config.python_version,
        "BRAID_RESULT_PUT_URL": result_put_url,
        "BRAID_ERROR_PUT_URL": error_put_url,
    }
    if mode == "project":
        env_overrides["BRAID_BUCKET"] = config.bucket
        env_overrides["BRAID_BUCKET_PREFIX"] = config.bucket_prefix
        env_overrides["BRAID_REQUIREMENTS_KEY"] = requirements_s3_key
        if editable_manifest:
            env_overrides["BRAID_EDITABLES"] = json.dumps(editable_manifest)
    if env_passthrough:
        env_overrides.update(env_passthrough)

    batch = batch_client.get_client(config.region)
    batch_job_id = batch_client.submit_single_job(
        batch,
        job_def_name=config.batch_job_definition,
        queue_name=config.batch_job_queue,
        job_id=job_id,
        env_overrides=env_overrides,
    )
    log.info(
        "Submitted Batch script job %s (braid job_id=%s, mode=%s)",
        batch_job_id,
        job_id,
        mode,
    )

    from braid.core import _record_batch_job
    from braid.detach import _write_job_record

    _record_batch_job(job_id, batch_job_id, 1, config)
    _write_job_record(job_id, batch_job_id, 1, config)

    return job_id, batch_job_id
