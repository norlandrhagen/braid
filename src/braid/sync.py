from __future__ import annotations

import hashlib
import json
import logging
import os
import shutil
import subprocess
import tempfile
import zipfile
from pathlib import Path

import boto3

from braid.aws import lambda_client, s3_client
from braid.config import (
    Config,
    _find_project_root,
    layer_cache_path,
)
from braid.errors import LayerBuildError

log = logging.getLogger(__name__)

_LAYER_NAME = "braid-deps"


def hash_uv_lock(lock_path: Path | None = None) -> str:
    path = lock_path or _find_project_root() / "uv.lock"
    if not path.exists():
        raise FileNotFoundError(f"uv.lock not found at {path}. Run `uv lock` first.")
    content = path.read_bytes()
    return hashlib.sha256(content).hexdigest()[:16]


def _cache_key(lock_hash: str, config: Config) -> str:
    key = f"{lock_hash}-{config.arch}-py{config.python_version.replace('.', '')}"
    if config.exclude_packages:
        excl = "-".join(sorted(p.lower() for p in config.exclude_packages))
        key += f"-excl{hashlib.sha256(excl.encode()).hexdigest()[:8]}"
    return key


def _read_local_cache(config: Config) -> dict:
    cache_path = layer_cache_path()
    if cache_path.exists():
        return json.loads(cache_path.read_text())
    return {}


def _write_local_cache(cache: dict, config: Config) -> None:
    cache_path = layer_cache_path()
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps(cache, indent=2))


def _platform_tag(config: Config) -> str:
    # Lambda runs AL2023 (glibc 2.34); manylinux_2_28 covers more packages than manylinux2014
    if config.arch == "arm64":
        return "aarch64-manylinux_2_28"
    return "x86_64-manylinux_2_28"


def _dir_size(path: Path) -> int:
    return sum(f.stat().st_size for f in path.rglob("*") if f.is_file())


def _log_top_packages(site_packages: Path, n: int = 10) -> None:
    sizes = []
    for pkg in site_packages.iterdir():
        if pkg.name.endswith((".dist-info", ".egg-info")):
            continue
        size = _dir_size(pkg) if pkg.is_dir() else pkg.stat().st_size
        sizes.append((size, pkg.name))
    sizes.sort(reverse=True)
    for size, name in sizes[:n]:
        log.info("  %6.1f MB  %s", size / 1024 / 1024, name)


def _remove_excluded_packages(prefix_dir: Path, exclude_packages: list[str]) -> None:
    """Delete installed package dirs for explicitly excluded packages."""
    excluded = {p.lower().replace("-", "_") for p in exclude_packages}
    for site_packages in prefix_dir.rglob("site-packages"):
        for entry in list(site_packages.iterdir()):
            name = entry.name.lower().replace("-", "_")
            # Match package dir (scipy) and its libs dir (scipy.libs)
            base = name.split(".")[0]
            if base in excluded:
                if entry.is_dir():
                    shutil.rmtree(entry)
                else:
                    entry.unlink()
                log.info("Removed excluded package: %s", entry.name)


def _strip_layer(prefix_dir: Path) -> None:
    """Remove files not needed at Lambda runtime to reduce layer size."""
    remove_dirs = {
        "tests",
        "test",
        "__pycache__",
        "benchmarks",
        "docs",
        "locale",
        "test_data",
        "testdata",
        "fixtures",
        "examples",
    }
    remove_suffixes = {
        ".pyi",
        ".h",
        ".c",
        ".pxd",
        ".pxi",
        ".pyx",
        ".cpp",
        ".cxx",
        ".cc",
        ".f",
        ".f90",
        ".pxi.in",
    }

    for path in list(prefix_dir.rglob("*")):
        if not path.exists():
            continue
        if path.is_dir():
            # A "docs" dir with __init__.py is an importable subpackage (e.g.
            # botocore.docs, boto3.docs) — stripping it breaks runtime imports.
            is_importable_docs = path.name == "docs" and (path / "__init__.py").exists()
            if (
                path.name in remove_dirs and not is_importable_docs
            ) or path.name.endswith(".egg-info"):
                shutil.rmtree(path)
        elif path.is_file():
            if path.suffix in remove_suffixes:
                path.unlink()
            # RECORD files inside .dist-info are large file manifests not needed at runtime.
            # Keep the rest of .dist-info so importlib.metadata.version() works.
            elif path.name == "RECORD" and path.parent.name.endswith(".dist-info"):
                path.unlink()


def _build_layer_zip(config: Config, tmp_dir: Path) -> Path:
    """Build Lambda layer zip locally using uv.

    Lambda layer structure must be:
      python/lib/python3.X/site-packages/...

    `uv pip install --prefix <dir>` produces:
      <dir>/lib/python3.X/site-packages/...

    So we install with --prefix into a dir named 'python' inside tmp,
    then zip the entire 'python' subtree from tmp root.
    """
    project_root = _find_project_root().resolve()
    prefix_dir = tmp_dir / "python"
    req_file = tmp_dir / "requirements.txt"
    zip_path = tmp_dir / "layer.zip"

    prefix_dir.mkdir(parents=True, exist_ok=True)

    export_cmd = [
        "uv",
        "export",
        "--frozen",
        "--all-groups",
        "--no-dev",
        "--no-editable",
        "-o",
        str(req_file),
    ]
    result = subprocess.run(
        export_cmd, cwd=project_root, capture_output=True, text=True
    )
    if result.returncode != 0:
        raise LayerBuildError(f"uv export failed:\n{result.stderr}")

    env = os.environ.copy()
    env["UV_NO_INSTALLER_METADATA"] = "1"

    result = subprocess.run(
        [
            "uv",
            "pip",
            "install",
            "-r",
            str(req_file),
            "--prefix",
            str(prefix_dir),
            "--python-platform",
            _platform_tag(config),
            "--python",
            config.python_version,
            "--no-compile",  # skip .pyc generation to stay under 250MB layer limit
        ],
        capture_output=True,
        text=True,
        env=env,
        cwd=project_root,
    )
    if result.returncode != 0:
        raise LayerBuildError(f"uv pip install failed:\n{result.stderr}")

    if config.exclude_packages:
        _remove_excluded_packages(prefix_dir, config.exclude_packages)

    site_packages = next(prefix_dir.rglob("site-packages"), None)
    if site_packages:
        pre_strip = _dir_size(prefix_dir)
        log.info(
            "Layer size before strip: %.1f MB — top packages:", pre_strip / 1024 / 1024
        )
        _log_top_packages(site_packages)

    _strip_layer(prefix_dir)

    _LAYER_UNZIPPED_LIMIT = 250 * 1024 * 1024  # Lambda layer limit
    unzipped_size = sum(f.stat().st_size for f in prefix_dir.rglob("*") if f.is_file())
    log.info("Layer size after strip: %.1f MB", unzipped_size / 1024 / 1024)
    if unzipped_size > _LAYER_UNZIPPED_LIMIT:
        raise LayerBuildError(
            f"Dependencies total {unzipped_size / 1024 / 1024:.0f}MB unzipped, "
            "exceeding Lambda's 250MB limit. "
            "Use a container image for large dependency sets (no size limit)."
        )

    # Zip: paths inside zip are relative to tmp_dir so they start with python/
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for f in sorted(prefix_dir.rglob("*")):
            if f.is_file():
                zf.write(f, f.relative_to(tmp_dir))

    return zip_path


def sync_uv_lock(config: Config) -> str:
    """Export requirements.txt from uv.lock, upload to S3. Return S3 key."""
    project_root = _find_project_root().resolve()
    lock_hash = hash_uv_lock()
    req_s3_key = config.s3_key(f"layers/{lock_hash}/requirements.txt")
    s3 = s3_client.get_client(config.region)
    if s3_client.key_exists(s3, config.bucket, req_s3_key):
        log.debug("requirements.txt already on S3: %s", req_s3_key)
        return req_s3_key
    result = subprocess.run(
        ["uv", "export", "--frozen", "--no-dev", "--no-editable"],
        cwd=project_root,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise LayerBuildError(f"uv export failed:\n{result.stderr}")
    s3_client.upload_bytes(s3, config.bucket, req_s3_key, result.stdout.encode())
    log.info("requirements.txt uploaded: s3://%s/%s", config.bucket, req_s3_key)
    return req_s3_key


def get_or_build_layer_s3(config: Config, force: bool = False) -> str:
    """Ensure layer.zip exists on S3. Return its S3 key. No Lambda registration."""
    lock_hash = hash_uv_lock()
    key = _cache_key(lock_hash, config)
    s3 = s3_client.get_client(config.region)
    layer_zip_key = config.s3_key(f"layers/{key}/layer.zip")
    if not force and s3_client.key_exists(s3, config.bucket, layer_zip_key):
        log.debug("Layer zip already on S3: %s", layer_zip_key)
        return layer_zip_key
    log.info("Building layer zip for %s...", key)
    with tempfile.TemporaryDirectory() as tmp:
        zip_path = _build_layer_zip(config, Path(tmp))
        s3_client.upload_file(s3, config.bucket, layer_zip_key, zip_path)
    log.info("Layer zip uploaded: s3://%s/%s", config.bucket, layer_zip_key)
    return layer_zip_key


def _bucket_region(bucket: str) -> str:
    s3 = boto3.client("s3", region_name="us-east-1")
    loc = s3.get_bucket_location(Bucket=bucket)["LocationConstraint"]
    return loc or "us-east-1"


def _ensure_regional_bucket(region: str) -> str:
    """Create (if needed) a braid-owned S3 bucket in `region` for layer zip staging."""
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    bucket = f"braid-layers-{account_id}-{region}"
    s3 = boto3.client("s3", region_name=region)
    try:
        kwargs: dict = {"Bucket": bucket}
        if region != "us-east-1":
            kwargs["CreateBucketConfiguration"] = {"LocationConstraint": region}
        s3.create_bucket(**kwargs)
        log.info("Created regional staging bucket: %s", bucket)
        s3.put_bucket_lifecycle_configuration(
            Bucket=bucket,
            LifecycleConfiguration={
                "Rules": [
                    {
                        "ID": "braid-layer-staging-expire",
                        "Filter": {"Prefix": ""},
                        "Status": "Enabled",
                        "Expiration": {"Days": 1},
                    }
                ]
            },
        )
    except s3.exceptions.BucketAlreadyOwnedByYou:
        pass
    return bucket


def _register_lambda_layer(layer_zip_key: str, cache_key: str, config: Config) -> str:
    """Register existing S3 zip as Lambda layer. Return ARN."""
    lam = lambda_client.get_client(config.region)
    compatible_arch = ["arm64"] if config.arch == "arm64" else ["x86_64"]

    src_bucket_region = _bucket_region(config.bucket)

    if src_bucket_region == config.region:
        # Same region: use S3 reference (no size limit)
        content = {"S3Bucket": config.bucket, "S3Key": layer_zip_key}
    else:
        # Cross-region: stage zip in a same-region bucket first
        log.info("Cross-region: staging layer zip in %s...", config.region)
        src_s3 = s3_client.get_client(src_bucket_region)
        zip_bytes = s3_client.download_bytes(src_s3, config.bucket, layer_zip_key)
        regional_bucket = _ensure_regional_bucket(config.region)
        regional_s3 = s3_client.get_client(config.region)
        s3_client.upload_bytes(regional_s3, regional_bucket, layer_zip_key, zip_bytes)
        log.info("Staged to s3://%s/%s", regional_bucket, layer_zip_key)
        content = {"S3Bucket": regional_bucket, "S3Key": layer_zip_key}

    resp = lam.publish_layer_version(
        LayerName=_LAYER_NAME,
        Description=f"braid deps ({cache_key})",
        Content=content,
        CompatibleRuntimes=[f"python{config.python_version}"],
        CompatibleArchitectures=compatible_arch,
    )
    arn = resp["LayerVersionArn"]

    meta = {"arn": arn, "cache_key": cache_key, "region": config.region}
    meta_s3 = s3_client.get_client(config.region)
    s3_client.upload_bytes(
        meta_s3,
        config.bucket,
        config.s3_key(f"layers/{cache_key}/metadata.json"),
        json.dumps(meta).encode(),
    )
    return arn


def _check_s3_cache(cache_key: str, config: Config) -> str | None:
    s3 = s3_client.get_client(config.region)
    meta_key = config.s3_key(f"layers/{cache_key}/metadata.json")
    if s3_client.key_exists(s3, config.bucket, meta_key):
        meta = json.loads(s3_client.download_bytes(s3, config.bucket, meta_key))
        if meta.get("region") != config.region:
            return None  # ARN is from a different region; register a new layer
        return meta["arn"]
    return None


def get_or_build_layer(config: Config, force: bool = False) -> str:
    """Return layer ARN for current uv.lock. Build and cache if needed."""
    lock_hash = hash_uv_lock()
    key = _cache_key(lock_hash, config)
    local_key = f"{config.region}/{key}"

    if not force:
        # 1. Local cache (keyed by region/cache_key to avoid cross-region ARN collisions)
        cache = _read_local_cache(config)
        if local_key in cache:
            return cache[local_key]

        # 2. S3 cache (another machine may have built it)
        arn = _check_s3_cache(key, config)
        if arn:
            cache[local_key] = arn
            _write_local_cache(cache, config)
            return arn

    # 3. Build locally: ensure zip on S3, then register as Lambda layer
    layer_zip_key = get_or_build_layer_s3(config, force=force)
    arn = _register_lambda_layer(layer_zip_key, key, config)

    # Update local cache
    cache = _read_local_cache(config)
    cache[local_key] = arn
    _write_local_cache(cache, config)

    log.info("Layer built and registered: %s", arn)
    return arn


def sync_editables(config: Config) -> list[dict]:
    """
    Find editable packages in uv.lock, zip source dirs, upload to S3.
    Returns manifest: [{name, s3_key, src_hash}]
    """
    project_root = _find_project_root().resolve()
    lock_path = project_root / "uv.lock"
    if not lock_path.exists():
        return []

    try:
        import tomllib
    except ImportError:
        import tomllib  # type: ignore

    with lock_path.open("rb") as f:
        lock_data = tomllib.load(f)
    editables = []
    for pkg in lock_data.get("package", []):
        source = pkg.get("source", {})
        if source.get("editable") is not None:
            src_path = Path(source["editable"]).resolve()
            # Skip if this editable is the project root itself (braid or the user's own project)
            if src_path == project_root:
                continue
            editables.append({"name": pkg["name"], "path": src_path})

    if not editables:
        return []

    def _get_package_files(src_path: Path) -> list[Path]:
        """Get all files to include in editable package, excluding .git."""
        return sorted(
            f for f in src_path.rglob("*") if f.is_file() and ".git" not in f.parts
        )

    s3 = s3_client.get_client(config.region)
    manifest = []

    for pkg in editables:
        src_path = pkg["path"]
        files = _get_package_files(src_path)

        # Hash source dir contents (same files as will be zipped)
        hasher = hashlib.sha256()
        for f in files:
            hasher.update(f.read_bytes())
        src_hash = hasher.hexdigest()[:12]

        s3_key = config.s3_key(f"editables/{pkg['name']}/{src_hash}/src.zip")

        if not s3_client.key_exists(s3, config.bucket, s3_key):
            with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
                tmp_path = Path(tmp.name)

            with zipfile.ZipFile(tmp_path, "w", zipfile.ZIP_DEFLATED) as zf:
                for f in files:
                    zf.write(f, f.relative_to(src_path))

            s3_client.upload_file(s3, config.bucket, s3_key, tmp_path)
            tmp_path.unlink()
            log.info("Uploaded editable package: %s (%s)", pkg["name"], src_hash)

        manifest.append({"name": pkg["name"], "s3_key": s3_key, "src_hash": src_hash})

    return manifest
