from __future__ import annotations

import json
import time

import click

from braid.aws import lambda_client, s3_client, sqs_client
from braid.config import load_config
from braid.sync import hash_uv_lock


@click.group()
@click.option(
    "-v", "--verbose", is_flag=True, default=False, help="Enable verbose logging."
)
@click.pass_context
def cli(ctx: click.Context, verbose: bool) -> None:
    """braid — serverless parallel map with automatic uv dep sync."""
    import logging

    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(message)s")


@cli.command()
@click.option("--region", default=None, help="AWS region (auto-detected if not set).")
@click.option(
    "--bucket", required=True, help="S3 bucket for layers, jobs, and editables."
)
@click.option("--bucket-prefix", default="", help="Key prefix within the bucket.")
@click.option(
    "--arch",
    type=click.Choice(["x86_64", "arm64"]),
    default="x86_64",
    show_default=True,
)
@click.option("--memory-mb", default=512, show_default=True)
@click.option(
    "--lambda-timeout-s",
    default=300,
    show_default=True,
    help="Lambda per-invocation timeout (max 900). Also seeds collect_timeout_s.",
)
@click.option("--role-arn", default=None, help="IAM role ARN. Prompted if not set.")
@click.option("--create-role", is_flag=True, help="Create IAM role automatically.")
@click.option(
    "--extra-bucket",
    "extra_buckets",
    multiple=True,
    help="Additional S3 buckets the worker role needs access to.",
)
@click.option(
    "--backend",
    type=click.Choice(["lambda", "batch"]),
    default="lambda",
    show_default=True,
)
@click.option(
    "--batch-vcpu", default=2.0, show_default=True, help="vCPU per Batch task."
)
@click.option(
    "--batch-memory-mb",
    default=4096,
    show_default=True,
    help="Memory per Batch task (MB).",
)
@click.option(
    "--instance-types",
    default=None,
    help="Comma-separated EC2 instance types for Batch (e.g. 'm8g.large'). Omit for Fargate.",
)
@click.option(
    "--spot",
    is_flag=True,
    default=False,
    help="Use Spot instances (EC2: SPOT, Fargate: FARGATE_SPOT). Requires --instance-types for EC2 Spot.",
)
@click.option(
    "--subnet-ids",
    default=None,
    help="Comma-separated subnet IDs (auto-detected if omitted).",
)
@click.option(
    "--security-group-ids",
    default=None,
    help="Comma-separated security group IDs (auto-detected if omitted).",
)
@click.option(
    "--batch-image",
    default="",
    help="Custom base container image for Batch workers (e.g. 'myrepo/gdal-python:latest'). Defaults to ghcr.io/astral-sh/uv:python{version}-bookworm-slim.",
)
def init(
    region,
    bucket,
    bucket_prefix,
    arch,
    memory_mb,
    lambda_timeout_s,
    role_arn,
    create_role,
    extra_buckets,
    backend,
    batch_vcpu,
    batch_memory_mb,
    instance_types,
    spot,
    subnet_ids,
    security_group_ids,
    batch_image,
) -> None:
    """Initialize braid: create AWS resources and write config."""
    from braid.api import init as api_init

    click.echo("Initializing braid...")

    subnet_list = [s.strip() for s in subnet_ids.split(",")] if subnet_ids else None
    sg_list = (
        [s.strip() for s in security_group_ids.split(",")]
        if security_group_ids
        else None
    )
    instance_list = (
        [s.strip() for s in instance_types.split(",")] if instance_types else None
    )

    if not create_role and not role_arn:
        role_arn = click.prompt("IAM execution role ARN (needs S3 + SQS permissions)")

    config = api_init(
        bucket,
        region=region,
        bucket_prefix=bucket_prefix,
        arch=arch,
        memory_mb=memory_mb,
        lambda_timeout_s=lambda_timeout_s,
        role_arn=role_arn,
        create_role=create_role,
        extra_s3_buckets=list(extra_buckets) or None,
        backend=backend,
        batch_vcpu=batch_vcpu,
        batch_memory_mb=batch_memory_mb,
        batch_instance_types=instance_list,
        spot=spot,
        subnet_ids=subnet_list,
        security_group_ids=sg_list,
        batch_image=batch_image,
    )
    click.echo(
        f"Done. Backend: {config.backend}. Run `braid sync` to build your dep layer."
    )


@cli.command()
@click.option("--force", is_flag=True, help="Rebuild layer even if cached.")
@click.option(
    "--exclude",
    multiple=True,
    metavar="PKG",
    help="Strip package from layer (repeatable).",
)
def sync(force: bool, exclude: tuple[str, ...]) -> None:
    """Build dep layer and deploy worker. Uses uv.lock as the source of truth."""
    from braid.api import sync as api_sync

    lock_hash = hash_uv_lock()
    click.echo(f"uv.lock hash: {lock_hash}")
    result = api_sync(force=force, exclude_packages=list(exclude) or None)
    click.echo(f"Sync complete: {result}")


def _print_job_status(job_id: str, watch: bool) -> None:
    from braid.detach import status as job_status

    def _once():
        st = job_status(job_id)
        click.echo(
            f"  job {job_id}: batch_state={st.batch_state} done={st.done} "
            f"failed={st.failed} pending={st.pending} complete={st.complete}"
        )
        return st

    st = _once()
    if not watch:
        return
    try:
        while not st.complete:
            time.sleep(5)
            st = _once()
    except KeyboardInterrupt:
        pass


@cli.command()
@click.argument("job_id", required=False)
@click.option(
    "--watch", is_flag=True, help="Poll every 5s until the detached job completes."
)
def status(job_id: str | None, watch: bool) -> None:
    """Check config reachability, or show a detached JOB_ID's status."""
    if job_id:
        _print_job_status(_resolve_id("job_id", job_id), watch)
        return

    try:
        config = load_config()
    except Exception as e:
        click.echo(f"  config: ERROR — {e}", err=True)
        return

    click.echo(f"  config: backend={config.backend}, region={config.region}")

    if config.backend == "batch":
        from braid.aws import batch_client

        batch = batch_client.get_client(config.region)
        try:
            resp = batch.describe_job_queues(jobQueues=[config.batch_job_queue])
            queues = resp.get("jobQueues", [])
            if queues:
                state = queues[0].get("state", "?")
                status = queues[0].get("status", "?")
                click.echo(
                    f"  batch queue: OK — {config.batch_job_queue} state={state} status={status}"
                )
            else:
                click.echo(
                    f"  batch queue: NOT FOUND — {config.batch_job_queue}", err=True
                )
        except Exception as e:
            click.echo(f"  batch queue: ERROR — {e}", err=True)
    else:
        lam = lambda_client.get_client(config.region)
        try:
            fn = lam.get_function_configuration(
                FunctionName=config.worker_function_name
            )
            memory = fn["MemorySize"]
            timeout = fn["Timeout"]
            runtime = fn.get("Runtime", "image")
            layers = fn.get("Layers", [])
            click.echo(
                f"  lambda: OK — {runtime}, {memory}MB, {timeout}s, {len(layers)} layer(s)"
            )
        except Exception as e:
            click.echo(f"  lambda: ERROR — {e}", err=True)

    sqs = sqs_client.get_client(config.region)
    try:
        attrs = sqs.get_queue_attributes(
            QueueUrl=config.sqs_queue_url,
            AttributeNames=[
                "ApproximateNumberOfMessages",
                "ApproximateNumberOfMessagesNotVisible",
            ],
        )["Attributes"]
        visible = attrs.get("ApproximateNumberOfMessages", "?")
        inflight = attrs.get("ApproximateNumberOfMessagesNotVisible", "?")
        click.echo(f"  sqs: OK — {visible} messages waiting, {inflight} in-flight")
    except Exception as e:
        click.echo(f"  sqs: ERROR — {e}", err=True)


@cli.command()
def layers() -> None:
    """List cached Lambda layers."""
    from braid.config import layer_cache_path

    cache_path = layer_cache_path()
    if not cache_path.exists():
        click.echo("No cached layers found.")
        return

    cache = json.loads(cache_path.read_text())
    if not cache:
        click.echo("No cached layers found.")
        return

    click.echo(f"{'Cache key':<40} {'ARN'}")
    click.echo("-" * 100)
    for key, arn in cache.items():
        click.echo(f"{key:<40} {arn}")


@cli.command("dry-run")
@click.argument("module_path")
@click.argument("function_name")
def dry_run(module_path: str, function_name: str) -> None:
    """Serialize a function and show payload sizes without invoking."""
    import importlib.util

    spec = importlib.util.spec_from_file_location("_mod", module_path)
    mod = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    fn = getattr(mod, function_name)

    from braid.serialization import serialize_fn

    data = serialize_fn(fn)
    click.echo(f"Function size: {len(data) / 1024:.1f} KB")


@cli.command()
@click.argument("job_id")
def clean(job_id: str) -> None:
    """Delete S3 artifacts for a completed job."""
    job_id = _resolve_id("job_id", job_id)
    config = load_config()
    s3 = s3_client.get_client(config.region)
    key = config.s3_key(f"jobs/{job_id}/")
    s3_client.delete_prefix(s3, config.bucket, key)
    click.echo(f"Deleted {key}")


@cli.command()
@click.argument("batch_job_id")
@click.option("--reason", default="cancelled by user", show_default=True)
def cancel(batch_job_id: str, reason: str) -> None:
    """Terminate a running or pending AWS Batch job (and all array children)."""
    from braid.aws import batch_client

    batch_job_id = _resolve_id("batch_job_id", batch_job_id)
    config = load_config()
    batch = batch_client.get_client(config.region)
    batch.terminate_job(jobId=batch_job_id, reason=reason)
    click.echo(f"Terminated Batch job {batch_job_id}")


@cli.group()
def batch() -> None:
    """Run coiled-batch-style script jobs on AWS Batch."""


@batch.command("run")
@click.argument("script", type=click.Path(exists=True, dir_okay=False, path_type=str))
@click.option("--vcpu", type=float, default=None, help="vCPU for the job.")
@click.option("--memory", type=int, default=None, help="Memory for the job (MB).")
@click.option(
    "--disk", "disk_gib", type=int, default=None, help="Ephemeral storage (GiB)."
)
@click.option(
    "--instance-type",
    "instance_types",
    multiple=True,
    help="EC2 instance type (repeatable). Implies EC2 mode.",
)
@click.option("--image", default=None, help="Container image override.")
@click.option(
    "--env",
    "env_vars",
    multiple=True,
    metavar="KEY=VALUE",
    help="Env var passed through to the script (repeatable).",
)
@click.option("--region", default=None, help="AWS region override.")
def batch_run(
    script: str,
    vcpu: float | None,
    memory: int | None,
    disk_gib: int | None,
    instance_types: tuple[str, ...],
    image: str | None,
    env_vars: tuple[str, ...],
    region: str | None,
) -> None:
    """Submit SCRIPT as a single fire-and-forget AWS Batch job."""
    from pathlib import Path

    from braid.dispatch_batch import _has_pep723_header, submit_script
    from braid.sync import sync_editables, sync_uv_lock

    config = load_config()

    overrides: dict = {}
    if vcpu is not None:
        overrides["batch_vcpu"] = vcpu
    if memory is not None:
        overrides["batch_memory_mb"] = memory
    if disk_gib is not None:
        overrides["batch_ephemeral_storage_gib"] = disk_gib
    if instance_types:
        overrides["batch_instance_types"] = list(instance_types)
    if image is not None:
        overrides["batch_image"] = image
    if region is not None:
        overrides["region"] = region
    if overrides:
        config = config.model_copy(update=overrides)

    if not config.batch_execution_role_arn:
        raise click.ClickException(
            "No Batch execution role configured. Run `braid init` with the batch "
            "backend first (--backend batch --role-arn ... or --create-role)."
        )

    script_path = Path(script)
    if _has_pep723_header(script_path):
        click.echo("PEP 723 header detected — inline deps via `uv run`.")
        requirements_key = ""
        editable_manifest: list[dict] = []
    else:
        click.echo("No inline script metadata — syncing project env from uv.lock.")
        requirements_key = sync_uv_lock(config)
        editable_manifest = sync_editables(config)

    env_passthrough: dict[str, str] = {}
    for pair in env_vars:
        if "=" not in pair:
            raise click.ClickException(f"--env expects KEY=VALUE, got: {pair!r}")
        key, _, value = pair.partition("=")
        env_passthrough[key] = value

    job_id, batch_job_id = submit_script(
        script_path,
        config,
        requirements_s3_key=requirements_key,
        editable_manifest=editable_manifest,
        env_passthrough=env_passthrough or None,
    )

    click.echo(f"Submitted: {script_path.name}")
    click.echo(f"  braid job id: {job_id}")
    click.echo(f"  batch job id: {batch_job_id}")
    click.echo(f"Monitor: braid status {job_id}   |   braid jobs --watch")
    click.echo(f"Cancel:  braid cancel {batch_job_id}")


# Fargate on-demand rates (us-east-1); other regions within ~10% — good enough for estimates
_FARGATE_VCPU_PER_S = 0.04048 / 3600  # $ per vCPU-second
_FARGATE_MEM_GB_PER_S = 0.004445 / 3600  # $ per GB-second


def _fargate_cost_estimate(vcpu: float, memory_mb: int, elapsed_s: float) -> str:
    cost = (
        vcpu * elapsed_s * _FARGATE_VCPU_PER_S
        + (memory_mb / 1024) * elapsed_s * _FARGATE_MEM_GB_PER_S
    )
    return f"~${cost:.3f}"


_STATUS_STYLE = {
    "SUCCEEDED": "green",
    "FAILED": "bold red",
    "RUNNING": "yellow",
    "RUNNABLE": "yellow",
    "STARTING": "yellow",
    "SUBMITTED": "cyan",
    "PENDING": "cyan",
    "UNKNOWN": "dim",
}


def _resolve_id(field: str, prefix: str) -> str:
    """Expand a truncated id (as shown in `braid jobs`) to its full value by
    matching `field` in the jobs cache. Passes through unchanged if there's no
    unique match, so callers still get AWS's own not-found error."""
    from braid.config import jobs_cache_path

    path = jobs_cache_path()
    if not path.exists():
        return prefix
    records = json.loads(path.read_text())
    matches = {r[field] for r in records if r.get(field, "").startswith(prefix)}
    if len(matches) == 1:
        return matches.pop()
    if len(matches) > 1:
        raise click.ClickException(
            f"Ambiguous id prefix {prefix!r} matches {len(matches)} jobs — use more characters."
        )
    return prefix


def _relative_time(iso_str: str) -> str:
    import datetime

    try:
        dt = datetime.datetime.fromisoformat(iso_str)
    except ValueError:
        return iso_str
    secs = (datetime.datetime.now(datetime.UTC) - dt).total_seconds()
    if secs < 60:
        return f"{int(secs)}s ago"
    if secs < 3600:
        return f"{int(secs // 60)}m ago"
    if secs < 86400:
        return f"{int(secs // 3600)}h ago"
    days = int(secs // 86400)
    if days < 7:
        return f"{days}d ago"
    return dt.strftime("%Y-%m-%d")


def _build_jobs_table(records: list[dict], job_details: dict, config, full_ids: bool):
    import datetime

    from rich.table import Table

    is_ec2 = bool(config.batch_instance_types)
    now = datetime.datetime.now(datetime.UTC)

    table = Table(title="Batch jobs")
    table.add_column("SUBMITTED", style="dim", no_wrap=True)
    table.add_column("STATUS", no_wrap=True)
    table.add_column("N", justify="right", no_wrap=True)
    table.add_column("COST EST", style="magenta", no_wrap=True)
    table.add_column("JOB ID", style="cyan", no_wrap=True)

    for r in reversed(records):
        detail = job_details.get(r["batch_job_id"], {})
        status = detail.get("status", "UNKNOWN")
        style = _STATUS_STYLE.get(status, "white")

        if is_ec2:
            cost_str = "EC2/varies"
        else:
            try:
                submitted = datetime.datetime.fromisoformat(r["submitted_at"])
                if status in ("SUCCEEDED", "FAILED"):
                    # Use stoppedAt from AWS if available, else fall back to now
                    stopped_ms = detail.get("stoppedAt")
                    end = (
                        datetime.datetime.fromtimestamp(
                            stopped_ms / 1000, tz=datetime.UTC
                        )
                        if stopped_ms
                        else now
                    )
                else:
                    end = now
                elapsed_s = max(0.0, (end - submitted).total_seconds())
                cost_str = _fargate_cost_estimate(
                    config.batch_vcpu, config.batch_memory_mb, elapsed_s
                )
            except Exception:
                cost_str = "?"

        job_id = r["batch_job_id"] if full_ids else r["batch_job_id"][:8]
        table.add_row(
            _relative_time(r["submitted_at"]),
            f"[{style}]{status}[/{style}]",
            str(r["n_inputs"]),
            cost_str,
            job_id,
        )
    return table


@cli.command()
@click.option(
    "--n", default=10, show_default=True, help="Number of recent jobs to show."
)
@click.option(
    "--watch", is_flag=True, help="Refresh every 10 seconds until interrupted."
)
@click.option("--full", is_flag=True, help="Show full (untruncated) job IDs.")
@click.option("--clear", is_flag=True, help="Clear job history and exit.")
@click.option(
    "--prune",
    is_flag=True,
    help="Drop cached jobs AWS no longer recognizes (shown as UNKNOWN) and exit.",
)
def jobs(n: int, watch: bool, full: bool, clear: bool, prune: bool) -> None:
    """Show recent Batch jobs and their AWS status.

    STATUS shows UNKNOWN when AWS Batch's describe_jobs no longer recognizes
    the cached batch_job_id — e.g. wrong region, or the job fell out of
    Batch's job history retention window. Use --prune to drop those rows.
    """
    from rich.console import Console
    from rich.live import Live

    from braid.aws import batch_client
    from braid.config import jobs_cache_path

    path = jobs_cache_path()

    if clear:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("[]")
        click.echo("Cleared job history.")
        return

    if not path.exists():
        click.echo("No jobs recorded.")
        return

    config = load_config()
    batch = batch_client.get_client(config.region)
    console = Console()

    if prune:
        records = json.loads(path.read_text())
        batch_ids = list(dict.fromkeys(r["batch_job_id"] for r in records))
        known = {j["jobId"] for j in batch_client.describe_jobs(batch, batch_ids)}
        kept = [r for r in records if r["batch_job_id"] in known]
        path.write_text(json.dumps(kept, indent=2))
        click.echo(f"Pruned {len(records) - len(kept)} unknown job(s).")
        return

    def _fetch_table():
        records = json.loads(path.read_text())[-n:] if path.exists() else []
        if not records:
            return None
        batch_ids = list(dict.fromkeys(r["batch_job_id"] for r in records))
        job_details = {
            j["jobId"]: j for j in batch_client.describe_jobs(batch, batch_ids)
        }
        return _build_jobs_table(records, job_details, config, full)

    if not watch:
        table = _fetch_table()
        if table is None:
            click.echo("No jobs recorded.")
            return
        console.print(table)
        return

    with Live(console=console, refresh_per_second=4, screen=False) as live:
        try:
            while True:
                table = _fetch_table()
                live.update(table if table is not None else "No jobs recorded.")
                time.sleep(10)
        except KeyboardInterrupt:
            pass
