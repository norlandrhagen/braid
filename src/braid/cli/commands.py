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


def _print_job_status(job_id: str, watch: bool):
    from braid.detach import status as job_status

    def _once():
        st = job_status(job_id)
        click.echo(
            f"  job {job_id}: batch_state={st.batch_state} done={st.done} "
            f"failed={st.failed} pending={st.pending} complete={st.complete}"
        )
        return st

    st = _once()
    # Batch state ends the wait too: a killed job (timeout, OOM) never writes all results.
    while watch and not st.complete and st.batch_state not in ("SUCCEEDED", "FAILED"):
        time.sleep(5)
        st = _once()
    return st


@cli.command()
def status() -> None:
    """Check backend resources and SQS are reachable."""
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
    job_id = _find_job(job_id)["job_id"]
    config = load_config()
    s3 = s3_client.get_client(config.region)
    key = config.s3_key(f"jobs/{job_id}/")
    s3_client.delete_prefix(s3, config.bucket, key)
    click.echo(f"Deleted {key}")


def _find_job(prefix: str) -> dict:
    """Job whose braid or Batch job id starts with `prefix`: jobs cache first, then
    AWS. With no match, echoes `prefix` as both ids so AWS/S3 give their own error."""
    from braid.config import jobs_cache_path

    path = jobs_cache_path()
    records = json.loads(path.read_text()) if path.exists() else []
    matches = {
        r["job_id"]: r
        for r in records
        if r.get("job_id", "").startswith(prefix)
        or r.get("batch_job_id", "").startswith(prefix)
    }
    if not matches:
        # Not submitted from here: look for it on AWS by its braid-<job_id> name.
        from braid.aws import batch_client

        config = load_config()
        batch = batch_client.get_client(config.region)
        matches = {
            j["jobName"].removeprefix("braid-"): {
                "job_id": j["jobName"].removeprefix("braid-"),
                "batch_job_id": j["jobId"],
            }
            for j in batch_client.list_braid_jobs(batch, config.batch_job_queue)
            if j["jobName"].removeprefix("braid-").startswith(prefix)
            or j["jobId"].startswith(prefix)
        }
    if len(matches) > 1:
        raise click.ClickException(
            f"Ambiguous id prefix {prefix!r} matches {len(matches)} jobs — use more characters."
        )
    if matches:
        return matches.popitem()[1]
    return {"job_id": prefix, "batch_job_id": prefix}


def _print_new_log_events(logs, stream: str, token: str | None) -> str | None:
    """Print events after `token`; return the token to resume from."""
    while True:
        kwargs = {
            "logGroupName": "/aws/batch/job",
            "logStreamName": stream,
            "startFromHead": True,
        }
        if token:
            kwargs["nextToken"] = token
        try:
            resp = logs.get_log_events(**kwargs)
        except logs.exceptions.ResourceNotFoundException:
            return token  # stream not created yet
        for event in resp["events"]:
            if not event["message"].startswith("BRAID_METRICS "):
                click.echo(event["message"])
        if resp["nextForwardToken"] == token:
            return token
        token = resp["nextForwardToken"]


def _stream_logs(batch_job_id: str, region: str, follow: bool) -> dict:
    """Print the job's logs, polling until it finishes if `follow`. Returns the job."""
    import boto3

    from braid.aws import batch_client

    batch = batch_client.get_client(region)
    logs = boto3.client("logs", region_name=region)
    token = None
    while True:
        jobs = batch_client.describe_jobs(batch, [batch_job_id])
        if not jobs:
            raise click.ClickException(
                f"Batch job {batch_job_id} not found (Batch forgets jobs after ~7 days)."
            )
        job = jobs[0]
        stream = job.get("container", {}).get("logStreamName")
        if stream:
            token = _print_new_log_events(logs, stream, token)
        elif "arrayProperties" in job and "index" not in job["arrayProperties"]:
            raise click.ClickException("Array job has no logs itself; pass --index N.")
        if job["status"] in ("SUCCEEDED", "FAILED") or not follow:
            return job
        time.sleep(5)


@cli.group()
def batch() -> None:
    """Run coiled-batch-style script jobs on AWS Batch."""


def _script_header_opts(ctx: click.Context, script: str) -> dict:
    """Parse `# BRAID key=value ...` lines into batch_run params, skipping flags set on the CLI."""
    import shlex
    from pathlib import Path

    opts = {
        o.lstrip("-"): p
        for p in ctx.command.params
        if isinstance(p, click.Option)
        for o in p.opts
    }
    raw: dict[str, list[str]] = {}
    for line in Path(script).read_text().splitlines():
        if not line.startswith("# BRAID "):
            continue
        for tok in shlex.split(line[len("# BRAID ") :]):
            key, sep, value = tok.partition("=")
            if not sep or key not in opts:
                raise click.ClickException(f"Invalid # BRAID option: {tok!r}")
            raw.setdefault(opts[key].name, []).append(value)

    out = {}
    for name, values in raw.items():
        if ctx.get_parameter_source(name) == click.core.ParameterSource.COMMANDLINE:
            continue
        param = next(p for p in ctx.command.params if p.name == name)
        out[name] = param.type_cast_value(ctx, values if param.multiple else values[-1])
    return out


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
    "--timeout",
    "timeout_s",
    type=click.IntRange(min=60),
    default=None,
    help="Kill the job after this many seconds (default: config batch_timeout_s).",
)
@click.option(
    "--env",
    "env_vars",
    multiple=True,
    metavar="KEY=VALUE",
    help="Env var passed through to the script (repeatable).",
)
@click.option("--region", default=None, help="AWS region override.")
@click.option(
    "--wait", is_flag=True, help="Stream logs until the job ends; exit 1 if it fails."
)
def batch_run(
    script: str,
    vcpu: float | None,
    memory: int | None,
    disk_gib: int | None,
    instance_types: tuple[str, ...],
    image: str | None,
    timeout_s: int | None,
    env_vars: tuple[str, ...],
    region: str | None,
    wait: bool,
) -> None:
    """Submit SCRIPT as a single AWS Batch job (fire-and-forget unless --wait).

    Options can also be set in the script with lines like
    `# BRAID memory=8192 vcpu=4`; CLI flags take precedence.
    """
    from pathlib import Path

    from braid.dispatch_batch import _has_pep723_header, submit_script
    from braid.sync import sync_editables, sync_uv_lock

    header = _script_header_opts(click.get_current_context(), script)
    vcpu = header.get("vcpu", vcpu)
    memory = header.get("memory", memory)
    disk_gib = header.get("disk_gib", disk_gib)
    instance_types = header.get("instance_types", instance_types)
    image = header.get("image", image)
    timeout_s = header.get("timeout_s", timeout_s)
    env_vars = header.get("env_vars", env_vars)
    region = header.get("region", region)

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
    if timeout_s is not None:
        overrides["batch_timeout_s"] = timeout_s
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
        project_key = ""
        editable_manifest: list[dict] = []
    else:
        click.echo("No inline script metadata — syncing project env from uv.lock.")
        project_key = sync_uv_lock(config)
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
        project_s3_key=project_key,
        editable_manifest=editable_manifest,
        env_passthrough=env_passthrough or None,
    )

    click.echo(f"Submitted {script_path.name} as job {job_id}")
    click.echo(f"  braid batch logs -f {job_id}   |   braid batch cancel {job_id}")

    if wait:
        try:
            job = _stream_logs(batch_job_id, config.region, follow=True)
        except KeyboardInterrupt:
            click.echo(f"\nStopped waiting; job still running. Resume: braid batch logs -f {job_id}")
            return
        if job["status"] != "SUCCEEDED":
            raise click.ClickException(
                f"Job {job['status']}: {job.get('statusReason', 'no reason given')}"
            )


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


def _iso_from_ms(ms: int) -> str:
    import datetime

    return datetime.datetime.fromtimestamp(ms / 1000, datetime.UTC).isoformat()


def _build_jobs_table(summaries: list[dict], names: dict[str, str], config):
    import datetime

    from rich.table import Table

    from braid.detach import _s3_idx_sets

    is_ec2 = bool(config.batch_instance_types)
    now_ms = datetime.datetime.now(datetime.UTC).timestamp() * 1000

    table = Table(title="Batch jobs")
    table.add_column("JOB ID", style="cyan", no_wrap=True)
    table.add_column("NAME", no_wrap=True)
    table.add_column("SUBMITTED", style="dim", no_wrap=True)
    table.add_column("FINISHED", style="dim", no_wrap=True)
    table.add_column("STATUS", no_wrap=True)
    table.add_column("TASKS", justify="right", no_wrap=True)
    table.add_column("COST EST", style="magenta", no_wrap=True)

    for j in summaries:
        status = j["status"]
        style = _STATUS_STYLE.get(status, "white")
        if is_ec2:
            cost_str = "EC2/varies"
        else:
            elapsed_s = max(0.0, (j.get("stoppedAt") or now_ms) - j["createdAt"]) / 1000
            cost_str = _fargate_cost_estimate(
                config.batch_vcpu, config.batch_memory_mb, elapsed_s
            )
        n = j.get("arrayProperties", {}).get("size", 1)
        job_id = j["jobName"].removeprefix("braid-")
        tasks = str(n)
        if status not in ("SUCCEEDED", "FAILED"):
            # Live progress from S3 markers; skipped for finished jobs (may be cleaned up).
            done, failed = _s3_idx_sets(
                config.bucket, config.s3_key(f"jobs/{job_id}/"), config.region
            )
            tasks = f"{len(done)}/{n}" + (f" ({len(failed)} failed)" if failed else "")
        table.add_row(
            job_id,
            names.get(j["jobId"], "(other)"),
            _relative_time(_iso_from_ms(j["createdAt"])),
            _relative_time(_iso_from_ms(j["stoppedAt"])) if j.get("stoppedAt") else "-",
            f"[{style}]{status}[/{style}]",
            tasks,
            cost_str,
        )
    return table


@batch.command("list")
@click.option(
    "--n", default=10, show_default=True, help="Number of recent jobs to show."
)
@click.option(
    "--watch", is_flag=True, help="Refresh every 10 seconds until interrupted."
)
def batch_list(n: int, watch: bool) -> None:
    """Show recent braid jobs on every braid* Batch queue in the configured region.

    Lists from AWS, so jobs from other machines or projects show up too (NAME
    "(other)"). AWS keeps finished jobs ~7 days; active jobs always show.
    """
    from rich.console import Console
    from rich.live import Live

    from braid.aws import batch_client
    from braid.config import jobs_cache_path

    config = load_config()
    batch = batch_client.get_client(config.region)
    console = Console()

    def _fetch_table():
        summaries = batch_client.list_braid_jobs(batch, config.batch_job_queue)[:n]
        if not summaries:
            return "No braid jobs on AWS in the last ~7 days."
        path = jobs_cache_path()
        records = json.loads(path.read_text()) if path.exists() else []
        names = {r["batch_job_id"]: r.get("name") or "" for r in records}
        return _build_jobs_table(summaries, names, config)

    if not watch:
        console.print(_fetch_table())
        return

    with Live(console=console, refresh_per_second=4, screen=False) as live:
        try:
            while True:
                live.update(_fetch_table())
                time.sleep(10)
        except KeyboardInterrupt:
            pass


@batch.command("status")
@click.argument("job_id")
def batch_status(job_id: str) -> None:
    """Show a job's AWS state and done/failed/pending task counts."""
    _print_job_status(_find_job(job_id)["job_id"], watch=False)


@batch.command("wait")
@click.argument("job_id")
def batch_wait(job_id: str) -> None:
    """Poll until the job finishes; exit 1 if any task failed."""
    job_id = _find_job(job_id)["job_id"]
    st = _print_job_status(job_id, watch=True)
    if st.failed or not st.complete:
        raise click.ClickException(f"Job {job_id} failed ({st.batch_state}).")


@batch.command("logs")
@click.argument("job_id")
@click.option("--index", type=int, default=None, help="Array child index.")
@click.option("--follow", "-f", is_flag=True, help="Keep printing until the job ends.")
def batch_logs(job_id: str, index: int | None, follow: bool) -> None:
    """Print a job's output from CloudWatch."""
    batch_job_id = _find_job(job_id)["batch_job_id"]
    if index is not None:
        batch_job_id = f"{batch_job_id}:{index}"
    _stream_logs(batch_job_id, load_config().region, follow)


@cli.command()
@click.option("--port", default=8787, show_default=True)
@click.option("--no-browser", is_flag=True, help="Don't open a browser tab.")
def dashboard(port: int, no_browser: bool) -> None:
    """Local web dashboard: Batch jobs with live CPU, memory, network, disk."""
    from braid.dashboard import serve

    serve(port, open_browser=not no_browser)


@batch.command("cancel")
@click.argument("job_id")
@click.option("--reason", default="cancelled by user", show_default=True)
def batch_cancel(job_id: str, reason: str) -> None:
    """Terminate a running or pending job (and all array children)."""
    from braid.aws import batch_client

    batch_job_id = _find_job(job_id)["batch_job_id"]
    config = load_config()
    batch_client.get_client(config.region).terminate_job(jobId=batch_job_id, reason=reason)
    click.echo(f"Terminated {job_id} (Batch job {batch_job_id})")
