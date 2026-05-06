from __future__ import annotations

import io
import json
import logging
import zipfile
from pathlib import Path

from braid.aws import lambda_client, s3_client, sqs_client
from braid.config import Config, load_config, state_dir, write_config
from braid.sync import get_or_build_layer, hash_uv_lock, sync_editables, sync_uv_lock

log = logging.getLogger(__name__)

# Batch jobs have no timeout limit and cold-start slowly; a Lambda-sized
# collection timeout would expire before the first result lands.
_BATCH_COLLECT_TIMEOUT_S = 3600


def sync(force: bool = False, exclude_packages: list[str] | None = None) -> str:
    """
    Build layer and deploy worker. Returns layer ARN (Lambda) or S3 key (Batch).

    Parameters
    ----------
    force:
        Rebuild even if a layer for the current uv.lock hash is cached.
    exclude_packages:
        Package names to strip from the layer. Overrides config.
    """
    config = load_config()
    if exclude_packages is not None:
        config = config.model_copy(update={"exclude_packages": exclude_packages})

    if config.backend == "batch":
        return _sync_batch(config, force=force)

    arn = get_or_build_layer(config, force=force)
    log.info("Layer ARN: %s", arn)

    manifest = sync_editables(config)
    if manifest:
        log.info("Editable packages synced: %s", [m["name"] for m in manifest])

    env: dict[str, str] = {
        "BRAID_BUCKET": config.bucket,
        "BRAID_BUCKET_PREFIX": config.bucket_prefix,
        "BRAID_SQS_QUEUE_URL": config.sqs_queue_url,
    }
    if manifest:
        env["BRAID_EDITABLES"] = json.dumps(manifest)

    worker_zip = _build_worker_zip()
    lam = lambda_client.get_client(config.region)
    lambda_client.wait_for_function_active(lam, config.worker_function_name)
    lam.update_function_code(
        FunctionName=config.worker_function_name,
        ZipFile=worker_zip,
        Architectures=["arm64" if config.arch == "arm64" else "x86_64"],
    )
    lambda_client.wait_for_function_active(lam, config.worker_function_name)

    lambda_client.update_function_config(
        lam,
        config.worker_function_name,
        layer_arn=arn,
        env_vars=env,
        memory_mb=config.memory_mb,
        timeout_s=config.lambda_timeout_s,
    )
    lambda_client.wait_for_function_active(lam, config.worker_function_name)
    log.info("Worker function updated.")

    lock_hash = hash_uv_lock()
    state_path = state_dir() / "cache" / "applied_state.json"
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps({"lock_hash": lock_hash}))

    return arn


def _sync_batch(config: Config, force: bool = False) -> str:
    from braid.dispatch_batch import _ensure_job_definition

    requirements_key = sync_uv_lock(config)
    log.info("requirements.txt S3 key: %s", requirements_key)

    manifest = sync_editables(config)
    if manifest:
        log.info("Editable packages synced: %s", [m["name"] for m in manifest])

    _ensure_job_definition(config)
    log.info("Batch job definition ready: %s", config.batch_job_definition)
    return requirements_key


def init(
    bucket: str,
    *,
    region: str | None = None,
    bucket_prefix: str = "",
    worker_name: str = "braid-worker",
    arch: str = "x86_64",
    memory_mb: int = 512,
    lambda_timeout_s: int = 300,
    collect_timeout_s: int | None = None,
    role_arn: str | None = None,
    create_role: bool = False,
    extra_s3_buckets: list[str] | None = None,
    backend: str = "lambda",
    batch_job_queue: str = "braid-queue",
    batch_job_definition: str = "braid-job",
    batch_vcpu: float = 2.0,
    batch_memory_mb: int = 4096,
    batch_instance_types: list[str] | None = None,
    spot: bool = False,
    subnet_ids: list[str] | None = None,
    security_group_ids: list[str] | None = None,
    batch_image: str = "",
) -> Config:
    """
    Create AWS resources and write .braid/config.toml. Returns Config.

    Parameters
    ----------
    bucket:
        S3 bucket for layers and job artifacts.
    region:
        AWS region. Falls back to the boto3 session region, then us-west-2.
    backend:
        "lambda" (default) or "batch".
    lambda_timeout_s:
        Lambda per-invocation timeout (max 900). Also sets the SQS visibility
        timeout. Ignored by the Batch backend.
    collect_timeout_s:
        How long fan waits for results. Defaults to lambda_timeout_s on Lambda,
        3600 on Batch.
    role_arn:
        Existing IAM execution role. Required unless create_role is True.
    create_role:
        Create the execution role with minimum permissions. For EC2 Batch,
        also creates ecsInstanceRole if missing.
    batch_instance_types:
        EC2 instance types for the Batch compute environment. Omit for Fargate.
    spot:
        Use Spot pricing (Fargate Spot, or EC2 Spot when instance types are set).
    batch_image:
        Custom base container image. Defaults to the uv slim image.

    Raises
    ------
    ValueError
        If neither role_arn nor create_role=True is provided.
    """
    import boto3

    if region is None:
        region = boto3.session.Session().region_name or "us-west-2"

    sqs = sqs_client.get_client(region)
    queue_resp = sqs.create_queue(
        QueueName="braid-results",
        Attributes={"VisibilityTimeout": str(lambda_timeout_s + 60)},
    )
    queue_url = queue_resp["QueueUrl"]
    log.info("SQS queue: %s", queue_url)

    s3 = s3_client.get_client(region)
    try:
        s3_client.set_lifecycle_rule(s3, bucket, "jobs/", expiry_days=3)
    except Exception as e:
        log.warning("Could not set S3 lifecycle rule: %s", e)

    if backend == "batch":
        use_ec2 = bool(batch_instance_types)
        if create_role:
            execution_role_arn = _create_batch_iam_role(
                bucket, queue_url, region, ec2=use_ec2
            )
        elif role_arn:
            execution_role_arn = role_arn
        else:
            raise ValueError("Provide role_arn or set create_role=True.")

        resolved_subnet_ids, resolved_sg_ids = _resolve_network(
            region, subnet_ids, security_group_ids
        )

        from braid.aws import batch_client

        batch = batch_client.get_client(region)
        ce_arn = batch_client.create_compute_environment(
            batch,
            name=f"{batch_job_queue}-ce",
            subnet_ids=resolved_subnet_ids,
            security_group_ids=resolved_sg_ids,
            instance_types=batch_instance_types or [],
            spot=spot,
        )
        log.info("Batch compute environment: %s", ce_arn)
        batch_client.wait_for_compute_environment_valid(batch, f"{batch_job_queue}-ce")
        batch_client.create_job_queue(
            batch, name=batch_job_queue, compute_env_arn=ce_arn
        )
        log.info("Batch job queue: %s", batch_job_queue)

        config = Config(
            region=region,
            bucket=bucket,
            bucket_prefix=bucket_prefix,
            sqs_queue_url=queue_url,
            arch=arch,
            collect_timeout_s=collect_timeout_s or _BATCH_COLLECT_TIMEOUT_S,
            backend="batch",
            batch_job_queue=batch_job_queue,
            batch_job_definition=batch_job_definition,
            batch_vcpu=batch_vcpu,
            batch_memory_mb=batch_memory_mb,
            batch_execution_role_arn=execution_role_arn,
            batch_subnet_ids=resolved_subnet_ids,
            batch_security_group_ids=resolved_sg_ids,
            batch_instance_types=batch_instance_types or [],
            batch_image=batch_image,
        )
        config_path = write_config(config)
        log.info("Config written: %s", config_path)
        return config

    # Lambda backend
    worker_zip = _build_worker_zip()
    lam = lambda_client.get_client(region)

    if create_role:
        resolved_role = _create_iam_role(
            bucket, queue_url, region, extra_s3_buckets=extra_s3_buckets
        )
    elif role_arn:
        resolved_role = role_arn
    else:
        raise ValueError("Provide role_arn or set create_role=True.")

    try:
        lam.create_function(
            FunctionName=worker_name,
            Runtime="python3.13",
            Role=resolved_role,
            Handler="_worker_lambda.handler",
            Code={"ZipFile": worker_zip},
            Timeout=lambda_timeout_s,
            MemorySize=memory_mb,
            Architectures=["arm64" if arch == "arm64" else "x86_64"],
            Environment={
                "Variables": {
                    "BRAID_BUCKET": bucket,
                    "BRAID_BUCKET_PREFIX": bucket_prefix,
                    "BRAID_SQS_QUEUE_URL": queue_url,
                }
            },
        )
        log.info("Lambda function created: %s", worker_name)
    except lam.exceptions.ResourceConflictException:
        log.info("Lambda function already exists: %s", worker_name)
        lambda_client.update_function_config(
            lam,
            worker_name,
            layer_arn=None,
            memory_mb=memory_mb,
            timeout_s=lambda_timeout_s,
        )

    config = Config(
        region=region,
        bucket=bucket,
        bucket_prefix=bucket_prefix,
        sqs_queue_url=queue_url,
        worker_function_name=worker_name,
        arch=arch,
        memory_mb=memory_mb,
        lambda_timeout_s=lambda_timeout_s,
        collect_timeout_s=collect_timeout_s or lambda_timeout_s,
    )
    config_path = write_config(config)
    log.info("Config written: %s", config_path)

    return config


def _build_worker_zip() -> bytes:
    pkg = Path(__file__).parent
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write(pkg / "_worker_lambda.py", "_worker_lambda.py")
        zf.write(pkg / "_worker_common.py", "_worker_common.py")
    return buf.getvalue()


def _create_worker_role(
    role_name: str,
    *,
    policy_name: str,
    principal_service: str,
    managed_policy_arn: str,
    bucket: str,
    queue_url: str,
    region: str,
    sqs_actions: list[str],
    extra_statements: list[dict] | None = None,
    extra_s3_buckets: list[str] | None = None,
) -> str:
    import time

    import boto3

    iam = boto3.client("iam")

    trust = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": principal_service},
                    "Action": "sts:AssumeRole",
                }
            ],
        }
    )

    try:
        resp = iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=trust)
        role_arn = resp["Role"]["Arn"]
        log.info("IAM role created: %s", role_arn)
    except iam.exceptions.EntityAlreadyExistsException:
        role_arn = iam.get_role(RoleName=role_name)["Role"]["Arn"]
        log.info("IAM role already exists: %s", role_arn)

    iam.attach_role_policy(RoleName=role_name, PolicyArn=managed_policy_arn)

    parts = queue_url.rstrip("/").split("/")
    account_id, queue_name = parts[-2], parts[-1]
    sqs_arn = f"arn:aws:sqs:{region}:{account_id}:{queue_name}"

    s3_resources = [f"arn:aws:s3:::{bucket}", f"arn:aws:s3:::{bucket}/*"]
    for b in extra_s3_buckets or []:
        s3_resources += [f"arn:aws:s3:::{b}", f"arn:aws:s3:::{b}/*"]

    policy = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "s3:GetObject",
                        "s3:PutObject",
                        "s3:DeleteObject",
                        "s3:ListBucket",
                    ],
                    "Resource": s3_resources,
                },
                {
                    "Effect": "Allow",
                    "Action": sqs_actions,
                    "Resource": sqs_arn,
                },
                *(extra_statements or []),
            ],
        }
    )
    iam.put_role_policy(
        RoleName=role_name, PolicyName=policy_name, PolicyDocument=policy
    )
    log.info("IAM policy attached (S3: %s, SQS: %s)", bucket, queue_name)

    log.info("Waiting for IAM role to propagate...")
    time.sleep(10)

    return role_arn


def _create_iam_role(
    bucket: str, queue_url: str, region: str, extra_s3_buckets: list[str] | None = None
) -> str:
    return _create_worker_role(
        "braid-worker-role",
        policy_name="braid-worker-policy",
        principal_service="lambda.amazonaws.com",
        managed_policy_arn="arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
        bucket=bucket,
        queue_url=queue_url,
        region=region,
        sqs_actions=[
            "sqs:SendMessage",
            "sqs:ReceiveMessage",
            "sqs:DeleteMessage",
            "sqs:GetQueueAttributes",
        ],
        extra_s3_buckets=extra_s3_buckets,
    )


def _ensure_ecs_instance_role() -> str:
    """Create ecsInstanceRole if it doesn't exist. Required for EC2 Batch compute environments."""
    import boto3

    iam = boto3.client("iam")
    role_name = "ecsInstanceRole"
    try:
        arn = iam.get_role(RoleName=role_name)["Role"]["Arn"]
        log.info("ecsInstanceRole already exists: %s", arn)
        return arn
    except iam.exceptions.NoSuchEntityException:
        pass

    trust = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "ec2.amazonaws.com"},
                    "Action": "sts:AssumeRole",
                }
            ],
        }
    )
    resp = iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=trust)
    arn = resp["Role"]["Arn"]
    iam.attach_role_policy(
        RoleName=role_name,
        PolicyArn="arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceforEC2Role",
    )
    # ecsInstanceRole must be an instance profile too
    try:
        iam.create_instance_profile(InstanceProfileName=role_name)
    except iam.exceptions.EntityAlreadyExistsException:
        pass
    iam.add_role_to_instance_profile(InstanceProfileName=role_name, RoleName=role_name)
    log.info("Created ecsInstanceRole: %s", arn)
    return arn


def _create_batch_iam_role(
    bucket: str, queue_url: str, region: str, ec2: bool = False
) -> str:
    if ec2:
        _ensure_ecs_instance_role()
    return _create_worker_role(
        "braid-batch-execution-role",
        policy_name="braid-batch-policy",
        principal_service="ecs-tasks.amazonaws.com",
        managed_policy_arn="arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy",
        bucket=bucket,
        queue_url=queue_url,
        region=region,
        sqs_actions=["sqs:SendMessage"],
        extra_statements=[
            {
                "Effect": "Allow",
                "Action": [
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                ],
                "Resource": "*",
            }
        ],
    )


def _resolve_network(
    region: str,
    subnet_ids: list[str] | None,
    security_group_ids: list[str] | None,
) -> tuple[list[str], list[str]]:
    if subnet_ids and security_group_ids:
        return subnet_ids, security_group_ids

    import boto3

    ec2 = boto3.client("ec2", region_name=region)

    vpc_id: str | None = None

    if not subnet_ids:
        vpcs = ec2.describe_vpcs(Filters=[{"Name": "isDefault", "Values": ["true"]}])[
            "Vpcs"
        ]
        if not vpcs:
            raise ValueError(
                "No default VPC found. Provide subnet_ids and security_group_ids explicitly."
            )
        vpc_id = vpcs[0]["VpcId"]
        subnets = ec2.describe_subnets(
            Filters=[{"Name": "vpc-id", "Values": [vpc_id]}]
        )["Subnets"]
        subnet_ids = [s["SubnetId"] for s in subnets]
        log.info("Auto-detected subnets: %s", subnet_ids)

    if not security_group_ids:
        # Scope to the subnets' VPC: an account can have a "default" SG in more
        # than one VPC, and an unscoped lookup can return one that doesn't match
        # the subnets above, producing a Batch compute environment AWS rejects.
        if vpc_id is None:
            vpc_id = ec2.describe_subnets(SubnetIds=subnet_ids[:1])["Subnets"][0][
                "VpcId"
            ]
        sgs = ec2.describe_security_groups(
            Filters=[
                {"Name": "group-name", "Values": ["default"]},
                {"Name": "vpc-id", "Values": [vpc_id]},
            ]
        )["SecurityGroups"]
        if not sgs:
            raise ValueError(
                f"No default security group found in VPC {vpc_id}. "
                "Provide security_group_ids explicitly."
            )
        security_group_ids = [sg["GroupId"] for sg in sgs[:1]]
        log.info("Auto-detected security group: %s", security_group_ids)

    return subnet_ids, security_group_ids
