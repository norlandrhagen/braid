from __future__ import annotations

import time

import boto3


def get_client(region: str):
    return boto3.client("batch", region_name=region)


def create_compute_environment(
    client,
    name: str,
    subnet_ids: list[str],
    security_group_ids: list[str],
    max_vcpus: int = 256,
    spot: bool = False,
    instance_types: list[str] | None = None,
) -> str:
    use_ec2 = bool(instance_types)
    if use_ec2:
        ce_type = "SPOT" if spot else "EC2"
        resources: dict = {
            "type": ce_type,
            "instanceRole": "ecsInstanceRole",
            "instanceTypes": instance_types,
            "maxvCpus": max_vcpus,
            "subnets": subnet_ids,
            "securityGroupIds": security_group_ids,
        }
    else:
        ce_type = "FARGATE_SPOT" if spot else "FARGATE"
        resources = {
            "type": ce_type,
            "maxvCpus": max_vcpus,
            "subnets": subnet_ids,
            "securityGroupIds": security_group_ids,
        }

    resp = client.create_compute_environment(
        computeEnvironmentName=name,
        type="MANAGED",
        state="ENABLED",
        computeResources=resources,
    )
    return resp["computeEnvironmentArn"]


def wait_for_compute_environment_valid(
    client, name: str, timeout_s: int = 120, poll_interval_s: int = 3
) -> None:
    """Block until a newly created compute environment reports status VALID.

    AWS validates a compute environment (subnets/SG/IAM) asynchronously after
    create_compute_environment returns, so attaching it to a job queue right
    away can race that check and fail with "not valid".
    """
    deadline = time.monotonic() + timeout_s
    while True:
        resp = client.describe_compute_environments(computeEnvironments=[name])
        envs = resp.get("computeEnvironments", [])
        if envs:
            status = envs[0].get("status")
            if status == "VALID":
                return
            if status == "INVALID":
                raise RuntimeError(
                    f"Compute environment {name} is INVALID: "
                    f"{envs[0].get('statusReason', 'unknown reason')}"
                )
        if time.monotonic() > deadline:
            raise TimeoutError(
                f"Compute environment {name} did not become VALID within {timeout_s}s"
            )
        time.sleep(poll_interval_s)


def create_job_queue(client, name: str, compute_env_arn: str) -> str:
    resp = client.create_job_queue(
        jobQueueName=name,
        state="ENABLED",
        priority=1,
        computeEnvironmentOrder=[{"order": 1, "computeEnvironment": compute_env_arn}],
    )
    return resp["jobQueueArn"]


def register_job_definition(
    client,
    name: str,
    image: str,
    command: list[str],
    execution_role_arn: str,
    vcpu: float,
    memory_mb: int,
    ec2: bool = False,
    ephemeral_storage_gib: int = 21,
) -> str:
    container: dict = {
        "image": image,
        "command": command,
        "executionRoleArn": execution_role_arn,
        "jobRoleArn": execution_role_arn,
        "resourceRequirements": [
            {"type": "VCPU", "value": str(vcpu)},
            {"type": "MEMORY", "value": str(memory_mb)},
        ],
        "logConfiguration": {"logDriver": "awslogs"},
    }
    if not ec2:
        container["networkConfiguration"] = {"assignPublicIp": "ENABLED"}
        if ephemeral_storage_gib > 21:
            container["ephemeralStorage"] = {"sizeInGiB": ephemeral_storage_gib}
    kwargs: dict = {
        "jobDefinitionName": name,
        "type": "container",
        "containerProperties": container,
        "platformCapabilities": ["EC2" if ec2 else "FARGATE"],
    }
    resp = client.register_job_definition(**kwargs)
    return resp["jobDefinitionArn"]


def describe_jobs(client, job_ids: list[str]) -> list[dict]:
    results = []
    for i in range(0, len(job_ids), 100):
        resp = client.describe_jobs(jobs=job_ids[i : i + 100])
        results.extend(resp["jobs"])
    return results


def submit_single_job(
    client,
    job_def_name: str,
    queue_name: str,
    job_id: str,
    env_overrides: dict[str, str],
) -> str:
    """Submit a non-array Batch job for a single input."""
    env_list = [{"name": k, "value": v} for k, v in env_overrides.items()]
    resp = client.submit_job(
        jobName=f"braid-{job_id[:8]}",
        jobQueue=queue_name,
        jobDefinition=job_def_name,
        containerOverrides={"environment": env_list},
    )
    return resp["jobId"]


def submit_array_job(
    client,
    job_def_name: str,
    queue_name: str,
    job_id: str,
    array_size: int,
    env_overrides: dict[str, str],
) -> str:
    env_list = [{"name": k, "value": v} for k, v in env_overrides.items()]
    resp = client.submit_job(
        jobName=f"braid-{job_id[:8]}",
        jobQueue=queue_name,
        jobDefinition=job_def_name,
        arrayProperties={"size": array_size},
        containerOverrides={"environment": env_list},
    )
    return resp["jobId"]
