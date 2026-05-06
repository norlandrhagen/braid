from __future__ import annotations

import json
import time

import boto3
from botocore.config import Config as BotocoreConfig
from botocore.exceptions import ClientError

from braid.errors import ThrottleError

_MAX_THROTTLE_RETRIES = 5
_THROTTLE_BASE_DELAY = 0.5  # seconds


def get_client(region: str, max_pool_connections: int = 50):
    return boto3.client(
        "lambda",
        region_name=region,
        config=BotocoreConfig(max_pool_connections=max_pool_connections),
    )


def invoke_async(
    client, function_name: str, payload: dict, retries: int = _MAX_THROTTLE_RETRIES
) -> None:
    """Fire-and-forget async Lambda invocation with throttle backoff."""
    data = json.dumps(payload).encode()
    for attempt in range(retries + 1):
        try:
            client.invoke(
                FunctionName=function_name,
                InvocationType="Event",
                Payload=data,
            )
            return
        except ClientError as e:
            code = e.response["Error"]["Code"]
            if code == "TooManyRequestsException" and attempt < retries:
                time.sleep(_THROTTLE_BASE_DELAY * (2**attempt))
                continue
            if code == "TooManyRequestsException":
                raise ThrottleError(f"Lambda throttled after {retries} retries") from e
            raise


def update_function_config(
    client,
    function_name: str,
    *,
    layer_arn: str | None,
    env_vars: dict[str, str] | None = None,
    memory_mb: int,
    timeout_s: int,
) -> None:
    """Single update_function_configuration call covering layer, env, memory, and timeout."""
    resp = client.get_function_configuration(FunctionName=function_name)
    existing_layers = [layer["Arn"] for layer in resp.get("Layers", [])]
    filtered = [a for a in existing_layers if ":braid-deps:" not in a]
    existing_env = resp.get("Environment", {}).get("Variables", {})
    new_layers = [*filtered, layer_arn] if layer_arn is not None else existing_layers
    client.update_function_configuration(
        FunctionName=function_name,
        Layers=new_layers,
        Environment={"Variables": {**existing_env, **(env_vars or {})}},
        MemorySize=memory_mb,
        Timeout=timeout_s,
    )


def wait_for_function_active(client, function_name: str, timeout_s: int = 60) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        resp = client.get_function_configuration(FunctionName=function_name)
        state = resp.get("LastUpdateStatus", "Successful")
        if state == "Successful":
            return
        if state == "Failed":
            raise RuntimeError(
                f"Lambda update failed: {resp.get('LastUpdateStatusReason')}"
            )
        time.sleep(2)
    raise TimeoutError(f"Lambda {function_name} not active after {timeout_s}s")
