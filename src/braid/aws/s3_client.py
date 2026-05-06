from __future__ import annotations

from pathlib import Path

import boto3
from botocore.config import Config as BotoConfig


def get_client(region: str):
    # Force SigV4: legacy SigV2 presigned URLs sign Content-Type into the
    # request, and clients (urllib, curl -d) auto-set a default Content-Type
    # that wasn't part of the signature, so PUTs to a SigV2 presigned URL
    # fail with SignatureDoesNotMatch. SigV4 only signs headers explicitly
    # requested at presign time, so this is safe by default.
    return boto3.client(
        "s3", region_name=region, config=BotoConfig(signature_version="s3v4")
    )


def upload_bytes(client, bucket: str, key: str, data: bytes) -> None:
    client.put_object(Bucket=bucket, Key=key, Body=data)


def download_bytes(client, bucket: str, key: str) -> bytes:
    resp = client.get_object(Bucket=bucket, Key=key)
    return resp["Body"].read()


def upload_file(client, bucket: str, key: str, path: Path) -> None:
    client.upload_file(str(path), bucket, key)


def key_exists(client, bucket: str, key: str) -> bool:
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except client.exceptions.ClientError:
        return False


def delete_prefix(client, bucket: str, prefix: str) -> None:
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        objects = page.get("Contents", [])
        if objects:
            client.delete_objects(
                Bucket=bucket,
                Delete={"Objects": [{"Key": o["Key"]} for o in objects]},
            )


def set_lifecycle_rule(client, bucket: str, prefix: str, expiry_days: int) -> None:
    client.put_bucket_lifecycle_configuration(
        Bucket=bucket,
        LifecycleConfiguration={
            "Rules": [
                {
                    "ID": f"braid-expire-{prefix.strip('/')}",
                    "Filter": {"Prefix": prefix},
                    "Status": "Enabled",
                    "Expiration": {"Days": expiry_days},
                }
            ]
        },
    )
