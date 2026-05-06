from __future__ import annotations

import json

import boto3


def get_client(region: str):
    return boto3.client("sqs", region_name=region)


def receive_messages(
    client, queue_url: str, max_messages: int = 10, wait_seconds: int = 20
) -> list[dict]:
    resp = client.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=min(max_messages, 10),
        WaitTimeSeconds=wait_seconds,
        AttributeNames=["All"],
    )
    return resp.get("Messages", [])


def delete_message(client, queue_url: str, receipt_handle: str) -> None:
    client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)


def parse_body(message: dict) -> dict:
    return json.loads(message["Body"])
