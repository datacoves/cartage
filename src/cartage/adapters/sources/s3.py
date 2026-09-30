# src/cartage/adapters/sources/s3.py
"""CSV objects from S3, optionally skipping objects already processed (by ETag). Also a state backend."""
from __future__ import annotations

import fnmatch
import io
from pathlib import Path
from typing import Iterator

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from cartage.adapters.sources.filesystem import FORMATS, iter_csv_batches
from cartage.core import CartageError, FatalRunError


def make_client(config: dict):
    kwargs = {"region_name": config.get("region"), "endpoint_url": config.get("endpoint_url"),
              "aws_access_key_id": config.get("access_key_id"), "aws_secret_access_key": config.get("secret_access_key")}
    return boto3.client("s3", **{k: v for k, v in kwargs.items() if v})


def _join(*parts: str) -> str:
    return "/".join(p.strip("/") for p in parts if p and p.strip("/"))


def _bucket(config: dict) -> str:
    if not config.get("bucket"):
        raise CartageError("An s3 connection needs 'bucket'")
    return config["bucket"]


class S3StateBackend:
    def __init__(self, client, bucket: str, prefix: str):
        self.client, self.bucket, self.prefix = client, bucket, prefix

    def get(self, key: str) -> bytes | None:
        try:
            return self.client.get_object(Bucket=self.bucket, Key=_join(self.prefix, key))["Body"].read()
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") in ("NoSuchKey", "404"):
                return None
            raise FatalRunError(f"Cannot read state from s3://{self.bucket}/{_join(self.prefix, key)}: {e}") from e
        except BotoCoreError as e:
            raise FatalRunError(f"Cannot read state from s3://{self.bucket}/{_join(self.prefix, key)}: {e}") from e

    def put(self, key: str, data: bytes) -> None:
        try:
            self.client.put_object(Bucket=self.bucket, Key=_join(self.prefix, key), Body=data)
        except (BotoCoreError, ClientError) as e:
            raise FatalRunError(f"Cannot write state to s3://{self.bucket}/{_join(self.prefix, key)}: {e}") from e

    def delete(self, key: str) -> None:
        try:
            self.client.delete_object(Bucket=self.bucket, Key=_join(self.prefix, key))
        except (BotoCoreError, ClientError) as e:
            raise FatalRunError(f"Cannot delete state at s3://{self.bucket}/{_join(self.prefix, key)}: {e}") from e


class S3Source:
    def __init__(self, config: dict, options: dict, root: Path):
        self.bucket = _bucket(config)
        self.prefix = config.get("prefix", "").strip("/")
        if not options.get("path"):
            raise CartageError("An s3 source needs 'path'", hint="A glob relative to the connection prefix, e.g. materials/*.csv")
        self.pattern = options["path"]
        fmt = options.get("format", "csv")
        if fmt not in FORMATS:
            raise CartageError(f"Unsupported format '{fmt}'", hint=f"Supported: {', '.join(FORMATS)}")
        self.incremental = bool(options.get("incremental", False))
        self.batch_size = int(options.get("batch_size", 100))
        self.client = make_client(config)

    @classmethod
    def check_connection(cls, config: dict, root: Path) -> str:
        bucket = _bucket(config)
        try:
            make_client(config).head_bucket(Bucket=bucket)
        except (BotoCoreError, ClientError) as e:
            raise FatalRunError(f"Cannot access s3://{bucket}: {e}") from e
        return f"s3://{bucket} reachable"

    @classmethod
    def state_backend(cls, config: dict, prefix: str, root: Path) -> S3StateBackend:
        return S3StateBackend(make_client(config), _bucket(config), _join(config.get("prefix", ""), prefix))

    def _objects(self) -> list[tuple[str, str, str]]:
        static = self.pattern.split("*")[0].split("?")[0].split("[")[0]
        found = []
        try:
            for page in self.client.get_paginator("list_objects_v2").paginate(Bucket=self.bucket, Prefix=_join(self.prefix, static)):
                for obj in page.get("Contents", []):
                    rel = obj["Key"][len(self.prefix) + 1:] if self.prefix else obj["Key"]
                    if fnmatch.fnmatchcase(rel, self.pattern):
                        found.append((rel, obj["ETag"].strip('"'), obj["Key"]))
        except (BotoCoreError, ClientError) as e:
            raise FatalRunError(f"Cannot list s3://{self.bucket}/{self.prefix}: {e}") from e
        return sorted(found)

    def plan_files(self, state: dict) -> list[tuple[str, bool]]:
        seen = state.get("files", {})
        return [(rel, not (self.incremental and seen.get(rel) == etag)) for rel, etag, _ in self._objects()]

    def read(self, state: dict) -> Iterator[list[dict]]:
        seen = state.setdefault("files", {})
        for rel, etag, key in self._objects():
            if self.incremental and seen.get(rel) == etag:
                continue
            try:
                body = self.client.get_object(Bucket=self.bucket, Key=key)["Body"].read()
            except (BotoCoreError, ClientError) as e:
                raise FatalRunError(f"Cannot read s3://{self.bucket}/{key}: {e}") from e
            try:
                text = body.decode("utf-8-sig")
            except UnicodeDecodeError as e:
                raise FatalRunError(f"{rel} is not valid UTF-8 ({e.reason} at byte {e.start})", hint="Save the file as UTF-8") from e
            # ponytail: whole object in memory; stream through TextIOWrapper if extracts outgrow RAM.
            yield from iter_csv_batches(io.StringIO(text, newline=""), rel, self.batch_size)
            if self.incremental:
                seen[rel] = etag
