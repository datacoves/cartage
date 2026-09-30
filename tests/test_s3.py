# tests/test_s3.py
import json
from pathlib import Path

import boto3
import pytest
from moto import mock_aws
from typer.testing import CliRunner

from cartage.adapters.sources.s3 import S3Source, S3StateBackend
from cartage.cli import app
from cartage.core import FatalRunError

CONFIG = {"bucket": "demo", "region": "us-east-1", "prefix": "in"}


@pytest.fixture
def s3(monkeypatch):
    for key in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
        monkeypatch.setenv(key, "testing")
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="demo")
        yield client


def test_reads_matching_keys_and_skips_processed(s3):
    s3.put_object(Bucket="demo", Key="in/materials/a.csv", Body=b"material\n1\n2\n")
    s3.put_object(Bucket="demo", Key="in/other/b.csv", Body=b"material\n3\n")
    src, state = S3Source(CONFIG, {"path": "materials/*.csv", "incremental": True}, Path(".")), {}
    batches = list(src.read(state))
    assert batches == [[{"material": "1", "_source": "materials/a.csv:2"}, {"material": "2", "_source": "materials/a.csv:3"}]]
    assert list(src.read(state)) == []
    s3.put_object(Bucket="demo", Key="in/materials/a.csv", Body=b"material\n1\n2\n4\n")
    assert sum(map(len, src.read(state))) == 3


def test_missing_bucket_is_fatal(s3):
    src = S3Source({**CONFIG, "bucket": "nope"}, {"path": "*.csv"}, Path("."))
    with pytest.raises(FatalRunError, match="s3://nope"):
        list(src.read({}))
    with pytest.raises(FatalRunError):
        S3Source.check_connection({**CONFIG, "bucket": "nope"}, Path("."))
    assert "s3://demo" in S3Source.check_connection(CONFIG, Path("."))


def test_state_backend(s3):
    backend = S3Source.state_backend(CONFIG, "state", Path("."))
    assert isinstance(backend, S3StateBackend)
    assert backend.get("p/dev/state.json") is None
    backend.put("p/dev/state.json", b"{}")
    assert s3.get_object(Bucket="demo", Key="in/state/p/dev/state.json")["Body"].read() == b"{}"
    assert backend.get("p/dev/state.json") == b"{}"
    backend.delete("p/dev/state.json")
    assert backend.get("p/dev/state.json") is None


S3_CONNECTION = """
  materials_s3:
    type: s3
    envs:
      dev:
        bucket: demo
        region: us-east-1
        access_key_id: ${secret:aws.access_key_id}
        secret_access_key: ${secret:aws.secret_access_key}
"""


def test_run_from_s3_with_state_on_s3(s3, project, monkeypatch):
    monkeypatch.setenv("CARTAGE_SECRET__AWS__ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("CARTAGE_SECRET__AWS__SECRET_ACCESS_KEY", "testing")
    s3.put_object(Bucket="demo", Key="materials/materials.csv", Body=(project / "data/materials/materials.csv").read_bytes())
    with open(project / "connections.yaml", "a") as f:
        f.write(S3_CONNECTION)
    pipeline = project / "pipelines/materials.yaml"
    pipeline.write_text(pipeline.read_text().replace("connection: local_files", "connection: materials_s3"))
    settings = project / "cartage.yaml"
    settings.write_text(settings.read_text().replace("dev: { path: .cartage/state }", "dev: { connection: materials_s3, prefix: state }"))

    result = CliRunner().invoke(app, ["-C", str(project), "run", "materials", "--engine", "python", "--json", "--advance-state"])
    assert result.exit_code == 1, result.output
    assert json.loads(result.stdout)["read"] == 20
    state = json.loads(s3.get_object(Bucket="demo", Key="state/materials_to_sap/dev/state.json")["Body"].read())
    assert list(state["files"]) == ["materials/materials.csv"]


def test_state_backend_connection_errors_are_fatal():
    from botocore.exceptions import EndpointConnectionError

    class Broken:
        def __getattr__(self, name):
            def fail(**kw):
                raise EndpointConnectionError(endpoint_url="http://x")
            return fail

    backend = S3StateBackend(Broken(), "demo", "state")
    with pytest.raises(FatalRunError):
        backend.get("k")
    with pytest.raises(FatalRunError):
        backend.delete("k")
