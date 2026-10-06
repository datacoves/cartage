import json

import pytest
from typer.testing import CliRunner

from cartage.cli import app
from cartage.config import load_project
from cartage.core import CartageError

PIPELINE = """\
name: people
source:
  connection: files
  path: real/*.csv
  format: csv
  envs:
    dev:
      connection: files
      path: sample/*.csv
      format: csv
destination:
  connection: out
  format: jsonl
"""


def make(tmp_path, pipeline=PIPELINE):
    (tmp_path / "cartage.yaml").write_text("project: t\nenvironments: [dev, prd]\ndefault_env: dev\n")
    (tmp_path / "connections.yaml").write_text(
        "connections:\n"
        "  files:\n    type: filesystem\n    envs:\n      dev: {bucket_url: ./data}\n      prd: {bucket_url: ./data}\n"
        "  out:\n    type: file_export\n    envs:\n      dev: {path: out}\n      prd: {path: out}\n")
    (tmp_path / "pipelines").mkdir()
    (tmp_path / "pipelines/people.yaml").write_text(pipeline)
    for folder, rows in (("real", 3), ("sample", 2)):
        (tmp_path / "data" / folder).mkdir(parents=True)
        (tmp_path / "data" / folder / "p.csv").write_text("id\n" + "".join(f"{i}\n" for i in range(rows)))
    return tmp_path


def run(root, env):
    return CliRunner().invoke(app, ["-C", str(root), "run", "people", "--env", env, "--json", "--engine", "python"])


def test_an_environment_replaces_the_whole_source(tmp_path):
    root = make(tmp_path)
    assert json.loads(run(root, "dev").stdout)["read"] == 2
    assert json.loads(run(root, "prd").stdout)["read"] == 3


def test_source_for_returns_the_pipeline_source_without_a_replacement(tmp_path):
    pipeline = load_project(make(tmp_path)).load_pipeline("people")
    assert pipeline.source_for("prd") is pipeline.source
    assert pipeline.source_for("dev").options()["path"] == "sample/*.csv"
    assert "envs" not in pipeline.source.options()


def test_an_invalid_replacement_names_the_environment(tmp_path):
    root = make(tmp_path, PIPELINE.replace("      connection: files\n      path: sample", "      path: sample"))
    with pytest.raises(CartageError, match="source.envs.dev"):
        load_project(root).load_pipeline("people").source_for("dev")


def test_plan_shows_the_environment_source(tmp_path):
    result = CliRunner().invoke(app, ["-C", str(make(tmp_path)), "plan", "people", "--json", "--engine", "python"])
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["source"]["options"]["path"] == "sample/*.csv"


def test_duckdb_files_are_relative_to_the_project(tmp_path):
    from cartage.adapters.destinations.dlt import DltDestinationAdapter

    adapter = DltDestinationAdapter({"destination": "duckdb", "credentials": "x.duckdb"}, {}, tmp_path)
    assert adapter.destination.config_params["credentials"] == str((tmp_path / "x.duckdb").resolve())
