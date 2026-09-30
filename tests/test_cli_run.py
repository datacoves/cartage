import json

import pytest
from typer.testing import CliRunner

from cartage.cli import app

runner = CliRunner()
ENGINES = ["python"]


def run(project, *args):
    return runner.invoke(app, ["-C", str(project), "run", "materials", *args])


@pytest.mark.parametrize("engine", ENGINES)
def test_run_json(project, engine):
    result = run(project, "--engine", engine, "--json")
    assert result.exit_code == 1, result.output
    data = json.loads(result.stdout)
    assert (data["read"], data["filtered"], data["sent"], data["ok"], len(data["errors"])) == (20, 2, 18, 16, 2)
    assert data["state_advanced"] is False


@pytest.mark.parametrize("engine", ENGINES)
def test_incremental_flow(project, engine):
    assert json.loads(run(project, "--engine", engine, "--json", "--advance-state").stdout)["state_advanced"]
    second = run(project, "--engine", engine, "--json")
    assert second.exit_code == 0 and json.loads(second.stdout)["read"] == 0
    with open(project / "data/materials/materials.csv", "a") as f:
        f.write("100020,M,FERT,New pump,EA,active\n")
    assert json.loads(run(project, "--engine", engine, "--json").stdout)["read"] == 21


@pytest.mark.parametrize("engine", ENGINES)
def test_rich_summary(project, engine):
    result = run(project, "--engine", engine)
    assert result.exit_code == 1
    assert "completed with 2 record error(s)" in result.output
    assert "Value 'ZXX' is not allowed for HEADDATA-MATL_TYPE" in result.output
    assert "materials/materials.csv:9" in result.output
    assert "state not advanced" in result.output


@pytest.mark.parametrize("engine", ENGINES)
def test_no_matching_files(project, engine):
    pipeline = project / "pipelines/materials.yaml"
    pipeline.write_text(pipeline.read_text().replace("path: materials/*.csv", "path: nothing/*.csv"))
    result = run(project, "--engine", engine)
    assert result.exit_code == 0, result.output
    assert "No records to process." in result.output


@pytest.mark.parametrize("engine", ENGINES)
def test_on_error_fail_aborts_with_exit_3(project, engine):
    (project / "transforms/boom.py").write_text("def explode(record):\n    raise ValueError('boom')\n")
    pipeline = project / "pipelines/materials.yaml"
    pipeline.write_text(pipeline.read_text().replace(
        "transforms:\n", "transforms:\n  - map: transforms.boom:explode\n    on_error: fail\n"))
    result = run(project, "--engine", engine)
    assert result.exit_code == 3, result.output
    assert "transforms.boom:explode" in result.output


@pytest.mark.parametrize("engine", ENGINES)
def test_unreachable_mock_server_exits_3(project, engine):
    conns = project / "connections.yaml"
    conns.write_text(conns.read_text().replace('transport: mock, client: "100"',
                                               'transport: mock, client: "100", url: "http://127.0.0.1:9"'))
    result = run(project, "--engine", engine)
    assert result.exit_code == 3, result.output
    assert "Cannot reach mock SAP" in result.output


def test_run_from_subdirectory_by_name(project, monkeypatch):
    monkeypatch.chdir(project / "pipelines")
    result = runner.invoke(app, ["run", "materials", "--engine", "python", "--json"])
    assert result.exit_code == 1
    assert json.loads(result.stdout)["read"] == 20
