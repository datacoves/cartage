import json

import pytest
from conftest import incremental
from typer.testing import CliRunner

from cartage.cli import app

runner = CliRunner()
ENGINES = ["python", "dlt"]


def run(project, *args):
    return runner.invoke(app, ["-C", str(project), "run", "materials", *args])


@pytest.mark.parametrize("engine", ENGINES)
def test_run_json(project, engine):
    result = run(project, "--engine", engine, "--json")
    assert result.exit_code == 1, result.output
    data = json.loads(result.stdout)
    assert (data["read"], data["filtered"], data["sent"], data["ok"], len(data["errors"])) == (20, 2, 18, 16, 2)
    assert data["state_advanced"] is False


def test_incremental_flow(project):  # incremental file reads need the dlt engine
    incremental(project)
    engine = "dlt"
    assert json.loads(run(project, "--engine", engine, "--json", "--advance-state").stdout)["state_advanced"]
    second = run(project, "--engine", engine, "--json")
    assert second.exit_code == 0 and json.loads(second.stdout)["read"] == 0
    with open(project / "data/sample/materials.csv", "a") as f:
        f.write("100020,M,FERT,New pump,EA,active\n")
    assert json.loads(run(project, "--engine", engine, "--json").stdout)["read"] == 21


@pytest.mark.parametrize("engine", ENGINES)
def test_rich_summary(project, engine):
    result = run(project, "--engine", engine)
    assert result.exit_code == 1
    assert "completed with 2 record error(s)" in result.output
    assert "Value 'ZXX' is not allowed for HEADDATA-MATL_TYPE" in result.output
    assert "state not advanced" in result.output


@pytest.mark.parametrize("engine", ENGINES)
def test_no_matching_files(project, engine):
    pipeline = project / "pipelines/materials.yaml"
    pipeline.write_text(pipeline.read_text().replace("      path: materials.csv\n", "      path: nothing.csv\n"))
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
    conns = project / ".cartage/connections.yaml"
    conns.write_text(conns.read_text().replace('transport: mock, client: "100"',
                                               'transport: mock, client: "100", url: "http://127.0.0.1:9"'))
    result = run(project, "--engine", engine)
    assert result.exit_code == 3, result.output
    assert "Cannot reach mock SAP" in result.output
    assert "materials · dev" in result.output and "run aborted" in result.output  # partial summary
    data = json.loads(run(project, "--engine", engine, "--json").stdout)
    assert data["read"] == 20 and data["ok"] == 0 and "Cannot reach mock SAP" in data["fatal"]


def test_unexpected_engine_error_exits_3_without_traceback(project):
    (project / "boom_src.py").write_text("import dlt\n\n\n@dlt.resource\ndef rows():\n    raise RuntimeError('kaboom')\n    yield\n")
    pipeline = project / "pipelines/materials.yaml"
    pipeline.write_text(pipeline.read_text().replace("      connection: samples\n      path: materials.csv\n", "      ref: boom_src:rows\n"))
    result = run(project, "--engine", "python")
    assert result.exit_code == 3, result.output
    assert "Run failed:" in result.output and "kaboom" in result.output and "Traceback" not in result.output


def test_run_from_subdirectory_by_name(project, monkeypatch):
    monkeypatch.chdir(project / "pipelines")
    result = runner.invoke(app, ["run", "materials", "--engine", "python", "--json"])
    assert result.exit_code == 1
    assert json.loads(result.stdout)["read"] == 20


def test_dependencies_run_the_command_again_under_uv(project, monkeypatch):
    config = project / ".cartage/config.yaml"
    config.write_text(config.read_text().replace("  engine: dlt\n", '  engine: dlt\n  dependencies: ["dlt[http]"]\n'))
    pipeline = project / "pipelines/materials.yaml"
    pipeline.write_text(pipeline.read_text() + "dependencies: [pandas, \"dlt[http]\"]\n")
    calls = []
    monkeypatch.setattr("cartage.cli.shutil.which", lambda name: "/bin/uv")
    monkeypatch.setattr("cartage.cli.subprocess.run",
                        lambda cmd, env, check: calls.append((cmd, env)) or type("R", (), {"returncode": 5}))
    monkeypatch.setattr("sys.argv", ["cartage", "run", "materials", "--json"])
    monkeypatch.delenv("CARTAGE_NO_DEPS", raising=False)

    assert run(project, "--json").exit_code == 5  # the exit code of the run under uv
    (cmd, env), = calls
    assert cmd[:4] == ["/bin/uv", "run", "--no-project", "--python"]
    assert cmd[5:] == ["--with", "dlt[http]", "--with", "pandas", "python", "-m", "cartage", "run", "materials", "--json"]
    assert env["CARTAGE_NO_DEPS"] == "1"  # the run under uv does not start again

    assert run(project, "--no-deps", "--engine", "python", "--json").exit_code == 1  # record errors, no uv
    monkeypatch.setenv("CARTAGE_NO_DEPS", "1")
    assert run(project, "--engine", "python", "--json").exit_code == 1
    assert len(calls) == 1

    monkeypatch.delenv("CARTAGE_NO_DEPS")
    monkeypatch.setattr("cartage.cli.shutil.which", lambda name: None)
    result = run(project)
    assert result.exit_code == 2
    assert "uv is not on PATH" in result.output and "--no-deps" in result.output
