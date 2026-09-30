import json

from typer.testing import CliRunner

from cartage.cli import app

runner = CliRunner()


def cli(project, *args):
    return runner.invoke(app, ["-C", str(project), *args])


def test_validate_demo_ok(project):
    result = cli(project, "validate")
    assert result.exit_code == 0, result.output
    assert "✔ materials_to_sap" in result.output
    assert "3 step(s)" in result.output


def test_validate_reports_bad_mapping(project):
    pipeline = project / "pipelines/materials.yaml"
    pipeline.write_text(pipeline.read_text().replace("HEADDATA.IND_SECTOR", "HEADDATA.NOPE"))
    result = cli(project, "validate")
    assert result.exit_code == 2
    assert "HEADDATA has no field NOPE" in result.output


def test_validate_reports_bad_transform(project):
    pipeline = project / "pipelines/materials.yaml"
    pipeline.write_text(pipeline.read_text().replace("transforms.materials:is_active", "transforms.materials:nope"))
    data = json.loads(cli(project, "validate", "--json").stdout)
    [transforms] = [c for c in data[0]["checks"] if c["label"] == "transforms"]
    assert transforms["ok"] is False and "'nope' not found" in transforms["detail"]


def test_validate_prd_needs_secrets(project):
    result = cli(project, "validate", "--env", "prd")
    assert result.exit_code == 2
    assert "Secret 'sap.user' not found" in result.output


def test_plan_json_previews_payloads_without_state(project):
    result = cli(project, "plan", "materials", "--engine", "python", "--json")
    assert result.exit_code == 0, result.output
    data = json.loads(result.stdout)
    assert data["files"] == [{"file": "materials/materials.csv", "process": True}]
    assert data["records"][2]["uom"] == "KGS"
    assert data["transformed"][2]["uom"] == "KG"
    assert data["payloads"][0]["HEADDATA"]["MATERIAL"] == "100001"
    assert data["payloads"][0]["CLIENTDATAX"] == {"BASE_UOM": "X"}
    assert data["destination"]["config"] == {"transport": "mock", "client": "100"}
    assert not (project / ".cartage").exists()


def test_plan_rich_output(project):
    result = cli(project, "plan", "materials", "--engine", "python", "-n", "1")
    assert result.exit_code == 0, result.output
    assert "BAPI payload" in result.output and "HEADDATA" in result.output
