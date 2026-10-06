import json

from typer.testing import CliRunner

from cartage.cli import app

runner = CliRunner()

SOURCE = """\
import json
from pathlib import Path

import dlt


@dlt.resource(name="erp_materials")
def materials(prefix=""):
    rows = json.loads((Path(__file__).parent / "rows.json").read_text())
    yield [dict(r, material=prefix + r["material"]) for r in rows]


@dlt.resource(name="noise")
def noise():
    yield [{"material": "X" * 40}]  # too long for SAP: fails if this resource is read


@dlt.source
def erp():
    return materials(), noise()
"""

PIPELINE = """\
name: erp_to_sap
source:
  type: dlt
  ref: erp_sources.legacy:materials
  with: { prefix: "" }
  incremental: { cursor: updated_at, initial: "2024-01-01" }
engine: dlt
destination:
  connection: sap_erp
  bapi: BAPI_MATERIAL_SAVEDATA
  mapping:
    HEADDATA.MATERIAL: material
    HEADDATA.IND_SECTOR: industry
    HEADDATA.MATL_TYPE: type
    CLIENTDATA.BASE_UOM: uom
"""


def row(material, updated_at):
    return {"material": material, "industry": "M", "type": "FERT", "uom": "EA", "updated_at": updated_at}


def setup(project, pipeline=PIPELINE):
    pkg = project / "erp_sources"
    pkg.mkdir()
    (pkg / "__init__.py").write_text("")
    (pkg / "legacy.py").write_text(SOURCE)
    (pkg / "rows.json").write_text(json.dumps([row("200001", "2024-02-01"), row("200002", "2024-02-02"),
                                               row("200003", "2024-02-03")]))
    (project / "pipelines" / "erp.yaml").write_text(pipeline)
    return pkg / "rows.json"


def run(project, *args):
    return runner.invoke(app, ["-C", str(project), "run", "erp", "--json", *args])


def test_incremental_loads_only_new_rows(project):
    rows_file = setup(project)
    first = run(project)
    assert first.exit_code == 0, first.output
    assert json.loads(first.stdout)["read"] == 3
    rows = json.loads(rows_file.read_text()) + [row("200004", "2024-03-01")]
    rows_file.write_text(json.dumps(rows))
    second = json.loads(run(project).stdout)
    assert (second["read"], second["ok"]) == (1, 1)


def test_python_engine_without_incremental(project):
    setup(project, PIPELINE.replace('  incremental: { cursor: updated_at, initial: "2024-01-01" }\n', "")
          .replace("engine: dlt", "engine: python"))
    result = run(project)
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["read"] == 3


def test_incremental_requires_dlt_engine(project):
    setup(project)
    result = runner.invoke(app, ["-C", str(project), "run", "erp", "--engine", "python"])
    assert result.exit_code == 2
    assert "need the dlt engine" in result.output


def test_ref_must_return_dlt_object(project):
    setup(project, PIPELINE.replace("erp_sources.legacy:materials", "erp_sources.legacy:Path").replace("  with: { prefix: \"\" }\n", ""))
    result = runner.invoke(app, ["-C", str(project), "run", "erp"])
    assert result.exit_code == 2
    assert "expected a dlt source or resource" in result.output


def test_ref_without_type_runs_as_a_dlt_source(project):
    setup(project, PIPELINE.replace("  type: dlt\n", ""))
    result = run(project)
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["read"] == 3


def test_resources_selects_from_a_source(project):
    setup(project, PIPELINE.replace("erp_sources.legacy:materials", "erp_sources.legacy:erp")
          .replace('  with: { prefix: "" }\n', "  resources: [erp_materials]\n"))
    result = run(project)
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["read"] == 3


def test_unknown_resource_is_rejected(project):
    setup(project, PIPELINE.replace("erp_sources.legacy:materials", "erp_sources.legacy:erp")
          .replace('  with: { prefix: "" }\n', "  resources: [nope]\n"))
    result = runner.invoke(app, ["-C", str(project), "run", "erp"])
    assert result.exit_code == 2
    assert "has no resource(s): nope" in result.output
    assert "erp_materials, noise" in result.output
