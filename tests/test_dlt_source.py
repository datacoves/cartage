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
    material: HEADDATA.MATERIAL
    industry: HEADDATA.IND_SECTOR
    type: HEADDATA.MATL_TYPE
    uom: CLIENTDATA.BASE_UOM
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
