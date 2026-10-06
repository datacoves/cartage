import csv
import json
import xml.etree.ElementTree as ET
from datetime import UTC, datetime

import pytest
from typer.testing import CliRunner

from cartage.adapters.destinations.file import FileDestination
from cartage.cli import app
from cartage.core import CartageError

runner = CliRunner()
MATERIALS = (
    "material,industry,type,description,uom,status\n"
    "300001,M,FERT,Pump,EA,active\n"
    "300002,M,ROH,Steel,KGS,active\n"
)


def cli(project, *args):
    return runner.invoke(app, ["-C", str(project), *args])


def write_pipeline(project, options, transforms="  - map: transforms.materials:normalize_uom\n"):
    (project / "pipelines/export.yaml").write_text(
        "name: export\nsource:\n  connection: samples\n  format: csv\n  path: materials.csv\n  reader_options: { dtype: str }\n"
        f"transforms:\n{transforms}destination:\n  connection: exports\n{options}")


def setup(project, options="", transforms="  - map: transforms.materials:normalize_uom\n"):
    connections = project / "connections.yaml"
    connections.write_text(connections.read_text() + "\n  exports:\n    type: file_export\n    envs:\n      dev: { path: ./out }\n")
    (project / "data/sample/materials.csv").write_text(MATERIALS)
    write_pipeline(project, options, transforms)


EXPECTED = [{"material": "300001", "uom": "EA"}, {"material": "300002", "uom": "KG"}]


def picked(rows):
    return [{"material": r["material"], "uom": r["uom"]} for r in rows]


@pytest.mark.parametrize("engine", ["python", "dlt"])
def test_writes_every_format(project, engine):
    setup(project)
    for fmt in ("json", "jsonl", "xml", "csv"):
        write_pipeline(project, f"  format: {fmt}\n")
        result = cli(project, "run", "export", "--engine", engine, "--json", "--full-refresh")
        assert result.exit_code == 0, result.output
        out = (project / "out" / f"export.{fmt}").read_text()
        if fmt == "json":
            rows = json.loads(out)
        elif fmt == "jsonl":
            rows = [json.loads(line) for line in out.splitlines()]
        elif fmt == "xml":
            rows = [{c.tag: c.text for c in r} for r in ET.fromstring(out)]
        else:
            rows = list(csv.DictReader(out.splitlines()))
        assert picked(rows) == EXPECTED, fmt
        assert all("_source" not in r for r in rows)
    assert not list((project / "out").glob("*.partial"))


def test_nested_values_and_xml_names(project, tmp_path):
    dest = FileDestination({"path": str(tmp_path)}, {"format": "xml", "root": "states", "record": "state"}, tmp_path)
    dest.run_name = "s"
    dest.write([{"name": "Alabama", "2010": 1, "years": [{"year": 2010}], "_source": "x:1"}])
    dest.finish(True)
    root = ET.parse(tmp_path / "s.xml").getroot()
    assert root.tag == "states"
    assert [c.tag for c in root[0]] == ["name", "_2010", "years"]
    assert root[0].find("years/item/year").text == "2010"


def test_failed_run_leaves_no_file(project):
    (project / "transforms/boom.py").write_text("def boom(record):\n    raise ValueError('no')\n")
    setup(project, "  format: json\n", "  - map: transforms.boom:boom\n    on_error: fail\n")
    result = cli(project, "run", "export", "--engine", "python")
    assert result.exit_code == 3
    assert not (project / "out").exists() or not list((project / "out").iterdir())


def test_csv_rejects_columns_missing_from_the_header(tmp_path):
    dest = FileDestination({"path": str(tmp_path)}, {"format": "csv"}, tmp_path)
    result = dest.write([{"a": 1, "b": {"x": 1}}, {"a": 2, "c": 3}])
    dest.finish(True)
    assert result.ok == 1 and "columns not in the CSV header: c" in result.errors[0].message
    assert (tmp_path / "output.csv").read_text().splitlines() == ["a,b", '1,"{""x"": 1}"']


def test_plan_shows_the_rendered_output(project):
    setup(project, "  format: xml\n")
    result = cli(project, "plan", "export", "--engine", "python", "-n", "1")
    assert result.exit_code == 0, result.output
    assert "output:" in result.output and "<material>300001</material>" in result.output


def test_file_name_template(project):
    setup(project, "  format: jsonl\n  file: \"materials_{date}.{ext}\"\n")
    assert cli(project, "run", "export", "--engine", "python").exit_code == 0
    assert (project / "out" / f"materials_{datetime.now(UTC):%Y%m%d}.jsonl").is_file()


def test_invalid_options(tmp_path):
    with pytest.raises(CartageError, match="Unsupported file format 'yaml'"):
        FileDestination({"path": "."}, {"format": "yaml"}, tmp_path)
    with pytest.raises(CartageError, match="needs 'path'"):
        FileDestination({}, {}, tmp_path)


def test_two_files_from_one_connection_need_names(project):
    setup(project)
    pipeline = project / "pipelines/export.yaml"
    text = pipeline.read_text()
    body = text[: text.index("destination:")]
    pipeline.write_text(body + "destinations:\n  - { connection: exports, format: json }\n  - { connection: exports, format: xml }\n")
    result = cli(project, "validate", "export")
    assert result.exit_code == 2 and "set 'name' on destinations that share a connection" in result.output

    pipeline.write_text(body + "destinations:\n  - { connection: exports, format: json }\n"
                               "  - { connection: exports, name: xml_copy, format: xml }\n")
    result = cli(project, "run", "export", "--engine", "python", "--json")
    assert result.exit_code == 0, result.output
    assert {p.name for p in (project / "out").iterdir()} == {"export.json", "export.xml"}
    assert (project / ".cartage/state/export__xml_copy/dev/state.json").is_file()
