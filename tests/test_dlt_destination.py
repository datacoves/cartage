import gzip
import json
import os
from pathlib import Path

import pytest
from typer.testing import CliRunner

from cartage.adapters.destinations.dlt import DltDestinationAdapter
from cartage.cli import app
from cartage.core import CartageError

runner = CliRunner()

MATERIALS = (
    "material,industry,type,description,uom,status\n"
    "300001,M,FERT,Pump,EA,active\n"
    "300002,M,ROH,Steel,KGS,active\n"
)


def rows(bucket: Path, dataset: str, table: str) -> list[dict]:
    out = []
    for path in sorted((bucket / dataset / table).iterdir()):
        if path.name.endswith(".jsonl.gz"):
            out += [json.loads(line) for line in gzip.decompress(path.read_bytes()).decode().splitlines()]
        elif path.suffix == ".parquet":
            out += __import__("pyarrow.parquet").parquet.read_table(path).to_pylist()
    return [{k: v for k, v in r.items() if not k.startswith("_dlt_")} for r in out]


def add_lakes(project: Path, tmp_path: Path, *names: str) -> None:
    path = project / "connections.yaml"
    path.write_text(path.read_text() + "".join(
        f"\n  {n}:\n    type: dlt\n    envs:\n      dev: {{ destination: filesystem, bucket_url: file://{tmp_path / n} }}\n"
        for n in names))
    (project / "data" / "materials" / "materials.csv").write_text(MATERIALS)


def pipeline(project: Path, name: str, body: str) -> None:
    (project / "pipelines" / f"{name}.yaml").write_text(f"name: {name}\n{body}")


CSV_SOURCE = """source:
  connection: local_files
  format: csv
  path: materials/*.csv
transforms:
  - map: transforms.materials:normalize_uom
engine: dlt
"""


def cli(project: Path, *args: str):
    return runner.invoke(app, ["-C", str(project), *args])


def test_loads_transformed_records_into_a_dlt_destination(project, tmp_path):
    add_lakes(project, tmp_path, "lake")
    pipeline(project, "to_lake", CSV_SOURCE + "destination:\n  connection: lake\n  dataset_name: raw\n  table_name: materials\n")

    result = cli(project, "run", "to_lake", "--json")

    assert result.exit_code == 0, result.output
    out = json.loads(result.stdout)
    assert (out["read"], out["ok"], out["errors"]) == (2, 2, [])
    assert [(r["material"], r["uom"]) for r in rows(tmp_path / "lake", "raw", "materials")] == [
        ("300001", "EA"), ("300002", "KG")]


def test_replace_replaces_on_every_run(project, tmp_path):
    add_lakes(project, tmp_path, "lake")
    pipeline(project, "snap", CSV_SOURCE + "destination:\n  connection: lake\n  table_name: materials\n"
                                           "  write_disposition: replace\n")

    for _ in range(2):
        assert cli(project, "run", "snap", "--full-refresh", "--json").exit_code == 0

    assert len(rows(tmp_path / "lake", "snap", "materials")) == 2


def test_arrow_batches_pass_through_untouched_and_tuning_applies_only_to_the_run(project, tmp_path):
    pytest.importorskip("pyarrow")
    add_lakes(project, tmp_path, "lake")
    (project / "arrow_src.py").write_text(
        "import dlt, pyarrow as pa\n\n"
        "@dlt.resource(name='events')\n"
        "def events():\n"
        "    yield pa.table({'Event Id': [1, 2, 3], 'Kind': ['a', 'b', 'c']})\n")
    pipeline(project, "arrow", "source:\n  type: dlt\n  ref: arrow_src:events\nengine: dlt\n"
                               "destination:\n  connection: lake\n  loader_file_format: parquet\n  naming: direct\n"
                               "  dlt_config: { data_writer.buffer_max_items: 5000 }\n")

    result = cli(project, "run", "arrow", "--json")

    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["ok"] == 3
    assert rows(tmp_path / "lake", "arrow", "events") == [
        {"Event Id": 1, "Kind": "a"}, {"Event Id": 2, "Kind": "b"}, {"Event Id": 3, "Kind": "c"}]
    assert "SCHEMA__NAMING" not in os.environ and "DATA_WRITER__BUFFER_MAX_ITEMS" not in os.environ


def test_each_destination_is_its_own_run_with_its_own_state(project, tmp_path):
    add_lakes(project, tmp_path, "lake_a", "lake_b")
    pipeline(project, "fan", CSV_SOURCE + "destinations:\n"
                                          "  - { connection: lake_a, table_name: materials }\n"
                                          "  - { connection: lake_b, table_name: archive }\n")

    planned = cli(project, "plan", "fan", "--json")
    assert planned.exit_code == 0, planned.output
    assert [p["destination"]["connection"] for p in json.loads(planned.stdout)] == ["lake_a", "lake_b"]

    result = cli(project, "run", "fan", "--json")

    assert result.exit_code == 0, result.output
    assert [json.loads(line)["ok"] for line in result.stdout.splitlines()] == [2, 2]
    assert len(rows(tmp_path / "lake_a", "fan", "materials")) == 2
    assert len(rows(tmp_path / "lake_b", "fan", "archive")) == 2
    state = project / ".cartage" / "state"
    assert (state / "fan__lake_a" / "dev" / "state.json").is_file()
    assert (state / "fan__lake_b" / "dev" / "state.json").is_file()
    assert set(json.loads(cli(project, "state", "show", "fan").stdout)) == {"fan__lake_a", "fan__lake_b"}
    assert cli(project, "state", "reset", "fan", "-y").exit_code == 0
    assert not (state / "fan__lake_a" / "dev" / "state.json").exists()


def test_dlt_destination_requires_dlt_engine(project, tmp_path):
    add_lakes(project, tmp_path, "lake")
    pipeline(project, "py", CSV_SOURCE.replace("engine: dlt", "engine: python") + "destination:\n  connection: lake\n")

    result = cli(project, "run", "py", "--json")

    assert result.exit_code == 2
    assert "dlt destinations need the dlt engine" in result.output


def test_connection_test_builds_the_destination(project, tmp_path):
    add_lakes(project, tmp_path, "lake")
    result = cli(project, "connections", "test", "lake")
    assert result.exit_code == 0, result.output
    assert "filesystem" in result.output


@pytest.mark.parametrize("options, message", [
    ({"write_disposition": "merge"}, "needs 'primary_key' or 'merge_key'"),
    ({"write_disposition": "scd2"}, "Unsupported dlt write_disposition"),
])
def test_invalid_options(tmp_path, options, message):
    with pytest.raises(CartageError, match=message):
        DltDestinationAdapter({"destination": "filesystem", "bucket_url": f"file://{tmp_path}"}, options, tmp_path)


def test_unknown_destination(tmp_path):
    with pytest.raises(CartageError, match="Unknown dlt destination 'nope'"):
        DltDestinationAdapter({"destination": "nope"}, {}, tmp_path)
