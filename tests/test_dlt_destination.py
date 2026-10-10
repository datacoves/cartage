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
    path = project / ".cartage/connections.yaml"
    path.write_text(path.read_text() + "".join(
        f"\n  {n}:\n    type: filesystem\n    envs:\n      dev: {{ bucket_url: file://{tmp_path / n} }}\n"
        for n in names))
    (project / "data" / "sample" / "materials.csv").write_text(MATERIALS)


def pipeline(project: Path, name: str, body: str) -> None:
    (project / "pipelines" / f"{name}.yaml").write_text(f"name: {name}\n{body}")


CSV_SOURCE = """source:
  connection: samples
  format: csv
  path: materials.csv
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
        (300001, "EA"), (300002, "KG")]


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


def test_project_naming_module_and_connection_level_tuning(project, tmp_path):
    add_lakes(project, tmp_path, "lake")
    connections = project / ".cartage/connections.yaml"
    connections.write_text(connections.read_text().replace(
        f"bucket_url: file://{tmp_path / 'lake'} }}", f"bucket_url: file://{tmp_path / 'lake'}, naming: naming_upper }}"))
    (project / "naming_upper.py").write_text(
        "from dlt.common.normalizers.naming.sql_cs_v1 import NamingConvention as Base\n\n"
        "class NamingConvention(Base):\n"
        "    def normalize_identifier(self, identifier):\n"
        "        return super().normalize_identifier(identifier).upper()\n")
    pipeline(project, "upper", CSV_SOURCE + "destination:\n  connection: lake\n  table_name: materials\n")

    result = cli(project, "run", "upper", "--json")

    assert result.exit_code == 0, result.output
    assert {"MATERIAL", "UOM"} <= set(rows(tmp_path / "lake", "UPPER", "MATERIALS")[0])


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
    assert not (project / ".cartage" / "state").exists()  # dlt keeps the state in each destination
    shown = json.loads(cli(project, "state", "show", "fan").stdout)
    assert {name: state["dlt_state"]["pipeline_name"] for name, state in shown.items()} == {
        "fan__lake_a": "fan__lake_a", "fan__lake_b": "fan__lake_b"}


def test_after_load_hooks_get_the_pipeline_and_resource_hints_are_kept(project, tmp_path):
    add_lakes(project, tmp_path, "lake")
    (project / "src_rows.py").write_text(
        "import dlt\n\n"
        "@dlt.resource(name='people', write_disposition='replace', primary_key='id')\n"
        "def people():\n"
        "    yield [{'id': 1}, {'id': 2}]\n")
    (project / "hooks.py").write_text(
        "import json, pathlib\n\n"
        "def record(pipeline, out, tables):\n"
        "    pathlib.Path(out).write_text(json.dumps([pipeline.dataset_name, tables]))\n\n"
        "def boom(pipeline):\n"
        "    raise RuntimeError('no grants')\n")
    out = tmp_path / "hook.json"
    pipeline(project, "hooked", "source:\n  type: dlt\n  ref: src_rows:people\nengine: dlt\n"
                                "destination:\n  connection: lake\n  after_load:\n"
                                f"    - {{ ref: hooks:record, with: {{ out: {json.dumps(str(out))}, tables: [people] }} }}\n")

    for _ in range(2):  # the resource's own replace is kept, so the second run does not append
        result = cli(project, "run", "hooked", "--json")
        assert result.exit_code == 0, result.output

    assert len(rows(tmp_path / "lake", "hooked", "people")) == 2
    assert json.loads(out.read_text()) == ["hooked", ["people"]]

    path = project / "pipelines" / "hooked.yaml"
    path.write_text(path.read_text().replace("hooks:record", "hooks:boom").replace(
        f", with: {{ out: {json.dumps(str(out))}, tables: [people] }}", ""))
    failed = cli(project, "run", "hooked", "--json")
    assert failed.exit_code == 3
    assert "after_load 'hooks:boom' failed: RuntimeError: no grants" in failed.output


def test_incremental_options_reach_dlt(project, tmp_path):
    add_lakes(project, tmp_path, "lake")
    seen = tmp_path / "seen.txt"
    (project / "src_inc.py").write_text(
        "import dlt\n\n"
        "@dlt.resource(name='events', primary_key='id')\n"
        f"def events(t=dlt.sources.incremental('t')):\n"
        f"    open({str(seen)!r}, 'a').write(f'{{t.start_value}}\\n')\n"
        "    yield [{'id': 1, 't': 100}, {'id': 2, 't': 300}]\n")
    pipeline(project, "inc", "source:\n  type: dlt\n  ref: src_inc:events\n  incremental: { cursor: t, lag: 250 }\n"
                             "engine: dlt\ndestination:\n  connection: lake\n  write_disposition: merge\n")

    for _ in range(2):
        assert cli(project, "run", "inc", "--json").exit_code == 0

    assert seen.read_text().split() == ["None", "50"]  # last value 300, minus lag 250
    assert not (project / ".cartage" / "state").exists()  # restored from the destination, like a fresh worker

    assert cli(project, "state", "reset", "inc", "-y").exit_code == 0
    assert cli(project, "run", "inc", "--json").exit_code == 0
    assert seen.read_text().split()[-1] == "None"

    assert cli(project, "run", "inc", "--full-refresh", "--json").exit_code == 0
    assert seen.read_text().split()[-1] == "None"
    assert len(rows(tmp_path / "lake", "inc", "events")) == 2  # tables dropped, then loaded again

    path = project / "pipelines" / "inc.yaml"
    path.write_text(path.read_text().replace("lag: 250", "lagg: 250"))
    result = cli(project, "validate", "inc")
    assert result.exit_code == 2
    assert "Unknown dlt source incremental option(s): lagg" in result.output


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
    ({"write_disposition": "scd2"}, "Unsupported dlt write_disposition"),
])
def test_invalid_options(tmp_path, options, message):
    with pytest.raises(CartageError, match=message):
        DltDestinationAdapter({"destination": "filesystem", "bucket_url": f"file://{tmp_path}"}, options, tmp_path)


def test_unknown_destination(tmp_path):
    with pytest.raises(CartageError, match="Unknown dlt destination 'nope'"):
        DltDestinationAdapter({"destination": "nope"}, {}, tmp_path)


def test_full_refresh_drops_a_table_another_pipeline_created(project, tmp_path):
    import duckdb
    db = tmp_path / "w.duckdb"
    with duckdb.connect(str(db)) as con:
        con.execute("create schema raw; create table raw.materials (legacy text); insert into raw.materials values ('x')")
    path = project / ".cartage/connections.yaml"
    path.write_text(path.read_text() + f"\n  wh:\n    type: duckdb\n    envs:\n      dev: {{ credentials: {db} }}\n")
    (project / "data" / "sample" / "materials.csv").write_text(MATERIALS)
    pipeline(project, "to_wh", CSV_SOURCE + "destination:\n  connection: wh\n  dataset_name: raw\n  table_name: materials\n"
                                            "  write_disposition: replace\n")
    result = cli(project, "run", "to_wh", "--full-refresh")
    assert result.exit_code == 0, result.output
    with duckdb.connect(str(db)) as con:
        columns = [c[0] for c in con.execute("describe raw.materials").fetchall()]
        assert "legacy" not in columns and con.execute("select count(*) from raw.materials").fetchone()[0] == 2


def test_progress_reports_the_dlt_stage(project, tmp_path):
    from cartage.config import load_project
    from cartage.runner import prepare, run_pipeline
    path = project / ".cartage/connections.yaml"
    path.write_text(path.read_text() + f"\n  wh:\n    type: duckdb\n    envs:\n      dev: {{ credentials: {tmp_path / 'w.duckdb'} }}\n")
    (project / "data" / "sample" / "materials.csv").write_text(MATERIALS)
    pipeline(project, "to_wh", CSV_SOURCE + "destination:\n  connection: wh\n  table_name: materials\n")
    stages = []
    run_pipeline(prepare(load_project(project), "to_wh"), on_progress=lambda r: stages.append(r.stage))
    assert [s for i, s in enumerate(stages) if s and s not in stages[:i]] == ["reading", "normalizing", "loading"]


def test_a_duckdb_file_under_home_gets_its_folder(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    DltDestinationAdapter({"destination": "duckdb", "credentials": "~/.cartage/p/w.duckdb"}, {}, tmp_path)
    assert (tmp_path / "home" / ".cartage" / "p").is_dir()


TWO_TABLES = '''import dlt


@dlt.source
def db(label="with"):
    @dlt.resource
    def family():
        yield [{"id": 1, "updated": 1, "label": label}, {"id": 2, "updated": 2, "label": label}]

    @dlt.resource
    def author():  # no cursor column
        yield [{"id": 1, "label": label}]

    return family, author
'''


def _two_tables(project, tmp_path, source: str, connection: str = "") -> Path:
    (project / "sources").mkdir(exist_ok=True)
    (project / "sources" / "two_tables.py").write_text(TWO_TABLES)
    db = tmp_path / "w.duckdb"
    path = project / ".cartage/connections.yaml"
    path.write_text(path.read_text() + f"\n  wh:\n    type: duckdb\n    envs:\n      dev: {{ credentials: {db} }}\n"
                    + connection)
    pipeline(project, "db", f"engine: dlt\nsource:\n{source}"
                            "destination:\n  connection: wh\n  dataset_name: raw\n  write_disposition: append\n")
    return db


def _counts(db, *tables):
    import duckdb
    with duckdb.connect(str(db)) as con:
        return [con.execute(f"select count(*), min(label) from raw.{t}").fetchone() for t in tables]


def test_tables_override_incremental_and_write_disposition_per_table(project, tmp_path):
    source = ("  ref: sources.two_tables:db\n  incremental: {cursor: updated}\n"
              "  tables:\n    author: {incremental: false, write_disposition: replace}\n")
    db = _two_tables(project, tmp_path, source)
    for _ in range(2):
        result = cli(project, "run", "db")
        assert result.exit_code == 0, result.output
    # family: incremental + append, so the second run adds nothing; author: replaced on each run
    assert _counts(db, "family", "author") == [(2, "with"), (1, "with")]


def test_without_the_override_a_table_missing_the_cursor_fails(project, tmp_path):
    _two_tables(project, tmp_path, "  ref: sources.two_tables:db\n  incremental: {cursor: updated}\n")
    result = cli(project, "run", "db")
    assert result.exit_code != 0 and "updated" in result.output


def test_tables_must_name_resources_and_known_settings(project, tmp_path):
    _two_tables(project, tmp_path, "  ref: sources.two_tables:db\n  tables:\n    authors: {write_disposition: replace}\n")
    assert "names no resource of 'sources.two_tables:db': authors" in cli(project, "run", "db").output
    pipeline(project, "db", "engine: dlt\nsource:\n  ref: sources.two_tables:db\n  resources: [family]\n"
                            "  tables:\n    family: {cursor: updated}\ndestination:\n  connection: wh\n")
    assert "Unknown setting(s) for table 'family': cursor" in cli(project, "validate", "db").output


def test_a_dlt_connection_holds_the_factory_settings(project, tmp_path):
    connection = ("\n  db_conn:\n    type: dlt\n    envs:\n"
                  "      dev: { ref: sources.two_tables:db, label: from_connection }\n")
    db = _two_tables(project, tmp_path, "  connection: db_conn\n  resources: [author]\n", connection)
    result = cli(project, "run", "db")
    assert result.exit_code == 0, result.output
    assert _counts(db, "author") == [(1, "from_connection")]
    assert "settings for the dlt source sources.two_tables:db" in cli(project, "connections", "test", "db_conn").output
