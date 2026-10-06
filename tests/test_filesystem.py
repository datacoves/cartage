import json
import os

import pytest
from conftest import incremental
from typer.testing import CliRunner

from cartage.adapters.sources.filesystem import FilesystemSource, FsspecStateBackend
from cartage.cli import app
from cartage.core import CartageError

CSV = "material,uom,qty\n000123,EA,1\n100002,KGS,2\n100003,PCS,\n"


@pytest.fixture
def data(tmp_path):
    (tmp_path / "data" / "materials").mkdir(parents=True)
    (tmp_path / "data" / "materials" / "a.csv").write_text(CSV)
    return tmp_path


def source(root, **options):
    return FilesystemSource({"bucket_url": "./data"}, {"path": "materials/*.csv", **options}, root)


def rows(src):
    return [row for batch in src.read({}) for row in batch]


def test_csv_is_read_typed_in_batches(data):
    assert [len(b) for b in source(data, batch_size=2).read({})] == [2, 1]
    first = rows(source(data))[0]
    assert (first["material"], first["qty"]) == (123, 1)  # dlt's pandas reader infers types


def test_reader_options_reach_the_reader(data):
    first, last = rows(source(data, reader_options={"dtype": "str", "keep_default_na": False}))[::2]
    assert (first["material"], last["qty"]) == ("000123", "")


def test_jsonl_format(data):
    (data / "data" / "materials" / "b.jsonl").write_text('{"material": "9"}\n{"material": "10"}\n')
    assert [r["material"] for r in rows(source(data, path="materials/*.jsonl", format="jsonl"))] == ["9", "10"]


def test_no_matching_files(data):
    assert rows(source(data, path="nothing/*.csv")) == []


def test_config_errors(data):
    with pytest.raises(CartageError, match="needs 'bucket_url'"):
        FilesystemSource({}, {"path": "*.csv"}, data)
    with pytest.raises(CartageError, match="needs 'path'"):
        FilesystemSource({"bucket_url": "./data"}, {}, data)
    with pytest.raises(CartageError, match="Unsupported format 'xlsx'"):
        source(data, format="xlsx")


def test_check_connection_resolves_relative_paths_against_the_project(data):
    assert str((data / "data").resolve()) + " (1 entry)" == FilesystemSource.check_connection({"bucket_url": "./data"}, data)


def test_state_backend_round_trip(data):
    backend = FilesystemSource.state_backend({"bucket_url": "./data"}, "state/", data)
    assert isinstance(backend, FsspecStateBackend)
    assert backend.get("p/dev/state.json") is None
    backend.put("p/dev/state.json", b"{}")
    assert (data / "data" / "state" / "p" / "dev" / "state.json").read_bytes() == b"{}"
    assert backend.get("p/dev/state.json") == b"{}"
    backend.delete("p/dev/state.json")
    assert backend.get("p/dev/state.json") is None


def run(project, *args):
    return CliRunner().invoke(app, ["-C", str(project), "run", "materials", "--json", "--advance-state", *args])


def test_incremental_reads_only_changed_files_with_state_on_a_filesystem_connection(project):
    incremental(project)
    settings = project / "cartage.yaml"
    settings.write_text(settings.read_text().replace("dev: { path: .cartage/state }",
                                                     "dev: { connection: local_files, prefix: state }"))
    assert json.loads(run(project).stdout)["read"] == 20
    assert (project / "data/state/materials_to_sap/dev/state.json").is_file()
    assert json.loads(run(project).stdout)["read"] == 0
    f = project / "data/materials/materials.csv"
    os.utime(f, ns=(f.stat().st_atime_ns, f.stat().st_mtime_ns + 5_000_000_000))
    assert json.loads(run(project).stdout)["read"] == 20  # a changed file is read again, in full


def test_incremental_needs_the_dlt_engine(project):
    incremental(project)
    result = run(project, "--engine", "python")
    assert result.exit_code == 2
    assert "'incremental' need the dlt engine" in result.output


def test_python_engine_reads_files_without_incremental(project):
    assert json.loads(run(project, "--engine", "python").stdout)["read"] == 20
