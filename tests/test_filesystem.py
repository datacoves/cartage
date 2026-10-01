import os

import pytest

from cartage.adapters.sources.filesystem import FilesystemSource
from cartage.core import CartageError, FatalRunError
from cartage.state import LocalStateBackend

CSV = "material,uom\n100001,EA\n100002,KGS\n100003,PCS\n"


@pytest.fixture
def data(tmp_path):
    (tmp_path / "data" / "materials").mkdir(parents=True)
    (tmp_path / "data" / "materials" / "a.csv").write_text(CSV)
    return tmp_path


def source(root, **options):
    return FilesystemSource({"path": "./data"}, {"path": "materials/*.csv", **options}, root)


def test_reads_batches_with_source_lines(data):
    batches = list(source(data, batch_size=2).read({}))
    assert [len(b) for b in batches] == [2, 1]
    assert batches[0][0] == {"material": "100001", "uom": "EA", "_source": "materials/a.csv:2"}
    assert batches[1][0]["_source"] == "materials/a.csv:4"


def test_incremental_skips_processed_and_rereads_changed(data):
    src, state = source(data, incremental=True), {}
    assert sum(map(len, src.read(state))) == 3
    assert list(src.read(state)) == []
    f = data / "data" / "materials" / "a.csv"
    f.write_text(CSV + "100004,EA\n")
    os.utime(f, ns=(f.stat().st_atime_ns, f.stat().st_mtime_ns + 1_000_000))
    assert sum(map(len, src.read(state))) == 4
    assert src.plan_files(state) == [("materials/a.csv", False)]


def test_non_incremental_leaves_state_alone(data):
    state = {}
    list(source(data).read(state))
    assert state == {"files": {}}


def test_no_matching_files(data):
    assert list(FilesystemSource({"path": "./data"}, {"path": "nothing/*.csv"}, data).read({})) == []


def test_missing_directory_is_fatal(tmp_path):
    with pytest.raises(FatalRunError, match="Directory not found"):
        list(source(tmp_path).read({}))


def test_non_utf8_file_is_fatal_and_named(data):
    (data / "data" / "materials" / "b.csv").write_bytes(b"material,description\n1,M\xfcller\n")
    with pytest.raises(FatalRunError, match="materials/b.csv is not valid UTF-8"):
        list(source(data).read({}))


def test_utf8_bom_is_stripped(data):
    (data / "data" / "materials" / "a.csv").write_bytes("﻿material\n1\n".encode())
    assert next(iter(source(data).read({})))[0]["material"] == "1"


def test_config_errors(data):
    with pytest.raises(CartageError, match="needs 'path'"):
        FilesystemSource({}, {"path": "*.csv"}, data)
    with pytest.raises(CartageError, match="Unsupported format 'xlsx'"):
        source(data, format="xlsx")


def test_check_connection_and_state_backend(data):
    assert "1 file" in FilesystemSource.check_connection({"path": "./data"}, data)
    backend = FilesystemSource.state_backend({"path": "./data"}, "state", data)
    assert isinstance(backend, LocalStateBackend)
    assert backend.root == (data / "data" / "state").resolve()
