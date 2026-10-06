import functools
import http.server
import threading

import pytest

from cartage.core import CartageError
from cartage.init.answers import InitAnswers, Origin, fetch_files, load_answers, safe_relative

ANSWERS = """\
environments: [dev, prd]
source: files
files: {location: "s3://b/materials", path: "materials/*.csv"}
destination: sap_bapi
sample_data: {path: data/m.csv, reader_options: {dtype: str}, incremental: true}
copy: [transforms/t.py]
"""


@pytest.fixture
def folder(tmp_path):
    (tmp_path / "answers.yaml").write_text(ANSWERS)
    (tmp_path / "data").mkdir()
    (tmp_path / "data" / "m.csv").write_text("material\n1\n")
    (tmp_path / "transforms").mkdir()
    (tmp_path / "transforms" / "t.py").write_text("def f(r):\n    return r\n")
    return tmp_path


def test_defaults_and_pipeline_name():
    a = InitAnswers()
    assert (a.environments, a.source, a.destination, a.sample_data) == (["dev", "prd"], "files", "duckdb", None)
    assert a.pipeline_name == "files_to_duckdb"
    assert InitAnswers(destination="sources.lake:factory").pipeline_name == "files_to_sources_lake_factory"


def test_sample_data_accepts_a_path_or_a_mapping():
    assert InitAnswers(sample_data="x/orders.CSV").sample_data.file_format == "csv"
    assert InitAnswers(sample_data="").sample_data is None
    assert InitAnswers(sample_data={"path": "https://h/p/o.jsonl"}).sample_data.file_name == "o.jsonl"
    with pytest.raises(CartageError, match="format"):
        _ = InitAnswers(sample_data="notes.txt").sample_data.file_format


def test_environments_must_be_unique_names():
    for bad in (["dev", "dev"], ["dev", ""], ["has space"]):
        with pytest.raises(ValueError):
            InitAnswers(environments=bad)


def test_load_from_a_path_tracks_what_was_answered_and_fetches_relative_files(folder):
    answers, answered, origin = load_answers(str(folder / "answers.yaml"))
    assert {"environments", "source", "files", "destination", "sample_data", "copy_"} <= answered
    assert "schedule" not in answered
    files = fetch_files(answers, origin, origin)
    assert files == {"data/sample/m.csv": b"material\n1\n", "transforms/t.py": b"def f(r):\n    return r\n"}


def test_a_misspelled_key_is_named(tmp_path):
    (tmp_path / "a.yaml").write_text("destinaton: duckdb\n")
    with pytest.raises(CartageError, match="destinaton"):
        load_answers(str(tmp_path / "a.yaml"))


def test_load_from_a_url(folder):
    handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=str(folder))
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        url = f"http://127.0.0.1:{server.server_port}/answers.yaml"
        answers, _, origin = load_answers(url)
        assert origin.is_url
        assert fetch_files(answers, origin, origin)["data/sample/m.csv"] == b"material\n1\n"
        with pytest.raises(CartageError, match="missing.csv"):
            fetch_files(InitAnswers(sample_data="missing.csv"), origin, origin)
    finally:
        server.shutdown()


def test_missing_sample_names_the_path(tmp_path):
    with pytest.raises(CartageError, match="nope.csv"):
        fetch_files(InitAnswers(sample_data="nope.csv"), Origin.cwd(), Origin(str(tmp_path / "_")))


@pytest.mark.parametrize("bad", ["../x.py", "/etc/passwd", "a/../../x", "https://h/x.py", ""])
def test_copy_targets_stay_inside_the_project(bad):
    with pytest.raises(CartageError, match="relative path"):
        safe_relative(bad)
