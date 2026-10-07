import os
from pathlib import Path

import pytest
from typer.testing import CliRunner

from cartage.cli import app

# Rich reads COLUMNS when the console is created; keep test output on one line per row.
os.environ.setdefault("COLUMNS", "200")

EXAMPLE = Path(__file__).resolve().parents[1] / "examples" / "sap" / "answers.yaml"
INCREMENTAL = "      incremental: true\n"


@pytest.fixture(autouse=True)
def home(tmp_path, monkeypatch):
    """An empty home folder: no ~/.cartage/secrets.yaml (or ~/.dlt) from the machine running the tests."""
    path = tmp_path / "home"
    path.mkdir()
    monkeypatch.setenv("HOME", str(path))
    return path


@pytest.fixture
def project(tmp_path):
    """The SAP example from `cartage init --answers`, without incremental reads so it runs on both engines."""
    root = tmp_path / "demo"
    result = CliRunner().invoke(app, ["init", str(root), "--answers", str(EXAMPLE), "--yes"])
    assert result.exit_code == 0, result.output
    pipeline = root / "pipelines/materials.yaml"
    assert INCREMENTAL in pipeline.read_text()
    pipeline.write_text(pipeline.read_text().replace(INCREMENTAL, ""))
    return root


def incremental(project):
    """Turn the example's incremental sample reads back on (dlt engine only)."""
    pipeline = project / "pipelines/materials.yaml"
    pipeline.write_text(pipeline.read_text().replace(
        "      path: materials.csv\n", "      path: materials.csv\n" + INCREMENTAL))
    return project
