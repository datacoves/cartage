import os

import pytest

from cartage.scaffold import init_project

# Rich reads COLUMNS when the console is created; keep test output on one line per row.
os.environ.setdefault("COLUMNS", "200")

INCREMENTAL = "  incremental: true          # only files changed since the last run\n"


@pytest.fixture
def project(tmp_path):
    """The `cartage init` demo, without incremental reads so it runs on both engines; see incremental()."""
    root = tmp_path / "demo"
    init_project(root)
    pipeline = root / "pipelines/materials.yaml"
    assert INCREMENTAL in pipeline.read_text()
    pipeline.write_text(pipeline.read_text().replace(INCREMENTAL, ""))
    return root


def incremental(project):
    """Turn the demo's incremental file reads back on (dlt engine only)."""
    pipeline = project / "pipelines/materials.yaml"
    pipeline.write_text(pipeline.read_text().replace("  path: materials/*.csv\n", "  path: materials/*.csv\n" + INCREMENTAL))
    return project
