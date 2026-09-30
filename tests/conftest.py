import os

import pytest

from cartage.scaffold import init_project

# Rich reads COLUMNS when the console is created; keep test output on one line per row.
os.environ.setdefault("COLUMNS", "200")


@pytest.fixture
def project(tmp_path):
    root = tmp_path / "demo"
    init_project(root)
    return root
