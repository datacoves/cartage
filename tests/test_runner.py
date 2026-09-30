import json

import pytest

from cartage.config import load_project
from cartage.core import CartageError
from cartage.runner import prepare, run_pipeline

STATE = ".cartage/state/materials_to_sap/dev/state.json"


def run(project, **kwargs):
    return run_pipeline(prepare(load_project(project), "materials", engine="python"), **kwargs)


def test_demo_run_counts_and_rejects(project):
    result = run(project)
    assert (result.read, result.filtered, result.sent, result.ok, len(result.errors)) == (20, 2, 18, 16, 2)
    assert {e.source for e in result.errors} == {"materials/materials.csv:9", "materials/materials.csv:15"}
    assert result.state_advanced is False
    assert not (project / STATE).exists()
    assert len(open(result.rejects_path).read().splitlines()) == 2


def test_advance_state_then_incremental_skip_then_full_refresh(project):
    assert run(project, advance_state=True).state_advanced is True
    state = json.loads((project / STATE).read_text())
    assert list(state["files"]) == ["materials/materials.csv"]
    assert state["last_run"]["errors"] == 2
    again = run(project)
    assert (again.read, again.errors, again.state_advanced) == (0, [], True)
    assert run(project, full_refresh=True).read == 20


def test_progress_callback_receives_running_totals(project):
    seen = []
    run(project, on_progress=lambda r: seen.append(r.read))
    assert seen and seen[-1] == 20


def test_unknown_engine(project):
    with pytest.raises(CartageError, match="Unknown engine 'spark'"):
        prepare(load_project(project), "materials", engine="spark")


def test_state_connection_must_support_state(project):
    (project / "cartage.yaml").write_text((project / "cartage.yaml").read_text().replace(
        "dev: { path: .cartage/state }", "dev: { connection: sap_erp }"))
    with pytest.raises(CartageError, match="cannot store state"):
        prepare(load_project(project), "materials", engine="python")
