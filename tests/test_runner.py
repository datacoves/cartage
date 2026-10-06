import json
from pathlib import Path

import pytest

from cartage.config import load_project
from cartage.core import CartageError, FatalRunError, RecordError, RunResult
from cartage.runner import prepare, run_pipeline

STATE = ".cartage/state/materials/dev/state.json"


def run(project, **kwargs):
    return run_pipeline(prepare(load_project(project), "materials", engine="python"), **kwargs)


def test_demo_run_counts_and_rejects(project):
    result = run(project)
    assert (result.read, result.filtered, result.sent, result.ok, len(result.errors)) == (20, 2, 18, 16, 2)
    assert {e.record["material"] for e in result.errors} == {"100008", "1000140000000000000001"}
    assert result.state_advanced is False
    assert not (project / STATE).exists()
    assert len(Path(result.rejects_path).read_text().splitlines()) == 2


def test_advance_state_then_incremental_skip_then_full_refresh(project):
    from conftest import incremental

    incremental(project)
    dlt = {"engine": "dlt"}
    assert run_pipeline(prepare(load_project(project), "materials", **dlt), advance_state=True).state_advanced is True
    assert json.loads((project / STATE).read_text())["last_run"]["errors"] == 2
    again = run_pipeline(prepare(load_project(project), "materials", **dlt))
    assert (again.read, again.errors, again.state_advanced) == (0, [], True)
    assert run_pipeline(prepare(load_project(project), "materials", **dlt), full_refresh=True).read == 20


def test_progress_callback_receives_running_totals(project):
    seen = []
    run(project, on_progress=lambda r: seen.append(r.read))
    assert seen and seen[-1] == 20


def test_unknown_engine(project):
    with pytest.raises(CartageError, match="Unknown engine 'spark'"):
        prepare(load_project(project), "materials", engine="spark")


def test_state_connection_must_support_state(project):
    (project / "cartage.yaml").write_text((project / "cartage.yaml").read_text().replace(
        "dev: {path: .cartage/state}", "dev: { connection: sap }"))
    with pytest.raises(CartageError, match="cannot store state"):
        prepare(load_project(project), "materials", engine="python")


class _FailingEngine:
    def run(self, *args):
        e = FatalRunError("SAP down")
        e.result = RunResult(read=5, sent=3, ok=2, errors=[RecordError(stage="destination", message="bad")])
        raise e


def test_fatal_run_writes_partial_rejects_and_keeps_state(project):
    prep = prepare(load_project(project), "materials", engine="python")
    prep.engine = _FailingEngine()
    with pytest.raises(FatalRunError) as info:
        run_pipeline(prep, advance_state=True)
    assert len(Path(info.value.result.rejects_path).read_text().splitlines()) == 1
    assert not (project / STATE).exists()
