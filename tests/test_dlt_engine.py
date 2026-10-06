import pytest

from cartage.adapters.engines.dlt import DltEngine
from cartage.core import BatchResult, FatalRunError, PipelineState


class Src:
    def read(self, state):
        yield [{"a": "1"}, {"a": "2"}]


class Dest:
    def __init__(self):
        self.calls = 0

    def write(self, batch):
        self.calls += 1
        if self.calls == 1:
            raise FatalRunError("boom")
        return BatchResult(ok=len(batch))


def test_failed_run_does_not_replay_in_reused_dlt_dir(tmp_path):
    state, dest = PipelineState(dlt_dir=tmp_path), Dest()
    with pytest.raises(FatalRunError):
        DltEngine().run("p", Src(), [], dest, state, lambda r: None)
    result = DltEngine().run("p", Src(), [], dest, state, lambda r: None)
    assert (result.read, result.ok) == (2, 2)


CLEAN_CSV = "material,industry,type,description,uom,status\n100001,M,FERT,Pump housing,EA,active\n100002,M,ROH,Steel,KG,active\n"


def test_state_archive_keeps_no_loaded_data(project):
    import json
    import tarfile

    from conftest import incremental
    from typer.testing import CliRunner

    from cartage.cli import app

    incremental(project)
    (project / "data/materials/materials.csv").write_text(CLEAN_CSV)
    run = lambda: CliRunner().invoke(app, ["-C", str(project), "run", "materials", "--engine", "dlt", "--json"])
    first = run()
    assert first.exit_code == 0, first.output
    second = run()
    assert second.exit_code == 0 and json.loads(second.stdout)["read"] == 0
    with tarfile.open(project / ".cartage/state/materials_to_sap/dev/dlt.tar.gz") as tar:
        names = tar.getnames()
    assert names and not [n for n in names if "completed_jobs" in n or "/load/loaded" in n or n.endswith("trace.pickle")], names
