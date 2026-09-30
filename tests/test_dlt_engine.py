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
