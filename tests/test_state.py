import json

from cartage.core import RecordError
from cartage.state import LocalStateBackend, StateStore, new_run_id, write_rejects


def test_local_backend_roundtrip(tmp_path):
    b = LocalStateBackend(tmp_path / "s")
    assert b.get("p/dev/state.json") is None
    b.put("p/dev/state.json", b"{}")
    assert b.get("p/dev/state.json") == b"{}"
    b.delete("p/dev/state.json")
    assert b.get("p/dev/state.json") is None


def test_empty_state(tmp_path):
    state = StateStore(LocalStateBackend(tmp_path), "p", "dev").load()
    assert state.data == {}
    assert state.dlt_dir.is_dir() and not any(state.dlt_dir.iterdir())


def test_save_and_restore_data_and_dlt_dir(tmp_path):
    store = StateStore(LocalStateBackend(tmp_path / "s"), "p", "dev")
    state = store.load()
    state.data["files"] = {"a.csv": "1-2"}
    (state.dlt_dir / "p").mkdir()
    (state.dlt_dir / "p" / "state.json").write_text('{"cursor": 5}')
    store.save(state)

    again = store.load()
    assert again.data == {"files": {"a.csv": "1-2"}}
    assert (again.dlt_dir / "p" / "state.json").read_text() == '{"cursor": 5}'
    assert again.dlt_dir != state.dlt_dir
    assert (tmp_path / "s" / "p" / "dev" / "state.json").is_file()
    assert store.show() == {"state": {"files": {"a.csv": "1-2"}}, "dlt_archive": True}


def test_envs_are_separate(tmp_path):
    backend = LocalStateBackend(tmp_path)
    dev = StateStore(backend, "p", "dev")
    state = dev.load()
    state.data["x"] = 1
    dev.save(state)
    assert StateStore(backend, "p", "prd").read_data() == {}


def test_reset(tmp_path):
    store = StateStore(LocalStateBackend(tmp_path), "p", "dev")
    state = store.load()
    state.data["x"] = 1
    store.save(state)
    store.reset()
    assert store.read_data() == {}
    assert store.show()["dlt_archive"] is False


def test_write_rejects(tmp_path):
    errors = [RecordError(stage="destination", message="bad type", source="m.csv:9", details=[{"TYPE": "E"}], record={"a": "1"})]
    path = write_rejects(tmp_path, "p", new_run_id(), errors)
    assert path.parent == tmp_path / ".cartage" / "rejects" / "p"
    [line] = path.read_text().splitlines()
    assert json.loads(line) == {"stage": "destination", "message": "bad type", "source": "m.csv:9",
                                "details": [{"TYPE": "E"}], "record": {"a": "1"}}
