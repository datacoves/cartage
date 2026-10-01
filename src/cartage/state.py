"""Per-pipeline, per-env state (cartage JSON + dlt working dir archive) and rejected-record files."""
from __future__ import annotations

import io
import json
import tarfile
import tempfile
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

from cartage.core import PipelineState, RecordError, StateBackend


class LocalStateBackend:
    def __init__(self, root: Path):
        self.root = Path(root)

    def get(self, key: str) -> bytes | None:
        path = self.root / key
        return path.read_bytes() if path.is_file() else None

    def put(self, key: str, data: bytes) -> None:
        path = self.root / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)

    def delete(self, key: str) -> None:
        (self.root / key).unlink(missing_ok=True)


class StateStore:
    def __init__(self, backend: StateBackend, pipeline: str, env: str):
        self.backend = backend
        self.data_key = f"{pipeline}/{env}/state.json"
        self.dlt_key = f"{pipeline}/{env}/dlt.tar.gz"

    def read_data(self) -> dict:
        raw = self.backend.get(self.data_key)
        return json.loads(raw) if raw else {}

    def empty(self) -> PipelineState:
        return PipelineState({}, Path(tempfile.mkdtemp(prefix="cartage-dlt-")))

    def load(self) -> PipelineState:
        state = PipelineState(self.read_data(), Path(tempfile.mkdtemp(prefix="cartage-dlt-")))
        archive = self.backend.get(self.dlt_key)
        if archive:
            with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as tar:
                tar.extractall(state.dlt_dir, filter="data")
        return state

    def save(self, state: PipelineState) -> None:
        self.backend.put(self.data_key, json.dumps(state.data, indent=2, sort_keys=True).encode())
        if state.dlt_dir is not None and any(state.dlt_dir.iterdir()):
            buf = io.BytesIO()
            with tarfile.open(fileobj=buf, mode="w:gz") as tar:
                tar.add(state.dlt_dir, arcname=".")
            self.backend.put(self.dlt_key, buf.getvalue())

    def show(self) -> dict:
        return {"state": self.read_data(), "dlt_archive": self.backend.get(self.dlt_key) is not None}

    def reset(self) -> None:
        self.backend.delete(self.data_key)
        self.backend.delete(self.dlt_key)


def new_run_id() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")


def write_rejects(root: Path, pipeline: str, run_id: str, errors: list[RecordError]) -> Path:
    path = root / ".cartage" / "rejects" / pipeline / f"{run_id}.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for error in errors:
            f.write(json.dumps(asdict(error), default=str, ensure_ascii=False) + "\n")
    return path
