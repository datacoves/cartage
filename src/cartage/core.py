"""Shared types: errors, run results, state, and the adapter protocols."""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterator, Protocol


class CartageError(Exception):
    """A user-facing error: config, usage, or validation. Rendered as a panel, no traceback."""

    exit_code = 2

    def __init__(self, message: str, hint: str | None = None):
        super().__init__(message)
        self.message = message
        self.hint = hint


class FatalRunError(CartageError):
    """The run cannot continue: transport/connection failure, state store, or on_error: fail."""

    exit_code = 3


@dataclass
class RecordError:
    stage: str  # "transform" | "destination"
    message: str
    source: str | None = None  # e.g. "materials/materials.csv:9"
    details: list[dict] = field(default_factory=list)  # BAPIRET2 rows
    record: dict | None = None


@dataclass
class BatchResult:
    ok: int = 0
    warnings: int = 0  # subset of ok
    errors: list[RecordError] = field(default_factory=list)


@dataclass
class RunResult:
    read: int = 0
    filtered: int = 0
    sent: int = 0
    ok: int = 0
    warnings: int = 0
    errors: list[RecordError] = field(default_factory=list)
    state_advanced: bool = False
    rejects_path: str | None = None

    def add_batch(self, batch: BatchResult) -> None:
        self.sent += batch.ok + len(batch.errors)
        self.ok += batch.ok
        self.warnings += batch.warnings
        self.errors.extend(batch.errors)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class PipelineState:
    data: dict = field(default_factory=dict)  # cartage state, e.g. {"files": {...}, "last_run": {...}}
    dlt_dir: Path | None = None  # temporary dlt pipelines_dir restored from the state store


class Source(Protocol):
    def read(self, state: dict) -> Iterator[list[dict]]: ...


class Destination(Protocol):
    def preview(self, records: list[dict]) -> list[dict]: ...
    def write(self, batch: list[dict]) -> BatchResult: ...


class StateBackend(Protocol):
    def get(self, key: str) -> bytes | None: ...
    def put(self, key: str, data: bytes) -> None: ...
    def delete(self, key: str) -> None: ...


class Engine(Protocol):
    def run(self, name: str, source: Source, steps: list, destination: Destination,
            state: PipelineState, on_progress: Callable[[RunResult], None]) -> RunResult: ...


class Orchestrator(Protocol):
    def generate(self, project: Any, pipelines: list, out_dir: Path | None = None) -> dict[Path, str]: ...
