"""The simplest engine: a loop. No dependencies; the reference behaviour for other engines."""
from __future__ import annotations

from typing import Callable

from cartage.core import FatalRunError, PipelineState, RunResult
from cartage.transforms import Step, apply_steps


class PythonEngine:
    def run(self, name: str, source, steps: list[Step], destination, state: PipelineState,
            on_progress: Callable[[RunResult], None]) -> RunResult:
        result = RunResult()
        try:
            for batch in source.read(state.data):
                result.read += len(batch)
                out = apply_steps(batch, steps)
                result.filtered += out.filtered
                result.errors.extend(out.errors)
                if out.records:
                    result.add_batch(destination.write(out.records))
                on_progress(result)
        except FatalRunError as e:
            e.result = result
            raise
        return result
