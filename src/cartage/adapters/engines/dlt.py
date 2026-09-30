"""Run pipelines on dlt: source resource | transformer (cartage steps) → custom sink (destination.write)."""
from __future__ import annotations

import os
import shutil
from typing import Callable, Iterator

import dlt
from dlt.common.destination.exceptions import DestinationTerminalException

from cartage.core import CartageError, FatalRunError, PipelineState, RunResult
from cartage.transforms import Step, apply_steps


class DltEngine:
    def run(self, name: str, source, steps: list[Step], destination, state: PipelineState,
            on_progress: Callable[[RunResult], None]) -> RunResult:
        os.environ.setdefault("RUNTIME__DLTHUB_TELEMETRY", "false")
        os.environ.setdefault("RUNTIME__LOG_LEVEL", "CRITICAL")
        os.environ.setdefault("LOAD__DELETE_COMPLETED_JOBS", "true")  # the state archive must not keep loaded data
        result = RunResult()
        fatal: list[CartageError] = []  # dlt wraps exceptions; keep ours to re-raise with exit code intact

        def guarded(items: Iterator[list[dict]]) -> Iterator[list[dict]]:
            try:
                yield from items
            except CartageError as e:
                fatal.append(e)
                raise

        def page(items):
            items = items if isinstance(items, list) else [items]
            result.read += len(items)
            try:
                out = apply_steps([dict(i) for i in items], steps)
            except CartageError as e:
                fatal.append(e)
                raise
            result.filtered += out.filtered
            result.errors.extend(out.errors)
            if out.records:
                yield out.records

        # ponytail: dlt drops None-valued fields before the sink, unlike the python engine. Destinations must treat
        # a missing key and None the same (the SAP mapping does); preserve nulls explicitly if that ever matters.
        @dlt.destination(name="cartage_sink", batch_size=100, loader_file_format="typed-jsonl", max_parallel_load_jobs=1)
        def sink(items, table) -> None:
            try:
                result.add_batch(destination.write([dict(i) for i in items]))
            except CartageError as e:
                fatal.append(e)
                raise DestinationTerminalException(str(e)) from e
            except Exception as e:
                fatal.append(FatalRunError(f"Destination failed: {type(e).__name__}: {e}"))
                raise DestinationTerminalException(str(e)) from e
            on_progress(result)

        if hasattr(source, "dlt_resources"):
            resources = source.dlt_resources()
        else:
            resources = [dlt.resource(guarded(source.read(state.data)), name=name, max_table_nesting=0)]
        piped = [r | dlt.transformer(page, name=f"{r.name}__cartage", max_table_nesting=0) for r in resources]

        schema = dlt.Schema(name)
        schema.remove_type_detection("iso_timestamp")  # keep ISO strings as strings, same as the python engine
        pipeline = dlt.pipeline(pipeline_name=name, destination=sink, pipelines_dir=str(state.dlt_dir))
        pipeline.abort_packages()  # a failed earlier run must not be replayed from the reused dlt_dir
        try:
            pipeline.run(piped, schema=schema)
        except Exception as e:
            error = fatal[0] if fatal else FatalRunError(f"dlt pipeline failed: {e}")
            if isinstance(error, FatalRunError):
                error.result = result
            raise error from e
        # Loaded packages and the run trace hold copies of the data; keep only dlt state in the archive.
        shutil.rmtree(state.dlt_dir / name / "load" / "loaded", ignore_errors=True)
        (state.dlt_dir / name / "trace.pickle").unlink(missing_ok=True)
        return result
