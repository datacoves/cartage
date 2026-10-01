"""Run pipelines on dlt: source resource | transformer (cartage steps) → custom sink (destination.write),
or, for a dlt destination, straight into that destination (no sink; no transformer when there are no steps)."""
from __future__ import annotations

import os
import shutil
from typing import Callable, Iterator
from unittest.mock import patch

import dlt
from dlt.common.destination.exceptions import DestinationTerminalException

from cartage.core import CartageError, FatalRunError, PipelineState, RunResult, to_rows
from cartage.transforms import Step, apply_steps


class DltEngine:
    def run(self, name: str, source, steps: list[Step], destination, state: PipelineState,
            on_progress: Callable[[RunResult], None]) -> RunResult:
        os.environ.setdefault("RUNTIME__DLTHUB_TELEMETRY", "false")
        os.environ.setdefault("RUNTIME__LOG_LEVEL", "CRITICAL")
        os.environ.setdefault("LOAD__DELETE_COMPLETED_JOBS", "true")  # the state archive must not keep loaded data
        os.environ.setdefault("RESTORE_FROM_DESTINATION", "false")  # cartage's state store is the source of truth
        native = hasattr(destination, "dlt_destination")
        result = RunResult()
        fatal: list[CartageError] = []  # dlt wraps exceptions; keep ours to re-raise with exit code intact

        def guarded(items: Iterator[list[dict]]) -> Iterator[list[dict]]:
            try:
                yield from items
            except CartageError as e:
                fatal.append(e)
                raise

        def page(items):
            items = to_rows(items)
            result.read += len(items)
            try:
                out = apply_steps(items, steps)
            except CartageError as e:
                fatal.append(e)
                raise
            result.filtered += out.filtered
            result.errors.extend(out.errors)
            on_progress(result)
            if out.records:
                yield out.records

        # ponytail: dlt drops None-valued fields before the sink, unlike the python engine. Destinations must treat
        # a missing key and None the same (the SAP mapping does); preserve nulls explicitly if that ever matters.
        # ponytail: the sink re-chunks at batch_size=100, so per_batch commit groups can differ from the python engine
        # (which commits per source batch); pass the source batch size through if exact parity matters.
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

        def pipe(r):
            table = r.table_name  # the transformer must not rename the destination table
            if steps or not native:  # without steps, dlt destinations get the source's batches untouched (Arrow)
                r = r | dlt.transformer(page, name=f"{r.name}__cartage", max_table_nesting=0)
            if native:
                r.apply_hints(**{"table_name": table, **destination.hints})
            return r

        piped = [pipe(r) for r in resources]
        if native:
            target, dataset, file_format = destination.dlt_destination(), destination.dataset_name or name, destination.loader_file_format
        else:
            target, dataset, file_format = sink, None, None

        with patch.dict(os.environ, destination.dlt_env if native else {}):  # per-run tuning, restored afterwards
            schema = dlt.Schema(name)
            schema.remove_type_detection("iso_timestamp")  # keep ISO strings as strings, same as the python engine
            pipeline = dlt.pipeline(pipeline_name=name, destination=target, dataset_name=dataset,
                                    pipelines_dir=str(state.dlt_dir))
            pipeline.abort_packages()  # a failed earlier run must not be replayed from the reused dlt_dir
            try:
                pipeline.run(piped, schema=schema, loader_file_format=file_format)
            except Exception as e:
                error = fatal[0] if fatal else FatalRunError(f"dlt pipeline failed: {e}")
                if isinstance(error, FatalRunError):
                    error.result = result
                raise error from e
        if native:  # dlt loads whole jobs: everything normalized into top-level tables was loaded
            tables = pipeline.default_schema.tables
            counts = pipeline.last_trace.last_normalize_info.row_counts
            result.ok = result.sent = sum(n for t, n in counts.items()
                                          if not t.startswith("_dlt") and not tables.get(t, {}).get("parent"))
            result.read = result.read or result.ok
            on_progress(result)
            hooks = getattr(destination, "after_load", None)
            if hooks:
                try:
                    hooks(pipeline)
                except FatalRunError as e:
                    e.result = result
                    raise
        # Loaded packages and the run trace hold copies of the data; keep only dlt state in the archive.
        shutil.rmtree(state.dlt_dir / name / "load" / "loaded", ignore_errors=True)
        (state.dlt_dir / name / "trace.pickle").unlink(missing_ok=True)
        return result
