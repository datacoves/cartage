# src/cartage/adapters/sources/dlt.py
"""Any dlt source or resource, referenced as module:function."""
from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Iterator

import dlt
from dlt.extract import DltResource, DltSource

from cartage.core import CartageError, to_rows
from cartage.transforms import import_ref

# Passed through to dlt.sources.incremental, e.g. lag: re-read a window before the last cursor value.
INCREMENTAL_OPTIONS = ("lag", "end_value", "primary_key", "row_order", "last_value_func", "on_cursor_value_missing",
                       "range_start", "range_end")


class DltSourceAdapter:
    def __init__(self, config: dict, options: dict, root: Path):
        if not options.get("ref"):
            raise CartageError("A dlt source needs 'ref'", hint="e.g. ref: sources.legacy_erp:materials")
        self.ref = options["ref"]
        self.kwargs = options.get("with") or {}
        if not isinstance(self.kwargs, Mapping):
            raise CartageError("dlt source 'with' must be a mapping of factory keyword arguments")
        self.incremental = options.get("incremental") or None
        if self.incremental is not None and not (isinstance(self.incremental, dict) and "cursor" in self.incremental):
            raise CartageError("dlt source 'incremental' must be a mapping with 'cursor'",
                               hint='incremental: { cursor: updated_at, initial: "2024-01-01" }')
        unknown = sorted(set(self.incremental or {}) - {"cursor", "initial", *INCREMENTAL_OPTIONS})
        if unknown:
            raise CartageError(f"Unknown dlt source incremental option(s): {', '.join(unknown)}",
                               hint=f"Supported: cursor, initial, {', '.join(INCREMENTAL_OPTIONS)}")
        try:
            self.batch_size = int(options.get("batch_size", 100))
        except (TypeError, ValueError) as error:
            raise CartageError("dlt source 'batch_size' must be a positive integer") from error
        if self.batch_size < 1:
            raise CartageError("dlt source 'batch_size' must be a positive integer")
        self.root = root

    def _resources(self) -> list[DltResource]:
        func = import_ref(self.ref, self.root)
        try:
            obj = func(**self.kwargs)
        except Exception as e:
            raise CartageError(f"Calling '{self.ref}' failed: {type(e).__name__}: {e}") from e
        if isinstance(obj, DltSource):
            return list(obj.selected_resources.values())
        if isinstance(obj, DltResource):
            return [obj]
        raise CartageError(f"'{self.ref}' returned {type(obj).__name__}, expected a dlt source or resource")

    def dlt_resources(self) -> list[DltResource]:
        resources = self._resources()
        if self.incremental:
            for r in resources:
                options = {k: v for k, v in self.incremental.items() if k in INCREMENTAL_OPTIONS}
                r.apply_hints(incremental=dlt.sources.incremental(self.incremental["cursor"],
                                                                  initial_value=self.incremental.get("initial"), **options))
        return resources

    def read(self, state: dict) -> Iterator[list[dict]]:
        batch: list[dict] = []
        for resource in self._resources():
            for item in resource:
                for row in to_rows(item):
                    batch.append(row)
                    if len(batch) >= self.batch_size:
                        yield batch
                        batch = []
        if batch:
            yield batch
