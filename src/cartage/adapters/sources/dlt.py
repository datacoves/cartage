# src/cartage/adapters/sources/dlt.py
"""Any dlt source or resource, referenced as module:function."""
from __future__ import annotations

from pathlib import Path
from typing import Iterator

import dlt
from dlt.extract import DltResource, DltSource

from cartage.core import CartageError
from cartage.transforms import import_ref


class DltSourceAdapter:
    def __init__(self, config: dict, options: dict, root: Path):
        if not options.get("ref"):
            raise CartageError("A dlt source needs 'ref'", hint="e.g. ref: sources.legacy_erp:materials")
        self.ref = options["ref"]
        self.kwargs = options.get("with") or {}
        self.incremental = options.get("incremental") or None
        if self.incremental is not None and not (isinstance(self.incremental, dict) and "cursor" in self.incremental):
            raise CartageError("dlt source 'incremental' must be a mapping with 'cursor'",
                               hint='incremental: { cursor: updated_at, initial: "2024-01-01" }')
        self.batch_size = int(options.get("batch_size", 100))
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
                r.apply_hints(incremental=dlt.sources.incremental(self.incremental["cursor"],
                                                                  initial_value=self.incremental.get("initial")))
        return resources

    def read(self, state: dict) -> Iterator[list[dict]]:
        batch: list[dict] = []
        for resource in self._resources():
            for item in resource:
                batch.append(dict(item))
                if len(batch) >= self.batch_size:
                    yield batch
                    batch = []
        if batch:
            yield batch
