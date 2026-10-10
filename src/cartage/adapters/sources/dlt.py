# src/cartage/adapters/sources/dlt.py
"""Any dlt source or resource, referenced as module:function."""
from __future__ import annotations

from collections.abc import Iterator, Mapping
from pathlib import Path

import dlt
from dlt.extract import DltResource, DltSource

from cartage.core import CartageError, to_rows
from cartage.transforms import import_ref

# Passed through to dlt.sources.incremental, e.g. lag: re-read a window before the last cursor value.
INCREMENTAL_OPTIONS = ("lag", "end_value", "primary_key", "row_order", "last_value_func", "on_cursor_value_missing",
                       "range_start", "range_end")
# What a `tables:` entry can set for one resource; all but incremental are dlt destination hints.
TABLE_HINTS = ("write_disposition", "primary_key", "merge_key", "columns")


def _incremental(value, where: str) -> dict | None:
    if not value:
        return None
    if not (isinstance(value, dict) and "cursor" in value):
        raise CartageError(f"{where} must be a mapping with 'cursor'",
                           hint='incremental: { cursor: updated_at, initial: "2024-01-01" }')
    unknown = sorted(set(value) - {"cursor", "initial", *INCREMENTAL_OPTIONS})
    if unknown:
        raise CartageError(f"Unknown {where} option(s): {', '.join(unknown)}",
                           hint=f"Supported: cursor, initial, {', '.join(INCREMENTAL_OPTIONS)}")
    return value


class DltSourceAdapter:
    def __init__(self, config: dict, options: dict, root: Path):
        """config: a `type: dlt` connection's settings for this environment, passed to the factory like `with:`
        (which wins), so a database's credentials live in one place; it may also hold the `ref`."""
        config = dict(config)
        self.ref = options.get("ref") or config.pop("ref", None)
        if not self.ref:
            raise CartageError("A dlt source needs 'ref'", hint="e.g. ref: sources.legacy_erp:materials")
        config.pop("ref", None)
        kwargs = options.get("with") or {}
        if not isinstance(kwargs, Mapping):
            raise CartageError("dlt source 'with' must be a mapping of factory keyword arguments")
        self.kwargs = {**config, **kwargs}
        self.resources = options.get("resources") or []
        if not isinstance(self.resources, list) or not all(isinstance(r, str) for r in self.resources):
            raise CartageError("dlt source 'resources' must be a list of resource names", hint="e.g. resources: [read_csv]")
        self.incremental = _incremental(options.get("incremental"), "dlt source incremental")
        self.tables = self._tables(options.get("tables") or {})
        known = self.resources or self.kwargs.get("table_names")  # checked here without calling the factory
        if isinstance(known, list):
            self._check_tables(known)
        try:
            self.batch_size = int(options.get("batch_size", 100))
        except (TypeError, ValueError) as error:
            raise CartageError("dlt source 'batch_size' must be a positive integer") from error
        if self.batch_size < 1:
            raise CartageError("dlt source 'batch_size' must be a positive integer")
        self.root = root

    @classmethod
    def check_connection(cls, config: dict, root: Path) -> str:
        ref = config.get("ref")
        return (f"settings for the dlt source {ref}" if ref else "settings for a dlt source (the pipeline gives ref:)") + \
            "; connectivity is checked on first read"

    def _tables(self, tables) -> dict[str, dict]:
        """Per-resource overrides: `tables: {clan_membership: {incremental: false, write_disposition: replace}}`."""
        if not isinstance(tables, Mapping) or not all(isinstance(v, Mapping) for v in tables.values()):
            raise CartageError("dlt source 'tables' must map resource names to settings",
                               hint="tables: { author: { incremental: false, write_disposition: replace } }")
        for name, entry in tables.items():
            unknown = sorted(set(entry) - {"incremental", *TABLE_HINTS})
            if unknown:
                raise CartageError(f"Unknown setting(s) for table '{name}': {', '.join(unknown)}",
                                   hint=f"Supported: incremental, {', '.join(TABLE_HINTS)}")
            _incremental(entry.get("incremental"), f"table '{name}' incremental")
        return dict(tables)

    def _check_tables(self, names) -> None:
        missing = sorted(set(self.tables) - set(names))
        if missing:
            raise CartageError(f"'tables' names no resource of '{self.ref}': {', '.join(missing)}",
                               hint=f"Resources: {', '.join(names)}")

    def table_hints(self, name: str) -> dict:
        """The destination hints a `tables:` entry sets for one resource (on top of the destination's)."""
        return {k: v for k, v in self.tables.get(name, {}).items() if k in TABLE_HINTS}

    def _resources(self) -> list[DltResource]:
        func = import_ref(self.ref, self.root)
        try:
            obj = func(**self.kwargs)
        except Exception as e:
            raise CartageError(f"Calling '{self.ref}' failed: {type(e).__name__}: {e}") from e
        if isinstance(obj, DltSource):
            if not self.resources:
                return list(obj.selected_resources.values())
            missing = [r for r in self.resources if r not in obj.resources]
            if missing:
                raise CartageError(f"'{self.ref}' has no resource(s): {', '.join(missing)}",
                                   hint=f"Resources: {', '.join(obj.resources)}")
            return [obj.resources[r] for r in self.resources]
        if isinstance(obj, DltResource):
            if self.resources and self.resources != [obj.name]:
                raise CartageError(f"'{self.ref}' returns the single resource '{obj.name}'; drop 'resources'")
            return [obj]
        raise CartageError(f"'{self.ref}' returned {type(obj).__name__}, expected a dlt source or resource")

    def dlt_resources(self) -> list[DltResource]:
        resources = self._resources()
        self._check_tables([r.name for r in resources])
        for r in resources:
            entry = self.tables.get(r.name, {})
            inc = _incremental(entry["incremental"], "") if "incremental" in entry else self.incremental
            if inc:
                options = {k: v for k, v in inc.items() if k in INCREMENTAL_OPTIONS}
                r.apply_hints(incremental=dlt.sources.incremental(inc["cursor"], initial_value=inc.get("initial"),
                                                                  **options))
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
