# src/cartage/adapters/sources/dlt.py
"""Any dlt source or resource, referenced as module:function."""
from __future__ import annotations

import inspect
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


def _incremental(value, where: str, per_table: bool = False) -> dict | bool | None:
    """None or false: read everything. A source-level `true`: incremental, each table sets its own cursor."""
    if value is None or value is False:
        return None
    if value is True and per_table:
        return True
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
        self.incremental = _incremental(options.get("incremental"), "dlt source incremental", per_table=True)
        self.tables = self._tables(options.get("tables") or [])
        known = self._names()  # checked here without calling the factory
        if isinstance(known, list):
            self._check_tables(known)
            for name in known:
                self._incremental_for(name)
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

    def _tables(self, items) -> dict[str, dict]:
        """The tables to read, in order, each with its overrides: `tables: [family, {author: {incremental: false}}]`."""
        hint = "tables:\n  - family\n  - author:\n      incremental: false\n      write_disposition: replace"
        if not isinstance(items, list):
            raise CartageError("dlt source 'tables' must be a list of table names, each optionally with settings",
                               hint=hint)
        tables: dict[str, dict] = {}
        for item in items:
            if isinstance(item, Mapping) and len(item) == 1:
                (name, entry), = item.items()
                entry = {} if entry is None else entry
            else:
                name, entry = item, {}
            if not isinstance(name, str) or not isinstance(entry, Mapping):
                raise CartageError(f"dlt source 'tables' item {item!r} must be a table name or one name with settings "
                                   "(a mapping with several keys usually means the settings are not indented)",
                                   hint=hint)
            if name in tables:
                raise CartageError(f"dlt source 'tables' lists '{name}' twice")
            tables[name] = dict(entry)
        for name, entry in tables.items():
            unknown = sorted(set(entry) - {"incremental", *TABLE_HINTS})
            if unknown:
                raise CartageError(f"Unknown setting(s) for table '{name}': {', '.join(unknown)}",
                                   hint=f"Supported: incremental, {', '.join(TABLE_HINTS)}")
            _incremental(entry.get("incremental"), f"table '{name}' incremental")
        return tables

    def _names(self) -> list[str] | None:
        """The resources this pipeline reads, when known without calling the factory."""
        return self.resources or self.kwargs.get("table_names") or list(self.tables) or None

    def _incremental_for(self, name: str) -> tuple[dict | None, str]:
        """A table's incremental settings and where they come from."""
        entry = self.tables.get(name, {})
        if "incremental" in entry:
            return _incremental(entry["incremental"], ""), "tables"
        if self.incremental is True:
            raise CartageError(f"Table '{name}' has no incremental cursor: the source's 'incremental: true' means "
                               "each table sets its own",
                               hint=f"tables:\n  - {name}:\n      incremental: {{ cursor: updated_at }}   # or false")
        return self.incremental, "source.incremental"

    def check_loads(self, defaults: dict) -> None:
        """Reject incremental with write_disposition: replace (from a table or the destination): replace reloads the
        whole table, so the cursor does nothing. defaults: the destination's hints."""
        for name in self._names() or [None]:
            inc, inc_from = self._incremental_for(name) if name else (self.incremental, "source.incremental")
            entry = self.tables.get(name, {})
            disposition, wd_from = ((entry["write_disposition"], "tables") if "write_disposition" in entry
                                    else (defaults.get("write_disposition"), "destination"))
            if isinstance(disposition, Mapping):
                disposition = disposition.get("disposition")
            if inc and disposition == "replace":
                where = f"tables.{name}" if name else "source"
                raise CartageError(f"{where}: incremental (from {inc_from}) has no effect with write_disposition: "
                                   f"replace (from {wd_from})", hint="Set incremental: false, or use merge or append")

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
        kwargs, resources = self.kwargs, self.resources
        if self.tables and not resources and "table_names" not in kwargs:  # tables: is the selection
            try:
                takes_names = "table_names" in inspect.signature(func).parameters
            except (TypeError, ValueError):
                takes_names = False
            if takes_names:  # e.g. sql_database: reflect only these tables
                kwargs = {**kwargs, "table_names": list(self.tables)}
            else:
                resources = list(self.tables)
        try:
            obj = func(**kwargs)
        except Exception as e:
            raise CartageError(f"Calling '{self.ref}' failed: {type(e).__name__}: {e}") from e
        if isinstance(obj, DltSource):
            if not resources:
                return list(obj.selected_resources.values())
            missing = [r for r in resources if r not in obj.resources]
            if missing:
                raise CartageError(f"'{self.ref}' has no resource(s): {', '.join(missing)}",
                                   hint=f"Resources: {', '.join(obj.resources)}")
            return [obj.resources[r] for r in resources]
        if isinstance(obj, DltResource):
            if resources and resources != [obj.name]:
                raise CartageError(f"'{self.ref}' returns the single resource '{obj.name}'; drop 'resources'")
            return [obj]
        raise CartageError(f"'{self.ref}' returned {type(obj).__name__}, expected a dlt source or resource")

    def dlt_resources(self) -> list[DltResource]:
        resources = self._resources()
        self._check_tables([r.name for r in resources])
        for r in resources:
            inc, _ = self._incremental_for(r.name)
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
