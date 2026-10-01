"""Any dlt destination (filesystem, duckdb, postgres, snowflake, ...), loaded natively by the dlt engine."""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import dlt

from cartage.core import CartageError, FatalRunError
from cartage.secrets import mask
from cartage.transforms import import_ref

DISPOSITIONS = ("append", "replace", "merge")


def dlt_env(config: dict) -> dict[str, str]:
    """dlt config keys as the env vars dlt reads: data_writer.buffer_max_items → DATA_WRITER__BUFFER_MAX_ITEMS."""
    return {k.upper().replace(".", "__"): v if isinstance(v, str) else json.dumps(v) for k, v in config.items()}


def _hook(spec: Any, root: Path) -> tuple[str, Any, dict]:
    """`module:function` or { ref: module:function, with: {...} }; the function gets (pipeline, **with)."""
    ref, kwargs = (spec, {}) if isinstance(spec, str) else (spec.get("ref"), spec.get("with") or {})
    if not isinstance(ref, str) or not isinstance(kwargs, dict):
        raise CartageError(f"Invalid after_load entry {spec!r}", hint="Use module:function or { ref: module:function, with: {...} }")
    return ref, import_ref(ref, root), kwargs


class DltDestinationAdapter:
    """Not a record sink: the dlt engine runs the whole pipeline into this destination (bulk load, staging,
    merge, schema evolution). Failures are per load job, so they stop the run instead of rejecting records."""

    def __init__(self, config: dict, options: dict, root: Path):
        settings = dict(config)
        self.name = settings.pop("destination", None)
        if not self.name:
            raise CartageError("A dlt destination connection needs 'destination'",
                               hint="e.g. destination: filesystem, duckdb, postgres or snowflake")
        # Tuning: connection-level defaults, pipeline-level overrides.
        dataset = settings.pop("dataset_name", None)
        naming = options.get("naming") or settings.pop("naming", None)
        tuning = {**(settings.pop("dlt_config", None) or {}), **(options.get("dlt_config") or {})}
        if naming:
            tuning["schema.naming"] = naming  # dlt built-in (direct, sql_ci_v1, ...) or a project module
            if str(root) not in sys.path:
                sys.path.insert(0, str(root))
        self.dlt_env = dlt_env(tuning)

        disposition = options.get("write_disposition")  # unset: the source resource's own hint, else dlt's append
        if disposition is not None and disposition not in DISPOSITIONS:
            raise CartageError(f"Unsupported dlt write_disposition '{disposition}'", hint=f"Use {', '.join(DISPOSITIONS)}")
        self.hints = {k: options[k] for k in ("table_name", "write_disposition", "primary_key", "merge_key", "columns")
                      if options.get(k)}
        self.after_load_hooks = [_hook(h, root) for h in options.get("after_load") or []]
        self.dataset_name = options.get("dataset_name") or dataset
        self.loader_file_format = options.get("loader_file_format")
        self.destination = self._make(settings, root)

    def _make(self, settings: dict, root: Path):
        if ":" in self.name:
            factory = import_ref(self.name, root)
        else:
            factory = getattr(dlt.destinations, self.name, None)
            if not callable(factory):
                raise CartageError(f"Unknown dlt destination '{self.name}'",
                                   hint="Use a dlt.destinations name or a module:function factory")
        try:
            return factory(**settings)
        except Exception as e:
            raise CartageError(f"Cannot configure dlt destination '{self.name}': {type(e).__name__}: {mask(str(e))}",
                               hint=f"Check the connection settings; drivers come from extras, e.g. dlt[{self.name}]") from e

    @classmethod
    def check_connection(cls, config: dict, root: Path) -> str:
        cls(config, {}, root)
        return f"dlt destination '{config['destination']}' configured; connectivity is checked on first load"

    def dlt_destination(self):
        return self.destination

    def after_load(self, pipeline) -> None:
        """Post-load steps (grants, tags, change tracking), called with the dlt pipeline after a successful load."""
        for ref, func, kwargs in self.after_load_hooks:
            try:
                func(pipeline, **kwargs)
            except Exception as e:
                raise FatalRunError(f"after_load '{ref}' failed: {type(e).__name__}: {mask(str(e))}",
                                    hint="The data is loaded; state is not saved, so the next run loads it again") from e

    def preview(self, records: list[dict]) -> list[dict]:
        return []  # rows load as transformed; there is no separate payload to show
