"""Record → BAPI parameters. Targets: PARAM.FIELD (structure) or PARAM[].FIELD (one table row)."""
from __future__ import annotations

import datetime
import re
from dataclasses import dataclass
from typing import Any

from cartage.adapters.destinations.sap.meta import BapiMeta
from cartage.core import CartageError

TARGET = re.compile(r"^([A-Z0-9_]+)(\[\])?\.([A-Z0-9_]+)$")


@dataclass(frozen=True)
class Target:
    param: str
    table: bool
    field: str


def parse_target(text: str) -> Target | None:
    m = TARGET.match(str(text).strip())
    return Target(m[1], bool(m[2]), m[3]) if m else None


def to_sap(value: Any) -> str:
    if isinstance(value, bool):
        return "X" if value else ""
    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.strftime("%Y%m%d")  # SAP DATS
    return str(value)


class Mapping:
    def __init__(self, fields: dict[str, str], constants: dict[str, Any], meta: BapiMeta):
        self.meta = meta
        problems: list[str] = []
        self.fields = [(src, self._target(tgt, problems)) for src, tgt in fields.items()]
        self.constants = [(self._target(tgt, problems), value) for tgt, value in constants.items()]
        if problems:
            raise CartageError(f"Invalid mapping for {meta.function}:\n" + "\n".join(f"  - {p}" for p in problems),
                               hint=f"Parameters: {', '.join(meta.parameters)}")

    def _target(self, text: str, problems: list[str]) -> Target | None:
        t = parse_target(text)
        if t is None:
            problems.append(f"'{text}' is not PARAM.FIELD or PARAM[].FIELD")
            return None
        param = self.meta.parameters.get(t.param)
        if param is None:
            problems.append(f"{self.meta.function} has no parameter {t.param}")
        elif param.kind == "table" and not t.table:
            problems.append(f"{t.param} is a table parameter: write {t.param}[].{t.field}")
        elif param.kind == "structure" and t.table:
            problems.append(f"{t.param} is a structure: write {t.param}.{t.field}")
        elif t.field not in param.fields:
            problems.append(f"{t.param} has no field {t.field} (fields: {', '.join(param.fields)})")
        else:
            return t
        return None

    def build(self, record: dict) -> dict:
        params: dict[str, Any] = {}

        def put(t: Target, value: str) -> None:
            if t.table:
                params.setdefault(t.param, [{}])[0][t.field] = value
            else:
                params.setdefault(t.param, {})[t.field] = value

        for src, target in self.fields:
            value = record.get(src)
            if value is None or value == "":
                continue
            put(target, to_sap(value))
        for target, value in self.constants:
            put(target, to_sap(value))
        for name, param in self.meta.parameters.items():
            if param.x_structure and name in params:
                params[param.x_structure] = {f: "X" for f in params[name]}
        return params
