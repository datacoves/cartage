"""Bundled BAPI interface metadata (parameters, fields, lengths, allowed values)."""
from __future__ import annotations

import functools
from dataclasses import dataclass
from pathlib import Path

from ruamel.yaml import YAML

from cartage.core import CartageError

META_DIR = Path(__file__).parent / "bapi_meta"


@dataclass(frozen=True)
class FieldMeta:
    length: int
    required: bool = False
    allowed: tuple[str, ...] | None = None


@dataclass(frozen=True)
class ParamMeta:
    kind: str  # "structure" | "table"
    fields: dict[str, FieldMeta]
    x_structure: str | None = None


@dataclass(frozen=True)
class BapiMeta:
    function: str
    object: str
    key: str  # PARAM.FIELD identifying the business object
    parameters: dict[str, ParamMeta]


def available() -> list[str]:
    return sorted(p.stem for p in META_DIR.glob("*.yaml"))


@functools.cache
def load_meta(function: str) -> BapiMeta:
    path = META_DIR / f"{function}.yaml"
    if not path.is_file():
        raise CartageError(f"Unknown BAPI '{function}'", hint=f"Bundled BAPI metadata: {', '.join(available())}")
    data = YAML(typ="safe").load(path.read_text(encoding="utf-8"))
    parameters = {
        name: ParamMeta(
            kind=param["kind"],
            fields={
                fname: FieldMeta(int(f["length"]), bool(f.get("required", False)),
                                 tuple(str(v) for v in f["allowed"]) if "allowed" in f else None)
                for fname, f in param["fields"].items()
            },
            x_structure=param.get("x_structure"),
        )
        for name, param in data["parameters"].items()
    }
    return BapiMeta(data["function"], data["object"], data["key"], parameters)
