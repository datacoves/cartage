"""Python transform steps referenced from YAML as module:function."""
from __future__ import annotations

import importlib
import inspect
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from cartage.core import CartageError, FatalRunError, RecordError

SIGNATURES = {
    "map": "def f(record: dict, **with) -> dict",
    "filter": "def f(record: dict, **with) -> bool",
    "batch": "def f(records: list[dict], **with) -> Iterable[dict]",
}


def _forget_other_project(top: str, root: Path) -> None:
    """Drop a cached top-level module (e.g. `transforms`) that was imported from a different project root."""
    cached = sys.modules.get(top)
    location = getattr(cached, "__file__", None)
    if location and (root / top).exists() and not Path(location).resolve().is_relative_to(root.resolve()):
        for name in [n for n in sys.modules if n == top or n.startswith(top + ".")]:
            del sys.modules[name]


def import_ref(ref: str, root: Path) -> Callable:
    module_name, _, attr = ref.partition(":")
    if not module_name or not attr:
        raise CartageError(f"Invalid reference '{ref}'", hint="Use 'module.path:function', e.g. transforms.materials:normalize_uom")
    root_str = str(root)
    if root_str in sys.path:
        sys.path.remove(root_str)
    sys.path.insert(0, root_str)
    _forget_other_project(module_name.split(".")[0], root)
    try:
        module = importlib.import_module(module_name)
    except Exception as e:  # ImportError, SyntaxError, or anything raised at import time
        raise CartageError(f"Cannot import module '{module_name}' for '{ref}': {type(e).__name__}: {e}",
                           hint=f"Modules are looked up in {root} and installed packages") from e
    func = getattr(module, attr, None)
    if not callable(func):
        raise CartageError(f"'{attr}' not found or not callable in module '{module_name}'")
    return func


@dataclass
class Step:
    kind: str
    ref: str
    func: Callable
    kwargs: dict[str, Any]
    on_error: str


def load_steps(specs: list, root: Path) -> list[Step]:
    steps = []
    for spec in specs:
        func = import_ref(spec.ref, root)
        try:
            sig = inspect.signature(func)
        except (TypeError, ValueError):
            sig = None  # builtins without a signature: trust them
        if sig is not None:
            try:
                sig.bind([] if spec.kind == "batch" else {}, **spec.with_)
            except TypeError as e:
                raise CartageError(f"Transform '{spec.ref}' cannot be used as a {spec.kind} step: {e}",
                                   hint=SIGNATURES[spec.kind]) from e
        steps.append(Step(spec.kind, spec.ref, func, dict(spec.with_), spec.on_error))
    return steps


@dataclass
class StepOutput:
    records: list[dict]
    filtered: int = 0
    errors: list[RecordError] = field(default_factory=list)


def _record_error(step: Step, record: dict, message: str) -> RecordError:
    return RecordError(stage="transform", message=f"{step.ref}: {message}", source=record.get("_source"), record=record)


def apply_steps(records: list[dict], steps: list[Step]) -> StepOutput:
    out = StepOutput(list(records))
    for step in steps:
        current, kept = out.records, []
        if step.kind == "batch":
            try:
                kept = list(step.func(current, **step.kwargs))
            except Exception as e:
                if step.on_error == "fail":
                    raise FatalRunError(f"Transform {step.ref} failed (on_error: fail): {type(e).__name__}: {e}") from e
                out.errors.extend(_record_error(step, r, f"{type(e).__name__}: {e}") for r in current)
                kept = []
            else:
                out.filtered += max(len(current) - len(kept), 0)
        else:
            for record in current:
                try:
                    value = step.func(record, **step.kwargs)
                except Exception as e:
                    if step.on_error == "fail":
                        raise FatalRunError(f"Transform {step.ref} failed (on_error: fail): {type(e).__name__}: {e}") from e
                    out.errors.append(_record_error(step, record, f"{type(e).__name__}: {e}"))
                    continue
                if step.kind == "map":
                    if isinstance(value, dict):
                        kept.append(value)
                    else:
                        out.errors.append(_record_error(step, record, f"a map step must return a dict, got {type(value).__name__}"))
                elif value:
                    kept.append(record)
                else:
                    out.filtered += 1
        out.records = kept
    return out
