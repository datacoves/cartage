"""`cartage validate`: load everything a run would load, without moving data."""
from __future__ import annotations

from dataclasses import dataclass

from cartage.config import Project
from cartage.core import CartageError
from cartage.runner import prepare
from cartage.transforms import load_steps


@dataclass
class Check:
    label: str
    ok: bool
    detail: str = ""


def _failed(label: str, error: CartageError) -> Check:
    return Check(label, False, error.message + (f" (hint: {error.hint})" if error.hint else ""))


def check_pipeline(project: Project, ref: str, env: str | None) -> tuple[str, list[Check]]:
    try:
        pipeline = project.load_pipeline(ref)
    except CartageError as e:
        return str(ref), [_failed("pipeline file", e)]
    checks = [Check("pipeline file", True, project.rel(pipeline.path))]
    try:
        checks.append(Check("environment", True, project.resolve_env(env)))
    except CartageError as e:
        return pipeline.name, [*checks, _failed("environment", e)]
    try:
        checks.append(Check("transforms", True, f"{len(load_steps(pipeline.transforms, project.root))} step(s)"))
    except CartageError as e:
        return pipeline.name, [*checks, _failed("transforms", e)]
    try:
        prep = prepare(project, str(pipeline.path), env)
        checks.append(Check("connections, secrets and adapters", True,
                            f"{prep.source_type} → {prep.destination_type} · engine {prep.engine_name}"))
    except CartageError as e:
        checks.append(_failed("connections, secrets and adapters", e))
    return pipeline.name, checks
