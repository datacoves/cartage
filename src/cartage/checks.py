"""`cartage validate`: load everything a run would load, without moving data."""
from __future__ import annotations

from dataclasses import dataclass

from cartage.config import Project
from cartage.core import CartageError
from cartage.runner import prepare_all
from cartage.secrets import FILL_ME, REF, Secrets, mask
from cartage.transforms import load_steps


@dataclass
class Check:
    label: str
    ok: bool
    detail: str = ""


def _failed(label: str, error: CartageError) -> Check:
    return Check(label, False, mask(error.message + (f" (hint: {error.hint})" if error.hint else "")))


def unfilled(project: Project, pipeline, env: str) -> list[str]:
    """Every "<fill me>" a run in `env` would hit: its connections' settings, the pipeline's options, and the secrets
    they reference. A run stops at the first; validate lists them all."""
    secrets, found = Secrets(project.root, env=env), []

    def walk(value, where: str) -> None:
        if isinstance(value, dict):
            for key, item in value.items():
                walk(item, f"{where}.{key}")
        elif isinstance(value, list):
            for i, item in enumerate(value):
                walk(item, f"{where}[{i}]")
        elif isinstance(value, str):
            if value == FILL_ME:
                found.append(where)
            for kind, key in REF.findall(value):
                key = key.replace("{env}", env)
                if kind == "secret" and secrets.peek(key) == FILL_ME:
                    found.append(f"{key} (secret)")

    spec = pipeline.source_for(env)
    for name in sorted({spec.connection, *(d.connection for d in pipeline.destination_specs)} - {None}):
        conn = project.connections.get(name)
        if conn is not None and env in conn.envs:
            walk(conn.envs[env], f"{name}.{env}")
    walk(spec.options(), "source")
    for d in pipeline.destination_specs:
        walk(d.options(), f"destination {d.key}")
    return found


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
        missing = unfilled(project, pipeline, project.resolve_env(env))
    except CartageError as e:
        return pipeline.name, [*checks, _failed("placeholders", e)]
    if missing:
        return pipeline.name, [*checks, Check("placeholders", False,
                                              f'{len(missing)} still "{FILL_ME}": {", ".join(missing)}')]
    try:
        preps = prepare_all(project, str(pipeline.path), env)
        checks.append(Check("connections, secrets and adapters", True,
                            f"{preps[0].source_type} → {', '.join(p.destination_type for p in preps)}"
                            f" · engine {preps[0].engine_name}"))
    except CartageError as e:
        checks.append(_failed("connections, secrets and adapters", e))
    return pipeline.name, checks
