"""Wire a pipeline to its adapters, run it, and apply the state and rejects rules."""
from __future__ import annotations

import copy
import shutil
from dataclasses import dataclass
from typing import Any, Callable

from cartage import registry
from cartage.config import Pipeline, Project, StateConfig
from cartage.core import CartageError, FatalRunError, RunResult, StateBackend
from cartage.secrets import Secrets
from cartage.state import LocalStateBackend, StateStore, new_run_id, write_rejects
from cartage.transforms import Step, apply_steps, load_steps


@dataclass
class Prepared:
    project: Project
    pipeline: Pipeline
    env: str
    source_type: str
    destination_type: str
    engine_name: str
    source: Any
    destination: Any
    engine: Any
    steps: list[Step]
    store: StateStore


def state_backend(project: Project, env: str, secrets: Secrets) -> StateBackend:
    cfg = project.config.state.get(env) or StateConfig(path=".cartage/state")
    if cfg.path is not None:
        return LocalStateBackend(project.root / cfg.path)
    ctype, conf, where = project.connection(cfg.connection, env)
    cls = registry.connection_class(ctype)
    if not hasattr(cls, "state_backend"):
        raise CartageError(f"Connection '{cfg.connection}' ({ctype}) cannot store state",
                           hint="Use a filesystem or s3 connection, or state: { path: ... }")
    return cls.state_backend(secrets.resolve(conf, where), cfg.prefix, project.root)


def prepare(project: Project, pipeline_ref: str, env: str | None = None, engine: str | None = None) -> Prepared:
    env = project.resolve_env(env)
    pipeline = project.load_pipeline(pipeline_ref)
    label = project.rel(pipeline.path)
    secrets = Secrets(project.root)

    spec = pipeline.source
    if spec.connection:
        source_type, source_conf, where = project.connection(spec.connection, env)
        source_conf = secrets.resolve(source_conf, where)
    else:
        source_type, source_conf = spec.type, {}
    source_opts = secrets.resolve(spec.options(), label)

    dest_type, dest_conf, where = project.connection(pipeline.destination.connection, env)
    dest_conf = secrets.resolve(dest_conf, where)
    dest_opts = secrets.resolve(pipeline.destination.options(), label)

    engine_name = engine or pipeline.engine or project.config.defaults.engine
    if source_type == "dlt" and source_opts.get("incremental") and engine_name != "dlt":
        raise CartageError(f"{label}: dlt sources with 'incremental' need the dlt engine (current: {engine_name})",
                           hint="Set engine: dlt in the pipeline, or remove incremental")

    steps = load_steps(pipeline.transforms, project.root)
    source = registry.get("sources", source_type)(source_conf, source_opts, project.root)
    destination = registry.get("destinations", dest_type)(dest_conf, dest_opts, project.root)
    engine_obj = registry.get("engines", engine_name)()
    store = StateStore(state_backend(project, env, secrets), pipeline.name, env)
    return Prepared(project, pipeline, env, source_type, dest_type, engine_name, source, destination, engine_obj, steps, store)


def run_pipeline(prep: Prepared, *, advance_state: bool = False, full_refresh: bool = False,
                 on_progress: Callable[[RunResult], None] | None = None) -> RunResult:
    state = prep.store.empty() if full_refresh else prep.store.load()
    try:
        try:
            result = prep.engine.run(prep.pipeline.name, prep.source, prep.steps, prep.destination, state,
                                     on_progress or (lambda _: None))
        except CartageError:
            raise
        except Exception as e:
            raise FatalRunError(f"Run failed: {type(e).__name__}: {e}") from e
        run_id = new_run_id()
        if result.errors:
            result.rejects_path = str(write_rejects(prep.project.root, prep.pipeline.name, run_id, result.errors))
        if advance_state or not result.errors:
            state.data["last_run"] = {"run_id": run_id, "read": result.read, "ok": result.ok, "errors": len(result.errors)}
            prep.store.save(state)
            result.state_advanced = True
        return result
    finally:
        if state.dlt_dir is not None:
            shutil.rmtree(state.dlt_dir, ignore_errors=True)


def _connection_view(project: Project, name: str, env: str) -> dict:
    conn = project.connections[name]
    return {"connection": name, "type": conn.type, "config": conn.envs[env]}


def preview(prep: Prepared, n: int = 3) -> dict:
    """Dry run: first batch through transforms and mapping. Reads state, never writes it."""
    project, pipeline, env = prep.project, prep.pipeline, prep.env
    spec = pipeline.source
    source = _connection_view(project, spec.connection, env) if spec.connection else {"type": spec.type}
    source["options"] = spec.options()
    destination = _connection_view(project, pipeline.destination.connection, env)
    destination["options"] = {k: v for k, v in pipeline.destination.options().items() if k not in ("mapping", "constants")}
    data = prep.store.read_data()
    files = ([{"file": f, "process": p} for f, p in prep.source.plan_files(data)]
             if hasattr(prep.source, "plan_files") else None)
    first = next(iter(prep.source.read(copy.deepcopy(data))), [])
    out = apply_steps([dict(r) for r in first], prep.steps)
    transformed = out.records[:n]
    return {"pipeline": pipeline.name, "env": env, "engine": prep.engine_name, "source": source,
            "destination": destination, "files": files, "records": first[:n], "transformed": transformed,
            "payloads": prep.destination.preview(transformed), "transform_errors": [e.message for e in out.errors[:n]]}
