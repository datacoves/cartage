"""Wire a pipeline to its adapters, run it, and apply the state and rejects rules."""
from __future__ import annotations

import shutil
from dataclasses import dataclass
from typing import Any, Callable

from cartage import registry
from cartage.config import Pipeline, Project, StateConfig
from cartage.core import CartageError, RunResult, StateBackend
from cartage.secrets import Secrets
from cartage.state import LocalStateBackend, StateStore, new_run_id, write_rejects
from cartage.transforms import Step, load_steps


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
        result = prep.engine.run(prep.pipeline.name, prep.source, prep.steps, prep.destination, state,
                                 on_progress or (lambda _: None))
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
