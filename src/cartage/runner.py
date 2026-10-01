"""Wire a pipeline to its adapters, run it, and apply the state and rejects rules."""
from __future__ import annotations

import copy
import shutil
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from cartage import registry
from cartage.config import DestinationSpec, Pipeline, Project, StateConfig
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
    name: str  # run name: the pipeline name, plus the destination when there are several
    destination_spec: DestinationSpec


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


def prepare(project: Project, pipeline_ref: str, env: str | None = None, engine: str | None = None,
            destination: str | None = None) -> Prepared:
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

    names = pipeline.run_names()
    if destination is None and len(names) > 1:
        raise CartageError(f"{label}: has several destinations; prepare one of {', '.join(names)}, or use prepare_all")
    dest_spec = next(d for d in pipeline.destination_specs if destination in (None, d.connection))
    dest_type, dest_conf, where = project.connection(dest_spec.connection, env)
    dest_conf = secrets.resolve(dest_conf, where)
    dest_opts = secrets.resolve(dest_spec.options(), label)
    dest_cls = registry.get("destinations", dest_type)

    engine_name = engine or pipeline.engine or project.config.defaults.engine
    if source_type == "dlt" and source_opts.get("incremental") and engine_name != "dlt":
        raise CartageError(f"{label}: dlt sources with 'incremental' need the dlt engine (current: {engine_name})",
                           hint="Set engine: dlt in the pipeline, or remove incremental")
    if hasattr(dest_cls, "dlt_destination") and engine_name != "dlt":
        raise CartageError(f"{label}: dlt destinations need the dlt engine (current: {engine_name})",
                           hint="Set engine: dlt in the pipeline, or in cartage.yaml defaults")

    steps = load_steps(pipeline.transforms, project.root)
    source = registry.get("sources", source_type)(source_conf, source_opts, project.root)
    destination = dest_cls(dest_conf, dest_opts, project.root)
    if getattr(destination, "dataset_name", "") is None:  # dlt destinations default to the pipeline, not the run name
        destination.dataset_name = pipeline.name
    engine_obj = registry.get("engines", engine_name)()
    name = names[dest_spec.connection]
    store = StateStore(state_backend(project, env, secrets), name, env)
    return Prepared(project, pipeline, env, source_type, dest_type, engine_name, source, destination, engine_obj, steps,
                    store, name, dest_spec)


def prepare_all(project: Project, pipeline_ref: str, env: str | None = None, engine: str | None = None) -> list[Prepared]:
    """One Prepared per destination; a multi-destination pipeline runs once per destination."""
    return [prepare(project, pipeline_ref, env, engine, d) for d in project.load_pipeline(pipeline_ref).run_names()]


def run_pipeline(prep: Prepared, *, advance_state: bool = False, full_refresh: bool = False,
                 on_progress: Callable[[RunResult], None] | None = None) -> RunResult:
    state = prep.store.empty() if full_refresh else prep.store.load()
    try:
        try:
            result = prep.engine.run(prep.name, prep.source, prep.steps, prep.destination, state,
                                     on_progress or (lambda _: None))
        except FatalRunError as e:
            if e.result is not None and e.result.errors:  # keep the partial rejects; state is never saved
                e.result.rejects_path = str(write_rejects(prep.project.root, prep.name, new_run_id(), e.result.errors))
            raise
        except CartageError:
            raise
        except Exception as e:
            raise FatalRunError(f"Run failed: {type(e).__name__}: {e}") from e
        run_id = new_run_id()
        if result.errors:
            result.rejects_path = str(write_rejects(prep.project.root, prep.name, run_id, result.errors))
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
    spec = prep.destination_spec
    destination = _connection_view(project, spec.connection, env)
    destination["options"] = {k: v for k, v in spec.options().items() if k not in ("mapping", "constants")}
    data = prep.store.read_data()
    files = ([{"file": f, "process": p} for f, p in prep.source.plan_files(data)]
             if hasattr(prep.source, "plan_files") else None)
    first = next(iter(prep.source.read(copy.deepcopy(data))), [])
    skipped: dict[int, str] = {}  # record index → why it has no output
    if all(step.kind != "batch" for step in prep.steps):  # map/filter only: pair each record with its own output
        transformed, payloads, errors = [], [], []
        for index, record in enumerate(first[:n]):
            out = apply_steps([dict(record)], prep.steps)
            errors += [e.message for e in out.errors]
            if out.records:
                transformed.append(out.records[0])
                payloads.append((prep.destination.preview(out.records) or [None])[0])
            else:
                transformed.append(None)
                payloads.append(None)
                skipped[index] = out.errors[0].message if out.errors else "filtered out"
    else:  # batch steps regroup records: show the first outputs as they come
        out = apply_steps([dict(r) for r in first], prep.steps)
        transformed, errors = out.records[:n], [e.message for e in out.errors[:n]]
        payloads = prep.destination.preview(transformed)
    return {"pipeline": pipeline.name, "env": env, "engine": prep.engine_name, "source": source,
            "destination": destination, "files": files, "records": first[:n], "transformed": transformed,
            "skipped": skipped, "payloads": payloads, "transform_errors": errors}
