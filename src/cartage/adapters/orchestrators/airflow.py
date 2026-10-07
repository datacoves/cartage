"""Generate thin TaskFlow Airflow DAGs that call `cartage run`. Settings: defaults ← .cartage/config.yaml ← pipeline."""
from __future__ import annotations

import keyword
import os
import re
import shlex
from pathlib import Path
from typing import Any

from jinja2 import ChoiceLoader, Environment, FileSystemLoader, PackageLoader, StrictUndefined

from cartage import __version__
from cartage.core import CartageError
from cartage.secrets import airflow_env_key, airflow_template, expand_env, references

DEFAULTS: dict[str, Any] = {
    "dags_dir": "dags",
    "task_decorator": "bash",  # @task.<name>, e.g. datacoves_bash
    "operator_args": {},  # keyword arguments of the task decorator
    "default_args": {},
    "default_args_from": None,  # "module:function" called with default_args as keyword arguments
    "schedule_from": None,  # "module:function" called with the schedule, e.g. to disable it in dev
    "image": None,
    "schedule": None,
    "tags": ["cartage"],
    "catchup": False,
    "env": "prd",
    "command": "cartage",
    "project_dir": None,
    "dag_id": None,
    "task_env": {},  # extra environment variables for the task, e.g. UV_CACHE_DIR
}
RESERVED_KWARGS = {"task_id", "bash_command", "executor_config", "env", "append_env"}


def _check_literal(name: str, value: Any, target: str = "Airflow") -> None:
    """Values are rendered with repr(), so only plain YAML scalars/lists/dicts are safe."""
    if isinstance(value, dict):
        for k, v in value.items():
            _check_literal(name, k, target)
            _check_literal(name, v, target)
    elif isinstance(value, list):
        for v in value:
            _check_literal(name, v, target)
    elif value is not None and not isinstance(value, (str, int, float, bool)):
        raise CartageError(f"{target} setting '{name}' contains a {type(value).__name__} value; quote it in YAML")


WIDTH = 100  # generated files wrap dicts, lists and calls longer than this


def py_wrapped(value: Any, indent: int = 0) -> str:
    """repr(value), or one item per line (at `indent`) when it does not fit on a line."""
    text = repr(value)
    if indent + len(text) <= WIDTH or not isinstance(value, dict | list) or not value:
        return text
    pad = " " * (indent + 4)
    if isinstance(value, dict):  # an entry that is still too long gets its value on the next line
        entries = (f"{pad}{k!r}: {v!r},\n" if len(pad) + len(f"{k!r}: {v!r},") <= WIDTH else f"{pad}{k!r}:\n{pad}    {v!r},\n"
                   for k, v in value.items())
        return "{\n" + "".join(entries) + " " * indent + "}"
    return "[\n" + "".join(f"{pad}{v!r},\n" for v in value) + " " * indent + "]"


def py_kwargs(mapping: dict, indent: int = 0) -> str:
    """Keyword arguments of a call, `(a=1, b=2)`, one per line when they do not fit on a line."""
    text = "(" + ", ".join(f"{k}={v!r}" for k, v in mapping.items()) + ")"
    if indent + len(text) <= WIDTH or not mapping:
        return text
    return "(\n" + "".join(f"{' ' * (indent + 4)}{k}={v!r},\n" for k, v in mapping.items()) + " " * indent + ")"


def jinja_filters(env) -> None:
    env.filters.update(py=repr, pyw=py_wrapped, kwargs=py_kwargs)


def merge(base: dict, override: dict) -> dict:
    """Scalars and lists replace; dicts merge by key."""
    out = dict(base)
    for key, value in override.items():
        out[key] = {**out[key], **value} if isinstance(value, dict) and isinstance(out.get(key), dict) else value
    return out


class AirflowOrchestrator:
    def settings(self, project, pipeline) -> dict:
        if "airflow" not in pipeline.schedule:
            raise CartageError(f"Pipeline '{pipeline.name}' has no schedule.airflow",
                               hint="Add schedule: { airflow: { schedule: '0 3 * * *' } }")
        project_settings = project.config.orchestrators.get("airflow", {})
        s = merge(merge(DEFAULTS, project_settings), expand_env(pipeline.schedule["airflow"], project.rel(pipeline.path)))
        unknown = sorted(set(s) - set(DEFAULTS))
        if unknown:
            hint = ("operator was replaced by task_decorator (e.g. bash, datacoves_bash)" if "operator" in unknown else
                    "dependencies moved to the pipeline (dependencies: [...]) and defaults.dependencies; cartage run "
                    "adds them with uv" if "dependencies" in unknown else "")
            raise CartageError(f"Unknown Airflow setting(s): {', '.join(unknown)}",
                               hint=hint or f"Supported: {', '.join(DEFAULTS)}")
        for name in ("default_args", "operator_args", "tags", "schedule", "image", "task_env"):
            _check_literal(name, s[name])
        if not str(s["task_decorator"]).isidentifier():
            raise CartageError(f"Invalid Airflow task_decorator '{s['task_decorator']}'",
                               hint="The name after @task., e.g. bash or datacoves_bash")
        for name in ("default_args_from", "schedule_from"):
            module, _, func = str(s[name] or "x:x").partition(":")
            if not func.isidentifier() or not all(p.isidentifier() for p in module.split(".")):
                raise CartageError(f"Airflow {name} '{s[name]}' must be 'module.path:function'")
        if s["default_args_from"]:
            for key in s["default_args"]:
                if not isinstance(key, str) or not key.isidentifier() or keyword.iskeyword(key):
                    raise CartageError(f"Invalid default_args key '{key}' for {s['default_args_from']}",
                                       hint="With default_args_from, keys are keyword arguments of that function")
        for key in s["operator_args"]:
            if not isinstance(key, str) or not key.isidentifier() or keyword.iskeyword(key) or key in RESERVED_KWARGS:
                raise CartageError(f"Invalid Airflow operator_args key '{key}'",
                                   hint="Keys must be Python identifiers, not keywords, and not "
                                        f"{', '.join(sorted(RESERVED_KWARGS))} (use task_env for variables)")
        dag_id = s["dag_id"] or pipeline.name
        if not re.fullmatch(r"[A-Za-z0-9_.-]+", str(dag_id)) or ".." in str(dag_id):
            raise CartageError(f"Invalid Airflow dag_id '{dag_id}'", hint="Use letters, digits, '_', '-' and '.' only")
        return s

    def task_env(self, project, pipeline, s: dict) -> dict[str, str]:
        """task_env plus every ${airflow:...} the pipeline's connections use (any env), rendered by Airflow at run time."""
        names = {pipeline.source.connection, *(d.connection for d in pipeline.destination_specs),
                 *(pipeline.source_for(e).connection for e in project.config.environments),
                 *(c.connection for c in project.config.state.values())} - {None}
        scanned = [project.connections[n].envs for n in sorted(names) if n in project.connections]
        refs = references([*scanned, pipeline.model_dump(mode="json", by_alias=True)], "airflow")
        return {**{str(k): str(v) for k, v in s["task_env"].items()},
                **{airflow_env_key(k): airflow_template(k) for k in sorted(refs)}}

    def context(self, project, pipeline, out_dir: Path | None = None) -> dict:
        s = self.settings(project, pipeline)
        dags_dir = Path(out_dir or project.root / s["dags_dir"]).resolve()
        rel = project.rel(pipeline.path)
        env_expr = "{{ var.value.get('cartage_env', " + repr(s["env"]) + ") }}"
        dag_id = s["dag_id"] or pipeline.name
        functions = {name: s[name].partition(":")[::2] if s[name] else None for name in ("default_args_from", "schedule_from")}
        return {
            "cartage_version": __version__,
            "pipeline": pipeline.name,
            "pipeline_path": rel,
            "dag_id": dag_id,
            "dag_function": re.sub(r"\W|^(?=\d)", "_", dag_id),
            "dags_dir": str(dags_dir),
            "schedule": s["schedule"],
            "tags": s["tags"],
            "catchup": s["catchup"],
            "default_args": s["default_args"],
            "default_args_from": functions["default_args_from"],  # (module, function) or None
            "schedule_from": functions["schedule_from"],
            "task_decorator": s["task_decorator"],
            "operator_args": s["operator_args"],
            "image": s["image"],
            "project_dir": s["project_dir"] or Path(os.path.relpath(project.root, dags_dir)).as_posix(),
            "command": s["command"],  # global options (--project-dir) must come before `run`, so the template assembles it
            "run_args": f"{shlex.quote(rel)} --env {env_expr}",  # run_target + --env + env_expr, for older overrides
            "run_target": shlex.quote(rel),
            "env_expr": env_expr,
            "task_env": self.task_env(project, pipeline, s),
        }

    def generate(self, project, pipelines: list, out_dir: Path | None = None) -> dict[Path, str]:
        env = Environment(
            loader=ChoiceLoader([FileSystemLoader(str(project.root / "templates" / "airflow")),
                                 PackageLoader("cartage.adapters.orchestrators", "templates")]),
            trim_blocks=True, lstrip_blocks=True, keep_trailing_newline=True, undefined=StrictUndefined, autoescape=False,
        )
        jinja_filters(env)
        template = env.select_template(["dag.py.j2", "cartage/airflow_dag.py.j2"])
        files: dict[Path, str] = {}
        for pipeline in pipelines:
            if "airflow" not in pipeline.schedule:
                continue
            ctx = self.context(project, pipeline, out_dir)
            files[Path(ctx["dags_dir"]) / f"{ctx['dag_id']}.py"] = template.render(**ctx)
        return files
