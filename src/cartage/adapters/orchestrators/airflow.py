"""Generate thin TaskFlow Airflow DAGs that call `cartage run`. Settings: defaults ← cartage.yaml ← pipeline."""
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
from cartage.secrets import airflow_env_key, airflow_template, references

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
    "dependencies": [],  # extra packages for a uvx / uv run command (--with); cartage.yaml + pipeline are combined
    "project_dir": None,
    "dag_id": None,
    "task_env": {},  # extra environment variables for the task, e.g. UV_CACHE_DIR
}
RESERVED_KWARGS = {"task_id", "bash_command", "executor_config", "env", "append_env"}
UV_LAUNCHERS = (["uvx"], ["uv", "tool", "run"], ["uv", "run"])


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
        s = merge(merge(DEFAULTS, project_settings), pipeline.schedule["airflow"])
        unknown = sorted(set(s) - set(DEFAULTS))
        if unknown:
            hint = "operator was replaced by task_decorator (e.g. bash, datacoves_bash)" if "operator" in unknown else ""
            raise CartageError(f"Unknown Airflow setting(s): {', '.join(unknown)}",
                               hint=hint or f"Supported: {', '.join(DEFAULTS)}")
        # dependencies add up instead of replacing, so a pipeline only lists what it needs on top of the project's
        deps = [*project_settings.get("dependencies", []), *pipeline.schedule["airflow"].get("dependencies", [])]
        s["dependencies"] = list(dict.fromkeys(str(d) for d in deps))
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
        if s["dependencies"] and not any(shlex.split(s["command"])[:len(p)] == p for p in UV_LAUNCHERS):
            raise CartageError("Airflow dependencies need a uv command",
                               hint='e.g. command: uvx --from "cartage>=0.7" cartage')
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
                 *(c.connection for c in project.config.state.values())} - {None}
        scanned = [project.connections[n].envs for n in sorted(names) if n in project.connections]
        refs = references([*scanned, pipeline.model_dump(mode="json", by_alias=True)], "airflow")
        return {**{str(k): str(v) for k, v in s["task_env"].items()},
                **{airflow_env_key(k): airflow_template(k) for k in sorted(refs)}}

    @staticmethod
    def command(s: dict) -> str:
        """The command with one --with per dependency, right after the uv launcher (uvx --with x --from y cartage)."""
        if not s["dependencies"]:
            return s["command"]
        words = shlex.split(s["command"])
        n = next(len(p) for p in UV_LAUNCHERS if words[:len(p)] == p)
        return shlex.join([*words[:n], *(w for d in s["dependencies"] for w in ("--with", d)), *words[n:]])

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
            "command": self.command(s),  # global options (--project-dir) must come before `run`, so the template assembles it
            "run_args": f"{shlex.quote(rel)} --env {env_expr}",
            "task_env": self.task_env(project, pipeline, s),
        }

    def generate(self, project, pipelines: list, out_dir: Path | None = None) -> dict[Path, str]:
        env = Environment(
            loader=ChoiceLoader([FileSystemLoader(str(project.root / "templates" / "airflow")),
                                 PackageLoader("cartage.adapters.orchestrators", "templates")]),
            trim_blocks=True, lstrip_blocks=True, keep_trailing_newline=True, undefined=StrictUndefined, autoescape=False,
        )
        env.filters["py"] = repr
        template = env.select_template(["dag.py.j2", "cartage/airflow_dag.py.j2"])
        files: dict[Path, str] = {}
        for pipeline in pipelines:
            if "airflow" not in pipeline.schedule:
                continue
            ctx = self.context(project, pipeline, out_dir)
            files[Path(ctx["dags_dir"]) / f"{ctx['dag_id']}.py"] = template.render(**ctx)
        return files
