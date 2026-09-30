"""Generate thin Airflow DAGs that call `cartage run`. Settings: defaults ← cartage.yaml ← pipeline."""
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

DEFAULTS: dict[str, Any] = {
    "dags_dir": "dags",
    "operator": "airflow.providers.standard.operators.bash:BashOperator",
    "operator_args": {},
    "default_args": {},
    "image": None,
    "schedule": None,
    "tags": ["cartage"],
    "catchup": False,
    "env": "prd",
    "command": "cartage",
    "project_dir": None,
    "dag_id": None,
}
RESERVED_KWARGS = {"task_id", "bash_command", "executor_config"}


def _check_literal(name: str, value: Any) -> None:
    """Values are rendered with repr(), so only plain YAML scalars/lists/dicts are safe."""
    if isinstance(value, dict):
        for k, v in value.items():
            _check_literal(name, k)
            _check_literal(name, v)
    elif isinstance(value, list):
        for v in value:
            _check_literal(name, v)
    elif value is not None and not isinstance(value, (str, int, float, bool)):
        raise CartageError(f"Airflow setting '{name}' contains a {type(value).__name__} value; quote it in YAML")


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
        s = merge(merge(DEFAULTS, project.config.orchestrators.get("airflow", {})), pipeline.schedule["airflow"])
        unknown = sorted(set(s) - set(DEFAULTS))
        if unknown:
            raise CartageError(f"Unknown Airflow setting(s): {', '.join(unknown)}", hint=f"Supported: {', '.join(DEFAULTS)}")
        if ":" not in s["operator"]:
            raise CartageError(f"Airflow operator '{s['operator']}' must be 'module.path:ClassName'")
        for name in ("default_args", "operator_args", "tags", "schedule", "image"):
            _check_literal(name, s[name])
        for key in s["operator_args"]:
            if not isinstance(key, str) or not key.isidentifier() or keyword.iskeyword(key) or key in RESERVED_KWARGS:
                raise CartageError(f"Invalid Airflow operator_args key '{key}'",
                                   hint="Keys must be Python identifiers, not keywords, and not task_id, bash_command or executor_config")
        dag_id = s["dag_id"] or pipeline.name
        if not re.fullmatch(r"[A-Za-z0-9_.-]+", str(dag_id)) or ".." in str(dag_id):
            raise CartageError(f"Invalid Airflow dag_id '{dag_id}'", hint="Use letters, digits, '_', '-' and '.' only")
        return s

    def context(self, project, pipeline, out_dir: Path | None = None) -> dict:
        s = self.settings(project, pipeline)
        dags_dir = Path(out_dir or project.root / s["dags_dir"]).resolve()
        module, _, cls = s["operator"].partition(":")
        rel = project.rel(pipeline.path)
        env_expr = "{{ var.value.get('cartage_env', " + repr(s["env"]) + ") }}"
        return {
            "cartage_version": __version__,
            "pipeline": pipeline.name,
            "pipeline_path": rel,
            "dag_id": s["dag_id"] or pipeline.name,
            "dags_dir": str(dags_dir),
            "schedule": s["schedule"],
            "tags": s["tags"],
            "catchup": s["catchup"],
            "default_args": s["default_args"],
            "operator_module": module,
            "operator_class": cls,
            "operator_args": s["operator_args"],
            "image": s["image"],
            "project_dir": s["project_dir"] or Path(os.path.relpath(project.root, dags_dir)).as_posix(),
            "bash_command": f"{s['command']} run {shlex.quote(rel)} --env {env_expr}",
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
