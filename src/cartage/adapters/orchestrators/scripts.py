"""Generate thin Dagster and Prefect definitions that shell out to `cartage run`, like the Airflow DAGs.
Settings: defaults ← .cartage/config.yaml `orchestrators.<target>` ← pipeline `schedule.<target>`."""
from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, ClassVar

from jinja2 import ChoiceLoader, Environment, FileSystemLoader, PackageLoader, StrictUndefined

from cartage import __version__
from cartage.adapters.orchestrators.airflow import _check_literal, jinja_filters, merge
from cartage.core import CartageError
from cartage.secrets import expand_env

COMMON: dict[str, Any] = {"schedule": None, "env": "prd", "command": "cartage", "project_dir": None, "name": None}


class ScriptOrchestrator:
    target = ""  # key under schedule: and orchestrators:, template name, and label
    title = ""
    defaults: ClassVar[dict[str, Any]] = {}

    def settings(self, project, pipeline) -> dict:
        if self.target not in pipeline.schedule:
            raise CartageError(f"Pipeline '{pipeline.name}' has no schedule.{self.target}",
                               hint=f"Add schedule: {{ {self.target}: {{ schedule: '0 3 * * *' }} }}")
        schedule = expand_env(pipeline.schedule[self.target], project.rel(pipeline.path))
        s = merge(merge(self.defaults, project.config.orchestrators.get(self.target, {})), schedule)
        unknown = sorted(set(s) - set(self.defaults))
        if unknown:
            raise CartageError(f"Unknown {self.title} setting(s): {', '.join(unknown)}",
                               hint=f"Supported: {', '.join(self.defaults)}")
        for name, value in s.items():
            _check_literal(name, value, self.title)
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", str(s["name"] or pipeline.name)):  # job/flow and module name
            raise CartageError(f"Invalid {self.title} name '{s['name']}'", hint="Use letters, digits and '_' only")
        return s

    def context(self, project, pipeline, out_dir: Path | None = None) -> dict:
        s = self.settings(project, pipeline)
        target_dir = Path(out_dir or project.root / s["out_dir"]).resolve()
        rel = project.rel(pipeline.path)
        return {
            **s,
            "cartage_version": __version__,
            "target": self.target,
            "pipeline": pipeline.name,
            "pipeline_path": rel,
            "name": s["name"] or pipeline.name,
            "out_dir": str(target_dir),
            "project_dir": s["project_dir"] or Path(os.path.relpath(project.root, target_dir)).as_posix(),
            "cartage_env_var": "CARTAGE_ENV",  # runtime override of `env`, like the Airflow `cartage_env` variable
        }

    def generate(self, project, pipelines: list, out_dir: Path | None = None) -> dict[Path, str]:
        env = Environment(
            loader=ChoiceLoader([FileSystemLoader(str(project.root / "templates" / self.target)),
                                 PackageLoader("cartage.adapters.orchestrators", "templates")]),
            trim_blocks=True, lstrip_blocks=True, keep_trailing_newline=True, undefined=StrictUndefined, autoescape=False,
        )
        jinja_filters(env)
        template = env.select_template([f"{self.target}.py.j2", f"cartage/{self.target}.py.j2"])
        files: dict[Path, str] = {}
        for pipeline in pipelines:
            if self.target in pipeline.schedule:
                ctx = self.context(project, pipeline, out_dir)
                files[Path(ctx["out_dir"]) / f"{ctx['name']}.py"] = template.render(**ctx)
        return files


class DagsterOrchestrator(ScriptOrchestrator):
    target, title = "dagster", "Dagster"
    defaults: ClassVar[dict[str, Any]] = {**COMMON, "out_dir": "orchestration/dagster", "tags": {"cartage": "true"}, "timezone": "UTC"}


class PrefectOrchestrator(ScriptOrchestrator):
    target, title = "prefect", "Prefect"
    defaults: ClassVar[dict[str, Any]] = {**COMMON, "out_dir": "orchestration/prefect", "tags": ["cartage"], "retries": 0}
