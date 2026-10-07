"""cartage scaffold: starting points to edit, for orchestrator template overrides, transforms and sources."""
from __future__ import annotations

from pathlib import Path

from jinja2 import Environment, PackageLoader, nodes

from cartage.core import CartageError

TEMPLATES = {  # target → (the override generate looks for, the template it extends)
    "airflow": ("templates/airflow/dag.py.j2", "cartage/airflow_dag.py.j2"),
    "dagster": ("templates/dagster/dagster.py.j2", "cartage/dagster.py.j2"),
    "prefect": ("templates/prefect/prefect.py.j2", "cartage/prefect.py.j2"),
}
KINDS = (*TEMPLATES, "transform", "source")

TRANSFORMS_STUB = '''"""Transforms for {name}. Use them in a pipeline:

transforms:
  - map: transforms.{name}:example
  - filter: transforms.{name}:keep
  - batch: transforms.{name}:dedupe
    with: {{key: id}}

A step that raises rejects the record (it goes to the rejects file), unless the step sets on_error: fail.
"""


def example(record: dict) -> dict:
    """map: return the record, changed, or a new dict."""
    return record


def keep(record: dict) -> bool:
    """filter: True keeps the record."""
    return True


def dedupe(records: list[dict], key: str = "id") -> list[dict]:
    """batch: gets a list of records and returns a list; `with:` values arrive as keyword arguments."""
    return list({{record.get(key): record for record in records}}.values())
'''

SOURCE_STUB = '''"""Source {name}: yield records (dicts), lists of them, or Arrow/pandas batches. Use it in a pipeline:

source:
  ref: sources.{name}:{function}
  with: {{}}          # keyword arguments of {function}
"""
import dlt


@dlt.resource(name="{name}")
def {function}():
    # Replace with your extraction: an API call, a database query, a file parser, ...
    yield [{{"id": 1, "name": "example"}}]
'''


def template_override(target: str) -> str:
    """Every block of Cartage's template, each rendering the default until it is edited."""
    base = TEMPLATES[target][1]
    env = Environment(loader=PackageLoader("cartage.adapters.orchestrators", "templates"))
    blocks = [b.name for b in env.parse(env.loader.get_source(env, base)[0]).find_all(nodes.Block)]
    lines = [f"{{# Overrides for Cartage's {target} template ({base}), used by `cartage generate --target {target}`.",
             "   Each block renders Cartage's default with {{ super() }}: replace it, or add before or after it, and",
             "   delete the blocks you keep as they are. Blocks nest (an outer block's super() renders the inner ones).",
             f"   Variables: `cartage generate --target {target} --show-context <pipeline>`. #}}",
             f'{{% extends "{base}" %}}']
    for name in blocks:
        lines += ["", f"{{% block {name} %}}", "{{ super() -}}", "{% endblock %}"]
    return "\n".join([*lines, ""])


def scaffold(root: Path, kind: str, name: str | None) -> list[Path]:
    """The files for `kind`, written under root; refuses to replace any."""
    if kind in TEMPLATES:
        if name:
            raise CartageError(f"'cartage scaffold {kind}' takes no name", hint=f"It writes {TEMPLATES[kind][0]}")
        files = {TEMPLATES[kind][0]: template_override(kind)}
    elif kind in ("transform", "source"):
        if not name or not name.isidentifier():
            raise CartageError(f"'cartage scaffold {kind}' needs a module name (a Python identifier)",
                               hint=f"e.g. cartage scaffold {kind} materials")
        folder = f"{kind}s"
        body = TRANSFORMS_STUB.format(name=name) if kind == "transform" else SOURCE_STUB.format(name=name, function=name)
        files = {f"{folder}/{name}.py": body, f"{folder}/__init__.py": ""}
    else:
        raise CartageError(f"Unknown scaffold '{kind}'", hint=f"Use one of: {', '.join(KINDS)}")
    main = root / next(iter(files))
    if main.exists():
        raise CartageError(f"{main} already exists", hint="Edit it, or delete it to start over")
    written = []
    for rel, text in files.items():
        path = root / rel
        if path.exists():  # e.g. an existing transforms/__init__.py
            continue
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
        written.append(path)
    return written
