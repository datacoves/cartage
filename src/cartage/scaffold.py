"""`cartage init`: copy the demo project template."""
from __future__ import annotations

from importlib import resources
from pathlib import Path

from cartage.core import CartageError

RENAMES = {"gitignore": ".gitignore"}  # dotfiles are easy to lose in packaging


def _walk(node, prefix: str = ""):
    for child in sorted(node.iterdir(), key=lambda c: c.name):
        if child.name == "__pycache__":
            continue
        rel = f"{prefix}{child.name}"
        if child.is_dir():
            yield from _walk(child, rel + "/")
        else:
            yield rel, child


def init_project(dest: Path) -> list[Path]:
    template = resources.files("cartage") / "project_template"
    targets = [(Path(dest) / RENAMES.get(rel, rel), src) for rel, src in _walk(template)]
    existing = [str(t) for t, _ in targets if t.exists()]
    if existing:
        raise CartageError(f"Refusing to overwrite existing files: {', '.join(existing)}", hint="Choose an empty directory")
    for target, src in targets:
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(src.read_bytes())
    return [t for t, _ in targets]
