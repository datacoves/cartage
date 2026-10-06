"""cartage init: create a project from answers (prompts, an answers file, or both)."""
from __future__ import annotations

from pathlib import Path

from cartage.config import PROJECT_FILE
from cartage.core import CartageError

ALWAYS_WRITTEN = (PROJECT_FILE, "connections.yaml", ".gitignore", ".cartage/secrets.yaml")


def refuse_existing(dest: Path, paths=ALWAYS_WRITTEN) -> None:
    """Fail before any question when a file init always writes is already there."""
    dest = Path(dest)
    if (dest / PROJECT_FILE).exists():
        raise CartageError(f"{dest} is already a Cartage project ({PROJECT_FILE} exists)",
                           hint="Choose a new folder; adding pipelines to a project is not part of init")
    existing = [str(dest / p) for p in paths if (dest / p).exists()]
    if existing:
        raise CartageError(f"Refusing to overwrite existing files: {', '.join(existing)}", hint="Choose an empty folder")


def write_project(dest: Path, files: dict[str, bytes]) -> list[Path]:
    """Write every file, or nothing: refuses an existing project and any file that is already there."""
    dest = Path(dest)
    refuse_existing(dest, list(files))
    targets = [dest / path for path in files]
    for target, content in zip(targets, files.values(), strict=True):
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
    return targets
