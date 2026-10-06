"""cartage init: create a project from answers (prompts, an answers file, or both)."""
from __future__ import annotations

from pathlib import Path

from cartage.config import PROJECT_FILE
from cartage.core import CartageError


def write_project(dest: Path, files: dict[str, bytes]) -> list[Path]:
    """Write every file, or nothing: refuses an existing project and any file that is already there."""
    dest = Path(dest)
    if (dest / PROJECT_FILE).exists():
        raise CartageError(f"{dest} is already a Cartage project ({PROJECT_FILE} exists)",
                           hint="Choose a new folder; adding pipelines to a project is not part of init")
    targets = [dest / path for path in files]
    existing = [str(t) for t in targets if t.exists()]
    if existing:
        raise CartageError(f"Refusing to overwrite existing files: {', '.join(existing)}", hint="Choose an empty folder")
    for target, content in zip(targets, files.values(), strict=True):
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
    return targets
