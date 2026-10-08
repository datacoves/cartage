"""cartage init: create a project from answers (prompts, an answers file, or both)."""
from __future__ import annotations

import io
from pathlib import Path

from ruamel.yaml import YAML
from ruamel.yaml.error import YAMLError

from cartage.config import PROJECT_FILE
from cartage.core import CartageError
from cartage.secrets import SECRETS_FILE

ALWAYS_WRITTEN = (PROJECT_FILE, ".cartage/connections.yaml", ".gitignore", ".cartage/secrets.yaml")


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
    blocking = sorted({str(p) for t in targets for p in t.relative_to(dest).parents
                       if p != Path(".") and (dest / p).exists() and not (dest / p).is_dir()})
    if blocking:
        raise CartageError(f"Files are in the way of folders init needs: {', '.join(blocking)}", hint="Choose an empty folder")
    written: list[Path] = []
    try:
        for target, content in zip(targets, files.values(), strict=True):
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(content)
            written.append(target)
    except OSError as e:
        for path in written:  # all or nothing
            path.unlink(missing_ok=True)
        raise CartageError(f"Cannot write {target}: {e}", hint="Nothing was kept; fix the folder and run init again") from e
    return targets


def _add_missing(into: dict, new: dict) -> None:
    for key, value in new.items():
        if isinstance(into.get(key), dict) and isinstance(value, dict):
            _add_missing(into[key], value)
        elif key not in into:
            into[key] = value


def merge_home_secrets(text: str, home: Path | None = None) -> tuple[Path, bytes]:
    """Add the planned placeholders to ~/.cartage/secrets.yaml. Keys already there (filled by an earlier project)
    are kept as they are, and so are the file's comments."""
    path = (home or Path.home()) / SECRETS_FILE
    if not path.is_file():
        merged = text.encode()
    else:
        yaml = YAML()  # round-trip: keeps the existing file's comments
        try:
            existing = yaml.load(path.read_text(encoding="utf-8"))
        except YAMLError as e:
            raise CartageError(f"~/{SECRETS_FILE.as_posix()}: invalid YAML: {e}") from e
        if not isinstance(existing, dict | None):
            raise CartageError(f"~/{SECRETS_FILE.as_posix()} must contain a mapping")
        if existing is None:
            existing = yaml.load("{}")
        # ponytail: commented-out placeholders (optional fields, auth alternatives) may not survive the merge
        _add_missing(existing, yaml.load(text) or {})
        buffer = io.StringIO()
        yaml.dump(existing, buffer)
        merged = buffer.getvalue().encode()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(merged)
    except OSError as e:
        raise CartageError(f"Cannot write {path}: {e}") from e
    return path, merged
