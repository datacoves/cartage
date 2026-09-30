"""CSV files from a local folder, optionally skipping files already processed."""
from __future__ import annotations

import csv
from pathlib import Path
from typing import IO, Iterator

from cartage.core import CartageError, FatalRunError
from cartage.state import LocalStateBackend

FORMATS = ("csv",)


def iter_csv_batches(stream: IO[str], rel: str, batch_size: int) -> Iterator[list[dict]]:
    reader = csv.DictReader(stream)
    batch: list[dict] = []
    for row in reader:
        row["_source"] = f"{rel}:{reader.line_num}"
        batch.append(row)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def _base(config: dict, root: Path) -> Path:
    if "path" not in config:
        raise CartageError("A filesystem connection needs 'path'", hint="e.g. dev: { path: ./data }")
    return (Path(root) / config["path"]).resolve()


class FilesystemSource:
    def __init__(self, config: dict, options: dict, root: Path):
        self.base = _base(config, root)
        if not options.get("path"):
            raise CartageError("A filesystem source needs 'path'", hint="A glob relative to the connection, e.g. materials/*.csv")
        self.pattern = options["path"]
        fmt = options.get("format", "csv")
        if fmt not in FORMATS:
            raise CartageError(f"Unsupported format '{fmt}'", hint=f"Supported: {', '.join(FORMATS)}")
        self.incremental = bool(options.get("incremental", False))
        self.batch_size = int(options.get("batch_size", 100))

    @classmethod
    def check_connection(cls, config: dict, root: Path) -> str:
        base = _base(config, root)
        if not base.is_dir():
            raise FatalRunError(f"Directory not found: {base}")
        count = sum(1 for p in base.rglob("*") if p.is_file())
        return f"{base} ({count} file{'s' if count != 1 else ''})"

    @classmethod
    def state_backend(cls, config: dict, prefix: str, root: Path) -> LocalStateBackend:
        return LocalStateBackend(_base(config, root) / prefix)

    def _files(self) -> list[tuple[str, str, Path]]:
        if not self.base.is_dir():
            raise FatalRunError(f"Directory not found: {self.base}", hint="Check the connection's 'path'")
        found = []
        for path in sorted(self.base.glob(self.pattern)):
            if path.is_file():
                st = path.stat()
                found.append((path.relative_to(self.base).as_posix(), f"{st.st_mtime_ns}-{st.st_size}", path))
        return found

    def plan_files(self, state: dict) -> list[tuple[str, bool]]:
        seen = state.get("files", {})
        return [(rel, not (self.incremental and seen.get(rel) == fp)) for rel, fp, _ in self._files()]

    def read(self, state: dict) -> Iterator[list[dict]]:
        seen = state.setdefault("files", {})
        for rel, fingerprint, path in self._files():
            if self.incremental and seen.get(rel) == fingerprint:
                continue
            try:
                with path.open(newline="", encoding="utf-8-sig") as f:
                    yield from iter_csv_batches(f, rel, self.batch_size)
            except UnicodeDecodeError as e:
                raise FatalRunError(f"{rel} is not valid UTF-8 ({e.reason} at byte {e.start})", hint="Save the file as UTF-8") from e
            if self.incremental:
                seen[rel] = fingerprint
