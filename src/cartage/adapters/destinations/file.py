"""Write records to one local file per run: JSON, JSON Lines, XML or CSV."""
from __future__ import annotations

import csv
import io
import json
import os
import xml.etree.ElementTree as ET
from datetime import UTC, datetime
from pathlib import Path
from typing import IO, Any

from cartage.core import BatchResult, CartageError, RecordError

FORMATS = ("json", "jsonl", "xml", "csv")


def _clean(record: dict) -> dict:
    return {k: v for k, v in record.items() if k != "_source"}  # cartage's own bookkeeping


def _text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str) if isinstance(value, (dict, list)) else str(value)


def _element(tag: str, value: Any) -> ET.Element:
    name = str(tag) if str(tag)[:1].isalpha() or str(tag)[:1] == "_" else f"_{tag}"  # XML names can't start with a digit
    element = ET.Element(name)
    if isinstance(value, dict):
        element.extend(_element(k, v) for k, v in value.items())
    elif isinstance(value, list):
        element.extend(_element("item", v) for v in value)
    elif value is not None:
        element.text = str(value)
    return element


class FileDestination:
    preview_label = "output"

    def __init__(self, config: dict, options: dict, root: Path):
        if not config.get("path"):
            raise CartageError("A file connection needs 'path'", hint="The output folder, e.g. path: ./output")
        self.folder = (Path(root) / Path(config["path"]).expanduser()).resolve()
        self.format = options.get("format", "jsonl")
        if self.format not in FORMATS:
            raise CartageError(f"Unsupported file format '{self.format}'", hint=f"Use one of: {', '.join(FORMATS)}")
        self.preview_syntax = {"json": "json", "jsonl": "json", "xml": "xml", "csv": "text"}[self.format]
        self.template = options.get("file", "{pipeline}.{ext}")
        self.root_tag = options.get("root", "records")
        self.record_tag = options.get("record", "record")
        self.run_name = "output"  # set by the runner
        self._out: IO[str] | None = None
        self._target: Path | None = None  # fixed when the file is opened
        self._count = 0
        self._columns: list[str] | None = None

    @classmethod
    def check_connection(cls, config: dict, root: Path) -> str:
        return f"writes to {cls(config, {}, root).folder}"

    @property
    def target(self) -> Path:
        name = self.template.format(pipeline=self.run_name, ext=self.format, date=f"{datetime.now(UTC):%Y%m%d}")
        return self.folder / name

    def _render(self, record: dict) -> str:
        record = _clean(record)
        if self.format == "jsonl":
            return json.dumps(record, ensure_ascii=False, default=str)
        if self.format == "json":
            return json.dumps(record, ensure_ascii=False, default=str, indent=2)
        if self.format == "xml":
            element = _element(self.record_tag, record)
            ET.indent(element)
            return ET.tostring(element, encoding="unicode")
        line = io.StringIO()
        csv.writer(line, lineterminator="\n").writerow(_text(record.get(c, "")) for c in self._columns or record)
        return line.getvalue().rstrip("\n")

    def preview(self, records: list[dict]) -> list[str]:
        return [self._render(r) for r in records]

    def _open(self, first: dict) -> IO[str]:
        self.folder.mkdir(parents=True, exist_ok=True)
        self._target = self.target
        out = self._target.with_name(self._target.name + ".partial").open("w", encoding="utf-8", newline="")
        if self.format == "json":
            out.write("[")
        elif self.format == "xml":
            out.write(f'<?xml version="1.0" encoding="utf-8"?>\n<{self.root_tag}>\n')
        elif self.format == "csv":
            self._columns = list(_clean(first))
            csv.writer(out, lineterminator="\n").writerow(self._columns)
        return out

    def write(self, batch: list[dict]) -> BatchResult:
        result = BatchResult()
        for record in batch:
            if self.format == "csv" and self._columns is not None and set(_clean(record)) - set(self._columns):
                extra = ", ".join(sorted(set(_clean(record)) - set(self._columns)))
                result.errors.append(RecordError(stage="destination", message=f"columns not in the CSV header: {extra}",
                                                 source=record.get("_source"), record=record))
                continue
            if self._out is None:
                self._out = self._open(record)
            text = self._render(record)
            if self.format == "json":
                text = ("\n" if self._count == 0 else ",\n") + "\n".join("  " + line for line in text.splitlines())
            elif self.format == "xml":
                text = "\n".join("  " + line for line in text.splitlines())
            self._out.write(text + "\n" if self.format != "json" else text)
            self._count += 1
            result.ok += 1
        return result

    def finish(self, ok: bool) -> None:
        """End of run: close and move the file into place, or drop it. A run that wrote nothing leaves no file."""
        if self._out is None:
            return
        partial = Path(self._out.name)
        if ok:
            self._out.write("\n]\n" if self.format == "json" else f"</{self.root_tag}>\n" if self.format == "xml" else "")
        self._out.close()
        self._out, self._count, self._columns = None, 0, None
        if ok:
            os.replace(partial, self._target)
        else:
            partial.unlink(missing_ok=True)
