"""Resolve ${secret:...} and ${env:...} references and keep resolved values out of output."""
from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, Mapping

from ruamel.yaml import YAML

from cartage.core import CartageError

REF = re.compile(r"\$\{(secret|env):([A-Za-z0-9_.\-]+)\}")
SECRETS_FILE = Path(".cartage") / "secrets.yaml"

# ponytail: process-wide set of revealed values; fine for a CLI process, pass a masker around if cartage becomes a long-lived service.
_REVEALED: set[str] = set()


def mask(text: str) -> str:
    for value in sorted(_REVEALED, key=len, reverse=True):
        text = text.replace(value, "****")
    return text


def env_key(key: str) -> str:
    return "CARTAGE_SECRET__" + key.replace(".", "__").upper()


class Secrets:
    def __init__(self, root: Path, environ: Mapping[str, str] | None = None):
        self.root = root
        self.environ = os.environ if environ is None else environ
        self._file: dict | None = None

    def _file_values(self) -> dict:
        if self._file is None:
            path = self.root / SECRETS_FILE
            if path.is_file():
                try:
                    data = YAML(typ="safe").load(path.read_text(encoding="utf-8"))
                except Exception as e:
                    raise CartageError(f"{SECRETS_FILE.as_posix()}: invalid YAML: {e}")
                if not isinstance(data, dict):
                    raise CartageError(f"{SECRETS_FILE.as_posix()} must contain a mapping")
                self._file = data or {}
            else:
                self._file = {}
        return self._file

    def lookup(self, kind: str, key: str, where: str) -> str:
        if kind == "env":
            if key not in self.environ:
                raise CartageError(f"Environment variable {key} is not set (referenced at {where})", hint=f"export {key}=...")
            value: Any = self.environ[key]
        else:
            value = self.environ.get(env_key(key))
            if value is None:
                node: Any = self._file_values()
                for part in key.split("."):
                    node = node.get(part) if isinstance(node, dict) else None
                if node is None or isinstance(node, dict):
                    raise CartageError(f"Secret '{key}' not found (referenced at {where})",
                                       hint=f"Set {env_key(key)} or add it to {SECRETS_FILE.as_posix()}")
                value = node
        value = str(value)
        # ponytail: mask only values >= 4 chars; secrets under 4 chars (e.g., "22", "h1") slip through to keep noise out of error output
        if len(value) >= 4:
            _REVEALED.add(value)
        return value

    def resolve(self, obj: Any, where: str) -> Any:
        if isinstance(obj, dict):
            return {k: self.resolve(v, where) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self.resolve(v, where) for v in obj]
        if isinstance(obj, str):
            return REF.sub(lambda m: self.lookup(m.group(1), m.group(2), where), obj)
        return obj
