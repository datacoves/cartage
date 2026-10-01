"""Resolve ${secret:...}, ${env:...} and ${airflow:...} references and keep resolved values out of output."""
from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, Mapping

from ruamel.yaml import YAML

from cartage.core import CartageError

REF = re.compile(r"\$\{(secret|env|airflow):([A-Za-z0-9_.\-]+)\}")
AIRFLOW_FIELDS = ("host", "login", "password", "schema", "port")
SECRETS_FILE = Path(".cartage") / "secrets.yaml"

# ponytail: process-wide set of revealed values; fine for a CLI process, pass a masker around if cartage becomes a long-lived service.
_REVEALED: set[str] = set()


def mask(text: str) -> str:
    for value in sorted(_REVEALED, key=len, reverse=True):
        text = text.replace(value, "****")
    return text


def env_key(key: str) -> str:
    return "CARTAGE_SECRET__" + key.replace(".", "__").upper()


def _airflow_parts(key: str) -> tuple[str, list[str]]:
    """`conn_id.login` or `conn_id.extra.<key>[.<key>...]` → (conn_id, path)."""
    conn_id, _, field = key.partition(".")
    path = field.split(".") if field else []
    if not conn_id or not path or not (path[0] in AIRFLOW_FIELDS and len(path) == 1 or path[0] == "extra" and len(path) > 1):
        raise CartageError(f"Invalid Airflow connection reference '${{airflow:{key}}}'",
                           hint=f"Use <conn_id>.<field> with field {', '.join(AIRFLOW_FIELDS)}, or <conn_id>.extra.<key>")
    return conn_id, path


def airflow_env_key(key: str) -> str:
    """The variable a generated DAG sets for ${airflow:key}: main.extra.account → CARTAGE_AIRFLOW__MAIN__EXTRA__ACCOUNT."""
    _airflow_parts(key)
    return "CARTAGE_AIRFLOW__" + re.sub(r"[^A-Za-z0-9]", "_", key.replace(".", "__")).upper()


def airflow_template(key: str) -> str:
    """The Jinja Airflow renders to that connection field when the task runs."""
    conn_id, path = _airflow_parts(key)
    field = f".{path[0]}" if path[0] != "extra" else ".extra_dejson" + "".join(f"[{p!r}]" for p in path[1:])
    return f"{{{{ conn.get({conn_id!r}){field} or '' }}}}"


def references(obj: Any, kind: str) -> set[str]:
    """Keys of every ${kind:...} reference inside obj (dicts, lists, strings)."""
    if isinstance(obj, dict):
        return set().union(*(references(v, kind) for v in obj.values()))
    if isinstance(obj, list):
        return set().union(*(references(v, kind) for v in obj))
    return {m.group(2) for m in REF.finditer(obj) if m.group(1) == kind} if isinstance(obj, str) else set()


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
        elif kind == "airflow":
            name = airflow_env_key(key)
            if name not in self.environ:
                raise CartageError(f"Airflow connection field '{key}' is not available (referenced at {where})",
                                   hint="It is set by DAGs from `cartage generate`; outside Airflow use another "
                                        f"environment, or set {name}")
            value = self.environ[name]
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
