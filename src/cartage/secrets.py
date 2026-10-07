"""Resolve ${secret:...}, ${env:...} and ${airflow:...} references and keep resolved values out of output."""
from __future__ import annotations

import os
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from ruamel.yaml import YAML
from ruamel.yaml.error import YAMLError

from cartage.core import CartageError

REF = re.compile(r"\$\{(secret|env|airflow):([A-Za-z0-9_.\-{}]+)(?::-([^}]*))?\}")  # ${env:NAME:-default}
FILL_ME = "<fill me>"  # the placeholder `cartage init` writes; resolving one is a configuration error
AIRFLOW_FIELDS = ("host", "login", "password", "schema", "port")
SECRETS_FILE = Path(".cartage") / "secrets.yaml"  # in the project, then in the home folder (like dlt's ~/.dlt)

# ponytail: process-wide set of revealed values; fine for a CLI process, pass a masker around if cartage becomes a long-lived service.
_REVEALED: set[str] = set()


def _env(environ: Mapping[str, str], name: str, default: str | None, where: str) -> str:
    if name in environ:
        return environ[name]
    if default is not None:
        return default
    raise CartageError(f"Environment variable {name} is not set (referenced at {where})",
                       hint=f"export {name}=..., or give a default: ${{env:{name}:-<value>}}")


def expand_env(obj: Any, where: str, environ: Mapping[str, str] | None = None) -> Any:
    """Fill in ${env:NAME} and ${env:NAME:-default} in every string of obj (dicts and lists in place, so YAML line
    numbers survive). Other references are left for when the value is used. Settings, not secrets: never masked."""
    environ = os.environ if environ is None else environ

    def sub(m: re.Match) -> str:
        return _env(environ, m.group(2), m.group(3), where) if m.group(1) == "env" else m.group(0)
    if isinstance(obj, dict):
        for k in obj:
            obj[k] = expand_env(obj[k], where, environ)
        return obj
    if isinstance(obj, list):
        for i, v in enumerate(obj):
            obj[i] = expand_env(v, where, environ)
        return obj
    return REF.sub(sub, obj) if isinstance(obj, str) else obj


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
    def __init__(self, root: Path, environ: Mapping[str, str] | None = None, env: str | None = None):
        self.root = root
        self.environ = os.environ if environ is None else environ
        self.env = env  # fills {env} in secret keys: ${secret:database.{env}.credentials}
        self._files: list[tuple[str, dict]] | None = None

    def _file_values(self) -> list[tuple[str, dict]]:
        """(label, values) of each secrets file that exists: the project's first, then ~/.cartage/secrets.yaml."""
        if self._files is None:
            self._files = []
            for path, label in ((self.root / SECRETS_FILE, SECRETS_FILE.as_posix()),
                                (Path.home() / SECRETS_FILE, f"~/{SECRETS_FILE.as_posix()}")):
                if not path.is_file():
                    continue
                try:
                    data = YAML(typ="safe").load(path.read_text(encoding="utf-8"))
                except YAMLError as e:
                    raise CartageError(f"{label}: invalid YAML: {e}") from e
                if not isinstance(data, dict | None):
                    raise CartageError(f"{label} must contain a mapping")
                self._files.append((label, data or {}))
        return self._files

    def _from_files(self, key: str) -> tuple[Any, str | None]:
        """The first secrets file with a value for key, and its label."""
        for label, node in self._file_values():
            for part in key.split("."):
                node = node.get(part) if isinstance(node, dict) else None
            if node is not None and not isinstance(node, dict):
                return node, label
        return None, None

    def peek(self, key: str) -> Any:
        """A secret's raw value (environment variable, then the secrets files), or None. Never raises on placeholders."""
        value = self.environ.get(env_key(key))
        return value if value is not None else self._from_files(key)[0]

    def lookup(self, kind: str, key: str, where: str, default: str | None = None) -> str:
        if "{env}" in key:
            if self.env is None:
                raise CartageError(f"'{key}' uses {{env}}, but no environment is set (referenced at {where})")
            key = key.replace("{env}", self.env)
        if kind == "env":
            value: Any = _env(self.environ, key, default, where)
            source = key
        elif kind == "airflow":
            name = airflow_env_key(key)
            if name not in self.environ:
                raise CartageError(f"Airflow connection field '{key}' is not available (referenced at {where})",
                                   hint="It is set by DAGs from `cartage generate`; outside Airflow use another "
                                        f"environment, or set {name}")
            value, source = self.environ[name], name
        else:
            value, source = self.environ.get(env_key(key)), env_key(key)
            if value is None:
                value, source = self._from_files(key)
                if value is None:
                    raise CartageError(f"Secret '{key}' not found (referenced at {where})",
                                       hint=f"Set {env_key(key)} or add it to {SECRETS_FILE.as_posix()} "
                                            f"(or ~/{SECRETS_FILE.as_posix()})")
                value = expand_env(value, f"{source} ({key})", self.environ)  # a secret can be ${env:...} itself
        value = str(value)
        if value == FILL_ME:
            raise CartageError(f'{key} is still "{FILL_ME}" ({source}, referenced at {where})',
                               hint=f"Replace the placeholder in {source}")
        # ponytail: mask only values >= 4 chars; secrets under 4 chars (e.g., "22", "h1") slip through to keep noise out of error output
        if len(value) >= 4:
            _REVEALED.add(value)
        return value

    def resolve(self, obj: Any, where: str, path: str = "") -> Any:
        if isinstance(obj, dict):
            return {k: self.resolve(v, where, f"{path}.{k}" if path else str(k)) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self.resolve(v, where, f"{path}[{i}]") for i, v in enumerate(obj)]
        if isinstance(obj, str):
            if obj == FILL_ME:
                raise CartageError(f'{path or "a value"} at {where} is still "{FILL_ME}"',
                                   hint="Replace the placeholder with the real value")
            return REF.sub(lambda m: self.lookup(m.group(1), m.group(2), where, m.group(3)), obj)
        return obj
