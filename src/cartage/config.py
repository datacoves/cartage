"""Load and validate cartage.yaml, connections.yaml and pipeline files, keeping line numbers for errors."""
from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator
from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap
from ruamel.yaml.error import YAMLError

from cartage.core import CartageError

PROJECT_FILE = "cartage.yaml"
CONNECTIONS_FILE = "connections.yaml"


class Defaults(BaseModel):
    model_config = ConfigDict(extra="forbid")
    engine: str = "dlt"  # python: the dependency-free reference engine, for tests and framework work


class StateConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    path: str | None = None
    connection: str | None = None
    prefix: str = ""

    @model_validator(mode="after")
    def _one_target(self):
        if (self.path is None) == (self.connection is None):
            raise ValueError("set exactly one of 'path' or 'connection'")
        return self


class ProjectConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    project: str
    environments: list[str] = Field(min_length=1)
    default_env: str
    defaults: Defaults = Field(default_factory=Defaults)
    state: dict[str, StateConfig] = Field(default_factory=dict)
    orchestrators: dict[str, dict[str, Any]] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _default_env_listed(self):
        if self.default_env not in self.environments:
            raise ValueError(f"default_env '{self.default_env}' is not in environments {self.environments}")
        return self


class Connection(BaseModel):
    model_config = ConfigDict(extra="forbid")
    type: str
    envs: dict[str, dict[str, Any]]


class ConnectionsFile(BaseModel):
    model_config = ConfigDict(extra="forbid")
    connections: dict[str, Connection] = Field(default_factory=dict)


class SourceSpec(BaseModel):
    model_config = ConfigDict(extra="allow")
    connection: str | None = None
    type: str | None = None
    incremental: bool | dict[str, Any] = False

    @model_validator(mode="after")
    def _connection_or_type(self):
        if self.connection is None and self.type is None and (self.model_extra or {}).get("ref"):
            self.type = "dlt"  # a `ref` is a dlt source written in Python
        if (self.connection is None) == (self.type is None):
            raise ValueError("set exactly one of 'connection' or 'type'")
        return self

    def options(self) -> dict[str, Any]:
        return self.model_dump(exclude={"connection", "type"})


class TransformSpec(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)
    map: str | None = None
    filter: str | None = None
    batch: str | None = None
    with_: dict[str, Any] = Field(default_factory=dict, alias="with")
    on_error: Literal["record", "fail"] = "record"

    @model_validator(mode="after")
    def _one_kind(self):
        if sum(1 for k in ("map", "filter", "batch") if getattr(self, k)) != 1:
            raise ValueError("each transform step needs exactly one of 'map', 'filter' or 'batch'")
        return self

    @property
    def kind(self) -> str:
        return next(k for k in ("map", "filter", "batch") if getattr(self, k))

    @property
    def ref(self) -> str:
        return getattr(self, self.kind)


class DestinationSpec(BaseModel):
    model_config = ConfigDict(extra="allow")
    connection: str
    name: str | None = None  # tells apart destinations of the same connection (e.g. a JSON and an XML file)

    @property
    def key(self) -> str:
        return self.name or self.connection

    def options(self) -> dict[str, Any]:
        return self.model_dump(exclude={"connection", "name"})


class Pipeline(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str = Field(pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    source: SourceSpec
    transforms: list[TransformSpec] = Field(default_factory=list)
    destination: DestinationSpec | None = None
    destinations: list[DestinationSpec] = Field(default_factory=list)
    engine: str | None = None
    schedule: dict[str, dict[str, Any]] = Field(default_factory=dict)
    path: Path | None = Field(default=None, exclude=True)

    @model_validator(mode="after")
    def _destination_selection(self):
        if self.destination is not None and self.destinations:
            raise ValueError("set either 'destination' or 'destinations', not both")
        if self.destination is None and not self.destinations:
            raise ValueError("set 'destination' or a non-empty 'destinations' list")
        names = [spec.key for spec in self.destinations]
        if len(names) != len(set(names)):
            raise ValueError("destinations must be unique within a pipeline: "
                             "set 'name' on destinations that share a connection")
        return self

    @property
    def destination_specs(self) -> list[DestinationSpec]:
        return [self.destination] if self.destination is not None else self.destinations

    def run_names(self) -> dict[str, str]:
        """Destination key (name, else connection) → run name. Each destination of a multi-destination pipeline is its
        own run, with its own state, rejects and dlt pipeline, so one failing target retries without reloading the others."""
        if self.destination is not None:
            return {self.destination.key: self.name}
        return {d.key: f"{self.name}__{re.sub(r'[^A-Za-z0-9_]', '_', d.key)}" for d in self.destinations}


_yaml = YAML(typ="rt")


def load_yaml(path: Path, label: str) -> Any:
    try:
        with path.open(encoding="utf-8") as f:
            data = _yaml.load(f)
    except YAMLError as e:
        mark = getattr(e, "problem_mark", None)
        line = mark.line + 1 if mark else "?"
        raise CartageError(f"{label}:{line}: invalid YAML: {getattr(e, 'problem', None) or e}") from e
    return CommentedMap() if data is None else data


def to_plain(value: Any) -> Any:
    """ruamel round-trip types → plain Python (models and adapters never see ruamel types)."""
    if isinstance(value, dict):
        return {str(k): to_plain(v) for k, v in value.items()}
    if isinstance(value, list):
        return [to_plain(v) for v in value]
    for plain in (bool, int, float, str):
        if isinstance(value, plain):
            return plain(value)
    return value


def line_of(raw: Any, loc: tuple) -> int | None:
    """1-based line of the deepest node in `loc` that exists in the ruamel document."""
    line, node = None, raw
    for key in loc:
        lc = getattr(node, "lc", None)
        try:
            if isinstance(node, dict) and key in node:
                line = lc.key(key)[0] + 1 if lc else line
            elif isinstance(node, list) and isinstance(key, int) and 0 <= key < len(node):
                line = lc.item(key)[0] + 1 if lc else line
            else:
                break
        except (KeyError, IndexError, TypeError):
            break
        node = node[key]
    return line


def validate_model(model: type[BaseModel], raw: Any, label: str):
    try:
        return model.model_validate(to_plain(raw))
    except ValidationError as e:
        lines = []
        for err in e.errors():
            loc = tuple(err["loc"])
            line = line_of(raw, loc)
            where = f"{label}:{line}" if line else label
            field = ".".join(str(p) for p in loc) or "(root)"
            lines.append(f"{where}: {field}: {err['msg']}")
        raise CartageError("\n".join(lines)) from e


@dataclass
class Project:
    root: Path
    config: ProjectConfig
    connections: dict[str, Connection]
    raw_connections: Any

    def rel(self, path: Path) -> str:
        try:
            return Path(path).resolve().relative_to(self.root).as_posix()
        except ValueError:
            return str(path)

    def resolve_env(self, env: str | None) -> str:
        env = env or self.config.default_env
        if env not in self.config.environments:
            raise CartageError(f"Unknown environment '{env}'",
                               hint=f"Environments in {PROJECT_FILE}: {', '.join(self.config.environments)}")
        return env

    def connection(self, name: str, env: str) -> tuple[str, dict[str, Any], str]:
        conn = self.connections.get(name)
        if conn is None:
            known = ", ".join(sorted(self.connections)) or "none"
            raise CartageError(f"Unknown connection '{name}'", hint=f"Connections in {CONNECTIONS_FILE}: {known}")
        if env not in conn.envs:
            where = f"{CONNECTIONS_FILE}:{line_of(self.raw_connections, ('connections', name)) or '?'}"
            raise CartageError(f"Connection '{name}' has no settings for environment '{env}' ({where})",
                               hint=f"Add envs.{env} to '{name}', or use one of: {', '.join(conn.envs)}")
        line = line_of(self.raw_connections, ("connections", name, "envs", env))
        return conn.type, dict(conn.envs[env]), f"{CONNECTIONS_FILE}:{line or '?'}"

    def pipeline_files(self) -> list[Path]:
        folder = self.root / "pipelines"
        return sorted([*folder.glob("*.yaml"), *folder.glob("*.yml")])

    def load_pipeline(self, ref: str | Path):
        ref = str(ref)
        candidates = [Path(ref), self.root / ref, self.root / "pipelines" / f"{ref}.yaml", self.root / "pipelines" / f"{ref}.yml"]
        path = next((p for p in candidates if p.is_file()), None)
        if path is None:
            names = ", ".join(p.stem for p in self.pipeline_files()) or "none"
            raise CartageError(f"Pipeline not found: {ref}", hint=f"Pipelines in this project: {names}")
        path = path.resolve()
        label = self.rel(path)
        pipeline = validate_model(Pipeline, load_yaml(path, label), label)
        pipeline.path = path
        return pipeline


def find_root(start: Path) -> Path:
    start = start.resolve()
    for folder in (start, *start.parents):
        if (folder / PROJECT_FILE).is_file():
            return folder
    raise CartageError(f"No {PROJECT_FILE} found in {start} or any parent folder",
                       hint="Run 'cartage init <dir>' to create a project, or pass --project-dir")


def load_project(project_dir: Path | None = None) -> Project:
    root = find_root(project_dir or Path.cwd())
    config = validate_model(ProjectConfig, load_yaml(root / PROJECT_FILE, PROJECT_FILE), PROJECT_FILE)
    conn_path = root / CONNECTIONS_FILE
    raw = load_yaml(conn_path, CONNECTIONS_FILE) if conn_path.is_file() else CommentedMap()
    connections = validate_model(ConnectionsFile, raw, CONNECTIONS_FILE).connections
    return Project(root, config, connections, raw)
