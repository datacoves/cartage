"""Answers for `cartage init`: from prompts, or from an answers file (a path or an http(s) URL). Files an answers file
names (the sample, `copy` entries) are read relative to it."""
from __future__ import annotations

import re
import urllib.error
import urllib.request
from pathlib import Path, PurePosixPath
from typing import Any, Literal
from urllib.parse import urljoin, urlparse

from pydantic import BaseModel, ConfigDict, Field, field_validator
from ruamel.yaml import YAML
from ruamel.yaml.error import YAMLError

from cartage.config import validate_model
from cartage.core import CartageError

SAMPLE_FORMATS = {".csv": "csv", ".jsonl": "jsonl", ".ndjson": "jsonl", ".parquet": "parquet"}
ENV_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9_-]*$")


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class Files(_Model):
    location: str = "./data"
    path: str = "*.csv"
    format: Literal["csv", "jsonl", "parquet"] = "csv"


class RestApi(_Model):
    base_url: str = "https://api.example.com/"
    path: str = "items"
    resource: str | None = None
    data_selector: str | None = None
    auth: Literal["none", "bearer", "api_key", "http_basic"] = "none"


class SqlDatabase(_Model):
    dialect: str = "postgresql"
    table: str = "orders"
    schema_: str | None = Field(default=None, alias="schema")
    cursor: str | None = None


class PythonSource(_Model):
    function: str = Field(default="rows", pattern=r"^[A-Za-z_]\w*$")


class FileExport(_Model):
    format: Literal["json", "jsonl", "xml", "csv"] = "jsonl"


class Sample(_Model):
    path: str
    format: Literal["csv", "jsonl", "parquet"] | None = None
    reader_options: dict[str, Any] = Field(default_factory=dict)
    incremental: bool = False

    @property
    def file_name(self) -> str:
        name = PurePosixPath(urlparse(self.path).path).name
        if not name:
            raise CartageError(f"Sample '{self.path}' does not name a file")
        return name

    @property
    def file_format(self) -> str:
        fmt = self.format or SAMPLE_FORMATS.get(PurePosixPath(self.file_name).suffix.lower())
        if fmt is None:
            raise CartageError(f"Cannot tell the format of sample '{self.path}'",
                               hint="Use a .csv, .jsonl or .parquet file, or set sample_data.format")
        return fmt


class Schedule(_Model):
    target: Literal["none", "airflow", "dagster", "prefect"] = "none"
    cron: str = "0 3 * * *"


class InitAnswers(_Model):
    project: str | None = None
    environments: list[str] = Field(default_factory=lambda: ["dev", "prd"], min_length=1)
    source: Literal["files", "rest_api", "sql_database", "python"] = "files"
    files: Files = Field(default_factory=Files)
    rest_api: RestApi = Field(default_factory=RestApi)
    sql_database: SqlDatabase = Field(default_factory=SqlDatabase)
    python: PythonSource = Field(default_factory=PythonSource)
    destination: str = "duckdb"
    auth_variant: str | None = None
    dataset: str | None = None
    write_disposition: Literal["append", "replace", "merge"] = "append"
    primary_key: str | None = None
    file_export: FileExport = Field(default_factory=FileExport)
    bapi: str | None = None
    sample_data: Sample | None = None
    pipeline: str | None = Field(default=None, pattern=r"^[A-Za-z_]\w*$")
    schedule: Schedule = Field(default_factory=Schedule)
    transforms: list[dict[str, Any]] | None = None
    destination_options: dict[str, Any] = Field(default_factory=dict)
    copy_: list[str] = Field(default_factory=list, alias="copy")

    @field_validator("sample_data", mode="before")
    @classmethod
    def _path_or_mapping(cls, value):
        if value in (None, "", False):
            return None
        return {"path": value} if isinstance(value, str) else value

    @field_validator("environments")
    @classmethod
    def _environment_names(cls, value: list[str]) -> list[str]:
        if len(set(value)) != len(value) or not all(ENV_NAME.match(e) for e in value):
            raise ValueError("environments must be unique names of letters, digits, '_' and '-', e.g. [dev, prd]")
        return value

    @property
    def pipeline_name(self) -> str:
        return self.pipeline or re.sub(r"\W", "_", f"{self.source}_to_{self.destination}")


def _is_url(text: str) -> bool:
    return urlparse(text).scheme in ("http", "https")


class Origin:
    """Where relative files come from: an answers file (its folder or URL) or the working directory."""

    def __init__(self, base: str):
        self.base = base  # a file path or URL; siblings resolve against it

    @classmethod
    def cwd(cls) -> Origin:
        return cls(str(Path.cwd() / "_"))

    @property
    def is_url(self) -> bool:
        return _is_url(self.base)

    def resolve(self, name: str) -> str:
        if _is_url(name) or Path(name).is_absolute():
            return name
        return urljoin(self.base, name) if self.is_url else str(Path(self.base).parent / name)

    def read(self, target: str) -> bytes:
        try:
            if _is_url(target):
                with urllib.request.urlopen(target, timeout=30) as response:  # http(s) only, see _is_url
                    return response.read()
            return Path(target).read_bytes()
        except (OSError, urllib.error.URLError) as e:
            raise CartageError(f"Cannot read {target}: {getattr(e, 'reason', None) or e}") from e


def load_answers(ref: str | None) -> tuple[InitAnswers, set[str], Origin]:
    """The answers, which fields the file set (those are not asked), and where its relative files come from."""
    if ref is None:
        return InitAnswers(), set(), Origin.cwd()
    origin = Origin(ref if _is_url(ref) else str(Path(ref).resolve()))
    text = origin.read(origin.base).decode("utf-8")
    try:
        raw = YAML(typ="rt").load(text)
    except YAMLError as e:
        raise CartageError(f"{ref}: invalid YAML: {e}") from e
    answers = validate_model(InitAnswers, raw or {}, ref)
    return answers, set(answers.model_fields_set), origin


def safe_relative(name: str) -> str:
    path = PurePosixPath(name.replace("\\", "/"))
    if not name or _is_url(name) or path.is_absolute() or ".." in path.parts:
        raise CartageError(f"copy entry '{name}' must be a relative path inside the answers file's folder",
                           hint="e.g. transforms/materials.py")
    return path.as_posix()


def fetch_files(answers: InitAnswers, origin: Origin, sample_origin: Origin) -> dict[str, bytes]:
    """Project path → content for the sample (as data/sample/<name>) and every `copy` entry."""
    files: dict[str, bytes] = {}
    if answers.sample_data is not None:
        sample = answers.sample_data
        files[f"data/sample/{sample.file_name}"] = sample_origin.read(sample_origin.resolve(sample.path))
    for name in answers.copy_:
        files[safe_relative(name)] = origin.read(origin.resolve(name))
    return files
