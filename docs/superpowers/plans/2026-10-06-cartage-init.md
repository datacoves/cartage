# `cartage init` Question-Driven Creator Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the fixed-template `cartage init` with a question-driven project creator (environments, source, destination, sample file, schedule), and turn the SAP demo into an ordinary answers-file example.

**Architecture:** A new `cartage.init` package: `answers.py` (pydantic model + loading from a path or URL), `questions.py` (rich prompts for what is unanswered), `credentials.py` (credential fields read from dlt's classes), `plan.py` (pure `plan_project(answers, fetched) -> {path: bytes}`), and `__init__.py` (`write_project`). Three small core changes support it: `source.envs` (per-environment source replacement), `{env}` in secret references, and a `"<fill me>"` placeholder check.

**Tech Stack:** Python 3.11+, typer, rich (`rich.prompt`), pydantic v2, ruamel.yaml, dlt ≥ 1.20, pytest. No new dependencies.

**Spec:** `docs/superpowers/specs/2026-10-06-cartage-init-design.md`

## Global Constraints

- Python ≥ 3.11.4; no new runtime dependencies (stdlib `urllib` for URLs, `rich.prompt` for questions).
- Line length 120; `ruff check src tests` must pass (`uv run --locked ruff check src tests`).
- The placeholder text is exactly `<fill me>` (constant `FILL_ME` in `cartage/secrets.py`).
- Secret keys are `<connection>.<env>.<field>`; `ref:` sources use `{env}`: `${secret:<name>.{env}.<field>}`.
- Connection names: source `landing` (files); destination `sap` (`sap_bapi`), `exports` (`file_export`), `lake` (dlt `filesystem`), `warehouse` (every other dlt destination); sample `samples`. Secret names for `ref:` sources: `api` (REST), `database` (SQL).
- Pipeline file is `pipelines/<pipeline>.yaml`; default pipeline name `<source>_to_<destination>` with non-word characters → `_`.
- `init` never overwrites: it refuses a folder containing `cartage.yaml` and any existing target file, before writing anything.
- Commits: the user commits only on request. Every "Commit" step below runs only if the user has asked for commits in this session; otherwise skip it and leave the changes staged-free in the working tree.

## Review Focus

- **Typo in `destination`** (e.g. `snowflak`) from an answers file with `--yes`: a configuration error listing the valid types, nothing written. Test in Task 5.
- **Target folder already holds one of the files** (e.g. a `.gitignore`) but no `cartage.yaml`: refuse, name the file, write nothing. Test in Task 6.
- **Sample path or URL that does not exist**: an error naming the path/URL, nothing written. Test in Task 4.
- **Environment list with blanks or duplicates** (`" dev, ,dev"`): rejected with a clear message. Test in Task 4.
- **A single environment with a sample** (`environments: [dev]`): no real environments, so no placeholders; the project still validates and runs. Test in Task 5.

---

## File Structure

| File | Responsibility | Task |
| --- | --- | --- |
| `src/cartage/config.py` | `SourceSpec.envs`, `Pipeline.source_for(env)` | 1 |
| `src/cartage/runner.py` | use `source_for(env)`; `Prepared.source_spec`; `Secrets(..., env=)` | 1, 2 |
| `src/cartage/adapters/orchestrators/airflow.py` | include `source.envs` connections in `task_env` | 1 |
| `src/cartage/adapters/destinations/dlt.py` | relative DuckDB files are project-relative | 1 |
| `src/cartage/secrets.py` | `FILL_ME`, `{env}` keys, placeholder errors | 2 |
| `src/cartage/cli.py` | `Secrets(..., env=)`; new `init` command | 2, 6 |
| `src/cartage/init/credentials.py` | dlt credential introspection | 3 |
| `src/cartage/init/answers.py` | `InitAnswers`, `Origin`, `load_answers`, `fetch_files` | 4 |
| `src/cartage/init/plan.py` | `plan_project`, `placeholders` | 5 |
| `src/cartage/init/questions.py` | `ask` | 6 |
| `src/cartage/init/__init__.py` | `write_project` | 6 |
| `src/cartage/ui.py` | `init_done(directory, created, placeholders, pipeline)` | 6 |
| `src/cartage/scaffold.py`, `src/cartage/project_template/` | removed | 7 |
| `examples/sap/` | `answers.yaml`, `materials.csv`, `transforms/materials.py` | 7 |
| `tests/conftest.py` + demo-dependent tests | fixture from the SAP example | 7 |
| `README.md`, `docs/connections.md`, `docs/cartage-demo.tape` | docs and demo | 8 |

---

### Task 1: Per-environment source replacement (`source.envs`)

**Files:**
- Modify: `src/cartage/config.py` (class `SourceSpec`, class `Pipeline`)
- Modify: `src/cartage/runner.py` (`Prepared`, `prepare`, `preview`)
- Modify: `src/cartage/adapters/orchestrators/airflow.py` (`task_env`)
- Modify: `src/cartage/adapters/destinations/dlt.py` (`DltDestinationAdapter.__init__`)
- Test: `tests/test_source_envs.py` (create)

**Interfaces:**
- Produces: `SourceSpec.envs: dict[str, dict[str, Any]]` (excluded from `options()`); `Pipeline.source_for(env: str) -> SourceSpec`; `Prepared.source_spec: SourceSpec`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_source_envs.py
import json

import pytest
from typer.testing import CliRunner

from cartage.cli import app
from cartage.config import load_project
from cartage.core import CartageError

PIPELINE = """\
name: people
source:
  connection: files
  path: real/*.csv
  format: csv
  envs:
    dev:
      connection: files
      path: sample/*.csv
      format: csv
destination:
  connection: out
  format: jsonl
"""


def make(tmp_path, pipeline=PIPELINE):
    (tmp_path / "cartage.yaml").write_text("project: t\nenvironments: [dev, prd]\ndefault_env: dev\n")
    (tmp_path / "connections.yaml").write_text(
        "connections:\n"
        "  files:\n    type: filesystem\n    envs:\n      dev: {bucket_url: ./data}\n      prd: {bucket_url: ./data}\n"
        "  out:\n    type: file_export\n    envs:\n      dev: {path: out}\n      prd: {path: out}\n")
    (tmp_path / "pipelines").mkdir()
    (tmp_path / "pipelines/people.yaml").write_text(pipeline)
    for folder, rows in (("real", 3), ("sample", 2)):
        (tmp_path / "data" / folder).mkdir(parents=True)
        (tmp_path / "data" / folder / "p.csv").write_text("id\n" + "".join(f"{i}\n" for i in range(rows)))
    return tmp_path


def run(root, env):
    return CliRunner().invoke(app, ["-C", str(root), "run", "people", "--env", env, "--json", "--engine", "python"])


def test_an_environment_replaces_the_whole_source(tmp_path):
    root = make(tmp_path)
    assert json.loads(run(root, "dev").stdout)["read"] == 2
    assert json.loads(run(root, "prd").stdout)["read"] == 3


def test_source_for_returns_the_pipeline_source_without_a_replacement(tmp_path):
    pipeline = load_project(make(tmp_path)).load_pipeline("people")
    assert pipeline.source_for("prd") is pipeline.source
    assert pipeline.source_for("dev").options()["path"] == "sample/*.csv"
    assert "envs" not in pipeline.source.options()


def test_an_invalid_replacement_names_the_environment(tmp_path):
    root = make(tmp_path, PIPELINE.replace("      connection: files\n      path: sample", "      path: sample"))
    with pytest.raises(CartageError, match="source.envs.dev"):
        load_project(root).load_pipeline("people").source_for("dev")


def test_plan_shows_the_environment_source(tmp_path):
    result = CliRunner().invoke(app, ["-C", str(make(tmp_path)), "plan", "people", "--json", "--engine", "python"])
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["source"]["options"]["path"] == "sample/*.csv"


def test_duckdb_files_are_relative_to_the_project(tmp_path):
    from cartage.adapters.destinations.dlt import DltDestinationAdapter

    adapter = DltDestinationAdapter({"destination": "duckdb", "credentials": "x.duckdb"}, {}, tmp_path)
    assert adapter.destination.config_params["credentials"] == str((tmp_path / "x.duckdb").resolve())
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv/bin/pytest tests/test_source_envs.py -q`
Expected: FAIL (`envs` passed to the adapter as an option; `source_for` missing; duckdb path unchanged).

- [ ] **Step 3: Implement `SourceSpec.envs` and `Pipeline.source_for`**

In `src/cartage/config.py`, change `SourceSpec`:

```python
class SourceSpec(BaseModel):
    model_config = ConfigDict(extra="allow")
    connection: str | None = None
    type: str | None = None
    incremental: bool | dict[str, Any] = False
    envs: dict[str, dict[str, Any]] = Field(default_factory=dict)  # env → a whole source used instead in that env

    @model_validator(mode="after")
    def _connection_or_type(self):
        if self.connection is None and self.type is None and (self.model_extra or {}).get("ref"):
            self.type = "dlt"  # a `ref` is a dlt source written in Python
        if (self.connection is None) == (self.type is None):
            raise ValueError("set exactly one of 'connection' or 'type'")
        return self

    def options(self) -> dict[str, Any]:
        return self.model_dump(exclude={"connection", "type", "envs"})
```

Add to `class Pipeline` (after `destination_specs`):

```python
    def source_for(self, env: str) -> SourceSpec:
        """The source in `env`: the one `source.envs` gives that environment, else the pipeline's."""
        replacement = self.source.envs.get(env)
        if replacement is None:
            return self.source
        try:
            return SourceSpec.model_validate(replacement)
        except ValidationError as e:
            problems = "; ".join(f"{'.'.join(map(str, err['loc'])) or '(root)'}: {err['msg']}" for err in e.errors())
            raise CartageError(f"source.envs.{env}: {problems}") from e
```

- [ ] **Step 4: Use it in the runner**

In `src/cartage/runner.py`: add `source_spec: SourceSpec` as the last field of `Prepared` (import `SourceSpec` from `cartage.config`). In `prepare`, replace `spec = pipeline.source` with `spec = pipeline.source_for(env)` and pass `spec` as `source_spec` when building `Prepared(...)` (append it as the last positional argument). In `preview`, replace

```python
    spec = pipeline.source
    source = _connection_view(project, spec.connection, env) if spec.connection else {"type": spec.type}
```

with

```python
    spec = prep.source_spec
    source = _connection_view(project, spec.connection, env) if spec.connection else {"type": spec.type}
```

- [ ] **Step 5: Airflow references and DuckDB paths**

In `src/cartage/adapters/orchestrators/airflow.py`, `task_env`, change the `names` set to include every environment's source connection:

```python
        names = {pipeline.source.connection, *(d.connection for d in pipeline.destination_specs),
                 *(pipeline.source_for(e).connection for e in project.config.environments),
                 *(c.connection for c in project.config.state.values())} - {None}
```

In `src/cartage/adapters/destinations/dlt.py`, right after the existing `if self.name == "filesystem":` block in `__init__`, add:

```python
        credentials = settings.get("credentials")
        if self.name == "duckdb" and isinstance(credentials, str) and credentials != ":memory:" \
                and "://" not in credentials and not Path(credentials).is_absolute():
            settings["credentials"] = str((Path(root) / credentials).resolve())  # project-relative, like bucket_url
```

- [ ] **Step 6: Run the tests**

Run: `.venv/bin/pytest tests/test_source_envs.py -q && .venv/bin/pytest -q`
Expected: the new tests PASS; full suite still PASSES (206+ passed).

- [ ] **Step 7: Commit**

```bash
git add src/cartage/config.py src/cartage/runner.py src/cartage/adapters/orchestrators/airflow.py src/cartage/adapters/destinations/dlt.py tests/test_source_envs.py
git commit -m "feat: source.envs replaces a pipeline's source per environment"
```

---

### Task 2: `{env}` in secret references and `"<fill me>"` placeholders

**Files:**
- Modify: `src/cartage/secrets.py`
- Modify: `src/cartage/runner.py:52` (`Secrets(project.root)` → `Secrets(project.root, env=env)`)
- Modify: `src/cartage/cli.py:175`, `src/cartage/cli.py:188` (pass `env=env`)
- Test: `tests/test_secrets.py` (append)

**Interfaces:**
- Produces: `FILL_ME = "<fill me>"` in `cartage.secrets`; `Secrets(root, environ=None, env: str | None = None)`; `Secrets.resolve(obj, where, path="")`.

- [ ] **Step 1: Write the failing tests** (append to `tests/test_secrets.py`)

```python
from cartage.secrets import FILL_ME, Secrets


def secrets_file(tmp_path, text):
    (tmp_path / ".cartage").mkdir(exist_ok=True)
    (tmp_path / ".cartage" / "secrets.yaml").write_text(text)
    return tmp_path


def test_env_placeholder_in_secret_keys(tmp_path):
    root = secrets_file(tmp_path, "db:\n  prd:\n    url: postgresql://x\n")
    assert Secrets(root, environ={}, env="prd").resolve("${secret:db.{env}.url}", "p.yaml") == "postgresql://x"


def test_env_placeholder_needs_an_environment(tmp_path):
    with pytest.raises(CartageError, match=r"\{env\}"):
        Secrets(tmp_path, environ={}).resolve("${secret:db.{env}.url}", "p.yaml")


def test_unfilled_secret_names_the_key_and_file(tmp_path):
    root = secrets_file(tmp_path, f'warehouse:\n  prd:\n    password: "{FILL_ME}"\n')
    with pytest.raises(CartageError) as info:
        Secrets(root, environ={}, env="prd").resolve({"password": "${secret:warehouse.prd.password}"}, "c.yaml:3")
    assert 'warehouse.prd.password is still "<fill me>"' in info.value.message
    assert ".cartage/secrets.yaml" in info.value.message


def test_unfilled_setting_names_its_path(tmp_path):
    with pytest.raises(CartageError, match='credentials.host at c.yaml:5 is still "<fill me>"'):
        Secrets(tmp_path, environ={}).resolve({"credentials": {"host": FILL_ME}}, "c.yaml:5")


def test_unfilled_value_from_the_environment_is_also_caught(tmp_path):
    with pytest.raises(CartageError, match="is still"):
        Secrets(tmp_path, environ={"CARTAGE_SECRET__A__B": FILL_ME}).resolve("${secret:a.b}", "x")
```

- [ ] **Step 2: Run them to verify they fail**

Run: `.venv/bin/pytest tests/test_secrets.py -q`
Expected: FAIL (`ImportError: FILL_ME`).

- [ ] **Step 3: Implement**

In `src/cartage/secrets.py`:

```python
REF = re.compile(r"\$\{(secret|env|airflow):([A-Za-z0-9_.\-{}]+)\}")
FILL_ME = "<fill me>"  # the placeholder `cartage init` writes; resolving one is a configuration error
```

Change `Secrets.__init__` to accept the environment:

```python
    def __init__(self, root: Path, environ: Mapping[str, str] | None = None, env: str | None = None):
        self.root = root
        self.environ = os.environ if environ is None else environ
        self.env = env
        self._file: dict | None = None
```

At the start of `lookup`, before the `if kind == "env":` branch, add:

```python
        if "{env}" in key:
            if self.env is None:
                raise CartageError(f"'{key}' uses {{env}}, but no environment is set (referenced at {where})")
            key = key.replace("{env}", self.env)
```

At the end of `lookup`, replace `value = str(value)` with:

```python
        value = str(value)
        if value == FILL_ME:
            source = env_key(key) if kind == "secret" and self.environ.get(env_key(key)) == FILL_ME else SECRETS_FILE.as_posix()
            raise CartageError(f'{key} is still "{FILL_ME}" ({source}, referenced at {where})',
                               hint=f"Replace the placeholder in {source}")
```

Replace `resolve` with a path-tracking version:

```python
    def resolve(self, obj: Any, where: str, path: str = "") -> Any:
        if isinstance(obj, dict):
            return {k: self.resolve(v, where, f"{path}.{k}" if path else str(k)) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self.resolve(v, where, f"{path}[{i}]") for i, v in enumerate(obj)]
        if isinstance(obj, str):
            if obj == FILL_ME:
                raise CartageError(f'{path or "a value"} at {where} is still "{FILL_ME}"',
                                   hint="Replace the placeholder with the real value")
            return REF.sub(lambda m: self.lookup(m.group(1), m.group(2), where), obj)
        return obj
```

- [ ] **Step 4: Pass the environment at every call site**

`src/cartage/runner.py`, in `prepare`: `secrets = Secrets(project.root, env=env)`.
`src/cartage/cli.py`, `connections_test`: `config = Secrets(project.root, env=env).resolve(config, where)`.
`src/cartage/cli.py`, `_stores`: `backend = state_backend(project, env, Secrets(project.root, env=env))`.

- [ ] **Step 5: Run the tests**

Run: `.venv/bin/pytest -q`
Expected: PASS (all).

- [ ] **Step 6: Commit**

```bash
git add src/cartage/secrets.py src/cartage/runner.py src/cartage/cli.py tests/test_secrets.py
git commit -m "feat: {env} in secret references and errors for unfilled <fill me> placeholders"
```

---

### Task 3: Credential fields from dlt

**Files:**
- Create: `src/cartage/init/__init__.py` (empty for now: `"""cartage init: create a project from answers."""`)
- Create: `src/cartage/init/credentials.py`
- Test: `tests/test_init_credentials.py`

**Interfaces:**
- Produces: `CredentialField(name: str, secret: bool, required: bool, default: object)`; `Credentials(fields: tuple[CredentialField, ...], fallback: bool = False, docs: str = "")` with property `alternatives -> tuple[str, ...]`; `variants(destination: str) -> list[str]`; `credentials(destination: str, variant: str | None = None, url: str | None = None) -> Credentials`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_init_credentials.py
from cartage.init import credentials as creds


def names(c, *, required=None, secret=None):
    return [f.name for f in c.fields if (required is None or f.required == required) and (secret is None or f.secret == secret)]


def test_flat_fields_postgres():
    c = creds.credentials("postgres")
    assert set(names(c, required=True)) == {"host", "database", "username", "password"}
    assert names(c, secret=True) == ["password"]
    assert "drivername" not in names(c) and "query" not in names(c)
    assert not c.fallback and c.alternatives == ()


def test_alternatives_snowflake():
    c = creds.credentials("snowflake")
    assert {"host", "database"} <= set(names(c, required=True))
    assert {"password", "private_key"} <= set(c.alternatives)


def test_variants_bigquery():
    labels = creds.variants("bigquery")
    assert len(labels) == 2 and "Gcp Service Account" in labels
    oauth = creds.credentials("bigquery", variant=next(v for v in labels if "OAuth" in v))
    assert "client_secret" in names(oauth, secret=True)
    assert set(names(creds.credentials("bigquery"), required=True)) == {"project_id", "private_key", "client_email"}


def test_filesystem_picks_by_url_scheme():
    assert "aws_access_key_id" in names(creds.credentials("filesystem", url="s3://b/p"))
    assert "azure_storage_account_name" in names(creds.credentials("filesystem", url="az://c/p"))
    for url in ("./data", "/abs/path", "https://example.com/files"):
        local = creds.credentials("filesystem", url=url)
        assert local.fields == () and not local.fallback


def test_introspection_failure_falls_back(monkeypatch):
    def boom(_):
        raise RuntimeError("dlt changed")

    monkeypatch.setattr(creds, "_classes", boom)
    c = creds.credentials("snowflake")
    assert c.fallback and c.fields == ()
    assert c.docs == "https://dlthub.com/docs/dlt-ecosystem/destinations/snowflake"
    assert creds.variants("snowflake") == []
```

- [ ] **Step 2: Run them to verify they fail**

Run: `.venv/bin/pytest tests/test_init_credentials.py -q`
Expected: FAIL (`ModuleNotFoundError: cartage.init`).

- [ ] **Step 3: Implement**

```python
# src/cartage/init/credentials.py
"""Credential fields for `cartage init`, read from dlt's own credential classes, so every dlt destination is covered
and the fields follow dlt upgrades. Anything unexpected falls back to a single `credentials` secret."""
from __future__ import annotations

import dataclasses
import re
import typing
from dataclasses import dataclass

SKIP = {"drivername", "query"}  # connection-string internals
SCHEMES = {"s3": "AwsCredentials", "gs": "GcpServiceAccountCredentials", "gcs": "GcpServiceAccountCredentials",
           "az": "AzureCredentialsWithoutDefaults", "abfss": "AzureCredentialsWithoutDefaults",
           "sftp": "SFTPCredentials"}


@dataclass(frozen=True)
class CredentialField:
    name: str
    secret: bool
    required: bool
    default: object = None


@dataclass(frozen=True)
class Credentials:
    fields: tuple[CredentialField, ...]
    fallback: bool = False  # dlt could not be read: write one `credentials` secret instead
    docs: str = ""

    @property
    def alternatives(self) -> tuple[str, ...]:
        """Optional secrets when no secret is required: the auth methods to choose from (fill one)."""
        if any(f.secret and f.required for f in self.fields):
            return ()
        names = tuple(f.name for f in self.fields if f.secret and not f.required)
        return names if len(names) > 1 else ()


def _classes(destination: str) -> list[type]:
    import dlt.destinations

    hint = getattr(dlt.destinations, destination)().spec.get_resolvable_fields()["credentials"]
    members = typing.get_args(hint) or (hint,)
    return [m for m in members if isinstance(m, type) and hasattr(m, "get_resolvable_fields")]


def label(cls: type) -> str:
    """GcpServiceAccountCredentials → 'Gcp Service Account'."""
    name = re.sub(r"(Credentials)?(WithoutDefaults)?$", "", cls.__name__)
    return re.sub(r"(?<=[a-z])(?=[A-Z])", " ", name)


def variants(destination: str) -> list[str]:
    """The auth variants a dlt destination offers; one or none means there is nothing to ask."""
    try:
        return [label(c) for c in _classes(destination)]
    except Exception:
        return []


def _fields(cls: type) -> tuple[CredentialField, ...]:
    from dlt.common.configuration.specs.base_configuration import is_secret_hint
    from dlt.common.typing import is_optional_type

    declared = {f.name: f for f in dataclasses.fields(cls)}
    out = []
    for name, hint in cls.get_resolvable_fields().items():
        if name.startswith("_") or name in SKIP:
            continue
        f = declared.get(name)
        default = None
        if f is not None and f.default is not dataclasses.MISSING:
            default = f.default
        elif f is not None and f.default_factory is not dataclasses.MISSING:
            default = "..."
        out.append(CredentialField(name, bool(is_secret_hint(hint)), default is None and not is_optional_type(hint),
                                   default))
    return tuple(out)


def credentials(destination: str, variant: str | None = None, url: str | None = None) -> Credentials:
    docs = f"https://dlthub.com/docs/dlt-ecosystem/destinations/{destination}"
    try:
        classes = _classes(destination)
        if destination == "filesystem":
            scheme = url.partition("://")[0].lower() if url and "://" in url else ""
            wanted = SCHEMES.get(scheme)
            if wanted is None:
                return Credentials((), docs=docs)  # local paths and http(s): nothing to fill
            classes = [c for c in classes if c.__name__ == wanted]
        elif variant is not None:
            classes = [c for c in classes if label(c) == variant]
        if not classes:
            raise LookupError(variant or url)
        return Credentials(_fields(classes[0]), docs=docs)
    except Exception:
        return Credentials((), fallback=True, docs=docs)
```

- [ ] **Step 4: Run the tests**

Run: `.venv/bin/pytest tests/test_init_credentials.py -q`
Expected: PASS. If `test_variants_bigquery`'s label differs (e.g. dlt renames a class), print `creds.variants("bigquery")` and adjust only the expected label string; do not special-case BigQuery in the code.

- [ ] **Step 5: Commit**

```bash
git add src/cartage/init/__init__.py src/cartage/init/credentials.py tests/test_init_credentials.py
git commit -m "feat(init): read credential fields from dlt's credential classes"
```

---

### Task 4: Answers model, answers files and fetching

**Files:**
- Create: `src/cartage/init/answers.py`
- Test: `tests/test_init_answers.py`

**Interfaces:**
- Consumes: `cartage.config.validate_model(model, raw, label)`.
- Produces: `InitAnswers` (fields below; property `pipeline_name -> str`); `Sample` (properties `file_name`, `file_format`); `Origin(base: str)` with `Origin.cwd()`, `.is_url`, `.resolve(name) -> str`, `.read(target) -> bytes`; `load_answers(ref: str | None) -> tuple[InitAnswers, set[str], Origin]`; `fetch_files(answers, origin, sample_origin) -> dict[str, bytes]`; `safe_relative(name: str) -> str`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_init_answers.py
import functools
import http.server
import threading

import pytest

from cartage.core import CartageError
from cartage.init.answers import InitAnswers, Origin, fetch_files, load_answers, safe_relative

ANSWERS = """\
environments: [dev, prd]
source: files
files: {location: "s3://b/materials", path: "materials/*.csv"}
destination: sap_bapi
sample_data: {path: data/m.csv, reader_options: {dtype: str}, incremental: true}
copy: [transforms/t.py]
"""


@pytest.fixture
def folder(tmp_path):
    (tmp_path / "answers.yaml").write_text(ANSWERS)
    (tmp_path / "data").mkdir()
    (tmp_path / "data" / "m.csv").write_text("material\n1\n")
    (tmp_path / "transforms").mkdir()
    (tmp_path / "transforms" / "t.py").write_text("def f(r):\n    return r\n")
    return tmp_path


def test_defaults_and_pipeline_name():
    a = InitAnswers()
    assert (a.environments, a.source, a.destination, a.sample_data) == (["dev", "prd"], "files", "duckdb", None)
    assert a.pipeline_name == "files_to_duckdb"
    assert InitAnswers(destination="sources.lake:factory").pipeline_name == "files_to_sources_lake_factory"


def test_sample_data_accepts_a_path_or_a_mapping():
    assert InitAnswers(sample_data="x/orders.CSV").sample_data.file_format == "csv"
    assert InitAnswers(sample_data="").sample_data is None
    assert InitAnswers(sample_data={"path": "https://h/p/o.jsonl"}).sample_data.file_name == "o.jsonl"
    with pytest.raises(CartageError, match="format"):
        InitAnswers(sample_data="notes.txt").sample_data.file_format


def test_environments_must_be_unique_names():
    for bad in (["dev", "dev"], ["dev", ""], ["has space"]):
        with pytest.raises(ValueError):
            InitAnswers(environments=bad)


def test_load_from_a_path_tracks_what_was_answered_and_fetches_relative_files(folder):
    answers, answered, origin = load_answers(str(folder / "answers.yaml"))
    assert {"environments", "source", "files", "destination", "sample_data", "copy_"} <= answered
    assert "schedule" not in answered
    files = fetch_files(answers, origin, origin)
    assert files == {"data/sample/m.csv": b"material\n1\n", "transforms/t.py": b"def f(r):\n    return r\n"}


def test_a_misspelled_key_is_named(tmp_path):
    (tmp_path / "a.yaml").write_text("destinaton: duckdb\n")
    with pytest.raises(CartageError, match="destinaton"):
        load_answers(str(tmp_path / "a.yaml"))


def test_load_from_a_url(folder):
    handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=str(folder))
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        url = f"http://127.0.0.1:{server.server_port}/answers.yaml"
        answers, _, origin = load_answers(url)
        assert origin.is_url
        assert fetch_files(answers, origin, origin)["data/sample/m.csv"] == b"material\n1\n"
        with pytest.raises(CartageError, match="missing.csv"):
            fetch_files(InitAnswers(sample_data="missing.csv"), origin, origin)
    finally:
        server.shutdown()


def test_missing_sample_names_the_path(tmp_path):
    with pytest.raises(CartageError, match="nope.csv"):
        fetch_files(InitAnswers(sample_data="nope.csv"), Origin.cwd(), Origin(str(tmp_path / "_")))


@pytest.mark.parametrize("bad", ["../x.py", "/etc/passwd", "a/../../x", "https://h/x.py", ""])
def test_copy_targets_stay_inside_the_project(bad):
    with pytest.raises(CartageError, match="relative path"):
        safe_relative(bad)
```

- [ ] **Step 2: Run them to verify they fail**

Run: `.venv/bin/pytest tests/test_init_answers.py -q`
Expected: FAIL (`ModuleNotFoundError: cartage.init.answers`).

- [ ] **Step 3: Implement**

```python
# src/cartage/init/answers.py
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
                with urllib.request.urlopen(target, timeout=30) as response:  # noqa: S310 - http(s) only
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
```

- [ ] **Step 4: Run the tests**

Run: `.venv/bin/pytest tests/test_init_answers.py -q`
Expected: PASS. (`InitAnswers(environments=bad)` raises pydantic's `ValidationError`, a `ValueError` subclass.)

- [ ] **Step 5: Commit**

```bash
git add src/cartage/init/answers.py tests/test_init_answers.py
git commit -m "feat(init): answers model, answers files from paths or URLs"
```

---

### Task 5: `plan_project`: answers → project files

**Files:**
- Create: `src/cartage/init/plan.py`
- Test: `tests/test_init_plan.py`

**Interfaces:**
- Consumes: `InitAnswers`, `Sample` (Task 4); `credentials`, `Credentials` (Task 3); `FILL_ME` (Task 2); `registry.connection_types`, `registry.is_dlt_destination`; `cartage.adapters.destinations.sap.meta.available`, `load_meta`.
- Produces: `plan_project(answers: InitAnswers, fetched: dict[str, bytes]) -> dict[str, bytes]`; `placeholders(files: dict[str, bytes]) -> list[str]` (`"path:line"` of every active `<fill me>`); `destination_connection_name(destination: str) -> str`.

Generated layout the later tasks rely on (from `examples/sap/answers.yaml`):

```yaml
# pipelines/materials.yaml
name: materials
source:
  connection: landing
  path: materials/*.csv
  format: csv
  envs:
    dev:                                   # the sample file: dev runs locally, no credentials
      connection: samples
      path: materials.csv
      format: csv
      reader_options:
        dtype: str
        keep_default_na: false
      incremental: true
transforms:
  - map: transforms.materials:normalize_uom
  - filter: transforms.materials:is_active
  - batch: transforms.materials:dedupe
    with:
      key: material
destination:
  connection: sap
  bapi: BAPI_MATERIAL_SAVEDATA
  ...                                      # destination_options, block style

schedule:
  airflow:
    schedule: "0 3 * * *"
```

```yaml
# connections.yaml (dev part)
  sap:
    type: sap_bapi
    envs:
      dev: {transport: mock, client: "100"}
  samples:
    type: filesystem
    envs:
      dev: {bucket_url: ./data/sample}
```

```yaml
# cartage.yaml (airflow scheduled)
state:
  dev: {path: .cartage/state}
  prd: {path: .cartage/state}

orchestrators:
  airflow:
    dags_dir: dags
    default_args: {owner: data-team, retries: 1}
```

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_init_plan.py
import json

import pytest
from ruamel.yaml import YAML
from typer.testing import CliRunner

from cartage.cli import app
from cartage.core import CartageError
from cartage.init.answers import InitAnswers
from cartage.init.plan import placeholders, plan_project

SAMPLE = b"id,name,amount,updated_at\n1,a,1.5,2026-01-01\n2,b,2.5,2026-01-02\n"
SOURCES = {
    "files": {"files": {"location": "s3://bucket/orders", "path": "orders/*.csv"}},
    "rest_api": {"rest_api": {"base_url": "https://api.example.com/v1/", "path": "orders", "auth": "bearer"}},
    "sql_database": {"sql_database": {"dialect": "postgresql", "table": "orders", "cursor": "updated_at"}},
    "python": {},
}
DESTINATIONS = ["duckdb", "snowflake", "file_export", "sap_bapi"]


def write(tmp_path, answers, fetched=None):
    files = plan_project(answers, fetched or {})
    for path, content in files.items():
        (tmp_path / path).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / path).write_bytes(content)
    return tmp_path, files


def cli(root, *args):
    return CliRunner().invoke(app, ["-C", str(root), *args])


def only_placeholders(result) -> bool:
    return result.exit_code == 0 or ("<fill me>" in result.output and result.exit_code == 2)


@pytest.mark.parametrize("source", SOURCES)
@pytest.mark.parametrize("destination", DESTINATIONS)
def test_with_a_sample_dev_is_local_and_runs(tmp_path, monkeypatch, source, destination):
    monkeypatch.chdir(tmp_path)
    answers = InitAnswers(project="p", source=source, destination=destination, sample_data="sample.csv",
                          **SOURCES[source])
    root, _ = write(tmp_path, answers, {"data/sample/sample.csv": SAMPLE})
    validate = cli(root, "validate", "--env", "dev")
    if destination == "sap_bapi":  # no destination_options: the mapping is a skeleton to fill
        assert only_placeholders(validate), validate.output
        return
    assert validate.exit_code == 0, validate.output
    run = cli(root, "run", answers.pipeline_name, "--env", "dev", "--json")
    assert run.exit_code == 0, run.output
    assert json.loads(run.stdout)["read"] == 2
    assert only_placeholders(cli(root, "validate", "--env", "prd"))


@pytest.mark.parametrize("source", SOURCES)
@pytest.mark.parametrize("destination", DESTINATIONS)
def test_without_a_sample_validate_reports_only_placeholders(tmp_path, source, destination):
    root, _ = write(tmp_path, InitAnswers(project="p", source=source, destination=destination, **SOURCES[source]))
    for env in ("dev", "prd"):
        result = cli(root, "validate", "--env", env)
        assert only_placeholders(result), result.output


def test_local_files_into_duckdb_needs_nothing(tmp_path):
    root, files = write(tmp_path, InitAnswers(project="p", files={"location": "./data"}))
    assert placeholders(files) == []
    assert cli(root, "validate", "--env", "prd").exit_code == 0


def test_secrets_and_references_are_per_connection_and_environment(tmp_path):
    _, files = write(tmp_path, InitAnswers(project="p", environments=["dev", "qa", "prd"], destination="postgres",
                                           sample_data="s.csv"), {"data/sample/s.csv": SAMPLE})
    secrets = YAML(typ="safe").load(files[".cartage/secrets.yaml"])
    assert set(secrets["warehouse"]) == {"qa", "prd"}  # dev is local
    assert secrets["warehouse"]["prd"]["password"] == "<fill me>"
    assert "${secret:warehouse.prd.password}" in files["connections.yaml"].decode()
    assert any(p.startswith("connections.yaml:") for p in placeholders(files))


def test_single_environment_with_a_sample_has_no_placeholders(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    answers = InitAnswers(project="p", environments=["dev"], destination="snowflake", sample_data="s.csv")
    root, files = write(tmp_path, answers, {"data/sample/s.csv": SAMPLE})
    assert placeholders(files) == []
    assert cli(root, "run", answers.pipeline_name, "--json").exit_code == 0


def test_ref_sources_use_env_in_secret_references(tmp_path):
    _, files = write(tmp_path, InitAnswers(project="p", source="sql_database", sample_data="s.csv"),
                     {"data/sample/s.csv": SAMPLE})
    pipeline = files["pipelines/sql_database_to_duckdb.yaml"].decode()
    assert "${secret:database.{env}.credentials}" in pipeline
    assert "postgresql://<username>:<password>@<host>:<port>/<database>" in files[".cartage/secrets.yaml"].decode()


def test_python_source_gets_a_stub(tmp_path):
    _, files = write(tmp_path, InitAnswers(project="p", source="python", pipeline="erp"))
    assert b"def rows():" in files["sources/erp.py"] and "sources/__init__.py" in files
    assert "ref: sources.erp:rows" in files["pipelines/erp.yaml"].decode()


def test_answers_content_and_copied_transforms(tmp_path):
    answers = InitAnswers(project="p", transforms=[{"map": "transforms.t:f"}], copy=["transforms/t.py"],
                          destination="file_export", destination_options={"file": "out.jsonl"})
    _, files = write(tmp_path, answers, {"transforms/t.py": b"def f(r):\n    return r\n"})
    pipeline = YAML(typ="safe").load(files["pipelines/files_to_file_export.yaml"])
    assert pipeline["transforms"] == [{"map": "transforms.t:f"}]
    assert pipeline["destination"]["file"] == "out.jsonl"
    assert "transforms/files_to_file_export.py" not in files and "transforms/__init__.py" in files


def test_schedule_writes_the_target(tmp_path):
    _, files = write(tmp_path, InitAnswers(project="p", schedule={"target": "airflow", "cron": "0 6 * * *"}))
    pipeline = YAML(typ="safe").load(files["pipelines/files_to_duckdb.yaml"])
    assert pipeline["schedule"] == {"airflow": {"schedule": "0 6 * * *"}}
    assert "airflow" in YAML(typ="safe").load(files["cartage.yaml"])["orchestrators"]


def test_unknown_destination_lists_the_types():
    with pytest.raises(CartageError, match="Unknown destination 'snowflak'") as info:
        plan_project(InitAnswers(destination="snowflak"), {})
    assert "snowflake" in info.value.hint
```

- [ ] **Step 2: Run them to verify they fail**

Run: `.venv/bin/pytest tests/test_init_plan.py -q -x`
Expected: FAIL (`ModuleNotFoundError: cartage.init.plan`).

- [ ] **Step 3: Implement `plan.py`**

```python
# src/cartage/init/plan.py
"""Answers → the files of a new project, as {relative path: bytes}. Pure: no prompts, no network, no disk."""
from __future__ import annotations

import io
import json
import re
from typing import Any

from ruamel.yaml import YAML

from cartage import registry
from cartage.adapters.destinations.sap.meta import available as available_bapis
from cartage.adapters.destinations.sap.meta import load_meta
from cartage.core import CartageError
from cartage.init.answers import InitAnswers
from cartage.init.credentials import Credentials, credentials
from cartage.secrets import FILL_ME

FILL = json.dumps(FILL_ME)
PLAIN = re.compile(r"[A-Za-z0-9_./][A-Za-z0-9_./*\- ]*")
CONNECTION_STRINGS = {"postgresql": "postgresql://<username>:<password>@<host>:<port>/<database>",
                      "mssql": "mssql+pyodbc://<username>:<password>@<host>:<port>/<database>?driver=ODBC+Driver+18+for+SQL+Server",
                      "mysql": "mysql+pymysql://<username>:<password>@<host>:<port>/<database>",
                      "oracle": "oracle+oracledb://<username>:<password>@<host>:<port>/?service_name=<service>",
                      "snowflake": "snowflake://<username>:<password>@<account>/<database>/<schema>?warehouse=<warehouse>"}


def _scalar(value: str) -> str:
    return value if PLAIN.fullmatch(value) and not value.endswith(" ") else json.dumps(value)


def _dump(value: Any) -> list[str]:
    """Block YAML lines for nested values (transforms, destination options, REST config)."""
    yaml = YAML()
    yaml.default_flow_style = False
    yaml.width = 120
    yaml.indent(mapping=2, sequence=4, offset=2)
    out = io.StringIO()
    yaml.dump(value, out)
    return out.getvalue().rstrip("\n").splitlines()


def _indent(lines: list[str], spaces: int) -> list[str]:
    return [" " * spaces + line if line else line for line in lines]


def destination_connection_name(destination: str) -> str:
    return {"sap_bapi": "sap", "file_export": "exports", "filesystem": "lake"}.get(destination, "warehouse")


class _Secrets:
    """Placeholders for .cartage/secrets.yaml, collected while the other files are built."""

    def __init__(self) -> None:
        self.entries: dict[str, dict[str, list[str]]] = {}

    def ref(self, conn: str, env: str, field: str, active: bool = True, note: str = "") -> str:
        lines = self.entries.setdefault(conn, {}).setdefault(env, [])
        if note:
            lines.append(f"# {note}")
        lines.append(f"{'' if active else '# '}{field}: {FILL}")
        return f"${{secret:{conn}.{env}.{field}}}"

    def ref_all(self, conn: str, envs: list[str], field: str, note: str = "") -> str:
        for env in envs:
            self.ref(conn, env, field, note=note)
        return f"${{secret:{conn}.{{env}}.{field}}}"

    def text(self) -> str:
        head = ['# Secrets for this project. Never commit this file (.cartage/ is git-ignored).',
                '# Replace every "<fill me>"; `cartage validate --env <env>` lists the ones left.',
                "# Any value can come from an environment variable instead: CARTAGE_SECRET__<CONNECTION>__<ENV>__<FIELD>."]
        if not self.entries:
            return "\n".join([*head, "{}", ""])
        body = []
        for conn, envs in self.entries.items():
            body.append(f"{conn}:")
            for env, lines in envs.items():
                body += [f"  {env}:", *_indent(lines, 4)]
        return "\n".join([*head, *body, ""])


def credential_lines(creds: Credentials, conn: str, env: str, secrets: _Secrets) -> list[str]:
    """`credentials:` for one environment: required fields active, optional ones commented, secrets as references."""
    if creds.fallback:
        return [f"credentials: {json.dumps(secrets.ref(conn, env, 'credentials'))}   # fields: {creds.docs}"]
    if not creds.fields:
        return []
    body: list[str] = []
    for f in creds.fields:
        if f.name in creds.alternatives:
            continue
        if f.secret:
            value = json.dumps(secrets.ref(conn, env, f.name, active=f.required))
        else:
            value = FILL if f.required else json.dumps("..." if f.default in (None, "...") else str(f.default))
        body.append(f"{f.name}: {value}" if f.required else f"# {f.name}: {value}   # optional")
    if creds.alternatives:
        body.append(f"# auth: fill one of {', '.join(creds.alternatives)}")
        for i, name in enumerate(creds.alternatives):
            body.append(f"{'' if i == 0 else '# '}{name}: {json.dumps(secrets.ref(conn, env, name, active=i == 0))}")
    active = any(not line.startswith("#") for line in body)
    return [f"{'' if active else '# '}credentials:", *_indent(body, 2)]


def _connection(name: str, ctype: str, envs: dict[str, str | list[str]]) -> list[str]:
    out = [f"  {name}:", f"    type: {ctype}"]
    if not envs:
        return [*out, "    envs: {}"]
    out.append("    envs:")
    for env, body in envs.items():
        if isinstance(body, str):
            out.append(f"      {env}: {body}")
        elif not any(not line.lstrip().startswith("#") for line in body):
            out += [f"      {env}: {{}}", *_indent(body, 8)]
        else:
            out += [f"      {env}:", *_indent(body, 8)]
    return out


def _connections(a: InitAnswers, first: str, real: list[str], secrets: _Secrets) -> str:
    lines = ["# Named connections, one settings block per environment.",
             '# Secrets are ${secret:...} references to .cartage/secrets.yaml; replace every "<fill me>".',
             "connections:"]
    sample = a.sample_data is not None
    if a.source == "files":
        envs = {e: [f"bucket_url: {_scalar(a.files.location)}",
                    *credential_lines(credentials("filesystem", url=a.files.location), "landing", e, secrets)]
                for e in real}
        lines += _connection("landing", "filesystem", envs)
    d, name = a.destination, destination_connection_name(a.destination)
    envs: dict[str, str | list[str]] = {}
    if d == "file_export":
        envs = {e: "{path: output}" for e in a.environments}
    elif d == "sap_bapi":
        if sample:
            envs[first] = '{transport: mock, client: "100"}'
        for e in real:
            envs[e] = ["transport: rfc", f"ashost: {FILL}", f"sysnr: {FILL}", f"client: {FILL}",
                       f"user: {json.dumps(secrets.ref(name, e, 'user'))}",
                       f"passwd: {json.dumps(secrets.ref(name, e, 'passwd'))}"]
    else:
        if sample:
            envs[first] = ("{bucket_url: ./lake}" if d == "filesystem" else f"{{credentials: {a.project}.duckdb}}"
                           if d == "duckdb" else f"{{type: duckdb, credentials: {a.project}.duckdb}}")
        for e in real:
            envs[e] = ([f"bucket_url: {FILL}"] if d == "filesystem"
                       else credential_lines(credentials(d, variant=a.auth_variant), name, e, secrets))
    lines += ["", *_connection(name, d, envs)]
    if sample:
        lines += ["", *_connection("samples", "filesystem", {first: "{bucket_url: ./data/sample}"})]
    return "\n".join([*lines, ""])


def _source(a: InitAnswers, real: list[str], secrets: _Secrets) -> list[str]:
    if a.source == "files":
        return ["connection: landing", f"path: {_scalar(a.files.path)}", f"format: {a.files.format}"]
    if a.source == "python":
        return [f"ref: sources.{a.pipeline_name}:{a.python.function}"]
    if a.source == "sql_database":
        s = a.sql_database
        note = "e.g. " + CONNECTION_STRINGS.get(s.dialect, f"{s.dialect}://<username>:<password>@<host>:<port>/<database>")
        ref = secrets.ref_all("database", real, "credentials", note=note)
        lines = ["ref: dlt.sources.sql_database:sql_table", "with:", f"  credentials: {json.dumps(ref)}",
                 f"  table: {_scalar(s.table)}"]
        lines += [f"  schema: {_scalar(s.schema_)}"] if s.schema_ else []
        return lines + ([f"incremental: {{cursor: {s.cursor}}}"] if s.cursor else [])
    r = a.rest_api
    endpoint: dict[str, Any] = {"path": r.path}
    if r.data_selector:
        endpoint["data_selector"] = r.data_selector
    client: dict[str, Any] = {"base_url": r.base_url}
    if r.auth == "bearer":
        client["auth"] = {"type": "bearer", "token": secrets.ref_all("api", real, "token")}
    elif r.auth == "api_key":
        client["auth"] = {"type": "api_key", "name": "X-API-Key", "location": "header",
                          "api_key": secrets.ref_all("api", real, "api_key")}
    elif r.auth == "http_basic":
        client["auth"] = {"type": "http_basic", "username": FILL_ME, "password": secrets.ref_all("api", real, "password")}
    resource = r.resource or r.path.rstrip("/").rsplit("/", 1)[-1] or a.pipeline_name
    config = {"client": client, "resources": [{"name": resource, "endpoint": endpoint}]}
    return ["ref: dlt.sources.rest_api:rest_api_source", "with:", "  config:", *_indent(_dump(config), 4)]


def _mapping_skeleton(bapi: str) -> list[str]:
    meta = load_meta(bapi)
    derived = {p.x_structure for p in meta.parameters.values() if p.x_structure}
    lines = ["  mapping:                                  # BAPI parameter: record field"]
    for pname, param in meta.parameters.items():
        if pname in derived:
            continue
        for fname, field in param.fields.items():
            target = f"{pname}[].{fname}" if param.kind == "table" else f"{pname}.{fname}"
            active = field.required or target == meta.key
            lines.append(f"    {'' if active else '# '}{target}: {FILL if active else '<record field>'}")
    return lines


def _pipeline(a: InitAnswers, first: str, real: list[str], secrets: _Secrets) -> str:
    lines = [f"name: {a.pipeline_name}", "source:", *_indent(_source(a, real, secrets), 2)]
    if a.sample_data is not None:
        s = a.sample_data
        lines += ["  envs:", f"    {first}:                                   # the sample file: {first} runs locally, "
                  "no credentials", "      connection: samples", f"      path: {_scalar(s.file_name)}",
                  f"      format: {s.file_format}"]
        if s.reader_options:
            lines += ["      reader_options:", *_indent(_dump(s.reader_options), 8)]
        if s.incremental:
            lines.append("      incremental: true")
    if a.transforms:
        lines += ["transforms:", *_indent(_dump(a.transforms), 0)]
    else:
        lines += ["# transforms:                           # reshape records between source and destination",
                  f"#   - map: transforms.{a.pipeline_name}:example"]
    d = a.destination
    options: dict[str, Any] = {"connection": destination_connection_name(d)}
    if d == "file_export":
        options["format"] = a.file_export.format
    elif d == "sap_bapi":
        options["bapi"] = a.bapi or available_bapis()[0]
    else:
        options["dataset_name"] = a.dataset or a.pipeline_name
        options["write_disposition"] = a.write_disposition
        if a.write_disposition == "merge" and a.primary_key:
            options["primary_key"] = a.primary_key
    options.update(a.destination_options)
    lines += ["destination:", *_indent(_dump(options), 2)]
    if d == "sap_bapi" and "mapping" not in options:
        lines += _mapping_skeleton(options["bapi"])
    if a.schedule.target != "none":
        lines += ["", "schedule:", f"  {a.schedule.target}:", f"    schedule: {json.dumps(a.schedule.cron)}"]
    return "\n".join([*lines, ""])


def _project(a: InitAnswers) -> str:
    envs = ", ".join(a.environments)
    lines = ["# Cartage project settings.", f"project: {_scalar(a.project or 'project')}", f"environments: [{envs}]",
             f"default_env: {a.environments[0]}", "", "defaults:", "  engine: dlt", "",
             "# Incremental state per environment; remote workers can keep it in a filesystem connection, e.g.",
             "# prd: {connection: landing, prefix: cartage/state/}", "state:",
             *[f"  {e}: {{path: .cartage/state}}" for e in a.environments]]
    if a.schedule.target == "airflow":
        lines += ["", "orchestrators:", "  airflow:", "    dags_dir: dags",
                  "    default_args: {owner: data-team, retries: 1}"]
    return "\n".join([*lines, ""])


TRANSFORMS_STUB = '''"""Transforms for the {name} pipeline: referenced from pipelines/{name}.yaml as transforms.{name}:<function>."""


def example(record):
    """map: return the record, changed. A filter returns True to keep it; a batch gets and returns a list."""
    return record
'''

SOURCE_STUB = '''"""Source for the {name} pipeline: yield records (dicts), lists of them, or Arrow/pandas batches."""
import dlt


@dlt.resource(name="{name}")
def {function}():
    # Replace with your extraction: an API call, a database query, a file parser, ...
    yield [{{"id": 1, "name": "example"}}]
'''


def plan_project(a: InitAnswers, fetched: dict[str, bytes]) -> dict[str, bytes]:
    if a.destination not in registry.connection_types() and not registry.is_dlt_destination(a.destination):
        raise CartageError(f"Unknown destination '{a.destination}'",
                           hint=f"Types: {', '.join(registry.connection_types())}")
    first = a.environments[0]
    real = a.environments[1:] if a.sample_data is not None else list(a.environments)
    secrets = _Secrets()
    files = {"cartage.yaml": _project(a), "connections.yaml": _connections(a, first, real, secrets),
             f"pipelines/{a.pipeline_name}.yaml": _pipeline(a, first, real, secrets),
             ".gitignore": ".cartage/\n*.duckdb\noutput/\n"}
    files[".cartage/secrets.yaml"] = secrets.text()
    copied_transforms = any(path.startswith("transforms/") for path in fetched)
    if not copied_transforms:
        files[f"transforms/{a.pipeline_name}.py"] = TRANSFORMS_STUB.format(name=a.pipeline_name)
    if "transforms/__init__.py" not in fetched:
        files["transforms/__init__.py"] = ""
    if a.source == "python":
        files["sources/__init__.py"] = ""
        files[f"sources/{a.pipeline_name}.py"] = SOURCE_STUB.format(name=a.pipeline_name, function=a.python.function)
    out = {path: text.encode() for path, text in files.items()}
    out.update(fetched)
    return out


def placeholders(files: dict[str, bytes]) -> list[str]:
    """`path:line` of every active (uncommented) <fill me> in the generated text files."""
    found = []
    for path, content in sorted(files.items()):
        if not path.endswith((".yaml", ".yml")):
            continue
        for number, line in enumerate(content.decode().splitlines(), 1):
            if FILL_ME in line and not line.lstrip().startswith("#"):
                found.append(f"{path}:{number}")
    return found
```

- [ ] **Step 4: Run the tests and fix rendering issues**

Run: `.venv/bin/pytest tests/test_init_plan.py -q -x`
Expected: PASS. If a matrix case fails, print the generated files (`print(files[...].decode())` in a scratch run) and fix the builder, never the expectation, unless the expectation contradicts the spec. Known checks: the REST/SQL sample cases must not resolve the `ref:` source in `dev` (the `envs` replacement covers it); `credentials:` blocks with no active field must be commented out.

- [ ] **Step 5: Lint and commit**

Run: `.venv/bin/ruff check src tests` → `All checks passed!`

```bash
git add src/cartage/init/plan.py tests/test_init_plan.py
git commit -m "feat(init): plan_project builds a project's files from answers"
```

---

### Task 6: Questions, `write_project` and the `init` command

**Files:**
- Create: `src/cartage/init/questions.py`
- Modify: `src/cartage/init/__init__.py` (add `write_project`)
- Modify: `src/cartage/cli.py` (`init` command; imports)
- Modify: `src/cartage/ui.py` (`init_done`)
- Test: `tests/test_init_cli.py` (create)

**Interfaces:**
- Consumes: `load_answers`, `fetch_files`, `Origin`, `InitAnswers`, `Sample` (Task 4); `plan_project`, `placeholders` (Task 5); `variants` (Task 3).
- Produces: `ask(answers: InitAnswers, answered: set[str], default_project: str, yes: bool, console=None) -> InitAnswers`; `write_project(dest: Path, files: dict[str, bytes]) -> list[Path]`; `ui.init_done(directory: Path, created: list[Path], placeholders: list[str], pipeline: str) -> None`; CLI `cartage init [DIR] [--answers REF] [--yes/-y]`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_init_cli.py
from typer.testing import CliRunner

from cartage.cli import app
from cartage.config import load_project

runner = CliRunner()


def init(*args, input=None):
    return runner.invoke(app, ["init", *args], input=input)


def test_yes_takes_every_default(tmp_path):
    result = init(str(tmp_path / "p"), "--yes")
    assert result.exit_code == 0, result.output
    assert load_project(tmp_path / "p").load_pipeline("files_to_duckdb").name == "files_to_duckdb"
    assert "cartage validate" in result.output


def test_interactive_answers(tmp_path):
    # project, environments, source, location, path, format, destination, dataset, disposition, sample, pipeline, schedule
    answers = "\n".join(["demo", "dev, qa", "files", "./in", "*.csv", "csv", "duckdb", "", "append", "", "orders",
                         "none"]) + "\n"
    result = init(str(tmp_path / "p"), input=answers)
    assert result.exit_code == 0, result.output
    project = load_project(tmp_path / "p")
    assert project.config.environments == ["dev", "qa"] and project.config.project == "demo"
    assert project.load_pipeline("orders").source.options()["path"] == "*.csv"


def test_answers_file_skips_its_questions(tmp_path):
    (tmp_path / "a.yaml").write_text("environments: [dev]\ndestination: file_export\npipeline: out\n")
    result = init(str(tmp_path / "p"), "--answers", str(tmp_path / "a.yaml"), "--yes")
    assert result.exit_code == 0, result.output
    assert load_project(tmp_path / "p").load_pipeline("out").destination.connection == "exports"


def test_placeholders_are_listed(tmp_path):
    (tmp_path / "a.yaml").write_text("destination: postgres\n")
    result = init(str(tmp_path / "p"), "--answers", str(tmp_path / "a.yaml"), "--yes")
    assert result.exit_code == 0, result.output
    assert "<fill me>" in result.output and ".cartage/secrets.yaml" in result.output


def test_refuses_an_existing_project(tmp_path):
    (tmp_path / "cartage.yaml").write_text("project: x\n")
    result = init(str(tmp_path), "--yes")
    assert result.exit_code == 2 and "already a Cartage project" in result.output


def test_refuses_to_overwrite_any_file_and_writes_nothing(tmp_path):
    (tmp_path / ".gitignore").write_text("mine\n")
    result = init(str(tmp_path), "--yes")
    assert result.exit_code == 2 and ".gitignore" in result.output
    assert not (tmp_path / "cartage.yaml").exists()
    assert (tmp_path / ".gitignore").read_text() == "mine\n"
```

- [ ] **Step 2: Run them to verify they fail**

Run: `.venv/bin/pytest tests/test_init_cli.py -q`
Expected: FAIL (the old `init` ignores `--yes`).

- [ ] **Step 3: Implement `questions.py`**

```python
# src/cartage/init/questions.py
"""Interactive questions for `cartage init`. Anything the answers file set is not asked; with --yes nothing is."""
from __future__ import annotations

from rich.prompt import Prompt

from cartage import registry
from cartage.adapters.destinations.sap.meta import available as available_bapis
from cartage.config import validate_model
from cartage.init.answers import InitAnswers, Sample
from cartage.init.credentials import variants

SOURCES = ["files", "rest_api", "sql_database", "python"]


def ask(answers: InitAnswers, answered: set[str], default_project: str, yes: bool, console=None) -> InitAnswers:
    a = answers.model_copy(deep=True)

    def need(key: str) -> bool:
        return not yes and key not in answered

    def text(question: str, default: str) -> str:
        return Prompt.ask(question, default=default, console=console)

    def choice(question: str, choices: list[str], default: str) -> str:
        return Prompt.ask(question, choices=choices, default=default, console=console)

    if need("project"):
        a.project = text("Project name", a.project or default_project)
    a.project = a.project or default_project
    if need("environments"):
        raw = text("Environments, comma-separated (the first is the default)", ", ".join(a.environments))
        a.environments = [e.strip() for e in raw.split(",")]
    if need("source"):
        a.source = choice("Source system", SOURCES, a.source)
    if need(a.source):
        if a.source == "files":
            f = a.files
            f.location = text("Where are the files (./data, s3://, gs://, az://, https://, sftp://)", f.location)
            f.path = text("Path glob under that location", f.path)
            f.format = choice("File format", ["csv", "jsonl", "parquet"], f.format)
        elif a.source == "rest_api":
            r = a.rest_api
            r.base_url = text("API base URL", r.base_url)
            r.path = text("Endpoint path", r.path)
            r.resource = text("Resource (table) name", r.resource or r.path.rstrip("/").rsplit("/", 1)[-1]) or None
            r.data_selector = text("Where the records are in the response (empty: auto)", r.data_selector or "") or None
            r.auth = choice("Auth", ["none", "bearer", "api_key", "http_basic"], r.auth)
        elif a.source == "sql_database":
            s = a.sql_database
            s.dialect = text("Database dialect (postgresql, mssql, mysql, oracle, snowflake, ...)", s.dialect)
            s.table = text("Table", s.table)
            s.schema_ = text("Schema (empty: the default)", s.schema_ or "") or None
            s.cursor = text("Incremental cursor column (empty: full load)", s.cursor or "") or None
        else:
            a.python.function = text("Function name in sources/<pipeline>.py", a.python.function)
    if need("destination"):
        a.destination = choice("Destination", registry.connection_types(), a.destination)
    own = set(registry.available("destinations"))
    if registry.is_dlt_destination(a.destination) and a.destination not in own:
        options = variants(a.destination)
        if len(options) > 1 and need("auth_variant"):
            a.auth_variant = choice("Auth method", options, a.auth_variant or options[0])
        if need("dataset"):
            a.dataset = text("Dataset (empty: the pipeline name)", a.dataset or "") or None
        if need("write_disposition"):
            a.write_disposition = choice("Write disposition", ["append", "replace", "merge"], a.write_disposition)
        if a.write_disposition == "merge" and need("primary_key"):
            a.primary_key = text("Primary key", a.primary_key or "id")
    elif a.destination == "file_export" and need("file_export"):
        a.file_export.format = choice("File format", ["json", "jsonl", "xml", "csv"], a.file_export.format)
    elif a.destination == "sap_bapi" and need("bapi"):
        bapis = available_bapis()
        a.bapi = choice("BAPI", bapis, a.bapi or bapis[0])
    if need("sample_data"):
        path = text("Sample file to test with (CSV, JSONL or Parquet; empty for none)", "")
        a.sample_data = Sample(path=path) if path else None
    if need("pipeline"):
        a.pipeline = text("Pipeline name", a.pipeline_name)
    if need("schedule"):
        a.schedule.target = choice("Schedule", ["none", "airflow", "dagster", "prefect"], a.schedule.target)
        if a.schedule.target != "none":
            a.schedule.cron = text("Cron expression", a.schedule.cron)
    return validate_model(InitAnswers, a.model_dump(by_alias=True), "answers")
```

- [ ] **Step 4: Implement `write_project`**

```python
# src/cartage/init/__init__.py
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
```

- [ ] **Step 5: Wire the command and the summary**

In `src/cartage/cli.py`, replace the `init` command and the `from cartage.scaffold import init_project` import with:

```python
from cartage.init import write_project
from cartage.init.answers import Origin, fetch_files, load_answers
from cartage.init.plan import placeholders, plan_project
from cartage.init.questions import ask
```

```python
@app.command()
def init(
    directory: Path = typer.Argument(Path("."), help="Folder for the new project."),
    answers: str | None = typer.Option(None, "--answers", help="Answers file (a path or an http(s) URL); "
                                                                "its answers are not asked."),
    yes: bool = typer.Option(False, "--yes", "-y", help="Take the defaults for every question not answered."),
) -> None:
    """Create a project: asks about environments, the source, the destination and scheduling, then writes it."""
    with ui.handle_errors(OPTS.debug):
        if (directory / PROJECT_FILE).exists():
            write_project(directory, {})  # raises: already a project
        loaded, answered, origin = load_answers(answers)
        final = ask(loaded, answered, directory.resolve().name, yes, ui.console)
        fetched = fetch_files(final, origin, origin if "sample_data" in answered else Origin.cwd())
        files = plan_project(final, fetched)
        created = write_project(directory, files)
        if origin.is_url:
            ui.console.print(f"Fetched from {origin.base}: {', '.join(sorted(fetched)) or 'nothing else'}")
    ui.init_done(directory, created, placeholders(files), final.pipeline_name)
```

Import `PROJECT_FILE` from `cartage.config` in `cli.py` if it is not already imported.

In `src/cartage/ui.py`, replace `init_done` with:

```python
def init_done(directory: Path, created: list[Path], placeholders: list[str], pipeline: str) -> None:
    body = Text()
    body.append(f"Created {len(created)} files in {directory}\n\n", style="green")
    if placeholders:
        body.append('Fill in every "<fill me>" (secrets go in .cartage/secrets.yaml):\n', style="bold")
        for where in placeholders:
            body.append(f"  {where}\n", style="yellow")
        body.append("\n")
    body.append("Next steps:\n", style="bold")
    for cmd in (f"cd {directory}", "cartage validate", f"cartage plan {pipeline}", f"cartage run {pipeline}"):
        body.append(f"  $ {cmd}\n", style="cyan")
    console.print(Panel(body, title="cartage init", title_align="left", border_style="green"))
```

- [ ] **Step 6: Run the tests**

Run: `.venv/bin/pytest tests/test_init_cli.py -q`
Expected: PASS. If `test_interactive_answers` fails on prompt order, compare the input list against the order in `ask` (the test comment lists it) and fix the test input, not the order.

- [ ] **Step 7: Commit**

```bash
git add src/cartage/init/questions.py src/cartage/init/__init__.py src/cartage/cli.py src/cartage/ui.py tests/test_init_cli.py
git commit -m "feat(init): interactive questions and the new init command"
```

---

### Task 7: The SAP example, and the test suite on it

**Files:**
- Create: `examples/sap/answers.yaml`
- Move: `src/cartage/project_template/data/materials/materials.csv` → `examples/sap/materials.csv`
- Move: `src/cartage/project_template/transforms/materials.py` → `examples/sap/transforms/materials.py`
- Delete: `src/cartage/project_template/`, `src/cartage/scaffold.py`, `tests/test_scaffold.py`
- Modify: `tests/conftest.py` and every test that depends on the demo's layout (table below)

**Interfaces:**
- Consumes: `cartage init --answers ... --yes` (Task 6) and the generated layout (Task 5).
- Produces: fixture `project` (the SAP example without incremental reads, for both engines) and helper `incremental(project)` in `tests/conftest.py`.

- [ ] **Step 1: Create the example**

```bash
mkdir -p examples/sap/transforms
git mv src/cartage/project_template/data/materials/materials.csv examples/sap/materials.csv
git mv src/cartage/project_template/transforms/materials.py examples/sap/transforms/materials.py
```

```yaml
# examples/sap/answers.yaml
# Cartage example: legacy materials CSVs → transforms → SAP via BAPI_MATERIAL_SAVEDATA.
#   cartage init demo --answers https://raw.githubusercontent.com/datacoves/cartage/main/examples/sap/answers.yaml --yes
# dev runs on the sample file and the mock SAP; prd reads S3 and calls SAP over RFC (fill .cartage/secrets.yaml).
project: demo
environments: [dev, prd]
source: files
files: {location: "s3://my-bucket/materials", path: "materials/*.csv", format: csv}
destination: sap_bapi
pipeline: materials
sample_data:
  path: materials.csv
  reader_options: {dtype: str, keep_default_na: false}   # keep codes such as 000123 as text, empty cells as ""
  incremental: true
transforms:
  - map: transforms.materials:normalize_uom
  - filter: transforms.materials:is_active
  - batch: transforms.materials:dedupe
    with: {key: material}
destination_options:
  bapi: BAPI_MATERIAL_SAVEDATA
  mapping:
    HEADDATA.MATERIAL: material
    HEADDATA.IND_SECTOR: industry
    HEADDATA.MATL_TYPE: type
    MATERIALDESCRIPTION[].MATL_DESC: description
    CLIENTDATA.BASE_UOM: uom
  constants:
    MATERIALDESCRIPTION[].LANGU_ISO: EN
    HEADDATA.BASIC_VIEW: X
  commit: per_record
copy: [transforms/materials.py]
schedule: {target: airflow, cron: "0 3 * * *"}
```

- [ ] **Step 2: Remove the old template and scaffold**

```bash
git rm -r -q src/cartage/project_template src/cartage/scaffold.py tests/test_scaffold.py
grep -rn "scaffold\|project_template" src tests pyproject.toml   # expected: no output
```

- [ ] **Step 3: Rewrite the fixture**

```python
# tests/conftest.py
import os
from pathlib import Path

import pytest
from typer.testing import CliRunner

from cartage.cli import app

# Rich reads COLUMNS when the console is created; keep test output on one line per row.
os.environ.setdefault("COLUMNS", "200")

EXAMPLE = Path(__file__).resolve().parents[1] / "examples" / "sap" / "answers.yaml"
INCREMENTAL = "      incremental: true\n"


@pytest.fixture
def project(tmp_path):
    """The SAP example from `cartage init --answers`, without incremental reads so it runs on both engines."""
    root = tmp_path / "demo"
    result = CliRunner().invoke(app, ["init", str(root), "--answers", str(EXAMPLE), "--yes"])
    assert result.exit_code == 0, result.output
    pipeline = root / "pipelines/materials.yaml"
    assert INCREMENTAL in pipeline.read_text()
    pipeline.write_text(pipeline.read_text().replace(INCREMENTAL, ""))
    return root


def incremental(project):
    """Turn the example's incremental sample reads back on (dlt engine only)."""
    pipeline = project / "pipelines/materials.yaml"
    pipeline.write_text(pipeline.read_text().replace(
        "      path: materials.csv\n", "      path: materials.csv\n" + INCREMENTAL))
    return project
```

- [ ] **Step 4: Run the suite and port the demo-dependent tests**

Run: `.venv/bin/pytest -q 2>&1 | grep -E "^FAILED|passed|failed"`

Apply these substitutions in the failing tests only (the generated layout is shown in Task 5):

| Old (test text) | New |
| --- | --- |
| `materials_to_sap` (pipeline name, state paths, DAG ids, `dags/materials_to_sap.py`, `✔ materials_to_sap`) | `materials` |
| `data/materials/materials.csv` | `data/sample/materials.csv` |
| source edits on `"path: materials/*.csv"` | `"      path: materials.csv\n"` (the dev replacement), e.g. no-match test → `"      path: nothing.csv\n"` |
| `"  connection: local_files\n"` (boom source test) | replace `"      connection: samples\n      path: materials.csv\n"` with `"      ref: boom_src:rows\n"` |
| `connection: local_files` + `path: materials/*.csv` in test-local pipelines (`test_dlt_destination.CSV_SOURCE`, `test_file_destination.write_pipeline`) | `connection: samples` + `path: materials.csv` |
| `connections test local_files` | `connections test samples` (still `(1 entry)`) |
| `sap_erp` | `sap` |
| `'client: "100" }'` | `'client: "100"}'` |
| `"dev: { path: .cartage/state }"` | `"dev: {path: .cartage/state}"` |
| `{ connection: local_files, prefix: state }` and `data/state/materials_to_sap/...` | `{connection: samples, prefix: state}` and `data/sample/state/materials/...` |
| `"dev: { path: ./data }"` (airflow integration) | `"dev: {bucket_url: ./data/sample}"` → `'dev: {bucket_url: "${airflow:files.schema}"}'`; `AIRFLOW_CONN_FILES` schema `./data/sample` |
| `"    tags: [sap, materials]\n"` anchors (`_edit_pipeline`, image/decorator tests) | `'    schedule: "0 3 * * *"\n'` |
| `tags=['sap', 'materials']` | `tags=['cartage']` |
| dagster/prefect `schedule("schedule:\n", ...)` | unchanged (the pipeline still has a `schedule:` line) |
| `"  - batch: transforms.materials:dedupe\n    with: { key: material }\n"` | `"  - batch: transforms.materials:dedupe\n    with:\n      key: material\n"` |
| `CARTAGE_SECRET__SAP__USER` / `__PASSWD` | `CARTAGE_SECRET__SAP__PRD__USER` / `__PASSWD` |
| rfc test `--env prd` expecting `'rfc' is not available` | assert `exit_code == 2` and `'is still "<fill me>"'` in the output |
| masking test (Leaky) | first `connections.yaml` text `.replace('"<fill me>"', '"x"')` so prd resolves |
| `test_cli_validate_plan` `"Value 'ZXX'"`, counts `(20, 2, 18, 16, 2)` | unchanged |

Run again until: `.venv/bin/pytest -q` → all pass (≈ 260+ passed, 4 skipped).

- [ ] **Step 5: Check the example end to end through the CLI**

```bash
D=$(mktemp -d) && .venv/bin/cartage init "$D/demo" --answers examples/sap/answers.yaml --yes \
  && .venv/bin/cartage -C "$D/demo" validate && .venv/bin/cartage -C "$D/demo" run materials --json | tail -1
```

Expected: validate ✔; run JSON `{"read": 20, "filtered": 2, "sent": 18, "ok": 16, ...}` and exit code 1 (the two rejects).

- [ ] **Step 6: Lint and commit**

Run: `.venv/bin/ruff check src tests` → `All checks passed!`

```bash
git add -A examples tests src/cartage
git commit -m "feat!: the SAP demo is an init answers file; tests run on it"
```

---

### Task 8: Documentation and the demo GIF

**Files:**
- Modify: `README.md` (Quickstart, Commands table, new "Creating a project" section)
- Modify: `docs/connections.md` (`source.envs`, `{env}` references)
- Modify: `docs/cartage-demo.tape`

- [ ] **Step 1: README Quickstart**

Replace the Quickstart code block with:

```bash
pip install cartage
cartage init demo --answers https://raw.githubusercontent.com/datacoves/cartage/main/examples/sap/answers.yaml --yes
cd demo
cartage validate
cartage plan materials        # dry run: records before/after transforms and the BAPI payloads
cartage run materials         # reads 20 rows, filters 2, sends 18: 16 load, 2 fail on purpose (exit 1)
cartage run materials --advance-state
cartage run materials         # incremental: nothing new to load
cartage generate              # dags/materials.py
```

Below it: "Run `cartage init <dir>` without `--answers` to create your own project; see [Creating a project](#creating-a-project)." Keep the split-screen paragraph, with `sap.dev` instead of `sap_erp.dev`.

- [ ] **Step 2: README "Creating a project" section** (after Quickstart)

```markdown
## Creating a project

`cartage init <dir>` asks a few questions and writes a project that validates straight away:

1. project name and environments (`dev, prd`; the first is the default),
2. the source system: files (a folder, `s3://`, `gs://`, `az://`, `https://`), a REST API, a SQL database or a Python
   function, plus a few details for it,
3. the destination: any dlt destination (`snowflake`, `duckdb`, `postgres`, ...), `file_export` or `sap_bapi`,
4. an optional sample file: with one, the first environment is fully local (the sample replaces the source through
   `source.envs`, and the destination becomes DuckDB, the SAP mock or a local folder), so `cartage run` works before
   any credentials exist,
5. the pipeline name and an optional schedule (Airflow, Dagster or Prefect).

Credentials become placeholders: settings in `connections.yaml` and `${secret:<connection>.<env>.<field>}`
references to `.cartage/secrets.yaml`, both `"<fill me>"`. The fields come from dlt's own credential classes, so every
dlt destination is covered. `cartage validate --env <env>` names every placeholder still to fill.

`--answers <path-or-url>` answers questions from a file (the rest are asked, or take their defaults with `--yes`).
An answers file can also carry pipeline content: `transforms`, `destination_options` and `copy` (files to copy, relative
to the answers file). [`examples/sap/answers.yaml`](examples/sap/answers.yaml) builds the demo above this way.
```

- [ ] **Step 3: Commands table and connections reference**

README Commands table row for init:
`| cartage init <dir> [--answers <file-or-url>] [--yes] | create a project from questions or an answers file |`

`docs/connections.md`, after the paragraph about per-environment `type`, add:

```markdown
A pipeline can also replace its whole source in one environment with `source.envs`, e.g. a sample file in `dev`:
`source: {ref: ..., envs: {dev: {connection: samples, path: sample.csv, format: csv}}}`. A `ref:` source has one
`with:` block for every environment, so its secrets name the environment with `{env}`:
`${secret:database.{env}.credentials}`.
```

- [ ] **Step 4: The demo tape**

Replace the hidden setup and add the wizard. The new `docs/cartage-demo.tape` from `Hide` onward:

```text
Hide
Type `CARTAGE_ROOT="$PWD"; DEMO_DIR="$(mktemp -d /tmp/cartage-demo.XXXXXX)"; trap 'rm -rf "$DEMO_DIR"' EXIT; cp "$CARTAGE_ROOT/examples/sap/materials.csv" "$DEMO_DIR/" && source "$CARTAGE_ROOT/.venv/bin/activate" && export PATH="$CARTAGE_ROOT/.venv/bin:$PATH" && hash -r && cd "$DEMO_DIR" && clear`
Enter
Show
Sleep 700ms

Type "Let's create a project: materials CSVs into SAP."
Sleep 1s
Backspace 49
Type "cartage init demo"
Enter
Wait+Screen /Project name/
Enter
Wait+Screen /Environments/
Enter
Wait+Screen /Source system/
Type "files"
Enter
Wait+Screen /Where are the files/
Type "s3://my-bucket/materials"
Enter
Wait+Screen /Path glob/
Type "materials/*.csv"
Enter
Wait+Screen /File format/
Enter
Wait+Screen /Destination/
Type "sap_bapi"
Enter
Wait+Screen /BAPI/
Enter
Wait+Screen /Sample file/
Type "materials.csv"
Enter
Wait+Screen /Pipeline name/
Type "materials"
Enter
Wait+Screen /Schedule/
Type "airflow"
Enter
Wait+Screen /Cron/
Enter
Wait+Line />$/
Sleep 3s
Type "cd demo && cartage validate"
Enter
Wait+Line />$/
Sleep 2s

Type "cartage plan materials -n 3"
Enter
Wait+Line />$/
Sleep 3s

Type "cartage generate && cat dags/materials.py"
Enter
Wait+Line />$/
Sleep 4s

Type "cartage run materials"
Enter
Wait+Line />$/
Sleep 4s
```

Note for the reader of the GIF: answered interactively, the project has no transforms or mapping yet, so `plan`/`run` show the mapping placeholders. To keep the original story (UOM conversion, 16 ok / 2 rejected), record the GIF with the example instead: replace the `cartage init demo` interaction with
`cartage init demo --answers "$CARTAGE_ROOT/examples/sap/answers.yaml"` and answer only what is unanswered (nothing; pass `--yes`). Choose the variant with the user before recording; the default is the answers-file variant, showing the generated summary panel.

- [ ] **Step 5: Record (if `vhs` is installed) and verify**

Run: `command -v vhs && vhs docs/cartage-demo.tape` → writes `docs/cartage-demo.gif`. If `vhs` is missing, leave the tape updated and report that the GIF needs recording.

Run: `.venv/bin/pytest -q && .venv/bin/ruff check src tests` → all pass.

- [ ] **Step 6: Commit**

```bash
git add README.md docs/connections.md docs/cartage-demo.tape docs/cartage-demo.gif
git commit -m "docs: question-driven init, answers files, and the SAP example"
```
