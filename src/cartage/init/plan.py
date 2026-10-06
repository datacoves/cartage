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
    """Plain when YAML reads it back as the same string; quoted otherwise (2024, true, null, *.csv, ...)."""
    if PLAIN.fullmatch(value) and not value.endswith(" ") and YAML(typ="safe").load(value) == value:
        return value
    return json.dumps(value)


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
        if not real:
            lines.append("  # no environment reads the real files yet: add an environment here, e.g. prd: {bucket_url: s3://...}")
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
            local = _scalar(f"{a.project}.duckdb")
            envs[first] = ("{bucket_url: ./lake}" if d == "filesystem" else f"{{credentials: {local}}}"
                           if d == "duckdb" else f"{{type: duckdb, credentials: {local}}}")
        for e in real:
            if d == "duckdb":  # a file in the project, named after it (dlt's default name clashes with the dataset)
                envs[e] = f"{{credentials: {_scalar(f'{a.project}.duckdb')}}}"
            else:
                envs[e] = ([f"bucket_url: {FILL}"] if d == "filesystem"
                           else credential_lines(credentials(d, variant=a.auth_variant), name, e, secrets))
    lines += [*([""] if a.source == "files" else []), *_connection(name, d, envs)]
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
        note = f"# the sample file: {first} runs locally, no credentials"
        lines += ["  envs:", f"    {first}:                                   {note}", "      connection: samples",
                  f"      path: {_scalar(s.file_name)}", f"      format: {s.file_format}"]
        reader_options = s.reader_options
        if not reader_options and s.file_format == "csv":  # samples hold codes such as 000123: read them as text
            reader_options = {"dtype": "str", "keep_default_na": False}
        if reader_options:
            lines += ["      reader_options:", *_indent(_dump(reader_options), 8)]
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
    clashes = sorted(set(fetched) & set(out))
    if clashes:
        raise CartageError(f"copy would replace files init writes: {', '.join(clashes)}",
                           hint="copy adds files (e.g. transforms/); it cannot replace the project's own files")
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
