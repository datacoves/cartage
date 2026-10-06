"""Interactive questions for `cartage init`. Anything the answers file set is not asked; with --yes nothing is."""
from __future__ import annotations

from pydantic import ValidationError
from rich.console import Console
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
    while need("environments"):  # checked here, so a typo does not cost the rest of the interview
        raw = text("Environments, comma-separated (the first is the default)", ", ".join(a.environments))
        try:
            a.environments = InitAnswers(environments=[e.strip() for e in raw.split(",")]).environments
            break
        except ValidationError as e:
            (console or Console()).print(f"[red]{e.errors()[0]['msg'].removeprefix('Value error, ')}[/red]")
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
