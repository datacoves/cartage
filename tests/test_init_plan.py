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


def test_duckdb_without_a_sample_runs_into_a_project_file(tmp_path):
    root, _ = write(tmp_path, InitAnswers(project="p", files={"location": "./data"}))
    (root / "data").mkdir()
    (root / "data" / "a.csv").write_text("id,name\n1,a\n2,b\n")
    result = cli(root, "run", "files_to_duckdb", "--env", "prd", "--json")
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["read"] == 2
    assert (root / "p.duckdb").is_file()


def test_yaml_typed_values_stay_strings(tmp_path):
    root, _ = write(tmp_path, InitAnswers(project="2024", files={"location": "true", "path": "null"}))
    from cartage.config import load_project

    project = load_project(root)
    assert project.config.project == "2024"
    assert project.load_pipeline("files_to_duckdb").source.options()["path"] == "null"


def test_copy_cannot_replace_generated_files():
    with pytest.raises(CartageError, match="cartage.yaml"):
        plan_project(InitAnswers(copy=["cartage.yaml"]), {"cartage.yaml": b"project: evil\n"})


def test_csv_samples_are_read_as_text_by_default(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    sample = b"material,uom\n000123,EA\n1000140000000000000001,EA\n"
    answers = InitAnswers(project="p", destination="file_export", sample_data="m.csv")
    root, _ = write(tmp_path, answers, {"data/sample/m.csv": sample})
    result = cli(root, "run", answers.pipeline_name, "--json")
    assert result.exit_code == 0, result.output
    assert '"000123"' in (root / "output" / f"{answers.pipeline_name}.jsonl").read_text()


def test_single_environment_explains_the_unused_source_connection(tmp_path):
    _, files = write(tmp_path, InitAnswers(project="p", environments=["dev"], sample_data="s.csv"),
                     {"data/sample/s.csv": SAMPLE})
    assert "add an environment" in files["connections.yaml"].decode()
