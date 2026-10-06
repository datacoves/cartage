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
    answers = "demo\ndev, qa\nfiles\n./in\n*.csv\ncsv\nduckdb\n\nappend\n\norders\nnone\n"
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


def test_bad_environments_are_asked_again(tmp_path):
    answers = "demo\n dev, ,dev\ndev, prd\nfiles\n./in\n*.csv\ncsv\nduckdb\n\nappend\n\norders\nnone\n"
    result = init(str(tmp_path / "p"), input=answers)
    assert result.exit_code == 0, result.output
    assert load_project(tmp_path / "p").config.environments == ["dev", "prd"]


def test_an_existing_file_is_refused_before_any_question(tmp_path):
    (tmp_path / ".gitignore").write_text("mine\n")
    result = init(str(tmp_path), input="")  # no answers: a question would fail differently
    assert result.exit_code == 2 and ".gitignore" in result.output


def test_a_file_in_the_way_of_a_folder_is_refused_and_nothing_is_written(tmp_path):
    (tmp_path / "transforms").write_text("not a folder\n")
    result = init(str(tmp_path), "--yes")
    assert result.exit_code == 2 and "transforms" in result.output
    assert not (tmp_path / "cartage.yaml").exists()
