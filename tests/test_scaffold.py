import importlib
import sys

import pytest
from typer.testing import CliRunner

from cartage.cli import app

runner = CliRunner()


def cli(project, *args):
    return runner.invoke(app, ["-C", str(project), *args])


@pytest.mark.parametrize("target", ["airflow", "dagster", "prefect"])
def test_a_template_override_renders_the_default_until_edited(project, target):
    if target != "airflow":
        pipeline = project / "pipelines/materials.yaml"
        pipeline.write_text(pipeline.read_text().replace("schedule:\n", f"schedule:\n  {target}: {{}}\n", 1))
    before, after = project / "before", project / "after"
    assert cli(project, "generate", "--target", target, "-o", str(before)).exit_code == 0

    result = cli(project, "scaffold", target)
    assert result.exit_code == 0, result.output
    override = (project / "templates" / target).iterdir().__next__()
    assert "{% block" in override.read_text()
    assert cli(project, "generate", "--target", target, "-o", str(after)).exit_code == 0
    assert [p.read_text() for p in sorted(before.iterdir())] == [p.read_text() for p in sorted(after.iterdir())]

    override.write_text(override.read_text().replace("{% block extra %}\n{{ super() -}}", "{% block extra %}\n# mine\n"))
    assert cli(project, "generate", "--target", target, "-o", str(after)).exit_code == 0
    assert next(after.iterdir()).read_text().rstrip().endswith("# mine")

    again = cli(project, "scaffold", target)
    assert again.exit_code == 2 and "already exists" in again.output


def test_transform_and_source_modules_work_as_written(project, monkeypatch):
    assert cli(project, "scaffold", "transform", "cleanup").exit_code == 0
    assert cli(project, "scaffold", "source", "my_api").exit_code == 0
    monkeypatch.syspath_prepend(str(project))
    for module in [m for m in sys.modules if m.split(".")[0] in ("transforms", "sources")]:
        monkeypatch.delitem(sys.modules, module)  # other tests' projects have their own transforms package

    transforms = importlib.import_module("transforms.cleanup")
    assert transforms.example({"id": 1}) == {"id": 1} and transforms.keep({"id": 1})
    assert transforms.dedupe([{"id": 1}, {"id": 1, "x": 2}]) == [{"id": 1, "x": 2}]
    assert list(importlib.import_module("sources.my_api").my_api()) == [{"id": 1, "name": "example"}]


@pytest.mark.parametrize(("args", "message"), [
    (["scaffold", "nope"], "Unknown scaffold 'nope'"),
    (["scaffold", "transform"], "needs a module name"),
    (["scaffold", "source", "my-api"], "needs a module name"),
    (["scaffold", "airflow", "x"], "takes no name"),
])
def test_invalid_scaffolds(project, args, message):
    result = cli(project, *args)
    assert result.exit_code == 2 and message in result.output
