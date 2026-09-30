from typer.testing import CliRunner

from cartage.cli import app
from cartage.config import load_project


def test_init_creates_loadable_project(tmp_path):
    result = CliRunner().invoke(app, ["init", str(tmp_path / "demo")])
    assert result.exit_code == 0, result.output
    assert "cartage run materials" in result.output
    root = tmp_path / "demo"
    assert (root / ".gitignore").read_text().strip() == ".cartage/"
    assert not (root / "gitignore").exists()
    project = load_project(root)
    assert project.load_pipeline("materials").name == "materials_to_sap"
    assert len((root / "data/materials/materials.csv").read_text().splitlines()) == 21


def test_init_refuses_to_overwrite(project):
    result = CliRunner().invoke(app, ["init", str(project)])
    assert result.exit_code == 2
    assert "Refusing to overwrite" in result.output
