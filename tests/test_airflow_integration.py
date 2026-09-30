import os
import shutil
import subprocess

import pytest
from typer.testing import CliRunner

from cartage.cli import app

AIRFLOW = shutil.which("airflow")
pytestmark = [pytest.mark.airflow,
              pytest.mark.skipif(AIRFLOW is None, reason="needs apache-airflow: uv run --extra airflow pytest -m airflow")]

CLEAN_CSV = "material,industry,type,description,uom,status\n100001,M,FERT,Pump housing,EA,active\n100002,M,ROH,Steel,KG,active\n"


def test_generated_dag_runs_cartage(project, tmp_path):
    (project / "data/materials/materials.csv").write_text(CLEAN_CSV)
    assert CliRunner().invoke(app, ["-C", str(project), "generate"]).exit_code == 0
    env = {**os.environ,
           "AIRFLOW_HOME": str(tmp_path / "airflow"),
           "AIRFLOW__CORE__DAGS_FOLDER": str(project / "dags"),
           "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
           "AIRFLOW_VAR_CARTAGE_ENV": "dev"}
    subprocess.run([AIRFLOW, "db", "migrate"], env=env, check=True, capture_output=True, timeout=600)
    result = subprocess.run([AIRFLOW, "dags", "test", "materials_to_sap"], env=env, capture_output=True, text=True, timeout=600)
    assert result.returncode == 0, result.stdout[-4000:] + result.stderr[-4000:]
    # State is only saved after a run without record errors, so this proves `cartage run` succeeded inside Airflow.
    assert (project / ".cartage/state/materials_to_sap/dev/state.json").is_file()
