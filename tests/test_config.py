import textwrap

import pytest

from cartage.config import load_project
from cartage.core import CartageError

PROJECT = """\
project: demo
environments: [dev, prd]
default_env: dev
"""

CONNECTIONS = """\
connections:
  local_files:
    type: filesystem
    envs:
      dev: { path: ./data }
"""

PIPELINE = """\
name: materials_to_sap
source:
  connection: local_files
  format: csv
  path: materials/*.csv
  incremental: true
transforms:
  - map: transforms.materials:normalize_uom
  - batch: transforms.materials:dedupe
    with: { key: material }
destination:
  connection: sap_erp
  bapi: BAPI_MATERIAL_SAVEDATA
"""


def write(root, files):
    for name, text in files.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(textwrap.dedent(text))


@pytest.fixture
def root(tmp_path):
    write(tmp_path, {"cartage.yaml": PROJECT, "connections.yaml": CONNECTIONS, "pipelines/materials.yaml": PIPELINE})
    return tmp_path


def test_finds_root_from_subdirectory(root):
    assert load_project(root / "pipelines").root == root.resolve()


def test_missing_project_file(tmp_path):
    with pytest.raises(CartageError, match="No cartage.yaml found"):
        load_project(tmp_path)


def test_default_env_must_be_listed(root):
    (root / "cartage.yaml").write_text(PROJECT.replace("default_env: dev", "default_env: qa"))
    with pytest.raises(CartageError, match="default_env 'qa'"):
        load_project(root)


def test_load_pipeline_by_bare_name(root):
    pipeline = load_project(root).load_pipeline("materials")
    assert pipeline.name == "materials_to_sap"
    assert pipeline.source.options() == {"format": "csv", "path": "materials/*.csv", "incremental": True}
    assert [(t.kind, t.ref, t.with_) for t in pipeline.transforms] == [
        ("map", "transforms.materials:normalize_uom", {}),
        ("batch", "transforms.materials:dedupe", {"key": "material"}),
    ]
    assert pipeline.destination.options() == {"bapi": "BAPI_MATERIAL_SAVEDATA"}
    assert pipeline.destination_specs == [pipeline.destination]
    assert pipeline.path == (root / "pipelines" / "materials.yaml").resolve()


def test_multiple_pipeline_destinations(root):
    pipeline_text = PIPELINE.replace(
        "destination:\n  connection: sap_erp\n  bapi: BAPI_MATERIAL_SAVEDATA\n",
        "destinations:\n  - connection: sap_erp\n    bapi: BAPI_MATERIAL_SAVEDATA\n  - connection: warehouse\n    table_name: materials\n",
    )
    connections_text = CONNECTIONS.replace(
        "  local_files:\n",
        "  warehouse:\n    type: dlt\n    envs:\n      dev: { destination: filesystem, bucket_url: file:///tmp/cartage-tests }\n  local_files:\n",
    )
    (root / "connections.yaml").write_text(connections_text)
    (root / "pipelines" / "materials.yaml").write_text(pipeline_text)

    destinations = load_project(root).load_pipeline("materials").destination_specs

    assert [spec.connection for spec in destinations] == ["sap_erp", "warehouse"]
    assert destinations[1].options() == {"table_name": "materials"}


@pytest.mark.parametrize(
    "replacement, message",
    [
        (
            "destination:\n  connection: sap_erp\n  bapi: BAPI_MATERIAL_SAVEDATA\ndestinations:\n  - connection: warehouse\n",
            "either 'destination' or 'destinations'",
        ),
        (
            "destinations:\n  - connection: sap_erp\n  - connection: sap_erp\n",
            "set .name. on destinations that share a connection",
        ),
        ("", "set 'destination' or a non-empty 'destinations' list"),
    ],
)
def test_invalid_destination_selection(root, replacement, message):
    pipeline_text = PIPELINE.replace(
        "destination:\n  connection: sap_erp\n  bapi: BAPI_MATERIAL_SAVEDATA\n", replacement
    )
    (root / "pipelines" / "materials.yaml").write_text(pipeline_text)
    with pytest.raises(CartageError, match=message):
        load_project(root).load_pipeline("materials")


def test_pipeline_error_points_at_line(root):
    bad = PIPELINE.replace("  - batch: transforms.materials:dedupe\n", "  - batch: a:b\n    map: c:d\n")
    (root / "pipelines" / "materials.yaml").write_text(bad)
    with pytest.raises(CartageError) as info:
        load_project(root).load_pipeline("materials")
    assert "pipelines/materials.yaml:9" in info.value.message
    assert "exactly one of 'map', 'filter' or 'batch'" in info.value.message


def test_invalid_yaml_reports_line(root):
    (root / "connections.yaml").write_text("connections:\n  a: [unclosed\n")
    with pytest.raises(CartageError, match="connections.yaml:.*invalid YAML"):
        load_project(root)


def test_pipeline_not_found_lists_names(root):
    with pytest.raises(CartageError) as info:
        load_project(root).load_pipeline("nope")
    assert "materials" in info.value.hint


def test_connection_lookup(root):
    project = load_project(root)
    assert project.connection("local_files", "dev") == ("filesystem", {"path": "./data"}, "connections.yaml:5")
    with pytest.raises(CartageError, match="no settings for environment 'prd'"):
        project.connection("local_files", "prd")
    with pytest.raises(CartageError, match="Unknown connection 'sap_erp'"):
        project.connection("sap_erp", "dev")


def test_unknown_env(root):
    with pytest.raises(CartageError, match="Unknown environment 'qa'"):
        load_project(root).resolve_env("qa")
