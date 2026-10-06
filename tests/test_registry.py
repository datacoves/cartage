import pytest
from typer.testing import CliRunner

from cartage import registry
from cartage.adapters.destinations.dlt import DltDestinationAdapter
from cartage.adapters.sources.filesystem import FilesystemSource
from cartage.cli import app
from cartage.core import CartageError


def test_builtins_are_registered():
    assert {"filesystem", "dlt"} <= set(registry.available("sources"))
    assert {"sap_bapi", "file_export"} <= set(registry.available("destinations"))
    assert {"python", "dlt"} <= set(registry.available("engines"))
    assert "airflow" in registry.available("orchestrators")


def test_unknown_name_lists_installed():
    with pytest.raises(CartageError) as info:
        registry.get("engines", "spark")
    assert "Unknown engine 'spark'" in info.value.message
    assert "python" in info.value.hint


def test_dlt_destinations_are_connection_types():
    assert registry.destination_class("snowflake") is DltDestinationAdapter
    assert registry.destination_class("sources.lake:factory") is DltDestinationAdapter  # a module:function factory
    assert registry.destination_config("snowflake", {"credentials": "x"}) == {"destination": "snowflake", "credentials": "x"}
    assert registry.destination_config("sap_bapi", {"transport": "mock"}) == {"transport": "mock"}
    assert registry.connection_class("filesystem") is FilesystemSource  # source, state store and dlt destination
    assert registry.destination_class("filesystem") is DltDestinationAdapter
    assert "destination" not in registry.dlt_destinations()  # dlt's custom-destination decorator, not a type


def test_dlt_types_take_no_destination_setting():
    with pytest.raises(CartageError, match="takes no 'destination' setting"):
        registry.destination_config("duckdb", {"destination": "duckdb"})


def test_a_plugin_named_like_a_dlt_destination_is_an_error(monkeypatch):
    monkeypatch.setattr(registry, "available", lambda kind: {"snowflake": None} if kind == "destinations" else {})
    with pytest.raises(CartageError, match="both a Cartage destination and a dlt destination"):
        registry.destination_class("snowflake")


@pytest.mark.parametrize("old, hint", [("dlt", "type: snowflake"), ("sap", "sap_bapi"), ("file", "file_export"),
                                       ("s3", "bucket_url: s3://")])
def test_old_type_names_point_to_the_new_ones(old, hint):
    with pytest.raises(CartageError, match=f"Unknown connection type '{old}'") as info:
        registry.connection_class(old)
    assert hint in info.value.hint


def test_unknown_connection_type_lists_types():
    with pytest.raises(CartageError, match="Unknown connection type 'ftp'") as info:
        registry.connection_class("ftp")
    assert "sap_bapi" in info.value.hint and "snowflake" in info.value.hint


def test_plugins_command_lists_adapters():
    result = CliRunner().invoke(app, ["plugins"])
    assert result.exit_code == 0
    for name in ("filesystem", "sap_bapi", "file_export", "python", "airflow", "snowflake"):
        assert name in result.output
