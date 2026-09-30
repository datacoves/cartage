import pytest
from typer.testing import CliRunner

from cartage import registry
from cartage.cli import app
from cartage.core import CartageError


def test_builtins_are_registered():
    assert {"filesystem", "s3", "dlt"} <= set(registry.available("sources"))
    assert "sap" in registry.available("destinations")
    assert {"python", "dlt"} <= set(registry.available("engines"))
    assert "airflow" in registry.available("orchestrators")


def test_unknown_name_lists_installed():
    with pytest.raises(CartageError) as info:
        registry.get("engines", "spark")
    assert "Unknown engine 'spark'" in info.value.message
    assert "python" in info.value.hint


def test_unknown_connection_type():
    with pytest.raises(CartageError, match="Unknown connection type 'ftp'"):
        registry.connection_class("ftp")


def test_plugins_command_lists_adapters():
    result = CliRunner().invoke(app, ["plugins"])
    assert result.exit_code == 0
    for name in ("filesystem", "sap", "python", "airflow"):
        assert name in result.output
