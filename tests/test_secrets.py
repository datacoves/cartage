import pytest
import typer
from typer.testing import CliRunner

from cartage import ui
from cartage.core import CartageError
from cartage.secrets import Secrets, mask


def test_env_reference(tmp_path):
    assert Secrets(tmp_path, environ={"HOST": "h1"}).resolve({"a": "${env:HOST}"}, "x") == {"a": "h1"}


def test_partial_string_and_non_strings(tmp_path):
    s = Secrets(tmp_path, environ={"HOST": "h1"})
    assert s.resolve({"url": "https://${env:HOST}/api", "port": 22, "tags": ["${env:HOST}"]}, "x") == {
        "url": "https://h1/api", "port": 22, "tags": ["h1"]}


def test_secret_env_var_wins_over_file(tmp_path):
    (tmp_path / ".cartage").mkdir()
    (tmp_path / ".cartage" / "secrets.yaml").write_text("sap:\n  user: file_user\n  passwd: file_pw\n")
    s = Secrets(tmp_path, environ={"CARTAGE_SECRET__SAP__USER": "env_user"})
    assert s.resolve("${secret:sap.user}", "x") == "env_user"
    assert s.resolve("${secret:sap.passwd}", "x") == "file_pw"


def test_missing_secret_names_reference_location_and_env_var(tmp_path):
    with pytest.raises(CartageError) as info:
        Secrets(tmp_path, environ={}).resolve({"passwd": "${secret:sap.passwd}"}, "connections.yaml:7")
    assert "sap.passwd" in info.value.message
    assert "connections.yaml:7" in info.value.message
    assert "CARTAGE_SECRET__SAP__PASSWD" in info.value.hint


def test_missing_env_var(tmp_path):
    with pytest.raises(CartageError, match="Environment variable NOPE is not set"):
        Secrets(tmp_path, environ={}).resolve("${env:NOPE}", "x")


def test_mask_scrubs_revealed_values(tmp_path):
    Secrets(tmp_path, environ={"CARTAGE_SECRET__AWS__KEY": "s3cr3tvalue"}).resolve("${secret:aws.key}", "x")
    assert mask("login failed for s3cr3tvalue") == "login failed for ****"


def test_error_panel_masks_secret_values(tmp_path):
    Secrets(tmp_path, environ={"CARTAGE_SECRET__SAP__PASSWD": "hunter2hunter2"}).resolve("${secret:sap.passwd}", "x")
    t = typer.Typer()

    @t.command()
    def boom():
        with ui.handle_errors():
            raise CartageError("RFC logon failed with password hunter2hunter2")

    result = CliRunner().invoke(t)
    assert "hunter2hunter2" not in result.output
    assert "****" in result.output


def test_short_values_not_masked(tmp_path):
    s = Secrets(tmp_path, environ={"PORT": "22"})
    s.resolve("${env:PORT}", "x")
    assert mask("connection on port 22") == "connection on port 22"


def test_long_values_are_masked(tmp_path):
    s = Secrets(tmp_path, environ={"PASSWORD": "secret123"})
    s.resolve("${env:PASSWORD}", "x")
    assert mask("auth failed with secret123") == "auth failed with ****"


def test_invalid_yaml_in_secrets_file(tmp_path):
    (tmp_path / ".cartage").mkdir()
    (tmp_path / ".cartage" / "secrets.yaml").write_text("invalid: yaml: content:")
    with pytest.raises(CartageError) as info:
        Secrets(tmp_path, environ={}).resolve("${secret:key}", "x")
    assert ".cartage/secrets.yaml" in info.value.message
    assert "invalid YAML" in info.value.message


def test_non_mapping_top_level_in_secrets_file(tmp_path):
    (tmp_path / ".cartage").mkdir()
    (tmp_path / ".cartage" / "secrets.yaml").write_text("- item1\n- item2\n")
    with pytest.raises(CartageError) as info:
        Secrets(tmp_path, environ={}).resolve("${secret:key}", "x")
    assert ".cartage/secrets.yaml must contain a mapping" in info.value.message


def test_debug_traceback_masks_secret_values(tmp_path):
    Secrets(tmp_path, environ={"CARTAGE_SECRET__SAP__PASSWD": "hunter3hunter3"}).resolve("${secret:sap.passwd}", "x")
    t = typer.Typer()

    @t.command()
    def boom():
        with ui.handle_errors(debug=True):
            raise CartageError("RFC logon failed with password hunter3hunter3")

    result = CliRunner().invoke(t)
    assert "Traceback" in result.output and "hunter3hunter3" not in result.output
