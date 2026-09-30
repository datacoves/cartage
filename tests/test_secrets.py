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
