import pytest
import typer
from typer.testing import CliRunner

from cartage import ui
from cartage.core import CartageError
from cartage.secrets import FILL_ME, Secrets, expand_env, mask


def test_env_reference(tmp_path):
    assert Secrets(tmp_path, environ={"HOST": "h1"}).resolve({"a": "${env:HOST}"}, "x") == {"a": "h1"}


def test_partial_string_and_non_strings(tmp_path):
    s = Secrets(tmp_path, environ={"HOST": "h1"})
    assert s.resolve({"url": "https://${env:HOST}/api", "port": 22, "tags": ["${env:HOST}"]}, "x") == {
        "url": "https://h1/api", "port": 22, "tags": ["h1"]}


def test_secret_env_var_wins_over_file(tmp_path):
    (tmp_path / ".cartage").mkdir()
    (tmp_path / ".cartage" / "secrets.yaml").write_text("sap:\n  user: file_user\n  passwd: file_pw\n")  # ggignore (fake test value)
    s = Secrets(tmp_path, environ={"CARTAGE_SECRET__SAP__USER": "env_user"})
    assert s.resolve("${secret:sap.user}", "x") == "env_user"
    assert s.resolve("${secret:sap.passwd}", "x") == "file_pw"  # ggignore (fake test value)


def test_home_secrets_fill_in_what_the_project_file_lacks(tmp_path, home):
    (home / ".cartage").mkdir()
    (home / ".cartage" / "secrets.yaml").write_text("sap:\n  user: home_user\n  passwd: home_pw\n")  # ggignore (fake test value)
    (tmp_path / ".cartage").mkdir()
    (tmp_path / ".cartage" / "secrets.yaml").write_text("sap:\n  user: project_user\n  client: <fill me>\n")
    s = Secrets(tmp_path, environ={})
    assert s.resolve("${secret:sap.user}", "x") == "project_user"
    assert s.resolve("${secret:sap.passwd}", "x") == "home_pw"  # ggignore (fake test value)
    with pytest.raises(CartageError, match=r"\(\.cartage/secrets\.yaml, referenced at x\)"):
        s.resolve("${secret:sap.client}", "x")


def test_missing_secret_names_reference_location_and_env_var(tmp_path):
    with pytest.raises(CartageError) as info:
        Secrets(tmp_path, environ={}).resolve({"passwd": "${secret:sap.passwd}"}, ".cartage/connections.yaml:7")
    assert "sap.passwd" in info.value.message
    assert ".cartage/connections.yaml:7" in info.value.message
    assert "CARTAGE_SECRET__SAP__PASSWD" in info.value.hint


def test_missing_env_var(tmp_path):
    with pytest.raises(CartageError, match="Environment variable NOPE is not set"):
        Secrets(tmp_path, environ={}).resolve("${env:NOPE}", "x")


def test_mask_scrubs_revealed_values(tmp_path):
    Secrets(tmp_path, environ={"CARTAGE_SECRET__AWS__KEY": "s3cr3tvalue"}).resolve("${secret:aws.key}", "x")
    assert mask("login failed for s3cr3tvalue") == "login failed for ****"


def test_error_panel_masks_secret_values(tmp_path):
    Secrets(tmp_path, environ={"CARTAGE_SECRET__SAP__PASSWD": "hunter2hunter2"}).resolve("${secret:sap.passwd}", "x")  # ggignore (fake test value)
    t = typer.Typer()

    @t.command()
    def boom():
        with ui.handle_errors():
            raise CartageError("RFC logon failed with password hunter2hunter2")  # ggignore (fake test value)

    result = CliRunner().invoke(t)
    assert "hunter2hunter2" not in result.output  # ggignore (fake test value)
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
    Secrets(tmp_path, environ={"CARTAGE_SECRET__SAP__PASSWD": "hunter3hunter3"}).resolve("${secret:sap.passwd}", "x")  # ggignore (fake test value)
    t = typer.Typer()

    @t.command()
    def boom():
        with ui.handle_errors(debug=True):
            raise CartageError("RFC logon failed with password hunter3hunter3")  # ggignore (fake test value)

    result = CliRunner().invoke(t)
    assert "Traceback" in result.output and "hunter3hunter3" not in result.output  # ggignore (fake test value)


def secrets_file(tmp_path, text):
    (tmp_path / ".cartage").mkdir(exist_ok=True)
    (tmp_path / ".cartage" / "secrets.yaml").write_text(text)
    return tmp_path


def test_env_placeholder_in_secret_keys(tmp_path):
    root = secrets_file(tmp_path, "db:\n  prd:\n    url: postgresql://x\n")
    assert Secrets(root, environ={}, env="prd").resolve("${secret:db.{env}.url}", "p.yaml") == "postgresql://x"


def test_env_placeholder_needs_an_environment(tmp_path):
    with pytest.raises(CartageError, match=r"\{env\}"):
        Secrets(tmp_path, environ={}).resolve("${secret:db.{env}.url}", "p.yaml")


def test_unfilled_secret_names_the_key_and_file(tmp_path):
    root = secrets_file(tmp_path, f'warehouse:\n  prd:\n    password: "{FILL_ME}"\n')
    with pytest.raises(CartageError) as info:
        Secrets(root, environ={}, env="prd").resolve({"password": "${secret:warehouse.prd.password}"}, "c.yaml:3")
    assert 'warehouse.prd.password is still "<fill me>"' in info.value.message
    assert ".cartage/secrets.yaml" in info.value.message


def test_unfilled_setting_names_its_path(tmp_path):
    with pytest.raises(CartageError, match='credentials.host at c.yaml:5 is still "<fill me>"'):
        Secrets(tmp_path, environ={}).resolve({"credentials": {"host": FILL_ME}}, "c.yaml:5")


def test_unfilled_value_from_the_environment_is_also_caught(tmp_path):
    with pytest.raises(CartageError, match="is still"):
        Secrets(tmp_path, environ={"CARTAGE_SECRET__A__B": FILL_ME}).resolve("${secret:a.b}", "x")


def test_env_defaults_and_secrets_that_are_env_references(tmp_path):
    (tmp_path / ".cartage").mkdir()
    (tmp_path / ".cartage" / "secrets.yaml").write_text('api: {token: "${env:API_TOKEN}"}\n')
    s = Secrets(tmp_path, environ={"API_TOKEN": "t0ken"})  # ggignore (fake test value)
    assert s.resolve("${env:NOPE:-fallback} ${env:NOPE:-} ${secret:api.token}", "x") == "fallback  t0ken"


def test_expand_env_fills_env_references_only():
    data = {"a": ["${env:X}", "${secret:k}", "${airflow:c.login}"], "b": "${env:Y:-y}"}
    assert expand_env(data, "f", {"X": "1"}) == {"a": ["1", "${secret:k}", "${airflow:c.login}"], "b": "y"}
    with pytest.raises(CartageError, match="Environment variable Z is not set \\(referenced at f\\)"):
        expand_env("${env:Z}", "f", {})
