import json

from typer.testing import CliRunner

from cartage.cli import app

runner = CliRunner()


def cli(project, *args):
    return runner.invoke(app, ["-C", str(project), *args])


def test_connections_list_shows_references_not_values(project, monkeypatch):
    monkeypatch.setenv("CARTAGE_SECRET__SAP__PRD__USER", "realuser")
    result = cli(project, "connections", "list")
    assert result.exit_code == 0
    assert "sap" in result.output and "samples" in result.output
    assert "${secret:sap.prd.user}" in result.output and "realuser" not in result.output


def test_connections_test(project, monkeypatch):
    ok = cli(project, "connections", "test", "sap")
    assert ok.exit_code == 0 and "mock (in-process)" in ok.output
    assert "(1 entry)" in cli(project, "connections", "test", "samples").output
    monkeypatch.setenv("CARTAGE_SECRET__SAP__PRD__USER", "u")
    monkeypatch.setenv("CARTAGE_SECRET__SAP__PRD__PASSWD", "p")
    rfc = cli(project, "connections", "test", "sap", "--env", "prd")  # ashost, sysnr, client are still placeholders
    assert rfc.exit_code == 2 and 'is still "<fill me>"' in rfc.output


def test_state_show_and_reset(project):
    cli(project, "run", "materials", "--engine", "python", "--advance-state", "--json")
    shown = cli(project, "state", "show", "materials")
    assert shown.exit_code == 0 and '"last_run"' in shown.output
    assert cli(project, "state", "reset", "materials", "--yes").exit_code == 0
    assert json.loads(cli(project, "state", "show", "materials").output) == {"state": {}, "dlt_archive": False}


def test_state_reset_asks_for_confirmation(project):
    cli(project, "run", "materials", "--engine", "python", "--advance-state", "--json")
    result = runner.invoke(app, ["-C", str(project), "state", "reset", "materials"], input="n\n")
    assert result.exit_code == 0 and "cancelled" in result.output
    assert json.loads(cli(project, "state", "show", "materials").output)["state"]  # nothing deleted


def test_connections_test_masks_secrets(project, monkeypatch):
    monkeypatch.setenv("CARTAGE_SECRET__SAP__PRD__USER", "realuser1")
    monkeypatch.setenv("CARTAGE_SECRET__SAP__PRD__PASSWD", "p4ssw0rd")

    class Leaky:
        @classmethod
        def check_connection(cls, config, root):
            return f"logged in as {config['user']}"

    connections = project / "connections.yaml"
    connections.write_text(connections.read_text().replace('"<fill me>"', '"x"'))
    monkeypatch.setattr("cartage.cli.registry.connection_class", lambda ctype: Leaky)
    result = cli(project, "connections", "test", "sap", "--env", "prd")
    assert result.exit_code == 0, result.output
    assert "logged in as ****" in result.output and "realuser1" not in result.output


def test_a_missing_source_folder_fails_connections_test(project):
    import shutil

    shutil.rmtree(project / "data" / "sample")
    result = cli(project, "connections", "test", "samples")
    assert result.exit_code == 3 and "Not found" in result.output
