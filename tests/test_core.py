import typer
from typer.testing import CliRunner

from cartage import ui
from cartage.cli import app
from cartage.core import (
    BatchResult,
    CartageError,
    FatalRunError,
    RecordError,
    RunResult,
)

runner = CliRunner()


def test_version():
    result = runner.invoke(app, ["--version"])
    assert result.exit_code == 0
    assert "cartage 0.8.0" in result.output


def _app_raising(error, debug=False):
    t = typer.Typer()

    @t.command()
    def boom():
        with ui.handle_errors(debug):
            raise error

    return t


def test_config_error_renders_panel_and_exits_2():
    result = runner.invoke(
        _app_raising(
            CartageError("Unknown connection 'x'", hint="check connections.yaml")
        )
    )
    assert result.exit_code == 2
    assert "Unknown connection 'x'" in result.output
    assert "check connections.yaml" in result.output


def test_fatal_error_exits_3():
    assert runner.invoke(_app_raising(FatalRunError("SAP down"))).exit_code == 3


def test_unexpected_error_renders_panel_and_exits_3():
    result = runner.invoke(_app_raising(ValueError("nope")))
    assert result.exit_code == 3
    assert "Unexpected error: ValueError: nope" in result.output
    assert "--debug" in result.output and "Traceback" not in result.output


def test_unexpected_error_traceback_only_with_debug():
    result = runner.invoke(_app_raising(ValueError("nope"), debug=True))
    assert result.exit_code == 3 and "Traceback" in result.output


def test_exit_passes_through_handle_errors():
    assert runner.invoke(_app_raising(typer.Exit(5))).exit_code == 5


def test_brackets_in_messages_are_not_markup():
    result = runner.invoke(
        _app_raising(CartageError("bad target MATERIALDESCRIPTION[].X"))
    )
    assert "MATERIALDESCRIPTION[].X" in result.output


def test_run_result_add_batch():
    r = RunResult()
    r.add_batch(
        BatchResult(
            ok=2, warnings=1, errors=[RecordError(stage="destination", message="bad")]
        )
    )
    assert (r.sent, r.ok, r.warnings, len(r.errors)) == (3, 2, 1, 1)
    assert r.to_dict()["errors"][0]["message"] == "bad"
