"""Cartage command line. Commands stay thin: load, delegate, render."""
from __future__ import annotations

import logging
from pathlib import Path

import typer

from cartage import __version__, ui

app = typer.Typer(
    name="cartage",
    help="Declarative data migrations: YAML pipelines, Python transforms, any engine, any orchestrator.",
    no_args_is_help=True,
    add_completion=False,
    pretty_exceptions_enable=False,
)


class Options:
    project_dir: Path | None = None
    debug: bool = False


OPTS = Options()


def _version(value: bool) -> None:
    if value:
        ui.console.print(f"cartage {__version__}")
        raise typer.Exit()


@app.callback()
def main(
    project_dir: Path | None = typer.Option(None, "--project-dir", "-C", help="Project folder (default: nearest folder with cartage.yaml)."),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Debug logging."),
    debug: bool = typer.Option(False, "--debug", help="Show tracebacks on errors."),
    no_color: bool = typer.Option(False, "--no-color", help="Disable colors."),
    version: bool = typer.Option(False, "--version", callback=_version, is_eager=True, help="Show the version and exit."),
) -> None:
    OPTS.project_dir = project_dir
    OPTS.debug = debug
    ui.console.no_color = no_color
    ui.err_console.no_color = no_color
    if verbose:
        logging.basicConfig(level=logging.DEBUG)
