"""Cartage command line. Commands stay thin: load, delegate, render."""
from __future__ import annotations

import json
import logging
from pathlib import Path

import typer

from cartage import __version__, registry, ui
from cartage.config import load_project
from cartage.runner import prepare, run_pipeline
from cartage.scaffold import init_project

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


@app.command()
def plugins() -> None:
    """List installed sources, destinations, engines and orchestrators."""
    ui.plugins_table({kind: registry.available(kind) for kind in registry.KINDS})


@app.command()
def init(directory: Path = typer.Argument(Path("."), help="Folder for the new project.")) -> None:
    """Create a demo project: CSV materials → transforms → mock SAP."""
    with ui.handle_errors(OPTS.debug):
        created = init_project(directory)
    ui.init_done(directory, created)


EnvOption = typer.Option(None, "--env", "-e", help="Environment (default: default_env).")
EngineOption = typer.Option(None, "--engine", help="Override the pipeline's engine.")
JsonOption = typer.Option(False, "--json", help="Print machine-readable JSON.")


@app.command()
def run(
    pipeline: str = typer.Argument(..., help="Pipeline file or name."),
    env: str | None = EnvOption,
    engine: str | None = EngineOption,
    advance_state: bool = typer.Option(False, "--advance-state", help="Save state even if records failed."),
    full_refresh: bool = typer.Option(False, "--full-refresh", help="Ignore stored state."),
    as_json: bool = JsonOption,
) -> None:
    """Run a pipeline."""
    with ui.handle_errors(OPTS.debug):
        prep = prepare(load_project(OPTS.project_dir), pipeline, env, engine)
        if as_json:
            result = run_pipeline(prep, advance_state=advance_state, full_refresh=full_refresh)
            typer.echo(json.dumps(result.to_dict(), default=str))
        else:
            ui.run_header(prep.pipeline.name, prep.env, prep.engine_name)
            with ui.RunProgress(prep.pipeline.name) as progress:
                result = run_pipeline(prep, advance_state=advance_state, full_refresh=full_refresh,
                                      on_progress=progress.update)
            ui.run_summary(result, prep.pipeline.name, prep.env)
    if result.errors:
        raise typer.Exit(1)
