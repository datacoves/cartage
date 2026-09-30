"""Cartage command line. Commands stay thin: load, delegate, render."""
from __future__ import annotations

import json
import logging
from dataclasses import asdict
from pathlib import Path

import typer
from rich.panel import Panel

from cartage import __version__, registry, ui
from cartage.checks import check_pipeline
from cartage.config import load_project
from cartage.adapters.destinations.sap.transports.mock import make_server
from cartage.runner import prepare, preview, run_pipeline, state_backend
from cartage.scaffold import init_project
from cartage.secrets import Secrets
from cartage.state import StateStore

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


@app.command()
def validate(
    pipelines: list[str] | None = typer.Argument(None, help="Pipelines to check (default: all)."),
    env: str | None = EnvOption,
    as_json: bool = JsonOption,
) -> None:
    """Check pipelines, connections, secrets, transforms and mappings without moving data."""
    with ui.handle_errors(OPTS.debug):
        project = load_project(OPTS.project_dir)
        refs = pipelines or [str(p) for p in project.pipeline_files()]
        results = [check_pipeline(project, ref, env) for ref in refs]
    if as_json:
        typer.echo(json.dumps([{"pipeline": name, "checks": [asdict(c) for c in checks]} for name, checks in results]))
    else:
        ui.checks_tree(results)
    if any(not c.ok for _, checks in results for c in checks):
        raise typer.Exit(2)


@app.command()
def plan(
    pipeline: str = typer.Argument(..., help="Pipeline file or name."),
    env: str | None = EnvOption,
    engine: str | None = EngineOption,
    n: int = typer.Option(3, "-n", help="Number of records to preview."),
    as_json: bool = JsonOption,
) -> None:
    """Dry run: show files to process and the first records before/after transforms and as BAPI payloads."""
    with ui.handle_errors(OPTS.debug):
        data = preview(prepare(load_project(OPTS.project_dir), pipeline, env, engine), n)
    if as_json:
        typer.echo(json.dumps(data, default=str))
    else:
        ui.plan_view(data)


connections_app = typer.Typer(help="Inspect and test connections.", no_args_is_help=True)
state_app = typer.Typer(help="Inspect or reset incremental state.", no_args_is_help=True)
sap_app = typer.Typer(help="SAP helpers.", no_args_is_help=True)
app.add_typer(connections_app, name="connections")
app.add_typer(state_app, name="state")
app.add_typer(sap_app, name="sap")


@connections_app.command("list")
def connections_list(env: str | None = EnvOption) -> None:
    """List connections (secrets are shown as references)."""
    with ui.handle_errors(OPTS.debug):
        ui.connections_table(load_project(OPTS.project_dir), env)


@connections_app.command("test")
def connections_test(name: str = typer.Argument(..., help="Connection name."), env: str | None = EnvOption) -> None:
    """Check that a connection is reachable."""
    with ui.handle_errors(OPTS.debug):
        project = load_project(OPTS.project_dir)
        env = project.resolve_env(env)
        ctype, config, where = project.connection(name, env)
        config = Secrets(project.root).resolve(config, where)
        message = registry.connection_class(ctype).check_connection(config, project.root)
    ui.console.print(ui.Text.assemble(("✔ ", "green"), (name, "bold"), f" ({ctype}, {env}): ", message))


def _store(pipeline: str, env: str | None) -> tuple[StateStore, str, str]:
    project = load_project(OPTS.project_dir)
    env = project.resolve_env(env)
    name = project.load_pipeline(pipeline).name
    return StateStore(state_backend(project, env, Secrets(project.root)), name, env), name, env


@state_app.command("show")
def state_show(pipeline: str = typer.Argument(..., help="Pipeline file or name."), env: str | None = EnvOption) -> None:
    """Show stored state (processed files, dlt archive, last run)."""
    with ui.handle_errors(OPTS.debug):
        store, _, _ = _store(pipeline, env)
        typer.echo(json.dumps(store.show(), indent=2))


@state_app.command("reset")
def state_reset(
    pipeline: str = typer.Argument(..., help="Pipeline file or name."),
    env: str | None = EnvOption,
    yes: bool = typer.Option(False, "--yes", "-y", help="Do not ask for confirmation."),
) -> None:
    """Delete stored state so the next run starts from scratch."""
    with ui.handle_errors(OPTS.debug):
        store, name, env = _store(pipeline, env)
    if not yes and not typer.confirm(f"Delete stored state for {name} ({env})?"):
        raise typer.Exit(1)
    with ui.handle_errors(OPTS.debug):
        store.reset()
    ui.console.print(ui.Text.assemble(("✔ ", "green"), f"state for {name} ({env}) deleted"))


@app.command()
def generate(
    pipelines: list[str] | None = typer.Argument(None, help="Pipelines (default: all with a schedule)."),
    target: str = typer.Option("airflow", "--target", "-t", help="Orchestrator."),
    output: Path | None = typer.Option(None, "--output", "-o", help="Output folder (default: dags_dir setting)."),
    check: bool = typer.Option(False, "--check", help="Exit 1 if generated files are missing or out of date."),
    show_context: str | None = typer.Option(None, "--show-context", metavar="PIPELINE", help="Print template variables."),
) -> None:
    """Generate orchestrator files (Airflow DAGs) that call `cartage run`."""
    with ui.handle_errors(OPTS.debug):
        project = load_project(OPTS.project_dir)
        orchestrator = registry.get("orchestrators", target)()
        if show_context:
            typer.echo(json.dumps(orchestrator.context(project, project.load_pipeline(show_context), output),
                                  indent=2, default=str))
            return
        refs = pipelines or [str(p) for p in project.pipeline_files()]
        files = orchestrator.generate(project, [project.load_pipeline(r) for r in refs], output)
        if check:
            stale = [p for p, content in files.items() if not p.is_file() or p.read_text(encoding="utf-8") != content]
            ui.generate_check(project, files, stale)
            if stale:
                raise typer.Exit(1)
            return
        for path, content in files.items():
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding="utf-8")
        ui.generated(project, files)


@sap_app.command("mock")
def sap_mock(port: int = typer.Option(8765, "--port", "-p", help="Port to listen on.")) -> None:
    """Run a mock SAP server that validates BAPI calls and logs them live."""
    server = make_server(port, on_call=ui.log_bapi_call)
    ui.console.print(Panel(f"Mock SAP listening on http://127.0.0.1:{port}\n"
                           f"Use it from a connection:  transport: mock, url: http://localhost:{port}",
                           title="cartage sap mock", title_align="left", border_style="green"))
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        ui.console.print("stopped")
    finally:
        server.server_close()
