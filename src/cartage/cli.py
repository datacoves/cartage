"""Cartage command line. Commands stay thin: load, delegate, render."""
from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import sys
from dataclasses import asdict
from pathlib import Path

import typer
from rich.panel import Panel

from cartage import __version__, registry, ui
from cartage.adapters.destinations.sap.transports.mock import make_server
from cartage.checks import check_pipeline
from cartage.config import load_project
from cartage.core import CartageError, FatalRunError
from cartage.init import merge_home_secrets, refuse_existing, write_project
from cartage.init.answers import Origin, fetch_files, load_answers
from cartage.init.plan import placeholders, plan_project
from cartage.init.questions import ask
from cartage.runner import prepare_all, preview, run_pipeline
from cartage.scaffold import KINDS, scaffold
from cartage.secrets import Secrets, mask

sys.dont_write_bytecode = True  # importing the project's transforms and sources must not leave __pycache__ in it

app = typer.Typer(
    name="cartage",
    help="Declarative data migrations: YAML pipelines, Python transforms, any engine, any orchestrator.",
    no_args_is_help=True,
    pretty_exceptions_enable=False,
)


class Options:
    project_dir: Path | None = None
    debug: bool = False


OPTS = Options()


def _completion_project(ctx: typer.Context):
    """The project for shell completion: -C if given, else the nearest one; None when there is no valid project."""
    project_dir = ctx.find_root().params.get("project_dir")  # completion leaves it unconverted: a str
    try:
        return load_project(Path(project_dir) if project_dir else None)
    except CartageError:
        return None


def _complete_pipelines(ctx: typer.Context, incomplete: str) -> list[str]:
    project = _completion_project(ctx)
    return [p.stem for p in project.pipeline_files() if p.stem.startswith(incomplete)] if project else []


def _complete_envs(ctx: typer.Context, incomplete: str) -> list[str]:
    project = _completion_project(ctx)
    return [e for e in project.config.environments if e.startswith(incomplete)] if project else []


def _version(value: bool) -> None:
    if value:
        ui.console.print(f"cartage {__version__}")
        raise typer.Exit()


@app.callback()
def main(
    project_dir: Path | None = typer.Option(None, "--project-dir", "-C", help="Project folder (default: nearest folder with .cartage/config.yaml)."),
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
    ui.console.print(f"Connection types from dlt (destinations): {', '.join(sorted(registry.dlt_destinations()))}")


@app.command()
def init(
    directory: Path = typer.Argument(Path("."), help="Folder for the new project."),
    answers: str | None = typer.Option(None, "--answers", help="Answers file (a path or an http(s) URL); "
                                                                "its answers are not asked."),
    yes: bool = typer.Option(False, "--yes", "-y", help="Take the defaults for every question not answered."),
    secrets_home: bool = typer.Option(False, "--secrets-home", help="Add the secret placeholders to "
                                      "~/.cartage/secrets.yaml (keeping keys already there) instead of the project."),
) -> None:
    """Create a project: asks about environments, the source, the destination and scheduling, then writes it."""
    with ui.handle_errors(OPTS.debug):
        refuse_existing(directory)  # before any question
        loaded, answered, origin = load_answers(answers)
        final = ask(loaded, answered, directory.resolve().name, yes, ui.console)
        fetched = fetch_files(final, origin, origin if "sample_data" in answered else Origin.cwd())
        files = plan_project(final, fetched)
        secrets = files.pop(".cartage/secrets.yaml") if secrets_home else None
        created = write_project(directory, files)
        if secrets is not None:
            home_file, merged = merge_home_secrets(secrets.decode())
            created.append(home_file)
            files["~/.cartage/secrets.yaml"] = merged
        if origin.is_url:
            ui.console.print(f"Fetched from {origin.base}: {', '.join(sorted(fetched)) or 'nothing else'}")
    ui.init_done(directory, created, placeholders(files), final.pipeline_name)


EnvOption = typer.Option(None, "--env", "-e", help="Environment (default: default_env).", autocompletion=_complete_envs)
EngineOption = typer.Option(None, "--engine", help="Override the pipeline's engine.")
JsonOption = typer.Option(False, "--json", help="Print machine-readable JSON.")
NoDepsOption = typer.Option(False, "--no-deps", envvar="CARTAGE_NO_DEPS",
                            help="Use the packages already installed instead of adding the pipelines' dependencies.")


def _with_dependencies(project, refs: list[str], no_deps: bool) -> None:
    """Run this command again under `uv run --with <dependencies>` when the pipelines declare any, like a PEP 723
    script: uv caches the environment and layers it on this one, so nothing is installed here."""
    deps = list(project.config.defaults.dependencies)
    for ref in refs:
        try:
            deps += project.load_pipeline(ref).dependencies
        except CartageError:
            continue  # the command itself reports an invalid pipeline
    deps = list(dict.fromkeys(deps))
    if not deps or no_deps:
        return
    uv = shutil.which("uv")
    if uv is None:
        raise CartageError(f"Needs {', '.join(deps)}, which Cartage adds with uv, and uv is not on PATH",
                           hint="Install uv (https://docs.astral.sh/uv/), or install the packages and pass --no-deps")
    command = [uv, "run", "--no-project", "--python", sys.executable, *(w for d in deps for w in ("--with", d)),
               "python", "-m", "cartage", *sys.argv[1:]]
    raise typer.Exit(subprocess.run(command, env={**os.environ, "CARTAGE_NO_DEPS": "1"}, check=False).returncode)


@app.command()
def run(
    pipeline: str = typer.Argument(..., help="Pipeline file or name.", autocompletion=_complete_pipelines),
    env: str | None = EnvOption,
    engine: str | None = EngineOption,
    advance_state: bool = typer.Option(False, "--advance-state", help="Save state even if records failed."),
    full_refresh: bool = typer.Option(False, "--full-refresh", help="Start from scratch: dlt destinations drop their tables and state; others ignore stored state."),
    as_json: bool = JsonOption,
    no_deps: bool = NoDepsOption,
) -> None:
    """Run a pipeline (once per destination, in order; stops at the first fatal error)."""
    failed = False
    with ui.handle_errors(OPTS.debug):
        project = load_project(OPTS.project_dir)
        _with_dependencies(project, [pipeline], no_deps)
        for prep in prepare_all(project, pipeline, env, engine):
            try:
                if as_json:
                    result = run_pipeline(prep, advance_state=advance_state, full_refresh=full_refresh)
                    typer.echo(json.dumps(result.to_dict(), default=str))
                else:
                    ui.run_header(prep.name, prep.env, prep.engine_name)
                    with ui.RunProgress(prep.name) as progress:
                        result = run_pipeline(prep, advance_state=advance_state, full_refresh=full_refresh,
                                              on_progress=progress.update)
                    ui.run_summary(result, prep.name, prep.env)
            except FatalRunError as e:  # report what was committed before the run stopped (spec §6)
                if e.result is not None:
                    if as_json:
                        typer.echo(json.dumps({**e.result.to_dict(), "fatal": mask(e.message)}, default=str))
                    else:
                        ui.run_summary(e.result, prep.name, prep.env, fatal=True)
                raise
            failed = failed or bool(result.errors)
    if failed:
        raise typer.Exit(1)


@app.command()
def validate(
    pipelines: list[str] | None = typer.Argument(None, help="Pipelines to check (default: all).", autocompletion=_complete_pipelines),
    env: str | None = EnvOption,
    as_json: bool = JsonOption,
    no_deps: bool = NoDepsOption,
) -> None:
    """Check pipelines, connections, secrets, transforms and mappings without moving data."""
    with ui.handle_errors(OPTS.debug):
        project = load_project(OPTS.project_dir)
        refs = pipelines or [str(p) for p in project.pipeline_files()]
        _with_dependencies(project, refs, no_deps)
        results = [check_pipeline(project, ref, env) for ref in refs]
    if as_json:
        typer.echo(json.dumps([{"pipeline": name, "checks": [asdict(c) for c in checks]} for name, checks in results]))
    else:
        ui.checks_tree(results)
    if any(not c.ok for _, checks in results for c in checks):
        raise typer.Exit(2)


@app.command()
def plan(
    pipeline: str = typer.Argument(..., help="Pipeline file or name.", autocompletion=_complete_pipelines),
    env: str | None = EnvOption,
    engine: str | None = EngineOption,
    n: int = typer.Option(3, "-n", help="Number of records to preview."),
    as_json: bool = JsonOption,
    no_deps: bool = NoDepsOption,
) -> None:
    """Dry run: show files to process and the first records before/after transforms and as BAPI payloads."""
    with ui.handle_errors(OPTS.debug):
        project = load_project(OPTS.project_dir)
        _with_dependencies(project, [pipeline], no_deps)
        previews = [preview(prep, n) for prep in prepare_all(project, pipeline, env, engine)]
    if as_json:
        typer.echo(json.dumps(previews[0] if len(previews) == 1 else previews, default=str))
    else:
        for data in previews:
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
def connections_test(name: str = typer.Argument(..., help="Connection name."), env: str | None = EnvOption,
                     no_deps: bool = NoDepsOption) -> None:
    """Check that a connection is reachable."""
    with ui.handle_errors(OPTS.debug):
        project = load_project(OPTS.project_dir)
        _with_dependencies(project, [str(p) for p in project.pipeline_files()], no_deps)
        env = project.resolve_env(env)
        ctype, config, where = project.connection(name, env)
        config = Secrets(project.root, env=env).resolve(config, where)
        cls = registry.connection_class(ctype)
        if not hasattr(cls, "state_backend"):  # destination adapters: dlt destinations get their type as settings
            config = registry.destination_config(ctype, config)
        check = cls.check_connection
        if hasattr(cls, "check_source") and _read_by_a_pipeline(project, name, env):
            check = cls.check_source
        message = check(config, project.root)
    ui.console.print(ui.Text.assemble(("✔ ", "green"), (name, "bold"), f" ({ctype}, {env}): ", mask(message)))


def _read_by_a_pipeline(project, name: str, env: str) -> bool:
    for path in project.pipeline_files():
        try:
            if project.load_pipeline(path).source_for(env).connection == name:
                return True
        except CartageError:
            continue
    return False


def _stores(pipeline: str, env: str | None, no_deps: bool) -> tuple[dict, str, str]:
    """State stores by run name: one per destination."""
    project = load_project(OPTS.project_dir)
    _with_dependencies(project, [pipeline], no_deps)  # dlt destinations read their state with the driver
    preps = prepare_all(project, pipeline, env)
    return {p.name: p.store for p in preps}, preps[0].pipeline.name, preps[0].env


@state_app.command("show")
def state_show(pipeline: str = typer.Argument(..., help="Pipeline file or name.", autocompletion=_complete_pipelines), env: str | None = EnvOption,
               no_deps: bool = NoDepsOption) -> None:
    """Show stored state (processed files, dlt archive, last run)."""
    with ui.handle_errors(OPTS.debug):
        stores, _, _ = _stores(pipeline, env, no_deps)
        shown = {name: store.show() for name, store in stores.items()}
        typer.echo(json.dumps(next(iter(shown.values())) if len(shown) == 1 else shown, indent=2, default=str))


@state_app.command("reset")
def state_reset(
    pipeline: str = typer.Argument(..., help="Pipeline file or name.", autocompletion=_complete_pipelines),
    env: str | None = EnvOption,
    yes: bool = typer.Option(False, "--yes", "-y", help="Do not ask for confirmation."),
    no_deps: bool = NoDepsOption,
) -> None:
    """Delete stored state so the next run starts from scratch (every destination)."""
    with ui.handle_errors(OPTS.debug):
        stores, name, env = _stores(pipeline, env, no_deps)
    if not yes and not typer.confirm(f"Delete stored state for {name} ({env})?"):
        ui.console.print("cancelled")
        raise typer.Exit(0)
    with ui.handle_errors(OPTS.debug):
        for store in stores.values():
            store.reset()
    ui.console.print(ui.Text.assemble(("✔ ", "green"), f"state for {name} ({env}) deleted"))


def _check_remote_state(project, pipeline, env: str) -> None:
    """Sinks (SAP, file exports) keep state in Cartage's state store: on an orchestrator it must not be local."""
    cfg = project.config.state.get(env)
    if cfg is not None and cfg.connection:
        return
    for d in pipeline.destination_specs:
        ctype = project.connection(d.connection, env)[0]
        if not hasattr(registry.destination_class(ctype), "dlt_destination"):
            raise CartageError(f"Pipeline '{pipeline.name}' writes to '{d.connection}' ({ctype}), which keeps its state "
                               f"in Cartage's state store, and env '{env}' keeps that state on local disk",
                               hint=f"In .cartage/config.yaml: state: {{ {env}: {{ connection: <a filesystem connection>, "
                                    "prefix: cartage/state/ } }")


@app.command()
def generate(
    pipelines: list[str] | None = typer.Argument(None, help="Pipelines (default: all with a schedule).", autocompletion=_complete_pipelines),
    target: str = typer.Option("airflow", "--target", "-t", help="Orchestrator: airflow, dagster or prefect."),
    output: Path | None = typer.Option(None, "--output", "-o", help="Output folder (default: dags_dir setting)."),
    check: bool = typer.Option(False, "--check", help="Exit 1 if generated files are missing or out of date."),
    show_context: str | None = typer.Option(None, "--show-context", metavar="PIPELINE", help="Print template variables."),
) -> None:
    """Generate orchestrator files (Airflow DAGs, Dagster jobs, Prefect flows) that call `cartage run`."""
    with ui.handle_errors(OPTS.debug):
        project = load_project(OPTS.project_dir)
        orchestrator = registry.get("orchestrators", target)()
        if show_context:
            typer.echo(json.dumps(orchestrator.context(project, project.load_pipeline(show_context), output),
                                  indent=2, default=str))
            return
        refs = pipelines or [str(p) for p in project.pipeline_files()]
        loaded = [project.load_pipeline(r) for r in refs]
        for p in loaded:
            if target in p.schedule:
                _check_remote_state(project, p, orchestrator.settings(project, p)["env"])
        files = orchestrator.generate(project, loaded, output)
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


@app.command("scaffold")
def scaffold_command(
    kind: str = typer.Argument(..., help=f"What to write: {', '.join(KINDS)}."),
    name: str | None = typer.Argument(None, help="Module name, for transform and source."),
) -> None:
    """Write a file to edit: a template override for generated files, or a transform or source module."""
    with ui.handle_errors(OPTS.debug):
        project = load_project(OPTS.project_dir)
        written = scaffold(project.root, kind, name)
    for path in written:
        ui.console.print(ui.Text.assemble(("✔ ", "green"), "wrote ", (project.rel(path), "bold")))


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
