"""Rich console output shared by all commands."""
from __future__ import annotations

import json
import traceback
from contextlib import contextmanager
from datetime import datetime
from itertools import zip_longest
from pathlib import Path
from typing import Iterator

import click
import typer
from rich import box
from rich.console import Console, Group
from rich.markup import escape
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn
from rich.syntax import Syntax
from rich.table import Table
from rich.text import Text
from rich.tree import Tree

from cartage.core import CartageError, RecordError, RunResult
from cartage.secrets import mask

console = Console(highlight=False)
err_console = Console(stderr=True, highlight=False)


def show_error(error: CartageError) -> None:
    body = Text(mask(error.message))
    if error.hint:
        body.append("\n\nhint: ", style="dim")
        body.append(mask(error.hint))
    err_console.print(Panel(body, title="error", title_align="left", border_style="red"))


@contextmanager
def handle_errors(debug: bool = False) -> Iterator[None]:
    try:
        yield
    except CartageError as e:
        show_error(e)
        if debug:
            err_console.print(Text(mask(traceback.format_exc())))
        raise typer.Exit(e.exit_code) from e
    except (typer.Exit, click.exceptions.Exit, click.exceptions.Abort, click.ClickException):
        raise
    except Exception as e:
        show_error(CartageError(f"Unexpected error: {type(e).__name__}: {e}", hint="Re-run with --debug for the traceback"))
        if debug:
            err_console.print(Text(mask(traceback.format_exc())))
        raise typer.Exit(3) from e


def plugins_table(found: dict[str, dict]) -> None:
    table = Table("kind", "name", "provided by", "target")
    for kind, eps in found.items():
        for name, ep in sorted(eps.items()):
            table.add_row(kind, name, ep.dist.name if ep.dist else "?", Text(ep.value))
    console.print(table)


def init_done(directory: Path, created: list[Path]) -> None:
    body = Text()
    body.append(f"Created {len(created)} files in {directory}\n\n", style="green")
    body.append("Next steps:\n", style="bold")
    for cmd in (f"cd {directory}", "cartage validate", "cartage plan materials", "cartage run materials"):
        body.append(f"  $ {cmd}\n", style="cyan")
    console.print(Panel(body, title="cartage init", title_align="left", border_style="green"))


MAX_ERRORS = 20


def run_header(pipeline: str, env: str, engine: str) -> None:
    console.print(Text.assemble(("cartage run ", "bold"), pipeline, "  env ", (env, "cyan"), "  engine ", (engine, "cyan")))


class RunProgress:
    def __init__(self, label: str):
        self.label = escape(label)
        self.progress = Progress(SpinnerColumn(), TextColumn("{task.description}"), TimeElapsedColumn(),
                                 console=console, transient=True)

    def __enter__(self) -> "RunProgress":
        self.progress.start()
        self.task = self.progress.add_task(self.label)
        return self

    def __exit__(self, *exc) -> None:
        self.progress.stop()

    def update(self, r: RunResult) -> None:
        self.progress.update(self.task, description=(
            f"{self.label}  read {r.read} · sent {r.sent} · [green]ok {r.ok}[/] · [red]errors {len(r.errors)}[/]"))


def _code(error: RecordError) -> str:
    first = next((d for d in error.details if d.get("TYPE") in ("E", "A")), None)
    return " ".join(str(first.get(k, "")) for k in ("TYPE", "ID", "NUMBER")) if first else "—"


def run_summary(result: RunResult, pipeline: str, env: str, fatal: bool = False) -> None:
    counts = Table(box=box.SIMPLE_HEAD)
    for col in ("read", "filtered", "sent", "ok", "warnings", "errors"):
        counts.add_column(col, justify="right")
    counts.add_row(str(result.read), str(result.filtered), str(result.sent), Text(str(result.ok), style="green"),
                   Text(str(result.warnings), style="yellow" if result.warnings else ""),
                   Text(str(len(result.errors)), style="red" if result.errors else ""))
    if fatal:
        status = Text("✘ run aborted by a fatal error; counts show what was done before it", style="red")
    elif result.errors:
        status = Text(f"⚠ completed with {len(result.errors)} record error(s)", style="yellow")
    else:
        status = Text("✔ completed", style="green")
    state = ("state saved" if result.state_advanced else "state not advanced" if fatal
             else "state not advanced: fix the rejects and rerun, or pass --advance-state")
    console.print(Panel(Group(status, counts, Text(state, style="dim")), title=f"{pipeline} · {env}",
                        title_align="left", border_style="red" if fatal else "yellow" if result.errors else "green"))
    if result.read == 0:
        console.print(Text("No records to process.", style="dim"))
    if result.errors:
        table = Table("source", "stage", "code", "message", box=box.SIMPLE_HEAD, title="record errors", title_justify="left")
        for e in result.errors[:MAX_ERRORS]:
            table.add_row(Text(e.source or "—"), e.stage, _code(e), Text(e.message))
        console.print(table)
        if len(result.errors) > MAX_ERRORS:
            console.print(Text(f"… and {len(result.errors) - MAX_ERRORS} more", style="dim"))
        console.print(Text(f"rejects: {result.rejects_path}", style="dim"))


def checks_tree(results: list) -> None:
    for name, checks in results:
        ok = all(c.ok for c in checks)
        tree = Tree(Text.assemble(("✔ " if ok else "✘ ", "green" if ok else "red"), (name, "bold")))
        for c in checks:
            tree.add(Text.assemble(("✔ " if c.ok else "✘ ", "green" if c.ok else "red"), c.label,
                                   (f"  {c.detail}", "dim" if c.ok else "red")))
        console.print(tree)


def _json(obj) -> Syntax:
    return Syntax(json.dumps(obj, indent=2, ensure_ascii=False, default=str), "json",
                  theme="ansi_dark", background_color="default", word_wrap=True)


def plan_view(data: dict) -> None:
    console.print(Text.assemble(("cartage plan ", "bold"), data["pipeline"], "  env ", (data["env"], "cyan"),
                                "  engine ", (data["engine"], "cyan")))
    console.print(Panel(_json({"source": data["source"], "destination": data["destination"]}),
                        title="connections (secrets stay as references)", title_align="left"))
    if data["files"] is not None:
        files = Table("file", "action", box=box.SIMPLE_HEAD)
        for f in data["files"]:
            files.add_row(Text(f["file"]), Text("process", style="green") if f["process"]
                          else Text("skip (already loaded)", style="dim"))
        console.print(files)
    if not data["records"]:
        console.print(Text("Nothing to process.", style="dim"))
        return
    table = Table("source record", "after transforms", "BAPI payload", box=box.SIMPLE_HEAD,
                  title=f"first {len(data['records'])} record(s)", title_justify="left")
    for row in zip_longest(data["records"], data["transformed"], data["payloads"]):
        table.add_row(*(_json(x) if x is not None else Text("—", style="dim") for x in row))
    console.print(table)
    for message in data["transform_errors"]:
        console.print(Text(f"transform error: {message}", style="red"))


def connections_table(project, env: str | None) -> None:
    table = Table("name", "type", "env", "settings", box=box.SIMPLE_HEAD)
    for name, conn in sorted(project.connections.items()):
        for conn_env, settings in conn.envs.items():
            if env is None or env == conn_env:
                table.add_row(name, conn.type, conn_env, Text(json.dumps(settings)))
    console.print(table)


def log_bapi_call(function: str, params: dict, result: dict) -> None:
    now = f"{datetime.now():%H:%M:%S} "
    returned = result.get("RETURN", [])
    if not returned:
        console.print(Text.assemble((now, "dim"), (function, "dim")))
    for r in returned:
        style = {"S": "green", "W": "yellow"}.get(r.get("TYPE"), "red")
        console.print(Text.assemble((now, "dim"), (function, "bold"), "  ",
                                    (f"{r.get('TYPE')} {r.get('ID')} {r.get('NUMBER')}", style), "  ", r.get("MESSAGE", "")))


def generated(project, files: dict) -> None:
    if not files:
        console.print(Text("No pipelines with schedule.airflow — nothing to generate.", style="dim"))
    for path in files:
        console.print(Text.assemble(("✔ ", "green"), "wrote ", project.rel(path)))


def generate_check(project, files: dict, stale: list) -> None:
    for path in stale:
        console.print(Text.assemble(("✘ ", "red"), "out of date: ", project.rel(path)))
    if stale:
        console.print(Text("Run: cartage generate", style="dim"))
    else:
        console.print(Text.assemble(("✔ ", "green"), f"{len(files)} generated file(s) up to date"))
