"""Rich console output shared by all commands."""
from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import typer
from rich import box
from rich.console import Console, Group
from rich.markup import escape
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn
from rich.table import Table
from rich.text import Text

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
            err_console.print_exception()
        raise typer.Exit(e.exit_code) from e


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


def run_summary(result: RunResult, pipeline: str, env: str) -> None:
    counts = Table(box=box.SIMPLE_HEAD)
    for col in ("read", "filtered", "sent", "ok", "warnings", "errors"):
        counts.add_column(col, justify="right")
    counts.add_row(str(result.read), str(result.filtered), str(result.sent), Text(str(result.ok), style="green"),
                   Text(str(result.warnings), style="yellow" if result.warnings else ""),
                   Text(str(len(result.errors)), style="red" if result.errors else ""))
    if result.errors:
        status = Text(f"⚠ completed with {len(result.errors)} record error(s)", style="yellow")
    else:
        status = Text("✔ completed", style="green")
    state = ("state saved" if result.state_advanced
             else "state not advanced: fix the rejects and rerun, or pass --advance-state")
    console.print(Panel(Group(status, counts, Text(state, style="dim")), title=f"{pipeline} · {env}",
                        title_align="left", border_style="yellow" if result.errors else "green"))
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
