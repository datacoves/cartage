"""Rich console output shared by all commands."""
from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from cartage.core import CartageError
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
