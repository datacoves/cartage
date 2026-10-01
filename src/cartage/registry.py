"""Adapter discovery through entry points: cartage.sources / destinations / engines / orchestrators."""
from __future__ import annotations

from importlib.metadata import EntryPoint, entry_points

from cartage.core import CartageError

KINDS = ("sources", "destinations", "engines", "orchestrators")
EXTRAS = {
    ("sources", "s3"): "s3",
    ("orchestrators", "airflow"): "airflow",
}


def available(kind: str) -> dict[str, EntryPoint]:
    return {ep.name: ep for ep in entry_points(group=f"cartage.{kind}")}


def get(kind: str, name: str) -> type:
    found = available(kind)
    singular = kind[:-1]
    if name not in found:
        raise CartageError(f"Unknown {singular} '{name}'", hint=f"Installed {kind}: {', '.join(sorted(found)) or 'none'}")
    try:
        return found[name].load()
    except ImportError as e:
        extra = EXTRAS.get((kind, name))
        raise CartageError(f"The {singular} '{name}' needs a dependency that is not installed ({e.name})",
                           hint=f"pip install 'cartage[{extra}]'" if extra else None) from e


def connection_class(type_name: str) -> type:
    for kind in ("destinations", "sources"):  # `dlt` is both; only its destination is a connection type
        if type_name in available(kind):
            return get(kind, type_name)
    known = sorted({*available("sources"), *available("destinations")})
    raise CartageError(f"Unknown connection type '{type_name}'", hint=f"Installed types: {', '.join(known)}")
