"""Adapter discovery through entry points: cartage.sources / destinations / engines / orchestrators.

Connection types are named after the system, not the engine: Cartage's own types (filesystem, file_export,
sap_bapi, plug-ins) and every dlt destination (snowflake, duckdb, postgres, ...), or a `module:function` factory."""
from __future__ import annotations

from importlib.metadata import EntryPoint, entry_points
from typing import Any

from cartage.core import CartageError

KINDS = ("sources", "destinations", "engines", "orchestrators")
EXTRAS: dict[tuple[str, str], str] = {
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


def dlt_destinations() -> set[str]:
    import dlt.destinations

    return {n for n in dir(dlt.destinations)
            if not n.startswith("_") and n != "destination" and callable(getattr(dlt.destinations, n))}


def is_dlt_destination(type_name: str) -> bool:
    return ":" in type_name or type_name in dlt_destinations()


def destination_class(type_name: str) -> type:
    """A Cartage destination plug-in, else the dlt destination adapter. A name that is both is an error."""
    own = set(available("destinations"))
    if type_name in own and type_name in dlt_destinations():
        raise CartageError(f"Connection type '{type_name}' is both a Cartage destination and a dlt destination",
                           hint="Rename the Cartage plug-in; dlt destination names are reserved")
    if type_name in own:
        return get("destinations", type_name)
    if is_dlt_destination(type_name):
        from cartage.adapters.destinations.dlt import DltDestinationAdapter

        return DltDestinationAdapter
    raise _unknown(type_name)


def destination_config(type_name: str, config: dict[str, Any]) -> dict[str, Any]:
    """The settings a destination adapter gets: for dlt destinations, the type is the dlt destination."""
    if not is_dlt_destination(type_name) or type_name in available("destinations"):
        return config
    if "destination" in config:
        raise CartageError(f"A '{type_name}' connection takes no 'destination' setting",
                           hint="The connection type is the dlt destination, e.g. type: snowflake")
    return {"destination": type_name, **config}


def connection_class(type_name: str) -> type:
    """The class that tests a connection or stores state: its source adapter, else its destination adapter."""
    if type_name in available("sources"):
        return get("sources", type_name)
    return destination_class(type_name)


def connection_types() -> list[str]:
    own = {*available("sources"), *available("destinations")}
    return sorted(own | dlt_destinations())


def _unknown(type_name: str) -> CartageError:
    hint = f"Types: {', '.join(connection_types())}, or a module:function dlt destination factory"
    if type_name == "dlt":
        hint = "Since cartage 0.8 the dlt destination is the type itself, e.g. type: snowflake (no destination:)"
    elif type_name in ("file", "sap", "s3"):
        hint = {"file": "Renamed to file_export", "sap": "Renamed to sap_bapi",
                "s3": "Use type: filesystem with bucket_url: s3://bucket/prefix"}[type_name]
    return CartageError(f"Unknown connection type '{type_name}'", hint=hint)
