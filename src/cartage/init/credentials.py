"""Credential fields for `cartage init`, read from dlt's own credential classes, so every dlt destination is covered
and the fields follow dlt upgrades. Anything unexpected falls back to a single `credentials` secret."""
from __future__ import annotations

import dataclasses
import re
import typing
from dataclasses import dataclass

SKIP = {"drivername", "query"}  # connection-string internals
SCHEMES = {"s3": "AwsCredentials", "gs": "GcpServiceAccountCredentials", "gcs": "GcpServiceAccountCredentials",
           "az": "AzureCredentialsWithoutDefaults", "abfss": "AzureCredentialsWithoutDefaults",
           "sftp": "SFTPCredentials"}


@dataclass(frozen=True)
class CredentialField:
    name: str
    secret: bool
    required: bool
    default: object = None


@dataclass(frozen=True)
class Credentials:
    fields: tuple[CredentialField, ...]
    fallback: bool = False  # dlt could not be read: write one `credentials` secret instead
    docs: str = ""

    @property
    def alternatives(self) -> tuple[str, ...]:
        """Optional secrets when no secret is required: the auth methods to choose from (fill one)."""
        if any(f.secret and f.required for f in self.fields):
            return ()
        names = tuple(f.name for f in self.fields if f.secret and not f.required)
        return names if len(names) > 1 else ()


def _classes(destination: str) -> list[type]:
    import dlt.destinations

    hint = getattr(dlt.destinations, destination)().spec.get_resolvable_fields()["credentials"]
    members = typing.get_args(hint) or (hint,)
    return [m for m in members if isinstance(m, type) and hasattr(m, "get_resolvable_fields")]


def label(cls: type) -> str:
    """GcpServiceAccountCredentials → 'Gcp Service Account'."""
    name = re.sub(r"(Credentials)?(WithoutDefaults)?$", "", cls.__name__)
    return re.sub(r"(?<=[a-z])(?=[A-Z])", " ", name)


def variants(destination: str) -> list[str]:
    """The auth variants a dlt destination offers; one or none means there is nothing to ask."""
    try:
        return [label(c) for c in _classes(destination)]
    except Exception:
        return []


def _fields(cls: type) -> tuple[CredentialField, ...]:
    from dlt.common.configuration.specs.base_configuration import is_secret_hint
    from dlt.common.typing import is_optional_type

    declared = {f.name: f for f in dataclasses.fields(cls)}
    out = []
    for name, hint in cls.get_resolvable_fields().items():
        if name.startswith("_") or name in SKIP:
            continue
        f = declared.get(name)
        default = None
        if f is not None and f.default is not dataclasses.MISSING:
            default = f.default
        elif f is not None and f.default_factory is not dataclasses.MISSING:
            default = "..."
        out.append(CredentialField(name, bool(is_secret_hint(hint)), default is None and not is_optional_type(hint),
                                   default))
    return tuple(out)


def credentials(destination: str, variant: str | None = None, url: str | None = None) -> Credentials:
    docs = f"https://dlthub.com/docs/dlt-ecosystem/destinations/{destination}"
    try:
        classes = _classes(destination)
        if destination == "filesystem":
            scheme = url.partition("://")[0].lower() if url and "://" in url else ""
            wanted = SCHEMES.get(scheme)
            if wanted is None:
                return Credentials((), docs=docs)  # local paths and http(s): nothing to fill
            classes = [c for c in classes if c.__name__ == wanted]
        elif variant is not None:
            classes = [c for c in classes if label(c) == variant]
        if not classes:
            raise LookupError(variant or url)
        return Credentials(_fields(classes[0]), docs=docs)
    except Exception:
        return Credentials((), fallback=True, docs=docs)
