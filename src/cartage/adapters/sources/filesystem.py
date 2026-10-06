"""Files from a dlt filesystem location (a local folder, s3://, gs://, az://, https://, sftp://, ...), read with dlt's
typed readers. The same connection can be a dlt filesystem destination and Cartage's state store."""
from __future__ import annotations

from pathlib import Path
from typing import Any

import dlt

from cartage.adapters.sources.dlt import DltSourceAdapter
from cartage.core import CartageError, FatalRunError

FORMATS = ("csv", "jsonl", "parquet")
FSSPEC_SETTINGS = ("credentials", "kwargs", "client_kwargs")  # passed to dlt's filesystem with bucket_url


def bucket_url(config: dict, root: Path) -> str:
    """The connection's bucket_url; a local path is relative to the project, not the working directory."""
    url = config.get("bucket_url")
    if not url:
        raise CartageError("A filesystem connection needs 'bucket_url'",
                           hint="e.g. bucket_url: ./data, s3://bucket/prefix or https://example.com/files")
    url = str(url)
    if "://" not in url and not Path(url).is_absolute():
        url = str((Path(root) / url).resolve())
    return url


def fsspec(config: dict, root: Path):
    """(fsspec filesystem, base path) for the connection, credentials resolved the way dlt does."""
    from dlt.common.configuration import resolve_configuration
    from dlt.common.storages.configuration import FilesystemConfiguration
    from dlt.common.storages.fsspec_filesystem import fsspec_from_config

    url = bucket_url(config, root)
    explicit = {"bucket_url": url, **{k: config[k] for k in FSSPEC_SETTINGS if config.get(k) is not None}}
    try:
        return fsspec_from_config(resolve_configuration(FilesystemConfiguration(), explicit_value=explicit))
    except Exception as e:  # missing extra (dlt[s3], dlt[http], ...), bad credentials
        raise CartageError(f"Cannot open {url}: {type(e).__name__}: {e}".strip()) from e


class FsspecStateBackend:
    def __init__(self, fs, base: str):
        self.fs, self.base = fs, base.rstrip("/")

    def _path(self, key: str) -> str:
        return f"{self.base}/{key}"

    def get(self, key: str) -> bytes | None:
        try:
            return self.fs.cat_file(self._path(key))
        except FileNotFoundError:
            return None
        except OSError as e:
            raise FatalRunError(f"Cannot read state {self._path(key)}: {e}") from e

    def put(self, key: str, data: bytes) -> None:
        path = self._path(key)
        try:
            self.fs.makedirs(path.rsplit("/", 1)[0], exist_ok=True)
            self.fs.pipe_file(path, data)
        except OSError as e:
            raise FatalRunError(f"Cannot write state {path}: {e}") from e

    def delete(self, key: str) -> None:
        if self.fs.exists(self._path(key)):
            self.fs.rm_file(self._path(key))


class FilesystemSource(DltSourceAdapter):
    def __init__(self, config: dict, options: dict, root: Path):
        self.url = bucket_url(config, root)
        self.fsspec_settings = {k: config[k] for k in FSSPEC_SETTINGS if config.get(k) is not None}
        self.path = options.get("path")
        if not self.path:
            raise CartageError("A filesystem source needs 'path'", hint="A glob under bucket_url, e.g. materials/*.csv")
        self.format = options.get("format", "csv")
        if self.format not in FORMATS:
            raise CartageError(f"Unsupported format '{self.format}'", hint=f"Supported: {', '.join(FORMATS)}")
        self.reader_options: dict[str, Any] = options.get("reader_options") or {}
        if not isinstance(self.reader_options, dict):
            raise CartageError("filesystem source 'reader_options' must be a mapping",
                               hint="e.g. reader_options: { dtype: str } (pandas.read_csv arguments for csv)")
        self.incremental_files = bool(options.get("incremental", False))
        self.batch_size = int(options.get("batch_size", 100))
        self.ref = f"{self.url}/{self.path}"  # for messages
        self.run_name = "files"  # the table name for dlt destinations; the runner sets the pipeline name
        self.resources, self.incremental = [], None  # DltSourceAdapter's ref-source options: not used here

    def _resources(self, incremental: bool = False) -> list:
        import dlt.sources.filesystem as readers

        files = readers.filesystem(bucket_url=self.url, file_glob=self.path, **self.fsspec_settings,
                                   incremental=dlt.sources.incremental("modification_date") if incremental else None)
        reader = getattr(readers, f"read_{self.format}")(**self.reader_options)
        return [(files | reader).with_name(self.run_name)]

    def dlt_resources(self) -> list:
        return self._resources(self.incremental_files)

    @classmethod
    def check_connection(cls, config: dict, root: Path) -> str:
        fs, base = fsspec(config, root)
        try:
            if not fs.exists(base):  # fine for a destination: dlt creates it on the first load
                return f"{bucket_url(config, root)} (does not exist yet)"
            count = len(fs.ls(base))
        except OSError as e:
            raise FatalRunError(f"Cannot list {bucket_url(config, root)}: {e}") from e
        return f"{bucket_url(config, root)} ({count} entr{'ies' if count != 1 else 'y'})"

    @classmethod
    def state_backend(cls, config: dict, prefix: str, root: Path) -> FsspecStateBackend:
        fs, base = fsspec(config, root)
        return FsspecStateBackend(fs, f"{base}/{prefix}" if prefix else base)
