"""A fake SAP that validates BAPI calls against bundled metadata and answers with BAPIRET2 rows.

Message class and numbers are illustrative, not SAP's real message catalogue.
"""
from __future__ import annotations

import json
import threading
import urllib.error
import urllib.request
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from cartage.adapters.destinations.sap.meta import BapiMeta, load_meta
from cartage.core import CartageError, FatalRunError


def bapiret(type_: str, number: int, message: str, parameter: str = "", field: str = "") -> dict:
    return {"TYPE": type_, "ID": "M3", "NUMBER": f"{number:03d}", "MESSAGE": message,
            "PARAMETER": parameter, "FIELD": field, "ROW": 0}


def validate_params(meta: BapiMeta, params: dict) -> list[dict]:
    errors = []
    x_structures = {p.x_structure for p in meta.parameters.values() if p.x_structure}
    for pname, value in params.items():
        if pname in x_structures:
            continue
        pm = meta.parameters.get(pname)
        if pm is None:
            errors.append(bapiret("E", 101, f"Parameter {pname} does not exist in {meta.function}", pname))
            continue
        # Check parameter type (structure vs table)
        if pm.kind == "structure" and value is not None and not isinstance(value, dict):
            errors.append(bapiret("E", 106, f"Parameter {pname} must be a structure", pname))
            continue
        if pm.kind == "table" and value is not None and not isinstance(value, list):
            errors.append(bapiret("E", 106, f"Parameter {pname} must be a table", pname))
            continue
        for row in (value or []) if pm.kind == "table" else [value or {}]:
            for fname, v in row.items():
                fm = pm.fields.get(fname)
                if fm is None:
                    errors.append(bapiret("E", 102, f"Field {pname}-{fname} does not exist", pname, fname))
                elif len(str(v)) > fm.length:
                    errors.append(bapiret("E", 103, f"Value '{v}' for {pname}-{fname} exceeds maximum length {fm.length}", pname, fname))
                elif fm.allowed is not None and str(v) not in fm.allowed:
                    errors.append(bapiret("E", 104, f"Value '{v}' is not allowed for {pname}-{fname}", pname, fname))
    for pname, pm in meta.parameters.items():
        value = params.get(pname)
        if (pm.kind == "structure" and value is not None and not isinstance(value, dict)) or (pm.kind == "table" and value is not None and not isinstance(value, list)):
            continue
        for row in (value or []) if pm.kind == "table" else [value or {}]:
            for fname, fm in pm.fields.items():
                if fm.required and not row.get(fname):
                    errors.append(bapiret("E", 105, f"Required field {pname}-{fname} is not filled", pname, fname))
    return errors


class MockSap:
    def __init__(self):
        self.materials: dict[str, dict] = {}
        self.pending: dict[str, dict] = {}
        self._lock = threading.Lock()

    def call(self, function: str, params: dict) -> dict:
        with self._lock:
            if function == "BAPI_TRANSACTION_COMMIT":
                self.materials.update(self.pending)
                self.pending.clear()
                return {"RETURN": []}
            if function == "BAPI_TRANSACTION_ROLLBACK":
                self.pending.clear()
                return {"RETURN": []}
            try:
                meta = load_meta(function)
            except CartageError:
                return {"RETURN": [bapiret("E", 100, f"Function module {function} not found")]}
            errors = validate_params(meta, params)
            if errors:
                return {"RETURN": errors}
            param, _, field = meta.key.partition(".")
            key = params[param][field]
            existed = key in self.materials or key in self.pending
            self.pending[key] = params
            if existed:
                return {"RETURN": [bapiret("S", 801, f"The {meta.object} {key} has been changed")]}
            return {"RETURN": [bapiret("S", 800, f"The {meta.object} {key} has been created or extended")]}


class InProcessTransport:
    def __init__(self, sap: MockSap | None = None):
        self.sap = sap or MockSap()

    def call(self, function: str, params: dict) -> dict:
        return self.sap.call(function, params)

    def ping(self) -> str:
        return "mock (in-process)"


class HttpTransport:
    def __init__(self, url: str, timeout: float = 10.0):
        self.url = url.rstrip("/")
        self.timeout = timeout

    def _request(self, path: str, payload: dict | None = None) -> Any:
        data = None if payload is None else json.dumps(payload).encode()
        request = urllib.request.Request(self.url + path, data=data, headers={"Content-Type": "application/json"},
                                         method="GET" if data is None else "POST")
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                return json.loads(response.read())
        except urllib.error.HTTPError as e:
            raise FatalRunError(f"Mock SAP at {self.url} returned HTTP {e.code} for {path}", hint="") from e
        except (urllib.error.URLError, OSError) as e:
            reason = getattr(e, "reason", e)
            raise FatalRunError(f"Cannot reach mock SAP at {self.url}: {reason}", hint="Start it with: cartage sap mock") from e

    def call(self, function: str, params: dict) -> dict:
        return self._request("/call", {"function": function, "params": params})

    def ping(self) -> str:
        self._request("/ping")
        return f"mock at {self.url}"


def make_server(port: int, sap: MockSap | None = None,
                on_call: Callable[[str, dict, dict], None] | None = None) -> ThreadingHTTPServer:
    sap = sap or MockSap()

    class Handler(BaseHTTPRequestHandler):
        def _send(self, status: int, body: Any) -> None:
            data = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self) -> None:
            self._send(200, {"ok": True}) if self.path == "/ping" else self._send(404, {"error": "not found"})

        def do_POST(self) -> None:
            if self.path != "/call":
                return self._send(404, {"error": "not found"})
            try:
                body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))) or b"{}")
            except ValueError:
                return self._send(400, {"error": "invalid JSON"})
            try:
                function, params = body.get("function", ""), body.get("params", {})
                result = sap.call(function, params)
                if on_call:
                    on_call(function, params, result)
                self._send(200, result)
            except Exception as e:  # noqa: BLE001 - a server answers any failure with a 500
                self._send(500, {"error": f"{type(e).__name__}: {e}"})

        def log_message(self, *args: Any) -> None:
            pass

    return ThreadingHTTPServer(("127.0.0.1", port), Handler)
