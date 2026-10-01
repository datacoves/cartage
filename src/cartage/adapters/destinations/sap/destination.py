"""SAP destination: one BAPI call per record, BAPIRET2 parsing, commit/rollback."""
from __future__ import annotations

from pathlib import Path

from cartage.adapters.destinations.sap.mapping import Mapping
from cartage.adapters.destinations.sap.meta import load_meta
from cartage.adapters.destinations.sap.transports import make_transport
from cartage.core import BatchResult, CartageError, RecordError

COMMIT_MODES = ("per_record", "per_batch", "none")
COMMIT = ("BAPI_TRANSACTION_COMMIT", {"WAIT": "X"})
ROLLBACK = ("BAPI_TRANSACTION_ROLLBACK", {})


class SapDestination:
    preview_label = "bapi"

    def __init__(self, config: dict, options: dict, root: Path):
        if not options.get("bapi"):
            raise CartageError("The SAP destination needs 'bapi'", hint="e.g. bapi: BAPI_MATERIAL_SAVEDATA")
        if not options.get("mapping"):
            raise CartageError("The SAP destination needs a 'mapping' of record fields to BAPI parameters",
                               hint="e.g. mapping: { material: HEADDATA.MATERIAL }")
        self.commit = options.get("commit", "per_record")
        if self.commit not in COMMIT_MODES:
            raise CartageError(f"Unknown commit mode '{self.commit}'", hint=f"Use one of: {', '.join(COMMIT_MODES)}")
        self.bapi = options["bapi"]
        self.mapping = Mapping(options["mapping"], options.get("constants") or {}, load_meta(self.bapi))
        self.transport = make_transport(config)

    @classmethod
    def check_connection(cls, config: dict, root: Path) -> str:
        return make_transport(config).ping()

    def preview(self, records: list[dict]) -> list[dict]:
        return [self.mapping.build(r) for r in records]

    def write(self, batch: list[dict]) -> BatchResult:
        result = BatchResult()
        accepted: list[dict] = []
        for record in batch:
            returned = self.transport.call(self.bapi, self.mapping.build(record)).get("RETURN", [])
            errors = [r for r in returned if r.get("TYPE") in ("E", "A")]
            if errors:
                result.errors.append(RecordError(stage="destination", message=errors[0].get("MESSAGE", "BAPI error"),
                                                 source=record.get("_source"), details=returned, record=record))
                if self.commit == "per_record":
                    self.transport.call(*ROLLBACK)
                continue
            result.ok += 1
            result.warnings += any(r.get("TYPE") == "W" for r in returned)
            accepted.append(record)
            if self.commit == "per_record":
                self.transport.call(*COMMIT)
        if self.commit == "per_batch":
            if result.errors:
                self.transport.call(*ROLLBACK)
                result.errors.extend(
                    RecordError(stage="destination", message="Rolled back: another record in the same batch failed",
                                source=r.get("_source"), record=r)
                    for r in accepted)
                result.ok = result.warnings = 0
            elif accepted:
                self.transport.call(*COMMIT)
        return result
