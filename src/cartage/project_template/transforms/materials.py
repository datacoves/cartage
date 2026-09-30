"""Transforms for the materials pipeline. Plain Python: referenced from pipelines/materials.yaml."""

UOM = {"PCS": "EA", "KGS": "KG"}


def normalize_uom(record):
    """map: legacy units of measure → SAP units."""
    record["uom"] = UOM.get(record["uom"], record["uom"])
    return record


def is_active(record):
    """filter: drop obsolete materials."""
    return record.get("status") != "obsolete"


def dedupe(records, key):
    """batch: keep the last record per key."""
    seen = {}
    for r in records:
        seen[r[key]] = r
    return seen.values()
