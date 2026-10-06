from pathlib import Path

import pytest

from cartage.adapters.destinations.sap.destination import SapDestination
from cartage.core import CartageError

OPTIONS = {
    "bapi": "BAPI_MATERIAL_SAVEDATA",
    "mapping": {"HEADDATA.MATERIAL": "material", "HEADDATA.IND_SECTOR": "industry", "HEADDATA.MATL_TYPE": "type",
                "CLIENTDATA.BASE_UOM": "uom"},
}
GOOD = {"material": "100001", "industry": "M", "type": "FERT", "uom": "EA", "_source": "m.csv:2"}
BAD = {"material": "100002", "industry": "M", "type": "ZXX", "uom": "EA", "_source": "m.csv:3"}


def dest(**options):
    return SapDestination({"transport": "mock"}, {**OPTIONS, **options}, Path("."))


def test_per_record_commits_good_and_reports_bad():
    d = dest()
    result = d.write([GOOD, BAD])
    assert (result.ok, len(result.errors)) == (1, 1)
    error = result.errors[0]
    assert (error.stage, error.source) == ("destination", "m.csv:3")
    assert "ZXX" in error.message
    assert error.details[0]["TYPE"] == "E"
    assert list(d.transport.sap.materials) == ["100001"]


def test_per_batch_rolls_back_whole_batch_on_error():
    d = dest(commit="per_batch")
    result = d.write([GOOD, BAD])
    assert (result.ok, len(result.errors)) == (0, 2)
    assert "Rolled back" in result.errors[1].message
    assert d.transport.sap.materials == {}


def test_per_batch_commits_clean_batch():
    d = dest(commit="per_batch")
    assert d.write([GOOD]).ok == 1
    assert list(d.transport.sap.materials) == ["100001"]


def test_commit_none_leaves_pending():
    d = dest(commit="none")
    d.write([GOOD])
    assert d.transport.sap.materials == {} and list(d.transport.sap.pending) == ["100001"]


def test_missing_mapped_column_is_a_record_error():
    result = dest().write([{"material": "100003", "industry": "M", "type": "FERT"}])
    assert "Required field CLIENTDATA-BASE_UOM is not filled" in result.errors[0].message


def test_preview_has_no_side_effects():
    d = dest()
    assert d.preview([GOOD])[0]["HEADDATA"]["MATERIAL"] == "100001"
    assert d.transport.sap.pending == {}


def test_option_errors():
    with pytest.raises(CartageError, match="needs 'bapi'"):
        SapDestination({"transport": "mock"}, {"mapping": {"HEADDATA.MATERIAL": "a"}}, Path("."))
    with pytest.raises(CartageError, match="needs a 'mapping'"):
        SapDestination({"transport": "mock"}, {"bapi": "BAPI_MATERIAL_SAVEDATA"}, Path("."))
    with pytest.raises(CartageError, match="Unknown commit mode 'always'"):
        dest(commit="always")


def test_check_connection():
    assert SapDestination.check_connection({"transport": "mock"}, Path(".")) == "mock (in-process)"
