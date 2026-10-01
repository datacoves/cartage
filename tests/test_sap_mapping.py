import datetime

import pytest

from cartage.adapters.destinations.sap.mapping import Mapping, to_sap
from cartage.adapters.destinations.sap.meta import load_meta
from cartage.core import CartageError

FIELDS = {
    "material": "HEADDATA.MATERIAL",
    "type": "HEADDATA.MATL_TYPE",
    "description": "MATERIALDESCRIPTION[].MATL_DESC",
    "uom": "CLIENTDATA.BASE_UOM",
}
CONSTANTS = {"MATERIALDESCRIPTION[].LANGU_ISO": "EN", "HEADDATA.BASIC_VIEW": "X"}


def test_meta_loads():
    meta = load_meta("BAPI_MATERIAL_SAVEDATA")
    assert meta.key == "HEADDATA.MATERIAL"
    assert meta.parameters["CLIENTDATA"].x_structure == "CLIENTDATAX"
    assert meta.parameters["HEADDATA"].fields["MATL_TYPE"].allowed == ("FERT", "HALB", "HAWA", "ROH")


def test_unknown_bapi_lists_bundled():
    with pytest.raises(CartageError) as info:
        load_meta("BAPI_NOPE")
    assert "BAPI_MATERIAL_SAVEDATA" in info.value.hint


def test_build_params():
    mapping = Mapping(FIELDS, CONSTANTS, load_meta("BAPI_MATERIAL_SAVEDATA"))
    params = mapping.build({"material": "100001", "type": "FERT", "description": "Pump", "uom": "EA", "_source": "x"})
    assert params == {
        "HEADDATA": {"MATERIAL": "100001", "MATL_TYPE": "FERT", "BASIC_VIEW": "X"},
        "MATERIALDESCRIPTION": [{"MATL_DESC": "Pump", "LANGU_ISO": "EN"}],
        "CLIENTDATA": {"BASE_UOM": "EA"},
        "CLIENTDATAX": {"BASE_UOM": "X"},
    }


def test_empty_and_missing_values_are_skipped():
    params = Mapping(FIELDS, {}, load_meta("BAPI_MATERIAL_SAVEDATA")).build({"material": "1", "type": "", "uom": None})
    assert params == {"HEADDATA": {"MATERIAL": "1"}}


@pytest.mark.parametrize("value, expected", [
    (True, "X"), (False, ""), (42, "42"), (datetime.date(2024, 3, 1), "20240301"),
    (datetime.datetime(2024, 3, 1, 10, 0, tzinfo=datetime.UTC), "20240301"), ("x", "x"),
])
def test_to_sap(value, expected):
    assert to_sap(value) == expected


def test_all_bad_targets_reported_together():
    bad = {"a": "HEADDATA.NOPE", "b": "NOPARAM.X", "c": "MATERIALDESCRIPTION.MATL_DESC", "d": "HEADDATA[].MATERIAL", "e": "lowercase"}
    with pytest.raises(CartageError) as info:
        Mapping(bad, {}, load_meta("BAPI_MATERIAL_SAVEDATA"))
    message = info.value.message
    assert "HEADDATA has no field NOPE" in message
    assert "has no parameter NOPARAM" in message
    assert "write MATERIALDESCRIPTION[].MATL_DESC" in message
    assert "write HEADDATA.MATERIAL" in message
    assert "'lowercase' is not PARAM.FIELD" in message
