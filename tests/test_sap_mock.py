import socket
import threading

import pytest

from cartage.adapters.destinations.sap.transports import make_transport
from cartage.adapters.destinations.sap.transports.mock import HttpTransport, InProcessTransport, MockSap, make_server
from cartage.core import CartageError, FatalRunError

GOOD = {"HEADDATA": {"MATERIAL": "100001", "IND_SECTOR": "M", "MATL_TYPE": "FERT"}, "CLIENTDATA": {"BASE_UOM": "EA"},
        "CLIENTDATAX": {"BASE_UOM": "X"}}


def messages(result):
    return [(r["TYPE"], r["MESSAGE"]) for r in result["RETURN"]]


def test_create_commit_then_change():
    sap = MockSap()
    assert messages(sap.call("BAPI_MATERIAL_SAVEDATA", GOOD)) == [("S", "The material 100001 has been created or extended")]
    assert sap.materials == {}
    sap.call("BAPI_TRANSACTION_COMMIT", {"WAIT": "X"})
    assert "100001" in sap.materials
    assert messages(sap.call("BAPI_MATERIAL_SAVEDATA", GOOD))[0][1] == "The material 100001 has been changed"


def test_rollback_discards_pending():
    sap = MockSap()
    sap.call("BAPI_MATERIAL_SAVEDATA", GOOD)
    sap.call("BAPI_TRANSACTION_ROLLBACK", {})
    sap.call("BAPI_TRANSACTION_COMMIT", {})
    assert sap.materials == {}


@pytest.mark.parametrize("params, expected", [
    ({**GOOD, "HEADDATA": {**GOOD["HEADDATA"], "MATL_TYPE": "ZXX"}}, "Value 'ZXX' is not allowed for HEADDATA-MATL_TYPE"),
    ({**GOOD, "HEADDATA": {**GOOD["HEADDATA"], "MATERIAL": "1" * 22}}, "exceeds maximum length 18"),
    ({**GOOD, "CLIENTDATA": {}}, "Required field CLIENTDATA-BASE_UOM is not filled"),
    ({**GOOD, "PLANTDATA": {"PLANT": "1000"}}, "Parameter PLANTDATA does not exist"),
    ({**GOOD, "CLIENTDATA": {"BASE_UOM": "EA", "NOPE": "1"}}, "Field CLIENTDATA-NOPE does not exist"),
])
def test_validation_errors(params, expected):
    result = MockSap().call("BAPI_MATERIAL_SAVEDATA", params)
    assert result["RETURN"][0]["TYPE"] == "E"
    assert expected in result["RETURN"][0]["MESSAGE"]


def test_unknown_function():
    assert "Function module Z_NOPE not found" in MockSap().call("Z_NOPE", {})["RETURN"][0]["MESSAGE"]


def test_http_server_roundtrip():
    calls = []
    server = make_server(0, on_call=lambda f, p, r: calls.append(f))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        transport = HttpTransport(f"http://127.0.0.1:{server.server_address[1]}")
        assert transport.ping().startswith("mock at http://127.0.0.1:")
        assert transport.call("BAPI_MATERIAL_SAVEDATA", GOOD)["RETURN"][0]["TYPE"] == "S"
        assert calls == ["BAPI_MATERIAL_SAVEDATA"]
    finally:
        server.shutdown()
        server.server_close()


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def test_unreachable_server_is_fatal():
    with pytest.raises(FatalRunError, match="Cannot reach mock SAP") as info:
        HttpTransport(f"http://127.0.0.1:{free_port()}", timeout=1).call("X", {})
    assert "cartage sap mock" in info.value.hint


def test_make_transport():
    assert isinstance(make_transport({"transport": "mock"}), InProcessTransport)
    assert isinstance(make_transport({"transport": "mock", "url": "http://x"}), HttpTransport)
    with pytest.raises(CartageError, match="'rfc' is not available"):
        make_transport({"transport": "rfc", "ashost": "h"})
    with pytest.raises(CartageError, match="Unknown SAP transport 'odata'"):
        make_transport({"transport": "odata"})
