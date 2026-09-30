from cartage.adapters.destinations.sap.transports.mock import HttpTransport, InProcessTransport
from cartage.adapters.destinations.sap.transports.rfc import RfcTransport
from cartage.core import CartageError


def make_transport(config: dict):
    kind = config.get("transport")
    if kind == "mock":
        return HttpTransport(config["url"]) if config.get("url") else InProcessTransport()
    if kind == "rfc":
        return RfcTransport(config)
    raise CartageError(f"Unknown SAP transport '{kind}'", hint="Supported: mock, rfc")
