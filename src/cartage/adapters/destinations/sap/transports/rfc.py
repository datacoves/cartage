"""Placeholder for the pyrfc-based transport."""
from cartage.core import CartageError


class RfcTransport:
    def __init__(self, config: dict):
        raise CartageError("SAP transport 'rfc' is not available in cartage v0.1",
                           hint="Use transport: mock for now; RFC via pyrfc is planned")
