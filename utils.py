import re

def normalize_ticker(ticker: str) -> str:
    """Strips suffixes like XD, XB, XR, NC from the ticker for consistent matching."""
    if not ticker:
        return ""
    return re.sub(r'(XD|XB|XR|NC)$', '', ticker.upper().strip())
