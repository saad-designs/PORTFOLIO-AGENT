import re

PSX_TICKER_ALIASES = {
    "HUBCO": "HUBC",
    "HUB POWER": "HUBC",
    "MEEZAN": "MEBL",
    "SYSTEMS": "SYS",
    "LUCKY": "LUCK",
    "FAUJI": "FFC",
    "ENGRO FERTILIZERS": "EFERT",
    "MARI PETROLEUM": "MARI",
    "MARI ENERGIES": "MARI",
    "FAYSAL": "FABL",
    "FAYSAL BANK": "FABL",
}


def normalize_ticker(ticker: str) -> str:
    """Strips PSX status suffixes (XD, XB, XR, NC, WU) and maps common aliases like HUBCO -> HUBC."""
    if not ticker:
        return ""
    cleaned = ticker.upper().strip()
    if cleaned.endswith(".KA"):
        cleaned = cleaned[:-3].strip()
    cleaned = re.sub(r"\s+(XD|XB|XR|NC|WU|SPOT)$", "", cleaned)
    cleaned = re.sub(r"(XD|XB|XR|NC)$", "", cleaned)
    return PSX_TICKER_ALIASES.get(cleaned, cleaned)
