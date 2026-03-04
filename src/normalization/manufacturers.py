"""Manufacturer name normalization for CPSC data cross-linking."""

import re

# Legal entity suffixes only — do NOT include descriptive words
CORP_SUFFIXES = [
    "INCORPORATED", "CORPORATION", "COMPANY", "LIMITED",
    "HOLDINGS", "GROUP", "ENTERPRISES",
    "CORP", "INC", "LLC", "LLP", "LP",
    "LTD", "CO", "PC", "PA", "NA",
    "PLC", "SA", "AG", "GMBH", "BV", "NV",
    "PTY", "SRL", "SARL",
]

ABBREVIATIONS = {
    "INTL": "INTERNATIONAL",
    "TECH": "TECHNOLOGY",
    "MFG": "MANUFACTURING",
    "MFRS": "MANUFACTURERS",
    "PRODS": "PRODUCTS",
    "PROD": "PRODUCTS",
    "ELEC": "ELECTRONICS",
    "ELECTR": "ELECTRONICS",
    "INDUS": "INDUSTRIES",
    "IND": "INDUSTRIES",
    "AMER": "AMERICA",
    "ASSOC": "ASSOCIATES",
    "DIST": "DISTRIBUTION",
    "ENT": "ENTERPRISES",
    "GRP": "GROUP",
}

# DBA/FKA/AKA pattern
_DBA_RE = re.compile(r"\b(?:D/?B/?A|F/?K/?A|A/?K/?A|FORMERLY|TRADING\s+AS)\b.*", re.IGNORECASE)
# Parenthetical content
_PAREN_RE = re.compile(r"\([^)]*\)")
# Punctuation (preserve spaces and hyphens)
_PUNCT_RE = re.compile(r"[^\w\s-]")
# Multiple spaces
_SPACE_RE = re.compile(r"\s+")
# Suffix pattern (built dynamically)
_SUFFIX_PATTERN = None


def _get_suffix_pattern():
    global _SUFFIX_PATTERN
    if _SUFFIX_PATTERN is None:
        escaped = [re.escape(s) for s in sorted(CORP_SUFFIXES, key=len, reverse=True)]
        _SUFFIX_PATTERN = re.compile(r"\b(?:" + "|".join(escaped) + r")\b")
    return _SUFFIX_PATTERN


def normalize_manufacturer(name: str) -> str:
    """Normalize a manufacturer name for cross-linking.

    Pipeline:
    1. Uppercase and strip
    2. Remove DBA/FKA/AKA clauses
    3. Remove parenthetical content
    4. Strip "THE" prefix
    5. Remove punctuation (preserve spaces and hyphens)
    6. Replace hyphens with spaces
    7. Expand abbreviations
    8. Strip legal entity suffixes (3 passes)
    9. Collapse whitespace
    """
    if not name:
        return ""

    result = name.upper().strip()

    # Remove DBA/FKA/AKA clauses
    result = _DBA_RE.sub("", result)

    # Remove parenthetical content
    result = _PAREN_RE.sub("", result)

    # Strip "THE" prefix
    result = re.sub(r"^THE\s+", "", result)

    # Remove punctuation
    result = _PUNCT_RE.sub(" ", result)

    # Replace hyphens with spaces
    result = result.replace("-", " ")

    # Expand abbreviations
    words = result.split()
    expanded = []
    for w in words:
        expanded.append(ABBREVIATIONS.get(w, w))
    result = " ".join(expanded)

    # Strip legal entity suffixes (3 passes to handle trailing combos)
    pat = _get_suffix_pattern()
    for _ in range(3):
        result = pat.sub("", result)

    # Collapse whitespace
    result = _SPACE_RE.sub(" ", result).strip()

    return result


def parse_units(units_str: str) -> int | None:
    """Parse units string like '1,200,000' or 'About 1.2 million' to integer."""
    if not units_str:
        return None

    text = units_str.upper().strip()

    # Remove common prefixes
    text = re.sub(r"^(ABOUT|APPROXIMATELY|APPROX\.?|NEARLY|OVER|MORE THAN|LESS THAN)\s+", "", text)

    multiplier = 1
    if "MILLION" in text:
        multiplier = 1_000_000
        text = text.replace("MILLION", "").strip()
    elif "THOUSAND" in text:
        multiplier = 1_000
        text = text.replace("THOUSAND", "").strip()

    # Remove commas, extract number
    text = text.replace(",", "")
    match = re.search(r"([\d.]+)", text)
    if match:
        try:
            val = float(match.group(1)) * multiplier
            return int(val)
        except (ValueError, OverflowError):
            return None

    return None


def extract_fiscal_year(date_str: str) -> int | None:
    """Extract fiscal year from a date string.

    Federal fiscal year: Oct 1 - Sep 30.
    FY2024 = Oct 2023 through Sep 2024.
    """
    if not date_str:
        return None

    # Try YYYY-MM-DD format
    match = re.search(r"(\d{4})-(\d{2})", date_str)
    if match:
        year = int(match.group(1))
        month = int(match.group(2))
        return year + 1 if month >= 10 else year

    # Try Month DD, YYYY format
    month_map = {
        "JANUARY": 1, "FEBRUARY": 2, "MARCH": 3, "APRIL": 4,
        "MAY": 5, "JUNE": 6, "JULY": 7, "AUGUST": 8,
        "SEPTEMBER": 9, "OCTOBER": 10, "NOVEMBER": 11, "DECEMBER": 12,
    }
    for mname, mnum in month_map.items():
        if mname in date_str.upper():
            year_match = re.search(r"(\d{4})", date_str)
            if year_match:
                year = int(year_match.group(1))
                return year + 1 if mnum >= 10 else year

    # Try MM/DD/YYYY
    match = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})", date_str)
    if match:
        month = int(match.group(1))
        year = int(match.group(3))
        return year + 1 if month >= 10 else year

    # Fallback: just extract year
    match = re.search(r"(\d{4})", date_str)
    if match:
        return int(match.group(1))

    return None
