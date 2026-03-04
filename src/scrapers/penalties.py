"""CPSC Penalty API scraper."""

import json
import logging
import time
from pathlib import Path

import httpx

from ..normalization.manufacturers import normalize_manufacturer, extract_fiscal_year
from ..validation.quality import score_penalty

logger = logging.getLogger(__name__)

CIVIL_URL = "https://www.saferproducts.gov/RestWebServices/Penalty?penaltytype=civil&format=json"
CRIMINAL_URL = "https://www.saferproducts.gov/RestWebServices/Penalty?penaltytype=criminal&format=json"

RAW_DIR = Path(__file__).parent.parent.parent / "data" / "raw"


def download_penalties() -> tuple[list[dict], list[dict]]:
    """Download civil and criminal penalties. Returns (civil, criminal) raw records."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    civil_records = _download_json(CIVIL_URL, "penalties_civil.json")
    time.sleep(2)
    criminal_records = _download_json(CRIMINAL_URL, "penalties_criminal.json")

    return civil_records, criminal_records


def _download_json(url: str, filename: str) -> list[dict]:
    """Download and cache a JSON endpoint."""
    cache_path = RAW_DIR / filename
    if cache_path.exists():
        logger.info(f"Loading cached {filename}")
        with open(cache_path) as f:
            return json.load(f)

    logger.info(f"Downloading {filename}...")
    resp = httpx.get(url, timeout=60, follow_redirects=True,
                     headers={"User-Agent": "CPSC-Product-Safety-Tracker/1.0 (nathanmauricegoldberg@gmail.com)"})
    resp.raise_for_status()
    data = resp.json()

    with open(cache_path, "w") as f:
        json.dump(data, f)
    logger.info(f"Downloaded {len(data)} records to {filename}")
    return data


def parse_penalty(raw: dict, penalty_type: str) -> dict:
    """Parse a single penalty record."""
    penalty_id = str(raw.get("PenaltyID", ""))
    if not penalty_id:
        penalty_id = f"{penalty_type}-{raw.get('RecallNo', '')}-{raw.get('Firm', '')}"

    firm_name = raw.get("Firm", "") or ""
    normalized = normalize_manufacturer(firm_name)

    fine_str = raw.get("Fine", "")
    fine_amount = 0.0
    if fine_str:
        # Parse "$1,200,000" or "1200000"
        cleaned = str(fine_str).replace("$", "").replace(",", "").strip()
        try:
            fine_amount = float(cleaned)
        except (ValueError, TypeError):
            fine_amount = 0.0

    penalty_date = raw.get("PenaltyDate", "")
    fiscal_year = extract_fiscal_year(penalty_date) or raw.get("FiscalYear")
    if fiscal_year:
        try:
            fiscal_year = int(fiscal_year)
        except (ValueError, TypeError):
            fiscal_year = None

    # Product types
    product_types = raw.get("ProductTypes", []) or []
    product_types_str = " | ".join(
        pt.get("Type", "") for pt in product_types if isinstance(pt, dict) and pt.get("Type")
    )

    record = {
        "penalty_id": penalty_id,
        "recall_number": raw.get("RecallNo", "") or "",
        "firm_name": firm_name,
        "penalty_type": penalty_type,
        "penalty_date": penalty_date,
        "act": raw.get("Act", "") or "",
        "fine_amount": fine_amount,
        "fiscal_year": fiscal_year,
        "release_title": raw.get("ReleaseTitle", "") or "",
        "release_url": raw.get("ReleaseURL", "") or "",
        "company_id": str(raw.get("CompanyID", "") or ""),
        "product_types": product_types_str,
        "normalized_firm": normalized,
    }
    record["quality_score"] = score_penalty(record)
    return record


def parse_all_penalties(civil_raw: list[dict], criminal_raw: list[dict]) -> list[dict]:
    """Parse all civil and criminal penalty records."""
    records = []

    for raw in civil_raw:
        records.append(parse_penalty(raw, "civil"))

    for raw in criminal_raw:
        records.append(parse_penalty(raw, "criminal"))

    logger.info(f"Parsed {len(records)} penalties ({len(civil_raw)} civil, {len(criminal_raw)} criminal)")
    return records
