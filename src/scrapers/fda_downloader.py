"""OpenFDA API scraper for adverse events and device recalls."""

import logging
import time
from typing import Iterator

import httpx

from ..normalization.manufacturers import normalize_manufacturer, extract_fiscal_year
from ..validation.quality import score_fda_event, score_fda_recall

logger = logging.getLogger(__name__)

BASE_URL = "https://api.fda.gov"
USER_AGENT = "CPSC-Product-Safety-Tracker/1.0 (nathanmauricegoldberg@gmail.com)"
PAGE_SIZE = 100
RATE_LIMIT_DELAY = 0.5  # seconds between requests (conservative)

# Consumer-product-relevant search filters for device events
# Focus on home-use injuries/malfunctions, not clinical/hospital device issues
# OpenFDA uses (field:value)+(field:value) for boolean AND
DEVICE_EVENT_SEARCHES = [
    # Home injuries from consumer devices
    "(event_location:HOME)+(event_type:Injury)",
    # Deaths from consumer devices (smaller set, high value)
    "(event_location:HOME)+(event_type:Death)",
]

# Keywords to filter for consumer-relevant device recalls
CONSUMER_RECALL_KEYWORDS = [
    "consumer", "household", "home use", "personal", "portable",
    "battery", "charger", "power tool", "heater", "space heater",
    "smoke detector", "carbon monoxide", "child", "infant", "baby",
    "electric blanket", "humidifier", "thermometer", "blood pressure",
]


def download_device_events(max_per_search: int = 20000) -> list[dict]:
    """Download FDA device adverse event reports for consumer products.

    Filters to home-use injuries and deaths.
    Returns list of parsed event records.
    """
    all_events = []
    seen_keys = set()

    for search_query in DEVICE_EVENT_SEARCHES:
        logger.info(f"Querying FDA device events: {search_query}")
        count = 0
        for batch in _paginate_api(
            f"{BASE_URL}/device/event.json",
            search=search_query,
            max_records=max_per_search,
        ):
            for raw in batch:
                record = _parse_device_event(raw)
                if record and record["event_id"] not in seen_keys:
                    seen_keys.add(record["event_id"])
                    all_events.append(record)
                    count += 1

        logger.info(f"  Got {count:,} events from this search")

    logger.info(f"Total FDA device events: {len(all_events):,}")
    return all_events


def download_device_recalls(max_records: int = 60000) -> list[dict]:
    """Download FDA device recalls. Returns list of parsed recall records."""
    all_recalls = []
    seen_ids = set()

    logger.info("Querying FDA device recalls...")
    for batch in _paginate_api(
        f"{BASE_URL}/device/recall.json",
        max_records=max_records,
    ):
        for raw in batch:
            record = _parse_device_recall(raw)
            if record and record["recall_id"] not in seen_ids:
                seen_ids.add(record["recall_id"])
                all_recalls.append(record)

    logger.info(f"Total FDA device recalls: {len(all_recalls):,}")
    return all_recalls


def _paginate_api(
    url: str,
    search: str | None = None,
    max_records: int = 50000,
) -> Iterator[list[dict]]:
    """Paginate through OpenFDA API results."""
    client = httpx.Client(
        timeout=30,
        headers={"User-Agent": USER_AGENT},
        follow_redirects=True,
    )

    skip = 0
    total_fetched = 0

    try:
        while total_fetched < max_records:
            params = {"limit": PAGE_SIZE, "skip": skip}
            if search:
                params["search"] = search

            try:
                resp = client.get(url, params=params)
                resp.raise_for_status()
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    logger.info("No more results (404)")
                    break
                elif e.response.status_code == 429:
                    logger.warning("Rate limited, waiting 60s...")
                    time.sleep(60)
                    continue
                else:
                    logger.warning(f"HTTP error: {e}")
                    break
            except httpx.RequestError as e:
                logger.warning(f"Request error: {e}")
                break

            data = resp.json()
            results = data.get("results", [])
            if not results:
                break

            yield results

            total_fetched += len(results)
            skip += PAGE_SIZE

            # OpenFDA caps skip at 25,000
            if skip >= 25000:
                logger.info(f"Reached OpenFDA skip limit at {total_fetched:,} records")
                break

            time.sleep(RATE_LIMIT_DELAY)

    finally:
        client.close()


def _parse_device_event(raw: dict) -> dict | None:
    """Parse a single FDA device adverse event record."""
    report_number = raw.get("report_number", "")
    if not report_number:
        return None

    # Extract device info (first device in list)
    devices = raw.get("device", [])
    device = devices[0] if devices else {}

    manufacturer_name = device.get("manufacturer_d_name", "")
    generic_name = device.get("generic_name", "")
    brand_name = device.get("brand_name", "")

    # Extract patient outcome
    patients = raw.get("patient", [])
    patient = patients[0] if patients else {}
    outcome = patient.get("sequence_number_outcome", "")

    # Parse event type
    event_type = raw.get("event_type", "")

    # Parse dates
    report_date = _parse_fda_date(raw.get("date_received", ""))

    # Determine source
    source_type = raw.get("source_type", [])
    source = source_type[0] if source_type else ""

    # Build product description
    product_name = brand_name if brand_name and brand_name != "N/A" else generic_name

    record = {
        "event_id": f"FDA-{report_number}",
        "report_date": report_date,
        "product_name": product_name[:500] if product_name else "",
        "product_type": "device",
        "manufacturer_name": manufacturer_name,
        "manufacturer_normalized": normalize_manufacturer(manufacturer_name) if manufacturer_name else "",
        "event_type": event_type.lower() if event_type else "",
        "patient_outcome": _normalize_outcome(outcome),
        "description": _extract_event_narrative(raw),
        "source": source,
    }
    record["quality_score"] = score_fda_event(record)
    return record


def _parse_device_recall(raw: dict) -> dict | None:
    """Parse a single FDA device recall record."""
    res_number = raw.get("product_res_number", "")
    if not res_number:
        return None

    product_desc = raw.get("product_description", "")
    reason = raw.get("reason_for_recall", "")
    firm = raw.get("recalling_firm", "")
    recall_status = raw.get("recall_status", "")
    event_date = raw.get("event_date_initiated", "")

    # Determine recall class from res_event_number or other signals
    # FDA device recalls use class I (most serious), II, III
    # The recall class isn't directly in this API; use product_res_number prefix
    recall_class = _infer_recall_class(raw)

    record = {
        "recall_id": f"FDA-{res_number}",
        "product_description": product_desc[:1000] if product_desc else "",
        "reason_for_recall": reason[:1000] if reason else "",
        "manufacturer_name": firm,
        "manufacturer_normalized": normalize_manufacturer(firm) if firm else "",
        "recall_class": recall_class,
        "recall_status": recall_status,
        "event_date": event_date,
    }
    record["quality_score"] = score_fda_recall(record)
    return record


def _parse_fda_date(date_str: str) -> str:
    """Parse FDA date format (YYYYMMDD) to YYYY-MM-DD."""
    if not date_str or len(date_str) < 8:
        return ""
    try:
        return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
    except (IndexError, ValueError):
        return ""


def _normalize_outcome(outcome: str) -> str:
    """Normalize patient outcome codes."""
    outcome_map = {
        "1": "death",
        "2": "hospitalization",
        "3": "disability",
        "4": "congenital_anomaly",
        "5": "required_intervention",
        "6": "other",
        "7": "life_threatening",
        "8": "no_answer",
        "9": "not_applicable",
    }
    if not outcome:
        return ""
    # outcome can be a list or a comma-separated string
    if isinstance(outcome, list):
        items = outcome
    else:
        items = [o.strip() for o in outcome.split(",") if o.strip()]
    parts = [outcome_map.get(str(o).strip(), str(o).strip()) for o in items if str(o).strip()]
    return ", ".join(parts)


def _extract_event_narrative(raw: dict) -> str:
    """Extract narrative text from MDR text fields."""
    texts = raw.get("mdr_text", [])
    narratives = []
    for t in texts[:3]:  # Limit to avoid oversized descriptions
        text = t.get("text", "")
        if text:
            narratives.append(text[:500])
    return " | ".join(narratives)[:1000]


def _infer_recall_class(raw: dict) -> str:
    """Infer recall class from available data."""
    # The device recall API doesn't directly include recall class
    # We infer from the res_event_number or classify as unknown
    res_number = raw.get("product_res_number", "")
    if res_number.startswith("Z-"):
        return "Class I"  # Most serious
    return "Unknown"
