"""CPSC Recalls API scraper and bulk CSV parser."""

import csv
import io
import json
import logging
import re
import time
from pathlib import Path

import httpx

from ..normalization.manufacturers import normalize_manufacturer, extract_fiscal_year, parse_units
from ..validation.quality import score_recall

logger = logging.getLogger(__name__)

RECALLS_API_URL = "https://www.saferproducts.gov/RestWebServices/Recall"
RECALLS_CSV_URL = "https://www.cpsc.gov/s3fs-public/recall-data/recalls_recall_listing.csv"

RAW_DIR = Path(__file__).parent.parent.parent / "data" / "raw"


def _join_nested(items: list, field: str) -> str:
    """Join a nested array field into a pipe-delimited string."""
    if not items:
        return ""
    values = []
    for item in items:
        val = item.get(field, "") if isinstance(item, dict) else str(item)
        if val:
            values.append(str(val))
    return " | ".join(values)


def download_recalls_api(start_year: int = 1973, end_year: int = 2026) -> list[dict]:
    """Download recalls from the CPSC REST API by year batches."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    all_records = []

    for year in range(start_year, end_year + 1):
        cache_path = RAW_DIR / f"recalls_api_{year}.json"
        if cache_path.exists():
            logger.info(f"Loading cached recalls for {year}")
            with open(cache_path) as f:
                records = json.load(f)
            all_records.extend(records)
            continue

        url = f"{RECALLS_API_URL}?format=json&RecallDateStart={year}-01-01&RecallDateEnd={year}-12-31"
        logger.info(f"Downloading recalls for {year}...")
        try:
            resp = httpx.get(url, timeout=60, follow_redirects=True,
                           headers={"User-Agent": "CPSC-Product-Safety-Tracker/1.0 (nathanmauricegoldberg@gmail.com)"})
            resp.raise_for_status()
            records = resp.json()
            logger.info(f"  {year}: {len(records)} recalls")

            with open(cache_path, "w") as f:
                json.dump(records, f)

            all_records.extend(records)
            time.sleep(2)
        except Exception as e:
            logger.warning(f"  {year}: Failed - {e}")

    return all_records


def parse_recall_api_record(raw: dict) -> dict:
    """Parse a single recall record from the API into normalized form."""
    recall_id = str(raw.get("RecallID", ""))
    recall_number = str(raw.get("RecallNumber", ""))
    recall_date = raw.get("RecallDate", "")
    # Normalize date to YYYY-MM-DD
    if recall_date:
        match = re.search(r"(\d{4}-\d{2}-\d{2})", recall_date)
        if match:
            recall_date = match.group(1)

    # Extract nested arrays
    products = raw.get("Products", []) or []
    product_names = _join_nested(products, "Name")
    product_types = _join_nested(products, "Type")
    product_categories = " | ".join(
        str(p.get("CategoryID", "")) for p in products if p.get("CategoryID")
    )
    # Units from first product
    number_of_units = ""
    for p in products:
        units = p.get("NumberOfUnits", "")
        if units:
            number_of_units = str(units)
            break

    hazards = raw.get("Hazards", []) or []
    hazard_description = _join_nested(hazards, "Name")
    hazard_types = _join_nested(hazards, "HazardType")

    remedies = raw.get("Remedies", []) or []
    remedy_description = _join_nested(remedies, "Name")

    remedy_options = raw.get("RemedyOptions", []) or []
    remedy_options_str = _join_nested(remedy_options, "Option")

    manufacturers = raw.get("Manufacturers", []) or []
    manufacturer_names = _join_nested(manufacturers, "Name")

    countries = raw.get("ManufacturerCountries", []) or []
    manufacturer_countries = _join_nested(countries, "Country")

    retailers = raw.get("Retailers", []) or []
    retailer_names = _join_nested(retailers, "Name")

    importers = raw.get("Importers", []) or []
    importer_names = _join_nested(importers, "Name")

    distributors = raw.get("Distributors", []) or []
    distributor_names = _join_nested(distributors, "Name")

    images = raw.get("Images", []) or []
    image_urls = _join_nested(images, "URL")

    # Normalize manufacturer
    first_manufacturer = ""
    if manufacturers:
        first_manufacturer = manufacturers[0].get("Name", "")
    normalized = normalize_manufacturer(first_manufacturer)

    # Parse units
    units_numeric = parse_units(number_of_units)

    # Fiscal year
    fiscal_year = extract_fiscal_year(recall_date)

    record = {
        "recall_id": recall_id,
        "recall_number": recall_number,
        "recall_date": recall_date,
        "title": raw.get("Title", ""),
        "description": raw.get("Description", ""),
        "consumer_contact": raw.get("ConsumerContact", ""),
        "url": raw.get("URL", ""),
        "last_publish_date": raw.get("LastPublishDate", ""),
        "product_names": product_names,
        "product_types": product_types,
        "product_categories": product_categories,
        "number_of_units": number_of_units,
        "hazard_description": hazard_description,
        "hazard_types": hazard_types,
        "remedy_description": remedy_description,
        "remedy_options": remedy_options_str,
        "manufacturer_names": manufacturer_names,
        "manufacturer_countries": manufacturer_countries,
        "retailer_names": retailer_names,
        "importer_names": importer_names,
        "distributor_names": distributor_names,
        "image_urls": image_urls,
        "normalized_manufacturer": normalized,
        "fiscal_year": fiscal_year,
        "units_numeric": units_numeric,
    }
    record["quality_score"] = score_recall(record)
    return record


def download_recalls_csv() -> str:
    """Download the bulk recalls CSV. Returns path to downloaded file."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    path = RAW_DIR / "recalls_listing.csv"
    if path.exists():
        logger.info(f"Using cached recalls CSV: {path}")
        return str(path)

    logger.info("Downloading CPSC recalls bulk CSV...")
    resp = httpx.get(RECALLS_CSV_URL, timeout=120, follow_redirects=True,
                     headers={"User-Agent": "CPSC-Product-Safety-Tracker/1.0 (nathanmauricegoldberg@gmail.com)"})
    resp.raise_for_status()
    path.write_bytes(resp.content)
    logger.info(f"Downloaded {len(resp.content):,} bytes to {path}")
    return str(path)


def parse_recalls_csv(csv_path: str) -> list[dict]:
    """Parse the bulk recalls CSV into normalized records.

    This is an alternative/supplement to the API. The CSV has different fields
    and may have slightly different recall counts.
    """
    records = []
    with open(csv_path, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            recall_number = (row.get("Recall Number") or "").strip()
            if not recall_number:
                continue

            recall_date_raw = (row.get("Date") or "").strip()
            # Convert "February 26, 2026" to "2026-02-26"
            recall_date = ""
            if recall_date_raw:
                fiscal_year = extract_fiscal_year(recall_date_raw)
                # Try to parse the date
                import datetime
                try:
                    dt = datetime.datetime.strptime(recall_date_raw, "%B %d, %Y")
                    recall_date = dt.strftime("%Y-%m-%d")
                except ValueError:
                    recall_date = recall_date_raw
            else:
                fiscal_year = None

            mfr_names = (row.get("Manufacturers") or "").strip()
            first_mfr = mfr_names.split(";")[0].strip() if mfr_names else ""
            normalized = normalize_manufacturer(first_mfr)

            units_str = (row.get("Units") or "").strip()
            units_numeric = parse_units(units_str)

            record = {
                "recall_id": f"CSV-{recall_number}",
                "recall_number": recall_number,
                "recall_date": recall_date,
                "title": (row.get("Recall Heading") or "").strip(),
                "description": (row.get("Description") or "").strip(),
                "consumer_contact": "",
                "url": "",
                "last_publish_date": "",
                "product_names": (row.get("Name of product") or "").strip(),
                "product_types": "",
                "product_categories": "",
                "number_of_units": units_str,
                "hazard_description": (row.get("Hazard Description") or "").strip(),
                "hazard_types": "",
                "remedy_description": (row.get("Remedy") or "").strip(),
                "remedy_options": (row.get("Remedy Type") or "").strip(),
                "manufacturer_names": mfr_names,
                "manufacturer_countries": (row.get("Manufactured In") or "").strip(),
                "retailer_names": (row.get("Sold At") or "").strip(),
                "importer_names": (row.get("Importers") or "").strip(),
                "distributor_names": (row.get("Distributors") or "").strip(),
                "image_urls": "",
                "normalized_manufacturer": normalized,
                "fiscal_year": fiscal_year,
                "units_numeric": units_numeric,
            }
            record["quality_score"] = score_recall(record)
            records.append(record)

    logger.info(f"Parsed {len(records)} recalls from CSV")
    return records
