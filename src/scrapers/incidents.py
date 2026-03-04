"""SaferProducts.gov incident report bulk download and parsing."""

import csv
import io
import logging
import zipfile
from pathlib import Path

import httpx

from ..normalization.manufacturers import normalize_manufacturer, extract_fiscal_year
from ..validation.quality import score_incident

logger = logging.getLogger(__name__)

INCIDENTS_ZIP_URL = "https://www.saferproducts.gov/SPDB.zip"

RAW_DIR = Path(__file__).parent.parent.parent / "data" / "raw"

# Column name mapping — handles variations in the CSV header names
COLUMN_MAP = {
    "report no.": "report_number",
    "report no": "report_number",
    "report_no": "report_number",
    "reportnumber": "report_number",
    "report number": "report_number",
    "report date": "report_date",
    "reportdate": "report_date",
    "report_date": "report_date",
    "publication date": "publication_date",
    "publicationdate": "publication_date",
    "category of submitter": "submitter_category",
    "categoryofsubmitter": "submitter_category",
    "product description": "product_description",
    "productdescription": "product_description",
    "product category": "product_category",
    "productcategory": "product_category",
    "product sub category": "product_subcategory",
    "productsubcategory": "product_subcategory",
    "product type": "product_type",
    "producttype": "product_type",
    "product code": "product_code",
    "productcode": "product_code",
    "manufacturer/importer/private labeler name": "manufacturer_name",
    "manufacturer / importer / private labeler name": "manufacturer_name",
    "manufacturer name": "manufacturer_name",
    "manufacturername": "manufacturer_name",
    "brand name": "brand_name",
    "brand": "brand_name",
    "brandname": "brand_name",
    "model name or number": "model_name",
    "modelname": "model_name",
    "serial number": "serial_number",
    "serialnumber": "serial_number",
    "upc": "upc",
    "date manufactured": "date_manufactured",
    "datemanufactured": "date_manufactured",
    "retailer": "retailer_name",
    "retailername": "retailer_name",
    "retailer state": "retailer_state",
    "retailerstate": "retailer_state",
    "purchase date": "purchase_date",
    "purchasedate": "purchase_date",
    "incident description": "incident_description",
    "incidentdescription": "incident_description",
    "city": "city",
    "state": "state",
    "zip": "zip_code",
    "zip code": "zip_code",
    "zipcode": "zip_code",
    "location": "location",
    "(primary) victim severity": "severity",
    "primaryvictimseverity": "severity",
    "victim severity": "severity",
    "victim's sex": "victim_sex",
    "(primary) victim's sex": "victim_sex",
    "victimssex": "victim_sex",
    "victim's age": "victim_age",
    "(primary) victim's age (years)": "victim_age",
    "victimsage": "victim_age",
    "company comments": "company_comments",
    "companycomments": "company_comments",
    "associated report numbers": "associated_reports",
    "associatedreportnumbers": "associated_reports",
}


def download_incidents() -> str:
    """Download the SaferProducts.gov bulk ZIP. Returns path to incidents CSV."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    incidents_csv = RAW_DIR / "incidents.csv"

    if incidents_csv.exists():
        logger.info(f"Using cached incidents CSV: {incidents_csv}")
        return str(incidents_csv)

    zip_path = RAW_DIR / "SPDB.zip"
    if not zip_path.exists():
        logger.info("Downloading SaferProducts.gov bulk database...")
        resp = httpx.get(INCIDENTS_ZIP_URL, timeout=300, follow_redirects=True,
                        headers={"User-Agent": "CPSC-Product-Safety-Tracker/1.0 (nathanmauricegoldberg@gmail.com)"})
        resp.raise_for_status()
        zip_path.write_bytes(resp.content)
        logger.info(f"Downloaded {len(resp.content):,} bytes")

    # Extract the incidents CSV
    logger.info("Extracting incidents CSV from ZIP...")
    with zipfile.ZipFile(zip_path) as zf:
        for name in zf.namelist():
            lower = name.lower()
            if "incident" in lower and lower.endswith(".csv"):
                with zf.open(name) as src:
                    incidents_csv.write_bytes(src.read())
                logger.info(f"Extracted {name} -> {incidents_csv}")
                return str(incidents_csv)

        # Fallback: extract first CSV
        for name in zf.namelist():
            if name.lower().endswith(".csv"):
                with zf.open(name) as src:
                    incidents_csv.write_bytes(src.read())
                logger.info(f"Extracted {name} -> {incidents_csv}")
                return str(incidents_csv)

    raise FileNotFoundError("No CSV found in SaferProducts ZIP")


def _normalize_column(col: str) -> str:
    """Map raw column name to normalized field name."""
    clean = col.strip().lower()
    # Direct lookup
    if clean in COLUMN_MAP:
        return COLUMN_MAP[clean]
    # Remove non-alphanumeric, try again
    collapsed = "".join(c for c in clean if c.isalnum())
    if collapsed in COLUMN_MAP:
        return COLUMN_MAP[collapsed]
    return clean.replace(" ", "_")


def parse_incidents(csv_path: str) -> list[dict]:
    """Parse incident reports from the CSV into normalized records."""
    records = []

    with open(csv_path, encoding="utf-8-sig", errors="replace") as f:
        # Skip disclaimer/header lines until we find the actual column headers
        header_line = None
        for line in f:
            stripped = line.strip()
            if stripped.startswith("Report No"):
                header_line = stripped
                break
            # Also check if it starts with a quote that contains Report
            if stripped.startswith('"Report No'):
                header_line = stripped
                break

        if not header_line:
            # Reset and try as normal CSV
            f.seek(0)
            reader = csv.DictReader(f)
        else:
            # Re-read using the found header
            # We need to create a reader from remaining lines with our header
            import io
            remaining = header_line + "\n" + f.read()
            reader = csv.DictReader(io.StringIO(remaining))

        if not reader.fieldnames:
            logger.error("No fieldnames found in incidents CSV")
            return []

        # Build column mapping
        col_map = {}
        for raw_col in reader.fieldnames:
            normalized = _normalize_column(raw_col)
            col_map[raw_col] = normalized

        for row in reader:
            # Remap columns
            mapped = {}
            for raw_col, val in row.items():
                if raw_col is None:
                    continue
                norm_col = col_map.get(raw_col, raw_col)
                if isinstance(val, list):
                    val = " ".join(str(v) for v in val if v)
                mapped[norm_col] = (str(val) if val else "").strip()

            report_number = mapped.get("report_number", "")
            if not report_number:
                continue

            manufacturer = mapped.get("manufacturer_name", "")
            normalized_mfr = normalize_manufacturer(manufacturer)
            fiscal_year = extract_fiscal_year(mapped.get("report_date", ""))

            record = {
                "report_number": report_number,
                "report_date": mapped.get("report_date", ""),
                "publication_date": mapped.get("publication_date", ""),
                "submitter_category": mapped.get("submitter_category", ""),
                "product_description": mapped.get("product_description", ""),
                "product_category": mapped.get("product_category", ""),
                "product_subcategory": mapped.get("product_subcategory", ""),
                "product_type": mapped.get("product_type", ""),
                "product_code": mapped.get("product_code", ""),
                "manufacturer_name": manufacturer,
                "brand_name": mapped.get("brand_name", ""),
                "model_name": mapped.get("model_name", ""),
                "serial_number": mapped.get("serial_number", ""),
                "upc": mapped.get("upc", ""),
                "date_manufactured": mapped.get("date_manufactured", ""),
                "retailer_name": mapped.get("retailer_name", ""),
                "retailer_state": mapped.get("retailer_state", ""),
                "purchase_date": mapped.get("purchase_date", ""),
                "incident_description": mapped.get("incident_description", ""),
                "city": mapped.get("city", ""),
                "state": mapped.get("state", ""),
                "zip_code": mapped.get("zip_code", ""),
                "location": mapped.get("location", ""),
                "severity": mapped.get("severity", ""),
                "victim_sex": mapped.get("victim_sex", ""),
                "victim_age": mapped.get("victim_age", ""),
                "company_comments": mapped.get("company_comments", ""),
                "associated_reports": mapped.get("associated_reports", ""),
                "normalized_manufacturer": normalized_mfr,
                "fiscal_year": fiscal_year,
            }
            record["quality_score"] = score_incident(record)
            records.append(record)

    logger.info(f"Parsed {len(records)} incident reports")
    return records
