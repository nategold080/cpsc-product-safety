"""NEISS (National Electronic Injury Surveillance System) data scraper."""

import csv
import logging
import time
from pathlib import Path

import httpx

from ..validation.quality import score_neiss

logger = logging.getLogger(__name__)

NEISS_URL_PATTERN = "https://www.cpsc.gov/cgibin/NEISSQuery/Data/Archived%20Data/{year}/neiss{year}.tsv"
AVAILABLE_YEARS = list(range(2019, 2025))  # 2019-2024 (6 years with modern format)

RAW_DIR = Path(__file__).parent.parent.parent / "data" / "raw"

# NEISS code lookups
DISPOSITION_CODES = {
    1: "Treated/Released", 2: "Treated and Transferred",
    4: "Treated and Admitted", 5: "Held for Observation",
    6: "Left Without Being Seen", 8: "Fatality", 9: "Unknown",
}

LOCATION_CODES = {
    0: "Not Recorded", 1: "Home", 2: "Farm/Ranch",
    3: "Street or Highway", 4: "Other Public Property",
    5: "Manufactured (Mobile) Home", 6: "Industrial Place",
    7: "School/Daycare", 8: "Place of Recreation/Sports",
    9: "Not Recorded",
}

SEX_CODES = {1: "Male", 2: "Female", 3: "Non-Binary/Other"}

BODY_PART_CODES = {
    0: "Internal Organs", 25: "Vertebrae", 26: "Trunk - Upper",
    30: "Shoulder", 31: "Upper Arm", 32: "Elbow", 33: "Lower Arm",
    34: "Wrist", 35: "Hand", 36: "Finger", 37: "Pubic Region",
    38: "Trunk - Lower", 75: "Head", 76: "Face", 77: "Eyeball",
    79: "Mouth", 80: "Ear", 81: "Forehead", 82: "Nose",
    83: "Upper Leg", 84: "Knee", 85: "Lower Leg", 86: "Ankle",
    87: "Foot", 88: "Toe", 89: "Neck", 92: "Not Recorded",
    94: "25-50% of Body", 95: "All Parts of Body",
}

DIAGNOSIS_CODES = {
    41: "Burns - Thermal", 42: "Burns - Scald", 43: "Burns - Chemical",
    46: "Burns - Electrical", 47: "Burns - Radiation",
    48: "Burns - Not Specified", 49: "Concussions",
    50: "Contusions/Abrasions", 52: "Dental Injury",
    53: "Dermatitis/Conjunctivitis", 54: "Dislocation",
    55: "Drowning", 56: "Electric Shock", 57: "Foreign Body",
    58: "Fracture", 59: "Hematoma", 60: "Hemorrhage",
    61: "Laceration", 62: "Nerve Damage", 63: "Poisoning",
    64: "Puncture", 65: "Strain/Sprain", 66: "Ingested Foreign Object",
    67: "Aspiration", 68: "Anoxia", 69: "Internal Organ Injury",
    71: "Amputation", 72: "Crushing", 73: "Avulsion",
    74: "Submersion", 77: "Other/Not Stated",
}


def download_neiss(years: list[int] | None = None) -> list[str]:
    """Download NEISS TSV files for specified years. Returns list of file paths."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    if years is None:
        years = AVAILABLE_YEARS

    paths = []
    for year in years:
        path = RAW_DIR / f"neiss{year}.tsv"
        if path.exists():
            logger.info(f"Using cached NEISS {year}: {path}")
            paths.append(str(path))
            continue

        url = NEISS_URL_PATTERN.format(year=year)
        logger.info(f"Downloading NEISS {year}...")
        try:
            resp = httpx.get(url, timeout=120, follow_redirects=True,
                           headers={"User-Agent": "CPSC-Product-Safety-Tracker/1.0 (nathanmauricegoldberg@gmail.com)"})
            resp.raise_for_status()
            path.write_bytes(resp.content)
            logger.info(f"  {year}: {len(resp.content):,} bytes")
            paths.append(str(path))
            time.sleep(2)
        except Exception as e:
            logger.warning(f"  {year}: Failed - {e}")

    return paths


def parse_neiss_tsv(tsv_path: str, product_code_names: dict | None = None) -> list[dict]:
    """Parse a NEISS TSV file into normalized records.

    Args:
        tsv_path: Path to the TSV file.
        product_code_names: Optional dict mapping product code -> product name.
    """
    if product_code_names is None:
        product_code_names = {}

    records = []
    year_from_path = None
    # Extract year from filename like "neiss2023.tsv"
    path_str = str(tsv_path)
    for y in range(2000, 2030):
        if str(y) in path_str:
            year_from_path = y
            break

    with open(tsv_path, encoding="utf-8", errors="replace") as f:
        # NEISS TSVs may have different header names, try to detect
        first_line = f.readline().strip()
        f.seek(0)

        # Determine delimiter
        delimiter = "\t"

        reader = csv.DictReader(f, delimiter=delimiter)
        if not reader.fieldnames:
            return []

        # Normalize field names
        field_map = {}
        for fn in reader.fieldnames:
            clean = fn.strip().lower().replace(" ", "_")
            field_map[fn] = clean

        for row in reader:
            mapped = {}
            for raw_col, val in row.items():
                norm = field_map.get(raw_col, raw_col)
                mapped[norm] = (val or "").strip()

            case_num = mapped.get("cpsc_case_number", "") or mapped.get("cpsc_case_#", "")
            if not case_num:
                continue

            # Parse numeric fields
            def _int(key, default=None):
                v = mapped.get(key, "")
                try:
                    return int(v)
                except (ValueError, TypeError):
                    return default

            def _float(key, default=None):
                v = mapped.get(key, "")
                try:
                    return float(v)
                except (ValueError, TypeError):
                    return default

            age = _int("age")
            sex_code = _int("sex")
            sex_name = SEX_CODES.get(sex_code, "")

            body_part = _int("body_part")
            body_part_name = BODY_PART_CODES.get(body_part, "")
            diagnosis = _int("diagnosis")
            diagnosis_name = DIAGNOSIS_CODES.get(diagnosis, "")

            disposition = _int("disposition")
            disposition_name = DISPOSITION_CODES.get(disposition, "")

            location = _int("location")
            location_name = LOCATION_CODES.get(location, "")

            product_1 = _int("product_1") or _int("product_1_code")
            product_2 = _int("product_2") or _int("product_2_code")
            product_3 = _int("product_3") or _int("product_3_code")

            product_1_name = product_code_names.get(product_1, "") if product_1 else ""

            treatment_date = mapped.get("treatment_date", "")
            narrative = mapped.get("narrative_1", "") or mapped.get("narrative", "")

            record = {
                "cpsc_case_number": case_num,
                "treatment_date": treatment_date,
                "age": age,
                "sex": sex_name,
                "race": mapped.get("race", ""),
                "hispanic": mapped.get("hispanic", ""),
                "body_part": body_part,
                "body_part_name": body_part_name,
                "diagnosis": diagnosis,
                "diagnosis_name": diagnosis_name,
                "body_part_2": _int("body_part_2"),
                "diagnosis_2": _int("diagnosis_2"),
                "disposition": disposition,
                "disposition_name": disposition_name,
                "location": location,
                "location_name": location_name,
                "fire_involvement": _int("fire_involvement"),
                "product_1": product_1,
                "product_1_name": product_1_name,
                "product_2": product_2,
                "product_3": product_3,
                "alcohol": _int("alcohol"),
                "drug": _int("drug"),
                "narrative": narrative,
                "stratum": mapped.get("stratum", ""),
                "psu": mapped.get("psu", ""),
                "weight": _float("weight"),
                "neiss_year": year_from_path,
            }
            record["quality_score"] = score_neiss(record)
            records.append(record)

    logger.info(f"Parsed {len(records)} NEISS records from {tsv_path}")
    return records
