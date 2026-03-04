"""Hazard-to-diagnosis validation for CPSC recalls vs NEISS injury data."""

import logging
import sqlite3
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)

CONFIG_PATH = Path(__file__).parent.parent.parent / "config" / "hazard_diagnosis_map.yaml"


def load_hazard_map(yaml_path: str | None = None) -> dict:
    """Load hazard-to-diagnosis mapping from YAML config.

    Returns dict: hazard_type -> set of NEISS diagnosis codes.
    """
    path = yaml_path or str(CONFIG_PATH)
    with open(path) as f:
        data = yaml.safe_load(f)

    hazard_map = {}
    for hazard_type, info in data.get("hazard_diagnosis_map", {}).items():
        codes = set(info.get("neiss_diagnosis_codes", []))
        hazard_map[hazard_type] = codes

    return hazard_map


def load_hazard_map_to_db(conn: sqlite3.Connection, yaml_path: str | None = None) -> int:
    """Load hazard-diagnosis mapping into the database table. Returns count loaded."""
    path = yaml_path or str(CONFIG_PATH)
    with open(path) as f:
        data = yaml.safe_load(f)

    # NEISS diagnosis code -> name lookup
    from ..scrapers.neiss import DIAGNOSIS_CODES

    conn.execute("DELETE FROM hazard_diagnosis_map")
    records = []
    for hazard_type, info in data.get("hazard_diagnosis_map", {}).items():
        for code in info.get("neiss_diagnosis_codes", []):
            records.append((hazard_type, code, DIAGNOSIS_CODES.get(code, "")))

    conn.executemany(
        "INSERT OR REPLACE INTO hazard_diagnosis_map (hazard_type, neiss_diagnosis_code, diagnosis_name) "
        "VALUES (?, ?, ?)",
        records,
    )
    conn.commit()
    logger.info(f"Loaded {len(records)} hazard-diagnosis mappings")
    return len(records)


def extract_hazard_type(hazard_description: str) -> str:
    """Extract the primary hazard type from a recall hazard description.

    Maps free-text hazard descriptions to standardized hazard types.
    """
    if not hazard_description:
        return ""

    text = hazard_description.upper()

    # Order matters — more specific patterns first
    HAZARD_PATTERNS = [
        ("ELECTRICAL SHOCK", "Electrical Shock Hazard"),
        ("ELECTRIC SHOCK", "Electrical Shock Hazard"),
        ("ELECTROCUTION", "Electrical Shock Hazard"),
        ("STRANGULATION", "Strangulation Hazard"),
        ("STRANGL", "Strangulation Hazard"),
        ("ENTRAPMENT", "Entrapment Hazard"),
        ("ENTRAP", "Entrapment Hazard"),
        ("TIP OVER", "Tip-Over Hazard"),
        ("TIP-OVER", "Tip-Over Hazard"),
        ("TIPOVER", "Tip-Over Hazard"),
        ("TIPPING", "Tip-Over Hazard"),
        ("EXPLOSION", "Explosion Hazard"),
        ("EXPLOD", "Explosion Hazard"),
        ("CHOKING", "Choking Hazard"),
        ("CHOKE", "Choking Hazard"),
        ("ASPIRATION", "Choking Hazard"),
        ("DROWNING", "Drowning Hazard"),
        ("SUBMERSION", "Drowning Hazard"),
        ("POISON", "Poisoning Hazard"),
        ("TOXIC", "Poisoning Hazard"),
        ("LEAD", "Poisoning Hazard"),
        ("FIRE", "Fire Hazard"),
        ("FLAME", "Fire Hazard"),
        ("OVERHEAT", "Fire Hazard"),
        ("BURN", "Burn Hazard"),
        ("THERMAL", "Burn Hazard"),
        ("SCALD", "Burn Hazard"),
        ("LACERAT", "Laceration Hazard"),
        ("CUT", "Laceration Hazard"),
        ("SHARP", "Laceration Hazard"),
        ("PUNCTURE", "Puncture Hazard"),
        ("FALL", "Fall Hazard"),
        ("IMPACT", "Impact Hazard"),
        ("STRUCK", "Impact Hazard"),
        ("PROJECTILE", "Impact Hazard"),
    ]

    for pattern, hazard_type in HAZARD_PATTERNS:
        if pattern in text:
            return hazard_type

    return ""


def build_hazard_validation(conn: sqlite3.Connection) -> int:
    """Validate recall hazards against NEISS injury diagnoses.

    For each manufacturer with recalls:
    1. Extract hazard type from recall description
    2. Find NEISS injuries for products associated with that manufacturer
    3. Check if injury diagnoses match expected diagnosis for the hazard type
    4. Compute match rate

    Returns count of validation records created.
    """
    hazard_map = load_hazard_map()
    if not hazard_map:
        logger.warning("No hazard-diagnosis mappings loaded")
        return 0

    # Clear old results
    conn.execute("DELETE FROM hazard_validation_results")

    # Get all manufacturers with recalls that have hazard descriptions
    # Note: hazard_types is often empty; hazard_description has the actual data
    mfr_hazards = conn.execute("""
        SELECT normalized_manufacturer, hazard_description, COUNT(*) as recall_count
        FROM recalls
        WHERE normalized_manufacturer != ''
          AND hazard_description IS NOT NULL AND hazard_description != ''
        GROUP BY normalized_manufacturer, hazard_description
    """).fetchall()

    if not mfr_hazards:
        logger.info("No manufacturers with hazard data found")
        return 0

    # For NEISS matching, we use the cross-link between incidents and recalls
    # to find relevant manufacturers, then query NEISS by product codes
    # associated with those incidents.
    # Simplified approach: query NEISS diagnosis distribution globally,
    # then per manufacturer we check if the hazard-expected diagnoses appear.

    results = []
    for norm_mfr, hazard_desc, recall_count in mfr_hazards:
        # Extract primary hazard type from description
        hazard_type = extract_hazard_type(hazard_desc)
        if not hazard_type or hazard_type not in hazard_map:
            continue

        expected_codes = hazard_map[hazard_type]

        # Get NEISS injuries linked to this manufacturer via incidents
        # First, find product codes from incidents for this manufacturer
        incident_products = conn.execute("""
            SELECT DISTINCT product_code FROM incidents
            WHERE normalized_manufacturer = ? AND product_code IS NOT NULL AND product_code != ''
        """, (norm_mfr,)).fetchall()

        if not incident_products:
            continue

        product_codes = [r[0] for r in incident_products]
        placeholders = ",".join("?" * len(product_codes))

        # Count total NEISS injuries for these product codes
        row = conn.execute(f"""
            SELECT COUNT(*) FROM neiss_injuries
            WHERE CAST(product_1 AS TEXT) IN ({placeholders})
        """, product_codes).fetchone()
        total_injuries = row[0] if row else 0

        if total_injuries == 0:
            continue

        # Count matching diagnoses
        row = conn.execute(f"""
            SELECT COUNT(*) FROM neiss_injuries
            WHERE CAST(product_1 AS TEXT) IN ({placeholders})
            AND diagnosis IN ({",".join("?" * len(expected_codes))})
        """, product_codes + list(expected_codes)).fetchone()
        matching = row[0] if row else 0
        unexpected = total_injuries - matching

        match_rate = matching / total_injuries if total_injuries > 0 else 0.0

        if match_rate >= 0.3:
            status = "confirmed"
        elif total_injuries < 10:
            status = "insufficient_data"
        else:
            status = "unexpected_pattern"

        results.append({
            "manufacturer_normalized": norm_mfr,
            "hazard_type": hazard_type,
            "total_recalls_with_hazard": recall_count,
            "total_neiss_injuries": total_injuries,
            "matching_diagnosis_count": matching,
            "unexpected_diagnosis_count": unexpected,
            "match_rate": round(match_rate, 4),
            "validation_status": status,
        })

    # Batch insert
    if results:
        conn.executemany("""
            INSERT OR REPLACE INTO hazard_validation_results (
                manufacturer_normalized, hazard_type,
                total_recalls_with_hazard, total_neiss_injuries,
                matching_diagnosis_count, unexpected_diagnosis_count,
                match_rate, validation_status
            ) VALUES (
                :manufacturer_normalized, :hazard_type,
                :total_recalls_with_hazard, :total_neiss_injuries,
                :matching_diagnosis_count, :unexpected_diagnosis_count,
                :match_rate, :validation_status
            )
        """, results)
        conn.commit()

    logger.info(f"Built {len(results)} hazard validation records")
    return len(results)
