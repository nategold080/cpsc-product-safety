"""Export module for CPSC Product Safety Tracker."""

import csv
import json
import logging
import sqlite3
from pathlib import Path

logger = logging.getLogger(__name__)

EXPORT_DIR = Path(__file__).parent.parent.parent / "data" / "exports"


def export_all(conn: sqlite3.Connection) -> dict:
    """Export all data to CSV, JSON, and Markdown. Returns dict of export counts."""
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    counts = {}

    counts["recalls_csv"] = _export_table_csv(conn, "recalls", "recalls.csv")
    counts["incidents_csv"] = _export_table_csv(conn, "incidents", "incidents.csv")
    counts["neiss_csv"] = _export_table_csv(conn, "neiss_injuries", "neiss_injuries.csv")
    counts["penalties_csv"] = _export_table_csv(conn, "penalties", "penalties.csv")
    counts["violations_csv"] = _export_table_csv(conn, "import_violations", "import_violations.csv")
    counts["profiles_csv"] = _export_table_csv(conn, "manufacturer_profiles", "manufacturer_profiles.csv")
    counts["cross_links_csv"] = _export_table_csv(conn, "cross_links", "cross_links.csv")

    counts["product_codes_csv"] = _export_table_csv(conn, "neiss_product_codes", "neiss_product_codes.csv")

    # FDA exports (only if tables have data)
    try:
        fda_count = conn.execute("SELECT COUNT(*) FROM fda_adverse_events").fetchone()[0]
        if fda_count > 0:
            counts["fda_events_csv"] = _export_table_csv(conn, "fda_adverse_events", "fda_adverse_events.csv")
        fda_recall_count = conn.execute("SELECT COUNT(*) FROM fda_device_recalls").fetchone()[0]
        if fda_recall_count > 0:
            counts["fda_recalls_csv"] = _export_table_csv(conn, "fda_device_recalls", "fda_device_recalls.csv")
    except Exception:
        pass

    # Hazard validation exports
    try:
        hv_count = conn.execute("SELECT COUNT(*) FROM hazard_validation_results").fetchone()[0]
        if hv_count > 0:
            counts["hazard_validation_csv"] = _export_table_csv(conn, "hazard_validation_results", "hazard_validation.csv")
            counts["hazard_map_csv"] = _export_table_csv(conn, "hazard_diagnosis_map", "hazard_diagnosis_map.csv")
    except Exception:
        pass

    counts["profiles_json"] = _export_profiles_json(conn)
    counts["summary_md"] = _export_summary_md(conn)
    counts["top_manufacturers_csv"] = _export_top_manufacturers(conn)
    counts["high_risk_csv"] = _export_high_risk(conn)

    return counts


def _export_table_csv(conn: sqlite3.Connection, table: str, filename: str) -> int:
    """Export a table to CSV. Returns row count."""
    path = EXPORT_DIR / filename
    cursor = conn.execute(f"SELECT * FROM {table}")
    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        writer.writerows(rows)

    logger.info(f"Exported {len(rows)} rows to {filename}")
    return len(rows)


def _export_profiles_json(conn: sqlite3.Connection) -> int:
    """Export manufacturer profiles to JSON."""
    path = EXPORT_DIR / "manufacturer_profiles.json"
    cursor = conn.execute("SELECT * FROM manufacturer_profiles ORDER BY compliance_score ASC")
    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()

    profiles = [dict(zip(columns, row)) for row in rows]
    with open(path, "w", encoding="utf-8") as f:
        json.dump(profiles, f, indent=2, default=str)

    logger.info(f"Exported {len(profiles)} profiles to JSON")
    return len(profiles)


def _export_top_manufacturers(conn: sqlite3.Connection) -> int:
    """Export top manufacturers by recall count."""
    path = EXPORT_DIR / "top_manufacturers.csv"
    cursor = conn.execute("""
        SELECT manufacturer_name, normalized_name,
               total_recalls, total_units_recalled,
               total_incidents, total_penalties, total_fines,
               total_import_violations, compliance_score, risk_tier,
               data_sources
        FROM manufacturer_profiles
        ORDER BY total_recalls DESC
        LIMIT 500
    """)
    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        writer.writerows(rows)

    logger.info(f"Exported {len(rows)} top manufacturers")
    return len(rows)


def _export_high_risk(conn: sqlite3.Connection) -> int:
    """Export high-risk and critical-risk manufacturers."""
    path = EXPORT_DIR / "high_risk_manufacturers.csv"
    cursor = conn.execute("""
        SELECT manufacturer_name, normalized_name,
               total_recalls, total_units_recalled,
               total_incidents, total_penalties, total_fines,
               total_import_violations, compliance_score, risk_tier,
               data_sources
        FROM manufacturer_profiles
        WHERE risk_tier IN ('HIGH', 'CRITICAL')
        ORDER BY compliance_score ASC
    """)
    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        writer.writerows(rows)

    logger.info(f"Exported {len(rows)} high-risk manufacturers")
    return len(rows)


def _export_summary_md(conn: sqlite3.Connection) -> int:
    """Export a Markdown summary of the database."""
    path = EXPORT_DIR / "summary.md"

    stats = {}
    for table in ["recalls", "incidents", "neiss_injuries", "penalties",
                   "import_violations", "manufacturer_profiles", "cross_links"]:
        try:
            row = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
            stats[table] = row[0]
        except Exception:
            stats[table] = 0

    # Key metrics
    total_fines = conn.execute("SELECT COALESCE(SUM(fine_amount), 0) FROM penalties").fetchone()[0]
    total_units = conn.execute("SELECT COALESCE(SUM(units_numeric), 0) FROM recalls WHERE units_numeric IS NOT NULL").fetchone()[0]

    try:
        neiss_weighted = conn.execute("SELECT COALESCE(SUM(weight), 0) FROM neiss_injuries").fetchone()[0]
    except Exception:
        neiss_weighted = 0

    try:
        date_range = conn.execute("SELECT MIN(recall_date), MAX(recall_date) FROM recalls WHERE recall_date IS NOT NULL").fetchone()
        date_str = f"{date_range[0]} to {date_range[1]}" if date_range[0] else "N/A"
    except Exception:
        date_str = "N/A"

    try:
        high_risk = conn.execute("SELECT COUNT(*) FROM manufacturer_profiles WHERE risk_tier IN ('HIGH', 'CRITICAL')").fetchone()[0]
    except Exception:
        high_risk = 0

    # Cross-agency stats
    try:
        fda_events = conn.execute("SELECT COUNT(*) FROM fda_adverse_events").fetchone()[0]
        fda_recalls = conn.execute("SELECT COUNT(*) FROM fda_device_recalls").fetchone()[0]
    except Exception:
        fda_events = 0
        fda_recalls = 0

    try:
        hazard_validations = conn.execute("SELECT COUNT(*) FROM hazard_validation_results").fetchone()[0]
        confirmed_validations = conn.execute("SELECT COUNT(*) FROM hazard_validation_results WHERE validation_status = 'confirmed'").fetchone()[0]
    except Exception:
        hazard_validations = 0
        confirmed_validations = 0

    # Top 20 manufacturers by recalls
    top_mfrs = conn.execute("""
        SELECT manufacturer_name, total_recalls, total_units_recalled,
               total_penalties, total_fines, compliance_score, risk_tier
        FROM manufacturer_profiles
        ORDER BY total_recalls DESC LIMIT 20
    """).fetchall()

    with open(path, "w", encoding="utf-8") as f:
        f.write("# CPSC Product Safety Tracker — Summary\n\n")
        f.write(f"**Generated from {stats.get('recalls', 0):,} recalls, "
                f"{stats.get('incidents', 0):,} incidents, "
                f"{stats.get('neiss_injuries', 0):,} NEISS injuries, "
                f"{stats.get('penalties', 0):,} penalties, "
                f"{stats.get('import_violations', 0):,} import violations**\n\n")

        f.write("## Key Statistics\n\n")
        f.write(f"| Metric | Value |\n|--------|-------|\n")
        f.write(f"| Recall Campaigns | {stats.get('recalls', 0):,} |\n")
        f.write(f"| Units Recalled | {total_units:,} |\n")
        f.write(f"| Incident Reports | {stats.get('incidents', 0):,} |\n")
        f.write(f"| NEISS ER Injuries | {stats.get('neiss_injuries', 0):,} |\n")
        f.write(f"| Est. National Injuries | {neiss_weighted:,.0f} |\n")
        f.write(f"| Penalties Assessed | {stats.get('penalties', 0):,} |\n")
        f.write(f"| Total Fines | ${total_fines:,.0f} |\n")
        f.write(f"| Import Violations | {stats.get('import_violations', 0):,} |\n")
        f.write(f"| Manufacturer Profiles | {stats.get('manufacturer_profiles', 0):,} |\n")
        f.write(f"| Cross-Links | {stats.get('cross_links', 0):,} |\n")
        f.write(f"| High/Critical Risk | {high_risk:,} |\n")
        f.write(f"| FDA Adverse Events | {fda_events:,} |\n")
        f.write(f"| FDA Device Recalls | {fda_recalls:,} |\n")
        f.write(f"| Hazard Validations | {hazard_validations:,} ({confirmed_validations} confirmed) |\n")
        f.write(f"| Date Range | {date_str} |\n")

        f.write("\n## Top 20 Manufacturers by Recall Count\n\n")
        f.write("| Manufacturer | Recalls | Units | Penalties | Fines | Score | Risk |\n")
        f.write("|-------------|---------|-------|-----------|-------|-------|------|\n")
        for mfr in top_mfrs:
            name = mfr[0][:40]
            f.write(f"| {name} | {mfr[1]:,} | {mfr[2]:,} | {mfr[3]} | ${mfr[4]:,.0f} | {mfr[5]:.3f} | {mfr[6]} |\n")

        f.write(f"\n---\n\nBuilt by **Nathan Goldberg** · nathanmauricegoldberg@gmail.com\n")

    logger.info("Exported summary.md")
    return 1
