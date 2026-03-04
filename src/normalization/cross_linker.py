"""Cross-linking engine for CPSC Product Safety data."""

import logging
import sqlite3
from collections import defaultdict

from .manufacturers import normalize_manufacturer
from ..validation.quality import compute_compliance_score, assign_risk_tier

logger = logging.getLogger(__name__)


def build_cross_links(conn: sqlite3.Connection) -> int:
    """Build cross-links between all tables. Returns count of links created."""
    conn.execute("DELETE FROM cross_links")
    conn.commit()

    total = 0
    total += _link_recalls_to_penalties(conn)
    total += _link_recalls_to_incidents(conn)
    total += _link_penalties_to_incidents(conn)
    total += _link_violations_to_recalls(conn)
    total += _link_fda_to_cpsc(conn)

    conn.commit()
    logger.info(f"Total cross-links created: {total:,}")
    return total


def _link_recalls_to_penalties(conn: sqlite3.Connection) -> int:
    """Link recalls to penalties via recall_number."""
    links = []

    # Direct recall number matching
    rows = conn.execute("""
        SELECT r.recall_id, p.penalty_id
        FROM recalls r
        JOIN penalties p ON r.recall_number = p.recall_number
        WHERE r.recall_number != '' AND p.recall_number != ''
    """).fetchall()

    for recall_id, penalty_id in rows:
        links.append({
            "source_table": "recalls",
            "source_id": recall_id,
            "target_table": "penalties",
            "target_id": penalty_id,
            "link_type": "recall_number",
            "confidence": 1.0,
        })

    # Manufacturer name matching
    penalty_firms = {}
    for row in conn.execute("SELECT penalty_id, normalized_firm FROM penalties WHERE normalized_firm != ''"):
        firm = row[1]
        if firm not in penalty_firms:
            penalty_firms[firm] = []
        penalty_firms[firm].append(row[0])

    for row in conn.execute("SELECT recall_id, normalized_manufacturer FROM recalls WHERE normalized_manufacturer != ''"):
        recall_id, norm_mfr = row
        if norm_mfr in penalty_firms:
            for penalty_id in penalty_firms[norm_mfr]:
                links.append({
                    "source_table": "recalls",
                    "source_id": recall_id,
                    "target_table": "penalties",
                    "target_id": penalty_id,
                    "link_type": "manufacturer_name",
                    "confidence": 0.85,
                })

    if links:
        conn.executemany("""
            INSERT OR IGNORE INTO cross_links
            (source_table, source_id, target_table, target_id, link_type, confidence)
            VALUES (:source_table, :source_id, :target_table, :target_id, :link_type, :confidence)
        """, links)
        conn.commit()

    logger.info(f"Recalls → Penalties: {len(links)} links")
    return len(links)


def _link_recalls_to_incidents(conn: sqlite3.Connection) -> int:
    """Link recalls to incidents via normalized manufacturer name."""
    links = []

    # Build incident manufacturer index
    incident_mfrs = defaultdict(list)
    for row in conn.execute(
        "SELECT report_number, normalized_manufacturer FROM incidents WHERE normalized_manufacturer != ''"
    ):
        incident_mfrs[row[1]].append(row[0])

    # Match recalls to incidents
    for row in conn.execute(
        "SELECT recall_id, normalized_manufacturer FROM recalls WHERE normalized_manufacturer != ''"
    ):
        recall_id, norm_mfr = row
        if norm_mfr in incident_mfrs:
            for report_number in incident_mfrs[norm_mfr]:
                links.append({
                    "source_table": "recalls",
                    "source_id": recall_id,
                    "target_table": "incidents",
                    "target_id": report_number,
                    "link_type": "manufacturer_name",
                    "confidence": 0.80,
                })

    if links:
        conn.executemany("""
            INSERT OR IGNORE INTO cross_links
            (source_table, source_id, target_table, target_id, link_type, confidence)
            VALUES (:source_table, :source_id, :target_table, :target_id, :link_type, :confidence)
        """, links)
        conn.commit()

    logger.info(f"Recalls → Incidents: {len(links)} links")
    return len(links)


def _link_penalties_to_incidents(conn: sqlite3.Connection) -> int:
    """Link penalties to incidents via normalized firm name."""
    links = []

    incident_mfrs = defaultdict(list)
    for row in conn.execute(
        "SELECT report_number, normalized_manufacturer FROM incidents WHERE normalized_manufacturer != ''"
    ):
        incident_mfrs[row[1]].append(row[0])

    for row in conn.execute(
        "SELECT penalty_id, normalized_firm FROM penalties WHERE normalized_firm != ''"
    ):
        penalty_id, norm_firm = row
        if norm_firm in incident_mfrs:
            for report_number in incident_mfrs[norm_firm]:
                links.append({
                    "source_table": "penalties",
                    "source_id": penalty_id,
                    "target_table": "incidents",
                    "target_id": report_number,
                    "link_type": "firm_name",
                    "confidence": 0.80,
                })

    if links:
        conn.executemany("""
            INSERT OR IGNORE INTO cross_links
            (source_table, source_id, target_table, target_id, link_type, confidence)
            VALUES (:source_table, :source_id, :target_table, :target_id, :link_type, :confidence)
        """, links)
        conn.commit()

    logger.info(f"Penalties → Incidents: {len(links)} links")
    return len(links)


def _link_violations_to_recalls(conn: sqlite3.Connection) -> int:
    """Link import violations to recalls via normalized firm name."""
    links = []

    recall_mfrs = defaultdict(list)
    for row in conn.execute(
        "SELECT recall_id, normalized_manufacturer FROM recalls WHERE normalized_manufacturer != ''"
    ):
        recall_mfrs[row[1]].append(row[0])

    for row in conn.execute(
        "SELECT violation_id, normalized_firm FROM import_violations WHERE normalized_firm != ''"
    ):
        violation_id, norm_firm = row
        if norm_firm in recall_mfrs:
            for recall_id in recall_mfrs[norm_firm]:
                links.append({
                    "source_table": "import_violations",
                    "source_id": str(violation_id),
                    "target_table": "recalls",
                    "target_id": recall_id,
                    "link_type": "firm_name",
                    "confidence": 0.80,
                })

    if links:
        conn.executemany("""
            INSERT OR IGNORE INTO cross_links
            (source_table, source_id, target_table, target_id, link_type, confidence)
            VALUES (:source_table, :source_id, :target_table, :target_id, :link_type, :confidence)
        """, links)
        conn.commit()

    logger.info(f"Import Violations → Recalls: {len(links)} links")
    return len(links)


def _link_fda_to_cpsc(conn: sqlite3.Connection) -> int:
    """Link FDA adverse events and recalls to CPSC data via manufacturer name."""
    links = []

    # Check if FDA tables exist and have data
    try:
        fda_event_count = conn.execute("SELECT COUNT(*) FROM fda_adverse_events").fetchone()[0]
        fda_recall_count = conn.execute("SELECT COUNT(*) FROM fda_device_recalls").fetchone()[0]
    except Exception:
        logger.info("FDA tables not available, skipping FDA cross-linking")
        return 0

    if fda_event_count == 0 and fda_recall_count == 0:
        logger.info("No FDA data to cross-link")
        return 0

    # Build CPSC manufacturer index
    cpsc_mfrs = set()
    for row in conn.execute(
        "SELECT DISTINCT normalized_manufacturer FROM recalls WHERE normalized_manufacturer != ''"
    ):
        cpsc_mfrs.add(row[0])
    for row in conn.execute(
        "SELECT DISTINCT normalized_manufacturer FROM incidents WHERE normalized_manufacturer != ''"
    ):
        cpsc_mfrs.add(row[0])

    # Clear old FDA links
    conn.execute("DELETE FROM cpsc_fda_manufacturer_links")

    # Link FDA adverse events to CPSC via exact normalized name match
    fda_event_mfrs = {}
    for row in conn.execute(
        "SELECT event_id, manufacturer_normalized FROM fda_adverse_events WHERE manufacturer_normalized != ''"
    ):
        event_id, norm = row
        if norm not in fda_event_mfrs:
            fda_event_mfrs[norm] = []
        fda_event_mfrs[norm].append(event_id)

    # Find matching manufacturers
    matched_mfrs = set()
    for fda_norm in fda_event_mfrs:
        if fda_norm in cpsc_mfrs:
            matched_mfrs.add(fda_norm)
            for event_id in fda_event_mfrs[fda_norm][:100]:  # Cap per manufacturer
                links.append({
                    "source_table": "fda_adverse_events",
                    "source_id": event_id,
                    "target_table": "recalls",
                    "target_id": fda_norm,
                    "link_type": "cross_agency_manufacturer",
                    "confidence": 0.85,
                })

    # Link FDA recalls to CPSC
    for row in conn.execute(
        "SELECT recall_id, manufacturer_normalized FROM fda_device_recalls WHERE manufacturer_normalized != ''"
    ):
        recall_id, norm = row
        if norm in cpsc_mfrs:
            matched_mfrs.add(norm)
            links.append({
                "source_table": "fda_device_recalls",
                "source_id": recall_id,
                "target_table": "recalls",
                "target_id": norm,
                "link_type": "cross_agency_manufacturer",
                "confidence": 0.85,
            })

    # Insert cross-agency manufacturer links
    mfr_links = [
        {"cpsc_manufacturer": m, "fda_manufacturer": m,
         "link_method": "exact", "confidence": 0.90}
        for m in matched_mfrs
    ]
    if mfr_links:
        conn.executemany("""
            INSERT OR IGNORE INTO cpsc_fda_manufacturer_links
            (cpsc_manufacturer, fda_manufacturer, link_method, confidence)
            VALUES (:cpsc_manufacturer, :fda_manufacturer, :link_method, :confidence)
        """, mfr_links)

    if links:
        conn.executemany("""
            INSERT OR IGNORE INTO cross_links
            (source_table, source_id, target_table, target_id, link_type, confidence)
            VALUES (:source_table, :source_id, :target_table, :target_id, :link_type, :confidence)
        """, links)
        conn.commit()

    logger.info(f"FDA ↔ CPSC: {len(links)} links, {len(matched_mfrs)} matched manufacturers")
    return len(links)


def build_manufacturer_profiles(conn: sqlite3.Connection) -> int:
    """Build unified manufacturer profiles from all data sources."""
    conn.execute("DELETE FROM manufacturer_profiles")
    conn.commit()

    # Collect all normalized manufacturer names across all sources
    all_manufacturers = {}  # normalized_name -> original_name

    for row in conn.execute(
        "SELECT DISTINCT manufacturer_names, normalized_manufacturer FROM recalls WHERE normalized_manufacturer != ''"
    ):
        orig, norm = row
        if norm and norm not in all_manufacturers:
            # Use first manufacturer from pipe-delimited list
            first = orig.split("|")[0].strip() if orig else norm
            all_manufacturers[norm] = first

    for row in conn.execute(
        "SELECT DISTINCT manufacturer_name, normalized_manufacturer FROM incidents WHERE normalized_manufacturer != ''"
    ):
        orig, norm = row
        if norm and norm not in all_manufacturers:
            all_manufacturers[norm] = orig

    for row in conn.execute(
        "SELECT DISTINCT firm_name, normalized_firm FROM penalties WHERE normalized_firm != ''"
    ):
        orig, norm = row
        if norm and norm not in all_manufacturers:
            all_manufacturers[norm] = orig

    for row in conn.execute(
        "SELECT DISTINCT firm_name, normalized_firm FROM import_violations WHERE normalized_firm != ''"
    ):
        orig, norm = row
        if norm and norm not in all_manufacturers:
            all_manufacturers[norm] = orig

    logger.info(f"Found {len(all_manufacturers)} unique manufacturers")

    # Build profiles
    profiles = []
    for norm_name, orig_name in all_manufacturers.items():
        profile = _build_single_profile(conn, norm_name, orig_name)
        if profile:
            profiles.append(profile)

    # Batch insert
    if profiles:
        conn.executemany("""
            INSERT INTO manufacturer_profiles (
                manufacturer_name, normalized_name,
                total_recalls, total_units_recalled, recall_years,
                recall_hazard_types, recall_product_types,
                total_incidents, incident_severities, incident_product_categories,
                total_neiss_injuries, total_neiss_weighted, neiss_product_codes,
                total_penalties, total_fines, penalty_types,
                total_import_violations, violation_types, violation_countries,
                compliance_score, risk_tier,
                first_seen_date, last_seen_date, data_sources
            ) VALUES (
                :manufacturer_name, :normalized_name,
                :total_recalls, :total_units_recalled, :recall_years,
                :recall_hazard_types, :recall_product_types,
                :total_incidents, :incident_severities, :incident_product_categories,
                :total_neiss_injuries, :total_neiss_weighted, :neiss_product_codes,
                :total_penalties, :total_fines, :penalty_types,
                :total_import_violations, :violation_types, :violation_countries,
                :compliance_score, :risk_tier,
                :first_seen_date, :last_seen_date, :data_sources
            )
        """, profiles)
        conn.commit()

    logger.info(f"Built {len(profiles)} manufacturer profiles")
    return len(profiles)


def _build_single_profile(conn: sqlite3.Connection, norm_name: str, orig_name: str) -> dict | None:
    """Build a single manufacturer profile by aggregating all data sources."""
    sources = []
    dates = []

    # Recall data
    row = conn.execute("""
        SELECT COUNT(*), COALESCE(SUM(units_numeric), 0),
               GROUP_CONCAT(DISTINCT fiscal_year),
               GROUP_CONCAT(DISTINCT hazard_types),
               GROUP_CONCAT(DISTINCT product_types),
               MIN(recall_date), MAX(recall_date)
        FROM recalls WHERE normalized_manufacturer = ?
    """, (norm_name,)).fetchone()

    total_recalls = row[0] or 0
    total_units = row[1] or 0
    recall_years = row[2] or ""
    recall_hazards = row[3] or ""
    recall_products = row[4] or ""
    if row[5]:
        dates.append(row[5])
    if row[6]:
        dates.append(row[6])
    if total_recalls > 0:
        sources.append("recalls")

    # Incident data
    row = conn.execute("""
        SELECT COUNT(*),
               GROUP_CONCAT(DISTINCT severity),
               GROUP_CONCAT(DISTINCT product_category),
               MIN(report_date), MAX(report_date)
        FROM incidents WHERE normalized_manufacturer = ?
    """, (norm_name,)).fetchone()

    total_incidents = row[0] or 0
    incident_severities = row[1] or ""
    incident_categories = row[2] or ""
    if row[3]:
        dates.append(row[3])
    if row[4]:
        dates.append(row[4])
    if total_incidents > 0:
        sources.append("incidents")

    # Penalty data
    row = conn.execute("""
        SELECT COUNT(*), COALESCE(SUM(fine_amount), 0),
               GROUP_CONCAT(DISTINCT penalty_type),
               MIN(penalty_date), MAX(penalty_date)
        FROM penalties WHERE normalized_firm = ?
    """, (norm_name,)).fetchone()

    total_penalties = row[0] or 0
    total_fines = row[1] or 0.0
    penalty_types = row[2] or ""
    if row[3]:
        dates.append(row[3])
    if row[4]:
        dates.append(row[4])
    if total_penalties > 0:
        sources.append("penalties")

    # Import violations
    row = conn.execute("""
        SELECT COUNT(*),
               GROUP_CONCAT(DISTINCT violation_type),
               GROUP_CONCAT(DISTINCT country),
               MIN(nov_date), MAX(nov_date)
        FROM import_violations WHERE normalized_firm = ?
    """, (norm_name,)).fetchone()

    total_violations = row[0] or 0
    violation_types = row[1] or ""
    violation_countries = row[2] or ""
    if row[3]:
        dates.append(row[3])
    if row[4]:
        dates.append(row[4])
    if total_violations > 0:
        sources.append("import_violations")

    # FDA adverse events
    fda_events = 0
    fda_recalls = 0
    try:
        row = conn.execute("""
            SELECT COUNT(*) FROM fda_adverse_events
            WHERE manufacturer_normalized = ?
        """, (norm_name,)).fetchone()
        fda_events = row[0] or 0
        if fda_events > 0:
            sources.append("fda_events")

        row = conn.execute("""
            SELECT COUNT(*) FROM fda_device_recalls
            WHERE manufacturer_normalized = ?
        """, (norm_name,)).fetchone()
        fda_recalls = row[0] or 0
        if fda_recalls > 0:
            sources.append("fda_recalls")
    except Exception:
        pass  # FDA tables may not exist yet

    # Skip manufacturers with no data in any source
    if not sources:
        return None

    # NEISS is not linked by manufacturer (product codes only), so leave at 0
    dates_clean = sorted(d for d in dates if d)

    # Compute compliance score
    profile_data = {
        "total_recalls": total_recalls,
        "total_fines": total_fines,
        "total_incidents": total_incidents,
        "total_import_violations": total_violations,
    }
    score = compute_compliance_score(profile_data)
    tier = assign_risk_tier(score)

    return {
        "manufacturer_name": orig_name,
        "normalized_name": norm_name,
        "total_recalls": total_recalls,
        "total_units_recalled": total_units,
        "recall_years": recall_years,
        "recall_hazard_types": recall_hazards[:500],
        "recall_product_types": recall_products[:500],
        "total_incidents": total_incidents,
        "incident_severities": incident_severities[:200],
        "incident_product_categories": incident_categories[:500],
        "total_neiss_injuries": 0,
        "total_neiss_weighted": 0.0,
        "neiss_product_codes": "",
        "total_penalties": total_penalties,
        "total_fines": total_fines,
        "penalty_types": penalty_types,
        "total_import_violations": total_violations,
        "violation_types": violation_types[:500],
        "violation_countries": violation_countries[:200],
        "compliance_score": score,
        "risk_tier": tier,
        "first_seen_date": dates_clean[0] if dates_clean else "",
        "last_seen_date": dates_clean[-1] if dates_clean else "",
        "data_sources": " | ".join(sources),
    }
