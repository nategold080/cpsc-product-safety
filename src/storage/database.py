"""SQLite database schema and operations for CPSC Product Safety Tracker."""

import sqlite3
import os
from pathlib import Path

DB_DIR = Path(__file__).parent.parent.parent / "data"
DB_PATH = DB_DIR / "cpsc_product_safety.db"

SCHEMA = """
-- Recall campaigns from CPSC Recalls API
CREATE TABLE IF NOT EXISTS recalls (
    recall_id TEXT PRIMARY KEY,
    recall_number TEXT NOT NULL,
    recall_date TEXT,
    title TEXT,
    description TEXT,
    consumer_contact TEXT,
    url TEXT,
    last_publish_date TEXT,
    -- Aggregated from nested arrays
    product_names TEXT,
    product_types TEXT,
    product_categories TEXT,
    number_of_units TEXT,
    hazard_description TEXT,
    hazard_types TEXT,
    remedy_description TEXT,
    remedy_options TEXT,
    manufacturer_names TEXT,
    manufacturer_countries TEXT,
    retailer_names TEXT,
    importer_names TEXT,
    distributor_names TEXT,
    image_urls TEXT,
    -- Normalized fields
    normalized_manufacturer TEXT,
    fiscal_year INTEGER,
    units_numeric INTEGER,
    quality_score REAL DEFAULT 0.0,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Incident/complaint reports from SaferProducts.gov bulk export
CREATE TABLE IF NOT EXISTS incidents (
    report_number TEXT PRIMARY KEY,
    report_date TEXT,
    publication_date TEXT,
    submitter_category TEXT,
    product_description TEXT,
    product_category TEXT,
    product_subcategory TEXT,
    product_type TEXT,
    product_code TEXT,
    manufacturer_name TEXT,
    brand_name TEXT,
    model_name TEXT,
    serial_number TEXT,
    upc TEXT,
    date_manufactured TEXT,
    retailer_name TEXT,
    retailer_state TEXT,
    purchase_date TEXT,
    incident_description TEXT,
    city TEXT,
    state TEXT,
    zip_code TEXT,
    location TEXT,
    severity TEXT,
    victim_sex TEXT,
    victim_age TEXT,
    company_comments TEXT,
    associated_reports TEXT,
    -- Normalized fields
    normalized_manufacturer TEXT,
    fiscal_year INTEGER,
    quality_score REAL DEFAULT 0.0,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- NEISS emergency room injury data
CREATE TABLE IF NOT EXISTS neiss_injuries (
    injury_id INTEGER PRIMARY KEY AUTOINCREMENT,
    cpsc_case_number TEXT NOT NULL,
    treatment_date TEXT,
    age INTEGER,
    sex TEXT,
    race TEXT,
    hispanic TEXT,
    body_part INTEGER,
    body_part_name TEXT,
    diagnosis INTEGER,
    diagnosis_name TEXT,
    body_part_2 INTEGER,
    diagnosis_2 INTEGER,
    disposition INTEGER,
    disposition_name TEXT,
    location INTEGER,
    location_name TEXT,
    fire_involvement INTEGER,
    product_1 INTEGER,
    product_1_name TEXT,
    product_2 INTEGER,
    product_3 INTEGER,
    alcohol INTEGER,
    drug INTEGER,
    narrative TEXT,
    stratum TEXT,
    psu TEXT,
    weight REAL,
    neiss_year INTEGER,
    quality_score REAL DEFAULT 0.0,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(cpsc_case_number, treatment_date, product_1)
);

-- Civil and criminal penalties from CPSC Penalty API
CREATE TABLE IF NOT EXISTS penalties (
    penalty_id TEXT PRIMARY KEY,
    recall_number TEXT,
    firm_name TEXT,
    penalty_type TEXT,
    penalty_date TEXT,
    act TEXT,
    fine_amount REAL,
    fiscal_year INTEGER,
    release_title TEXT,
    release_url TEXT,
    company_id TEXT,
    product_types TEXT,
    -- Normalized fields
    normalized_firm TEXT,
    quality_score REAL DEFAULT 0.0,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Import violations (Notice of Violation) data
CREATE TABLE IF NOT EXISTS import_violations (
    violation_id INTEGER PRIMARY KEY AUTOINCREMENT,
    nov_date TEXT,
    product_name TEXT,
    model_number TEXT,
    sample_number TEXT,
    domestic_action TEXT,
    cbp_action TEXT,
    violation_type TEXT,
    citation TEXT,
    firm_name TEXT,
    firm_address TEXT,
    firm_city TEXT,
    country TEXT,
    -- Normalized fields
    normalized_firm TEXT,
    fiscal_year INTEGER,
    quality_score REAL DEFAULT 0.0,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(nov_date, sample_number, firm_name)
);

-- Manufacturer profiles (cross-linked aggregation)
CREATE TABLE IF NOT EXISTS manufacturer_profiles (
    profile_id INTEGER PRIMARY KEY AUTOINCREMENT,
    manufacturer_name TEXT NOT NULL UNIQUE,
    normalized_name TEXT NOT NULL,
    -- Recall aggregates
    total_recalls INTEGER DEFAULT 0,
    total_units_recalled INTEGER DEFAULT 0,
    recall_years TEXT,
    recall_hazard_types TEXT,
    recall_product_types TEXT,
    -- Incident aggregates
    total_incidents INTEGER DEFAULT 0,
    incident_severities TEXT,
    incident_product_categories TEXT,
    -- NEISS injury aggregates
    total_neiss_injuries INTEGER DEFAULT 0,
    total_neiss_weighted REAL DEFAULT 0.0,
    neiss_product_codes TEXT,
    -- Penalty aggregates
    total_penalties INTEGER DEFAULT 0,
    total_fines REAL DEFAULT 0.0,
    penalty_types TEXT,
    -- Import violation aggregates
    total_import_violations INTEGER DEFAULT 0,
    violation_types TEXT,
    violation_countries TEXT,
    -- Compliance scoring
    compliance_score REAL DEFAULT 0.0,
    risk_tier TEXT,
    -- Metadata
    first_seen_date TEXT,
    last_seen_date TEXT,
    data_sources TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Cross-links between tables
CREATE TABLE IF NOT EXISTS cross_links (
    link_id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_table TEXT NOT NULL,
    source_id TEXT NOT NULL,
    target_table TEXT NOT NULL,
    target_id TEXT NOT NULL,
    link_type TEXT NOT NULL,
    confidence REAL DEFAULT 0.9,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source_table, source_id, target_table, target_id, link_type)
);

-- FDA adverse events (consumer product subset from OpenFDA)
CREATE TABLE IF NOT EXISTS fda_adverse_events (
    event_id TEXT PRIMARY KEY,
    report_date TEXT,
    product_name TEXT,
    product_type TEXT,
    manufacturer_name TEXT,
    manufacturer_normalized TEXT,
    event_type TEXT,
    patient_outcome TEXT,
    description TEXT,
    source TEXT,
    quality_score REAL DEFAULT 0.0,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- FDA device recalls
CREATE TABLE IF NOT EXISTS fda_device_recalls (
    recall_id TEXT PRIMARY KEY,
    product_description TEXT,
    reason_for_recall TEXT,
    manufacturer_name TEXT,
    manufacturer_normalized TEXT,
    recall_class TEXT,
    recall_status TEXT,
    event_date TEXT,
    quality_score REAL DEFAULT 0.0,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Cross-agency manufacturer links (CPSC ↔ FDA)
CREATE TABLE IF NOT EXISTS cpsc_fda_manufacturer_links (
    cpsc_manufacturer TEXT NOT NULL,
    fda_manufacturer TEXT NOT NULL,
    link_method TEXT,
    confidence REAL DEFAULT 0.9,
    PRIMARY KEY (cpsc_manufacturer, fda_manufacturer)
);

-- Hazard-to-diagnosis mapping reference table
CREATE TABLE IF NOT EXISTS hazard_diagnosis_map (
    hazard_type TEXT NOT NULL,
    neiss_diagnosis_code INTEGER NOT NULL,
    diagnosis_name TEXT,
    PRIMARY KEY (hazard_type, neiss_diagnosis_code)
);

-- Hazard validation results per manufacturer
CREATE TABLE IF NOT EXISTS hazard_validation_results (
    manufacturer_normalized TEXT NOT NULL,
    hazard_type TEXT NOT NULL,
    total_recalls_with_hazard INTEGER DEFAULT 0,
    total_neiss_injuries INTEGER DEFAULT 0,
    matching_diagnosis_count INTEGER DEFAULT 0,
    unexpected_diagnosis_count INTEGER DEFAULT 0,
    match_rate REAL DEFAULT 0.0,
    validation_status TEXT,
    PRIMARY KEY (manufacturer_normalized, hazard_type)
);

CREATE INDEX IF NOT EXISTS idx_validation_mfr ON hazard_validation_results(manufacturer_normalized);
CREATE INDEX IF NOT EXISTS idx_validation_status ON hazard_validation_results(validation_status);

CREATE INDEX IF NOT EXISTS idx_fda_events_mfr ON fda_adverse_events(manufacturer_normalized);
CREATE INDEX IF NOT EXISTS idx_fda_events_type ON fda_adverse_events(product_type);
CREATE INDEX IF NOT EXISTS idx_fda_events_date ON fda_adverse_events(report_date);
CREATE INDEX IF NOT EXISTS idx_fda_events_event_type ON fda_adverse_events(event_type);
CREATE INDEX IF NOT EXISTS idx_fda_recalls_mfr ON fda_device_recalls(manufacturer_normalized);
CREATE INDEX IF NOT EXISTS idx_fda_recalls_class ON fda_device_recalls(recall_class);
CREATE INDEX IF NOT EXISTS idx_fda_recalls_date ON fda_device_recalls(event_date);

-- NEISS product code reference (expanded for enrichment)
CREATE TABLE IF NOT EXISTS neiss_product_codes (
    product_code INTEGER PRIMARY KEY,
    product_name TEXT NOT NULL,
    category TEXT,
    is_deleted INTEGER DEFAULT 0,
    is_child_related INTEGER DEFAULT 0,
    is_outdoor INTEGER DEFAULT 0
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_recalls_number ON recalls(recall_number);
CREATE INDEX IF NOT EXISTS idx_recalls_date ON recalls(recall_date);
CREATE INDEX IF NOT EXISTS idx_recalls_manufacturer ON recalls(normalized_manufacturer);
CREATE INDEX IF NOT EXISTS idx_recalls_fiscal_year ON recalls(fiscal_year);

CREATE INDEX IF NOT EXISTS idx_incidents_date ON incidents(report_date);
CREATE INDEX IF NOT EXISTS idx_incidents_manufacturer ON incidents(normalized_manufacturer);
CREATE INDEX IF NOT EXISTS idx_incidents_state ON incidents(state);
CREATE INDEX IF NOT EXISTS idx_incidents_product_code ON incidents(product_code);
CREATE INDEX IF NOT EXISTS idx_incidents_category ON incidents(product_category);

CREATE INDEX IF NOT EXISTS idx_neiss_year ON neiss_injuries(neiss_year);
CREATE INDEX IF NOT EXISTS idx_neiss_product1 ON neiss_injuries(product_1);
CREATE INDEX IF NOT EXISTS idx_neiss_disposition ON neiss_injuries(disposition);
CREATE INDEX IF NOT EXISTS idx_neiss_case ON neiss_injuries(cpsc_case_number);

CREATE INDEX IF NOT EXISTS idx_penalties_firm ON penalties(normalized_firm);
CREATE INDEX IF NOT EXISTS idx_penalties_recall ON penalties(recall_number);
CREATE INDEX IF NOT EXISTS idx_penalties_type ON penalties(penalty_type);

CREATE INDEX IF NOT EXISTS idx_violations_firm ON import_violations(normalized_firm);
CREATE INDEX IF NOT EXISTS idx_violations_country ON import_violations(country);
CREATE INDEX IF NOT EXISTS idx_violations_type ON import_violations(violation_type);

CREATE INDEX IF NOT EXISTS idx_profiles_normalized ON manufacturer_profiles(normalized_name);
CREATE INDEX IF NOT EXISTS idx_profiles_compliance ON manufacturer_profiles(compliance_score);
CREATE INDEX IF NOT EXISTS idx_profiles_risk ON manufacturer_profiles(risk_tier);

CREATE INDEX IF NOT EXISTS idx_cross_links_source ON cross_links(source_table, source_id);
CREATE INDEX IF NOT EXISTS idx_cross_links_target ON cross_links(target_table, target_id);
CREATE INDEX IF NOT EXISTS idx_cross_links_type ON cross_links(link_type);
"""


def get_connection(db_path=None, conn=None):
    """Get or create a database connection."""
    if conn is not None:
        return conn, False
    path = db_path or DB_PATH
    os.makedirs(os.path.dirname(path), exist_ok=True)
    c = sqlite3.connect(str(path))
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA foreign_keys=ON")
    c.execute("PRAGMA busy_timeout=5000")
    return c, True


def init_db(db_path=None, conn=None):
    """Initialize the database schema."""
    c, owned = get_connection(db_path, conn)
    try:
        c.executescript(SCHEMA)
        c.commit()
        return c if not owned else None
    finally:
        if owned:
            c.close()


def upsert_recall(record: dict, db_path=None, conn=None):
    """Insert or update a recall record."""
    c, owned = get_connection(db_path, conn)
    try:
        c.execute("""
            INSERT INTO recalls (
                recall_id, recall_number, recall_date, title, description,
                consumer_contact, url, last_publish_date,
                product_names, product_types, product_categories, number_of_units,
                hazard_description, hazard_types, remedy_description, remedy_options,
                manufacturer_names, manufacturer_countries,
                retailer_names, importer_names, distributor_names, image_urls,
                normalized_manufacturer, fiscal_year, units_numeric, quality_score
            ) VALUES (
                :recall_id, :recall_number, :recall_date, :title, :description,
                :consumer_contact, :url, :last_publish_date,
                :product_names, :product_types, :product_categories, :number_of_units,
                :hazard_description, :hazard_types, :remedy_description, :remedy_options,
                :manufacturer_names, :manufacturer_countries,
                :retailer_names, :importer_names, :distributor_names, :image_urls,
                :normalized_manufacturer, :fiscal_year, :units_numeric, :quality_score
            )
            ON CONFLICT(recall_id) DO UPDATE SET
                recall_date = excluded.recall_date,
                title = excluded.title,
                description = excluded.description,
                product_names = excluded.product_names,
                hazard_description = excluded.hazard_description,
                manufacturer_names = excluded.manufacturer_names,
                normalized_manufacturer = excluded.normalized_manufacturer,
                units_numeric = excluded.units_numeric,
                quality_score = excluded.quality_score
        """, record)
        if owned:
            c.commit()
    finally:
        if owned:
            c.close()


def upsert_recalls_batch(records: list, db_path=None, conn=None):
    """Batch insert/update recall records."""
    c, owned = get_connection(db_path, conn)
    try:
        c.executemany("""
            INSERT INTO recalls (
                recall_id, recall_number, recall_date, title, description,
                consumer_contact, url, last_publish_date,
                product_names, product_types, product_categories, number_of_units,
                hazard_description, hazard_types, remedy_description, remedy_options,
                manufacturer_names, manufacturer_countries,
                retailer_names, importer_names, distributor_names, image_urls,
                normalized_manufacturer, fiscal_year, units_numeric, quality_score
            ) VALUES (
                :recall_id, :recall_number, :recall_date, :title, :description,
                :consumer_contact, :url, :last_publish_date,
                :product_names, :product_types, :product_categories, :number_of_units,
                :hazard_description, :hazard_types, :remedy_description, :remedy_options,
                :manufacturer_names, :manufacturer_countries,
                :retailer_names, :importer_names, :distributor_names, :image_urls,
                :normalized_manufacturer, :fiscal_year, :units_numeric, :quality_score
            )
            ON CONFLICT(recall_id) DO UPDATE SET
                recall_date = excluded.recall_date,
                title = excluded.title,
                description = excluded.description,
                product_names = excluded.product_names,
                hazard_description = excluded.hazard_description,
                manufacturer_names = excluded.manufacturer_names,
                normalized_manufacturer = excluded.normalized_manufacturer,
                units_numeric = excluded.units_numeric,
                quality_score = excluded.quality_score
        """, records)
        if owned:
            c.commit()
    finally:
        if owned:
            c.close()


def upsert_incident(record: dict, db_path=None, conn=None):
    """Insert or update an incident report."""
    c, owned = get_connection(db_path, conn)
    try:
        c.execute("""
            INSERT INTO incidents (
                report_number, report_date, publication_date, submitter_category,
                product_description, product_category, product_subcategory,
                product_type, product_code,
                manufacturer_name, brand_name, model_name, serial_number, upc,
                date_manufactured, retailer_name, retailer_state, purchase_date,
                incident_description, city, state, zip_code, location,
                severity, victim_sex, victim_age, company_comments,
                associated_reports, normalized_manufacturer, fiscal_year, quality_score
            ) VALUES (
                :report_number, :report_date, :publication_date, :submitter_category,
                :product_description, :product_category, :product_subcategory,
                :product_type, :product_code,
                :manufacturer_name, :brand_name, :model_name, :serial_number, :upc,
                :date_manufactured, :retailer_name, :retailer_state, :purchase_date,
                :incident_description, :city, :state, :zip_code, :location,
                :severity, :victim_sex, :victim_age, :company_comments,
                :associated_reports, :normalized_manufacturer, :fiscal_year, :quality_score
            )
            ON CONFLICT(report_number) DO UPDATE SET
                product_description = excluded.product_description,
                manufacturer_name = excluded.manufacturer_name,
                normalized_manufacturer = excluded.normalized_manufacturer,
                quality_score = excluded.quality_score
        """, record)
        if owned:
            c.commit()
    finally:
        if owned:
            c.close()


def upsert_incidents_batch(records: list, db_path=None, conn=None):
    """Batch insert/update incident records."""
    c, owned = get_connection(db_path, conn)
    try:
        c.executemany("""
            INSERT INTO incidents (
                report_number, report_date, publication_date, submitter_category,
                product_description, product_category, product_subcategory,
                product_type, product_code,
                manufacturer_name, brand_name, model_name, serial_number, upc,
                date_manufactured, retailer_name, retailer_state, purchase_date,
                incident_description, city, state, zip_code, location,
                severity, victim_sex, victim_age, company_comments,
                associated_reports, normalized_manufacturer, fiscal_year, quality_score
            ) VALUES (
                :report_number, :report_date, :publication_date, :submitter_category,
                :product_description, :product_category, :product_subcategory,
                :product_type, :product_code,
                :manufacturer_name, :brand_name, :model_name, :serial_number, :upc,
                :date_manufactured, :retailer_name, :retailer_state, :purchase_date,
                :incident_description, :city, :state, :zip_code, :location,
                :severity, :victim_sex, :victim_age, :company_comments,
                :associated_reports, :normalized_manufacturer, :fiscal_year, :quality_score
            )
            ON CONFLICT(report_number) DO UPDATE SET
                product_description = excluded.product_description,
                manufacturer_name = excluded.manufacturer_name,
                normalized_manufacturer = excluded.normalized_manufacturer,
                quality_score = excluded.quality_score
        """, records)
        if owned:
            c.commit()
    finally:
        if owned:
            c.close()


def insert_neiss_batch(records: list, db_path=None, conn=None):
    """Batch insert NEISS injury records."""
    c, owned = get_connection(db_path, conn)
    try:
        c.executemany("""
            INSERT OR IGNORE INTO neiss_injuries (
                cpsc_case_number, treatment_date, age, sex, race, hispanic,
                body_part, body_part_name, diagnosis, diagnosis_name,
                body_part_2, diagnosis_2, disposition, disposition_name,
                location, location_name, fire_involvement,
                product_1, product_1_name, product_2, product_3,
                alcohol, drug, narrative, stratum, psu, weight,
                neiss_year, quality_score
            ) VALUES (
                :cpsc_case_number, :treatment_date, :age, :sex, :race, :hispanic,
                :body_part, :body_part_name, :diagnosis, :diagnosis_name,
                :body_part_2, :diagnosis_2, :disposition, :disposition_name,
                :location, :location_name, :fire_involvement,
                :product_1, :product_1_name, :product_2, :product_3,
                :alcohol, :drug, :narrative, :stratum, :psu, :weight,
                :neiss_year, :quality_score
            )
        """, records)
        if owned:
            c.commit()
    finally:
        if owned:
            c.close()


def upsert_penalties_batch(records: list, db_path=None, conn=None):
    """Batch insert/update penalty records."""
    c, owned = get_connection(db_path, conn)
    try:
        c.executemany("""
            INSERT INTO penalties (
                penalty_id, recall_number, firm_name, penalty_type,
                penalty_date, act, fine_amount, fiscal_year,
                release_title, release_url, company_id, product_types,
                normalized_firm, quality_score
            ) VALUES (
                :penalty_id, :recall_number, :firm_name, :penalty_type,
                :penalty_date, :act, :fine_amount, :fiscal_year,
                :release_title, :release_url, :company_id, :product_types,
                :normalized_firm, :quality_score
            )
            ON CONFLICT(penalty_id) DO UPDATE SET
                fine_amount = excluded.fine_amount,
                normalized_firm = excluded.normalized_firm,
                quality_score = excluded.quality_score
        """, records)
        if owned:
            c.commit()
    finally:
        if owned:
            c.close()


def insert_import_violations_batch(records: list, db_path=None, conn=None):
    """Batch insert import violation records."""
    c, owned = get_connection(db_path, conn)
    try:
        c.executemany("""
            INSERT OR IGNORE INTO import_violations (
                nov_date, product_name, model_number, sample_number,
                domestic_action, cbp_action, violation_type, citation,
                firm_name, firm_address, firm_city, country,
                normalized_firm, fiscal_year, quality_score
            ) VALUES (
                :nov_date, :product_name, :model_number, :sample_number,
                :domestic_action, :cbp_action, :violation_type, :citation,
                :firm_name, :firm_address, :firm_city, :country,
                :normalized_firm, :fiscal_year, :quality_score
            )
        """, records)
        if owned:
            c.commit()
    finally:
        if owned:
            c.close()


def upsert_profiles_batch(records: list, db_path=None, conn=None):
    """Batch insert/update manufacturer profiles."""
    c, owned = get_connection(db_path, conn)
    try:
        c.executemany("""
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
            ON CONFLICT(manufacturer_name) DO UPDATE SET
                total_recalls = excluded.total_recalls,
                total_units_recalled = excluded.total_units_recalled,
                total_incidents = excluded.total_incidents,
                total_neiss_injuries = excluded.total_neiss_injuries,
                total_penalties = excluded.total_penalties,
                total_fines = excluded.total_fines,
                total_import_violations = excluded.total_import_violations,
                compliance_score = excluded.compliance_score,
                risk_tier = excluded.risk_tier,
                last_seen_date = excluded.last_seen_date,
                data_sources = excluded.data_sources
        """, records)
        if owned:
            c.commit()
    finally:
        if owned:
            c.close()


def insert_cross_links_batch(records: list, db_path=None, conn=None):
    """Batch insert cross-links."""
    c, owned = get_connection(db_path, conn)
    try:
        c.executemany("""
            INSERT OR IGNORE INTO cross_links (
                source_table, source_id, target_table, target_id,
                link_type, confidence
            ) VALUES (
                :source_table, :source_id, :target_table, :target_id,
                :link_type, :confidence
            )
        """, records)
        if owned:
            c.commit()
    finally:
        if owned:
            c.close()


def upsert_fda_events_batch(records: list, db_path=None, conn=None):
    """Batch insert/update FDA adverse event records."""
    c, owned = get_connection(db_path, conn)
    try:
        c.executemany("""
            INSERT INTO fda_adverse_events (
                event_id, report_date, product_name, product_type,
                manufacturer_name, manufacturer_normalized,
                event_type, patient_outcome, description, source,
                quality_score
            ) VALUES (
                :event_id, :report_date, :product_name, :product_type,
                :manufacturer_name, :manufacturer_normalized,
                :event_type, :patient_outcome, :description, :source,
                :quality_score
            )
            ON CONFLICT(event_id) DO UPDATE SET
                product_name = excluded.product_name,
                manufacturer_normalized = excluded.manufacturer_normalized,
                quality_score = excluded.quality_score
        """, records)
        if owned:
            c.commit()
    finally:
        if owned:
            c.close()


def upsert_fda_recalls_batch(records: list, db_path=None, conn=None):
    """Batch insert/update FDA device recall records."""
    c, owned = get_connection(db_path, conn)
    try:
        c.executemany("""
            INSERT INTO fda_device_recalls (
                recall_id, product_description, reason_for_recall,
                manufacturer_name, manufacturer_normalized,
                recall_class, recall_status, event_date, quality_score
            ) VALUES (
                :recall_id, :product_description, :reason_for_recall,
                :manufacturer_name, :manufacturer_normalized,
                :recall_class, :recall_status, :event_date, :quality_score
            )
            ON CONFLICT(recall_id) DO UPDATE SET
                product_description = excluded.product_description,
                manufacturer_normalized = excluded.manufacturer_normalized,
                quality_score = excluded.quality_score
        """, records)
        if owned:
            c.commit()
    finally:
        if owned:
            c.close()


def insert_cpsc_fda_links_batch(records: list, db_path=None, conn=None):
    """Batch insert CPSC ↔ FDA manufacturer links."""
    c, owned = get_connection(db_path, conn)
    try:
        c.executemany("""
            INSERT OR IGNORE INTO cpsc_fda_manufacturer_links
            (cpsc_manufacturer, fda_manufacturer, link_method, confidence)
            VALUES (:cpsc_manufacturer, :fda_manufacturer, :link_method, :confidence)
        """, records)
        if owned:
            c.commit()
    finally:
        if owned:
            c.close()


def insert_product_codes_batch(records: list, db_path=None, conn=None):
    """Batch insert NEISS product code reference data."""
    c, owned = get_connection(db_path, conn)
    try:
        c.executemany("""
            INSERT OR REPLACE INTO neiss_product_codes
            (product_code, product_name, category, is_deleted, is_child_related, is_outdoor)
            VALUES (:product_code, :product_name, :category,
                    :is_deleted, :is_child_related, :is_outdoor)
        """, records)
        if owned:
            c.commit()
    finally:
        if owned:
            c.close()


def load_product_codes_from_yaml(yaml_path: str, db_path=None, conn=None) -> int:
    """Load NEISS product codes from YAML config into the database. Returns count loaded."""
    import yaml

    with open(yaml_path) as f:
        data = yaml.safe_load(f)

    codes = data.get("neiss_product_codes", {})
    records = []
    for code, info in codes.items():
        records.append({
            "product_code": int(code),
            "product_name": info["product_name"],
            "category": info.get("category", ""),
            "is_deleted": 1 if info.get("is_deleted", False) else 0,
            "is_child_related": 1 if info.get("is_child_related", False) else 0,
            "is_outdoor": 1 if info.get("is_outdoor", False) else 0,
        })

    insert_product_codes_batch(records, db_path=db_path, conn=conn)
    return len(records)


def update_neiss_product_names(db_path=None, conn=None) -> int:
    """Update NEISS injury records with resolved product names from product code table.
    Returns count of records updated."""
    c, owned = get_connection(db_path, conn)
    try:
        result = c.execute("""
            UPDATE neiss_injuries
            SET product_1_name = (
                SELECT product_name FROM neiss_product_codes
                WHERE neiss_product_codes.product_code = neiss_injuries.product_1
            )
            WHERE product_1 IS NOT NULL
              AND EXISTS (
                SELECT 1 FROM neiss_product_codes
                WHERE neiss_product_codes.product_code = neiss_injuries.product_1
              )
        """)
        updated = result.rowcount
        if owned:
            c.commit()
        return updated
    finally:
        if owned:
            c.close()


def get_product_code_name(code: int, db_path=None, conn=None) -> str:
    """Look up a NEISS product code name. Returns empty string if not found."""
    c, owned = get_connection(db_path, conn)
    try:
        row = c.execute(
            "SELECT product_name FROM neiss_product_codes WHERE product_code = ?",
            (code,)
        ).fetchone()
        return row[0] if row else ""
    finally:
        if owned:
            c.close()


def get_stats(db_path=None, conn=None):
    """Get database statistics."""
    c, owned = get_connection(db_path, conn)
    try:
        stats = {}
        for table in ["recalls", "incidents", "neiss_injuries", "penalties",
                       "import_violations", "manufacturer_profiles", "cross_links",
                       "neiss_product_codes", "fda_adverse_events",
                       "fda_device_recalls", "cpsc_fda_manufacturer_links"]:
            try:
                row = c.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                stats[table] = row[0]
            except sqlite3.OperationalError:
                stats[table] = 0

        # Additional stats
        try:
            row = c.execute("SELECT COUNT(DISTINCT normalized_manufacturer) FROM recalls WHERE normalized_manufacturer IS NOT NULL").fetchone()
            stats["unique_recall_manufacturers"] = row[0]
        except sqlite3.OperationalError:
            stats["unique_recall_manufacturers"] = 0

        try:
            row = c.execute("SELECT SUM(units_numeric) FROM recalls WHERE units_numeric IS NOT NULL").fetchone()
            stats["total_units_recalled"] = row[0] or 0
        except sqlite3.OperationalError:
            stats["total_units_recalled"] = 0

        try:
            row = c.execute("SELECT SUM(fine_amount) FROM penalties WHERE fine_amount IS NOT NULL").fetchone()
            stats["total_fines"] = row[0] or 0.0
        except sqlite3.OperationalError:
            stats["total_fines"] = 0.0

        try:
            row = c.execute("SELECT COUNT(DISTINCT state) FROM incidents WHERE state IS NOT NULL AND state != ''").fetchone()
            stats["incident_states"] = row[0]
        except sqlite3.OperationalError:
            stats["incident_states"] = 0

        try:
            row = c.execute("SELECT MIN(recall_date), MAX(recall_date) FROM recalls WHERE recall_date IS NOT NULL").fetchone()
            stats["recall_date_range"] = f"{row[0]} to {row[1]}" if row[0] else "N/A"
        except sqlite3.OperationalError:
            stats["recall_date_range"] = "N/A"

        try:
            row = c.execute("SELECT SUM(weight) FROM neiss_injuries").fetchone()
            stats["neiss_national_estimate"] = row[0] or 0.0
        except sqlite3.OperationalError:
            stats["neiss_national_estimate"] = 0.0

        try:
            row = c.execute(
                "SELECT COUNT(*) FROM neiss_injuries WHERE product_1_name IS NOT NULL AND product_1_name != ''"
            ).fetchone()
            stats["neiss_with_product_name"] = row[0]
        except sqlite3.OperationalError:
            stats["neiss_with_product_name"] = 0

        try:
            row = c.execute("SELECT COUNT(DISTINCT category) FROM neiss_product_codes WHERE category != ''").fetchone()
            stats["product_categories"] = row[0]
        except sqlite3.OperationalError:
            stats["product_categories"] = 0

        return stats
    finally:
        if owned:
            c.close()
