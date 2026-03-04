"""Tests for database schema and operations."""

import sqlite3
import pytest
from src.storage.database import (
    init_db, get_connection, upsert_recall, upsert_recalls_batch,
    upsert_incident, upsert_incidents_batch,
    insert_neiss_batch, upsert_penalties_batch,
    insert_import_violations_batch, upsert_profiles_batch,
    insert_cross_links_batch, get_stats,
)


@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    init_db(conn=conn)
    yield conn
    conn.close()


def _make_recall(**overrides):
    base = {
        "recall_id": "R1", "recall_number": "24-001",
        "recall_date": "2024-01-15", "title": "Test Recall",
        "description": "Test description", "consumer_contact": "",
        "url": "", "last_publish_date": "",
        "product_names": "Widget", "product_types": "Toy",
        "product_categories": "123", "number_of_units": "1000",
        "hazard_description": "Choking", "hazard_types": "Choking",
        "remedy_description": "Refund", "remedy_options": "Refund",
        "manufacturer_names": "ACME Corp", "manufacturer_countries": "China",
        "retailer_names": "Walmart", "importer_names": "",
        "distributor_names": "", "image_urls": "",
        "normalized_manufacturer": "ACME", "fiscal_year": 2024,
        "units_numeric": 1000, "quality_score": 0.95,
    }
    base.update(overrides)
    return base


def _make_incident(**overrides):
    base = {
        "report_number": "INC001", "report_date": "2024-03-01",
        "publication_date": "2024-03-15", "submitter_category": "Consumer",
        "product_description": "Widget", "product_category": "Toys",
        "product_subcategory": "", "product_type": "",
        "product_code": "1234", "manufacturer_name": "ACME Corp",
        "brand_name": "Acme", "model_name": "W100",
        "serial_number": "", "upc": "", "date_manufactured": "",
        "retailer_name": "", "retailer_state": "",
        "purchase_date": "", "incident_description": "Broke apart",
        "city": "Denver", "state": "CO", "zip_code": "80202",
        "location": "Home", "severity": "Injury",
        "victim_sex": "Female", "victim_age": "3",
        "company_comments": "", "associated_reports": "",
        "normalized_manufacturer": "ACME", "fiscal_year": 2024,
        "quality_score": 0.85,
    }
    base.update(overrides)
    return base


class TestInitDb:
    def test_creates_all_tables(self, db):
        tables = [r[0] for r in db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()]
        assert "recalls" in tables
        assert "incidents" in tables
        assert "neiss_injuries" in tables
        assert "penalties" in tables
        assert "import_violations" in tables
        assert "manufacturer_profiles" in tables
        assert "cross_links" in tables
        assert "neiss_product_codes" in tables

    def test_idempotent(self, db):
        init_db(conn=db)
        init_db(conn=db)
        count = db.execute("SELECT COUNT(*) FROM recalls").fetchone()[0]
        assert count == 0


class TestUpsertRecall:
    def test_insert(self, db):
        upsert_recall(_make_recall(), conn=db)
        count = db.execute("SELECT COUNT(*) FROM recalls").fetchone()[0]
        assert count == 1

    def test_upsert_updates(self, db):
        upsert_recall(_make_recall(), conn=db)
        upsert_recall(_make_recall(title="Updated"), conn=db)
        row = db.execute("SELECT title FROM recalls").fetchone()
        assert row[0] == "Updated"

    def test_batch_insert(self, db):
        records = [_make_recall(recall_id=f"R{i}") for i in range(10)]
        upsert_recalls_batch(records, conn=db)
        count = db.execute("SELECT COUNT(*) FROM recalls").fetchone()[0]
        assert count == 10


class TestUpsertIncident:
    def test_insert(self, db):
        upsert_incident(_make_incident(), conn=db)
        count = db.execute("SELECT COUNT(*) FROM incidents").fetchone()[0]
        assert count == 1

    def test_batch_insert(self, db):
        records = [_make_incident(report_number=f"INC{i:03d}") for i in range(20)]
        upsert_incidents_batch(records, conn=db)
        count = db.execute("SELECT COUNT(*) FROM incidents").fetchone()[0]
        assert count == 20


class TestNeiss:
    def test_batch_insert(self, db):
        records = [{
            "cpsc_case_number": f"CASE{i}",
            "treatment_date": "2023-06-15",
            "age": 25, "sex": "Male", "race": "", "hispanic": "",
            "body_part": 35, "body_part_name": "Hand",
            "diagnosis": 61, "diagnosis_name": "Laceration",
            "body_part_2": None, "diagnosis_2": None,
            "disposition": 1, "disposition_name": "Treated/Released",
            "location": 1, "location_name": "Home",
            "fire_involvement": 0,
            "product_1": 1234, "product_1_name": "Knives",
            "product_2": None, "product_3": None,
            "alcohol": 0, "drug": 0,
            "narrative": "Cut hand with knife",
            "stratum": "S", "psu": "1", "weight": 45.2,
            "neiss_year": 2023, "quality_score": 0.9,
        } for i in range(5)]
        insert_neiss_batch(records, conn=db)
        count = db.execute("SELECT COUNT(*) FROM neiss_injuries").fetchone()[0]
        assert count == 5


class TestPenalties:
    def test_batch_insert(self, db):
        records = [{
            "penalty_id": f"PEN{i}",
            "recall_number": f"24-{i:03d}",
            "firm_name": "Test Corp",
            "penalty_type": "civil",
            "penalty_date": "2024-01-01",
            "act": "CPSA",
            "fine_amount": 100000.0,
            "fiscal_year": 2024,
            "release_title": "Test",
            "release_url": "",
            "company_id": "C1",
            "product_types": "Toy",
            "normalized_firm": "TEST",
            "quality_score": 0.8,
        } for i in range(3)]
        upsert_penalties_batch(records, conn=db)
        count = db.execute("SELECT COUNT(*) FROM penalties").fetchone()[0]
        assert count == 3


class TestGetStats:
    def test_empty_db(self, db):
        stats = get_stats(conn=db)
        assert stats["recalls"] == 0
        assert stats["incidents"] == 0

    def test_with_data(self, db):
        upsert_recalls_batch([_make_recall(recall_id=f"R{i}") for i in range(5)], conn=db)
        stats = get_stats(conn=db)
        assert stats["recalls"] == 5
