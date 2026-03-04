"""Tests for cross-linking engine."""

import sqlite3
import pytest
from src.storage.database import init_db, upsert_recalls_batch, upsert_incidents_batch
from src.storage.database import upsert_penalties_batch, insert_import_violations_batch
from src.normalization.cross_linker import build_cross_links, build_manufacturer_profiles
from src.validation.quality import compute_compliance_score, assign_risk_tier


@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    init_db(conn=conn)
    yield conn
    conn.close()


def _make_recall(recall_id, norm_mfr, recall_number="24-001", **kw):
    base = {
        "recall_id": recall_id, "recall_number": recall_number,
        "recall_date": "2024-01-15", "title": "Test",
        "description": "", "consumer_contact": "",
        "url": "", "last_publish_date": "",
        "product_names": "Widget", "product_types": "Toy",
        "product_categories": "", "number_of_units": "1000",
        "hazard_description": "Choking", "hazard_types": "Choking",
        "remedy_description": "Refund", "remedy_options": "",
        "manufacturer_names": norm_mfr, "manufacturer_countries": "China",
        "retailer_names": "", "importer_names": "",
        "distributor_names": "", "image_urls": "",
        "normalized_manufacturer": norm_mfr, "fiscal_year": 2024,
        "units_numeric": 1000, "quality_score": 0.9,
    }
    base.update(kw)
    return base


def _make_incident(report_number, norm_mfr, **kw):
    base = {
        "report_number": report_number, "report_date": "2024-03-01",
        "publication_date": "", "submitter_category": "Consumer",
        "product_description": "Widget", "product_category": "Toys",
        "product_subcategory": "", "product_type": "",
        "product_code": "", "manufacturer_name": norm_mfr,
        "brand_name": "", "model_name": "", "serial_number": "",
        "upc": "", "date_manufactured": "",
        "retailer_name": "", "retailer_state": "",
        "purchase_date": "", "incident_description": "Broke",
        "city": "", "state": "CO", "zip_code": "",
        "location": "", "severity": "Injury",
        "victim_sex": "", "victim_age": "",
        "company_comments": "", "associated_reports": "",
        "normalized_manufacturer": norm_mfr, "fiscal_year": 2024,
        "quality_score": 0.7,
    }
    base.update(kw)
    return base


def _make_penalty(penalty_id, norm_firm, recall_number="", **kw):
    base = {
        "penalty_id": penalty_id, "recall_number": recall_number,
        "firm_name": norm_firm, "penalty_type": "civil",
        "penalty_date": "2024-01-01", "act": "CPSA",
        "fine_amount": 100000.0, "fiscal_year": 2024,
        "release_title": "Test", "release_url": "",
        "company_id": "", "product_types": "",
        "normalized_firm": norm_firm, "quality_score": 0.8,
    }
    base.update(kw)
    return base


class TestBuildCrossLinks:
    def test_recall_to_penalty_by_recall_number(self, db):
        upsert_recalls_batch([_make_recall("R1", "ACME", recall_number="24-001")], conn=db)
        upsert_penalties_batch([_make_penalty("P1", "OTHER", recall_number="24-001")], conn=db)
        db.commit()

        count = build_cross_links(db)
        assert count >= 1

        links = db.execute("SELECT * FROM cross_links WHERE link_type = 'recall_number'").fetchall()
        assert len(links) >= 1

    def test_recall_to_penalty_by_manufacturer(self, db):
        upsert_recalls_batch([_make_recall("R1", "ACME")], conn=db)
        upsert_penalties_batch([_make_penalty("P1", "ACME")], conn=db)
        db.commit()

        count = build_cross_links(db)
        assert count >= 1

    def test_recall_to_incident_by_manufacturer(self, db):
        upsert_recalls_batch([_make_recall("R1", "ACME")], conn=db)
        upsert_incidents_batch([_make_incident("INC1", "ACME")], conn=db)
        db.commit()

        count = build_cross_links(db)
        assert count >= 1

        links = db.execute(
            "SELECT * FROM cross_links WHERE source_table='recalls' AND target_table='incidents'"
        ).fetchall()
        assert len(links) >= 1

    def test_no_false_cross_links(self, db):
        upsert_recalls_batch([_make_recall("R1", "ACME")], conn=db)
        upsert_incidents_batch([_make_incident("INC1", "COMPLETELY DIFFERENT")], conn=db)
        db.commit()

        count = build_cross_links(db)
        # No links should be created between unrelated manufacturers
        links = db.execute(
            "SELECT * FROM cross_links WHERE source_table='recalls' AND target_table='incidents'"
        ).fetchall()
        assert len(links) == 0

    def test_violations_to_recalls(self, db):
        upsert_recalls_batch([_make_recall("R1", "ACME")], conn=db)
        insert_import_violations_batch([{
            "nov_date": "2024-01-01", "product_name": "Widget",
            "model_number": "", "sample_number": "S001",
            "domestic_action": "Recall", "cbp_action": "SEIZE",
            "violation_type": "Lead", "citation": "16 CFR",
            "firm_name": "ACME Corp", "firm_address": "",
            "firm_city": "", "country": "China",
            "normalized_firm": "ACME", "fiscal_year": 2024,
            "quality_score": 0.8,
        }], conn=db)
        db.commit()

        count = build_cross_links(db)
        links = db.execute(
            "SELECT * FROM cross_links WHERE source_table='import_violations'"
        ).fetchall()
        assert len(links) >= 1


class TestBuildManufacturerProfiles:
    def test_creates_profiles(self, db):
        upsert_recalls_batch([
            _make_recall("R1", "ACME"),
            _make_recall("R2", "ACME", recall_number="24-002"),
        ], conn=db)
        upsert_incidents_batch([_make_incident("INC1", "ACME")], conn=db)
        db.commit()

        count = build_manufacturer_profiles(db)
        assert count >= 1

        profile = db.execute(
            "SELECT * FROM manufacturer_profiles WHERE normalized_name = 'ACME'"
        ).fetchone()
        assert profile is not None

    def test_profile_aggregates_recalls(self, db):
        upsert_recalls_batch([
            _make_recall("R1", "ACME", units_numeric=1000),
            _make_recall("R2", "ACME", recall_number="24-002", units_numeric=2000),
        ], conn=db)
        db.commit()

        build_manufacturer_profiles(db)
        row = db.execute(
            "SELECT total_recalls, total_units_recalled FROM manufacturer_profiles WHERE normalized_name = 'ACME'"
        ).fetchone()
        assert row[0] == 2  # total_recalls
        assert row[1] == 3000  # total_units

    def test_compliance_score_assigned(self, db):
        upsert_recalls_batch([_make_recall("R1", "ACME")], conn=db)
        upsert_penalties_batch([_make_penalty("P1", "ACME", fine_amount=5000000.0)], conn=db)
        db.commit()

        build_manufacturer_profiles(db)
        row = db.execute(
            "SELECT compliance_score, risk_tier FROM manufacturer_profiles WHERE normalized_name = 'ACME'"
        ).fetchone()
        assert row[0] < 1.0  # Should be penalized
        assert row[1] in ("LOW", "MEDIUM", "HIGH", "CRITICAL")

    def test_multiple_manufacturers(self, db):
        upsert_recalls_batch([
            _make_recall("R1", "ACME"),
            _make_recall("R2", "BETA"),
        ], conn=db)
        db.commit()

        count = build_manufacturer_profiles(db)
        assert count == 2
