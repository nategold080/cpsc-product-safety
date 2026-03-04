"""Tests for FDA FAERS Integration (Enrichment 2)."""

import sqlite3

import pytest

from src.storage.database import (
    init_db,
    upsert_fda_events_batch,
    upsert_fda_recalls_batch,
    insert_cpsc_fda_links_batch,
    upsert_recalls_batch,
    get_stats,
)
from src.scrapers.fda_downloader import (
    _parse_device_event,
    _parse_device_recall,
    _parse_fda_date,
    _normalize_outcome,
)
from src.validation.quality import score_fda_event, score_fda_recall
from src.normalization.cross_linker import _link_fda_to_cpsc


@pytest.fixture
def db_conn():
    conn = sqlite3.connect(":memory:")
    init_db(conn=conn)
    yield conn
    conn.close()


@pytest.fixture
def sample_fda_events():
    return [
        {"event_id": "FDA-100001", "report_date": "2023-01-15",
         "product_name": "Home Blood Pressure Monitor", "product_type": "device",
         "manufacturer_name": "ACME Medical", "manufacturer_normalized": "ACME MEDICAL",
         "event_type": "injury", "patient_outcome": "hospitalization",
         "description": "Device malfunctioned causing injury", "source": "consumer",
         "quality_score": 0.85},
        {"event_id": "FDA-100002", "report_date": "2023-03-20",
         "product_name": "Electric Heating Pad", "product_type": "device",
         "manufacturer_name": "Samsung Electronics", "manufacturer_normalized": "SAMSUNG ELECTRONICS",
         "event_type": "injury", "patient_outcome": "disability",
         "description": "Heating pad caused burns", "source": "healthcare_professional",
         "quality_score": 0.9},
        {"event_id": "FDA-100003", "report_date": "2023-05-10",
         "product_name": "Digital Thermometer", "product_type": "device",
         "manufacturer_name": "XYZ Corp", "manufacturer_normalized": "XYZ",
         "event_type": "malfunction", "patient_outcome": "",
         "description": "Inaccurate readings", "source": "consumer",
         "quality_score": 0.7},
    ]


@pytest.fixture
def sample_fda_recalls():
    return [
        {"recall_id": "FDA-Z-0001-23", "product_description": "Home Defibrillator",
         "reason_for_recall": "Battery failure risk",
         "manufacturer_name": "ACME Medical", "manufacturer_normalized": "ACME MEDICAL",
         "recall_class": "Class I", "recall_status": "Ongoing",
         "event_date": "2023-06-01", "quality_score": 0.95},
        {"recall_id": "FDA-Z-0002-23", "product_description": "Blood Glucose Monitor",
         "reason_for_recall": "Incorrect readings",
         "manufacturer_name": "MedTech Inc", "manufacturer_normalized": "MEDTECH",
         "recall_class": "Class II", "recall_status": "Terminated",
         "event_date": "2023-04-15", "quality_score": 0.9},
    ]


class TestFDAEventParsing:
    def test_parse_device_event(self):
        raw = {
            "report_number": "12345",
            "date_received": "20230115",
            "event_type": "Injury",
            "event_location": "HOME",
            "source_type": ["Consumer"],
            "device": [{
                "manufacturer_d_name": "ACME Medical Corp",
                "generic_name": "BLOOD PRESSURE MONITOR",
                "brand_name": "ACME BP Pro",
            }],
            "patient": [{
                "sequence_number_outcome": "2",
            }],
            "mdr_text": [{
                "text": "Patient was injured by device",
            }],
        }
        record = _parse_device_event(raw)
        assert record is not None
        assert record["event_id"] == "FDA-12345"
        assert record["report_date"] == "2023-01-15"
        assert record["product_name"] == "ACME BP Pro"
        assert record["manufacturer_name"] == "ACME Medical Corp"
        assert "ACME MEDICAL" in record["manufacturer_normalized"]
        assert record["event_type"] == "injury"
        assert "hospitalization" in record["patient_outcome"]

    def test_parse_device_event_no_report_number(self):
        raw = {"event_type": "Injury"}
        assert _parse_device_event(raw) is None

    def test_parse_device_event_minimal(self):
        raw = {"report_number": "99999", "device": [], "patient": [], "mdr_text": []}
        record = _parse_device_event(raw)
        assert record is not None
        assert record["event_id"] == "FDA-99999"
        assert record["product_name"] == ""

    def test_parse_fda_date(self):
        assert _parse_fda_date("20230115") == "2023-01-15"
        assert _parse_fda_date("") == ""
        assert _parse_fda_date("2023") == ""
        assert _parse_fda_date("20231231") == "2023-12-31"

    def test_normalize_outcome(self):
        assert _normalize_outcome("1") == "death"
        assert _normalize_outcome("2") == "hospitalization"
        assert _normalize_outcome("") == ""
        assert _normalize_outcome("2,5") == "hospitalization, required_intervention"


class TestFDARecallParsing:
    def test_parse_device_recall(self):
        raw = {
            "product_res_number": "Z-0001-23",
            "product_description": "Home Defibrillator Model X",
            "reason_for_recall": "Battery may fail during use",
            "recalling_firm": "ACME Medical Corp",
            "recall_status": "Ongoing",
            "event_date_initiated": "2023-06-01",
        }
        record = _parse_device_recall(raw)
        assert record is not None
        assert record["recall_id"] == "FDA-Z-0001-23"
        assert record["manufacturer_name"] == "ACME Medical Corp"
        assert record["recall_class"] == "Class I"

    def test_parse_device_recall_no_number(self):
        raw = {"reason_for_recall": "Some reason"}
        assert _parse_device_recall(raw) is None


class TestFDAQualityScoring:
    def test_score_complete_event(self):
        record = {
            "event_id": "FDA-1", "report_date": "2023-01-01",
            "product_name": "Test", "manufacturer_name": "ACME",
            "event_type": "injury", "patient_outcome": "hospitalization",
            "description": "Detailed narrative", "source": "consumer",
        }
        score = score_fda_event(record)
        assert score == 1.0

    def test_score_empty_event(self):
        score = score_fda_event({})
        assert score == 0.0

    def test_score_complete_recall(self):
        record = {
            "recall_id": "FDA-R1", "product_description": "Device",
            "reason_for_recall": "Battery failure",
            "manufacturer_name": "ACME", "recall_class": "Class I",
            "recall_status": "Ongoing", "event_date": "2023-01-01",
        }
        score = score_fda_recall(record)
        assert score == 1.0

    def test_score_empty_recall(self):
        score = score_fda_recall({})
        assert score == 0.0


class TestFDADatabaseOperations:
    def test_upsert_events(self, db_conn, sample_fda_events):
        upsert_fda_events_batch(sample_fda_events, conn=db_conn)
        count = db_conn.execute("SELECT COUNT(*) FROM fda_adverse_events").fetchone()[0]
        assert count == 3

    def test_event_fields(self, db_conn, sample_fda_events):
        upsert_fda_events_batch(sample_fda_events, conn=db_conn)
        row = db_conn.execute(
            "SELECT * FROM fda_adverse_events WHERE event_id = 'FDA-100001'"
        ).fetchone()
        assert row is not None

    def test_upsert_recalls(self, db_conn, sample_fda_recalls):
        upsert_fda_recalls_batch(sample_fda_recalls, conn=db_conn)
        count = db_conn.execute("SELECT COUNT(*) FROM fda_device_recalls").fetchone()[0]
        assert count == 2

    def test_upsert_events_idempotent(self, db_conn, sample_fda_events):
        upsert_fda_events_batch(sample_fda_events, conn=db_conn)
        upsert_fda_events_batch(sample_fda_events, conn=db_conn)
        count = db_conn.execute("SELECT COUNT(*) FROM fda_adverse_events").fetchone()[0]
        assert count == 3

    def test_insert_manufacturer_links(self, db_conn):
        links = [
            {"cpsc_manufacturer": "SAMSUNG ELECTRONICS", "fda_manufacturer": "SAMSUNG ELECTRONICS",
             "link_method": "exact", "confidence": 0.9},
        ]
        insert_cpsc_fda_links_batch(links, conn=db_conn)
        count = db_conn.execute("SELECT COUNT(*) FROM cpsc_fda_manufacturer_links").fetchone()[0]
        assert count == 1


class TestFDACrossLinking:
    def test_link_fda_to_cpsc(self, db_conn, sample_fda_events):
        # Add a CPSC recall for a matching manufacturer
        cpsc_recall = {
            "recall_id": "R1", "recall_number": "23-001",
            "recall_date": "2023-01-01", "title": "Test",
            "description": "Test recall", "consumer_contact": "",
            "url": "", "last_publish_date": "",
            "product_names": "Monitor", "product_types": "Electronics",
            "product_categories": "", "number_of_units": "100",
            "hazard_description": "Fire hazard", "hazard_types": "Fire",
            "remedy_description": "Refund", "remedy_options": "",
            "manufacturer_names": "Samsung Electronics",
            "manufacturer_countries": "South Korea",
            "retailer_names": "", "importer_names": "",
            "distributor_names": "", "image_urls": "",
            "normalized_manufacturer": "SAMSUNG ELECTRONICS",
            "fiscal_year": 2023, "units_numeric": 100,
            "quality_score": 0.9,
        }
        upsert_recalls_batch([cpsc_recall], conn=db_conn)
        upsert_fda_events_batch(sample_fda_events, conn=db_conn)
        db_conn.commit()

        link_count = _link_fda_to_cpsc(db_conn)
        assert link_count > 0

        # Check manufacturer links
        mfr_links = db_conn.execute(
            "SELECT COUNT(*) FROM cpsc_fda_manufacturer_links"
        ).fetchone()[0]
        assert mfr_links >= 1

    def test_no_fda_data_no_crash(self, db_conn):
        """Cross-linking with no FDA data should not crash."""
        count = _link_fda_to_cpsc(db_conn)
        assert count == 0


class TestFDAStatsIntegration:
    def test_stats_include_fda(self, db_conn, sample_fda_events, sample_fda_recalls):
        upsert_fda_events_batch(sample_fda_events, conn=db_conn)
        upsert_fda_recalls_batch(sample_fda_recalls, conn=db_conn)
        db_conn.commit()

        stats = get_stats(conn=db_conn)
        assert stats["fda_adverse_events"] == 3
        assert stats["fda_device_recalls"] == 2
