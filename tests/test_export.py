"""Tests for export module."""

import sqlite3
import pytest
from pathlib import Path
from src.storage.database import init_db, upsert_recalls_batch, upsert_incidents_batch
from src.normalization.cross_linker import build_cross_links, build_manufacturer_profiles
from src.export.exporter import export_all


@pytest.fixture
def db_with_data(tmp_path):
    conn = sqlite3.connect(":memory:")
    init_db(conn=conn)

    # Insert sample data
    recalls = [{
        "recall_id": f"R{i}", "recall_number": f"24-{i:03d}",
        "recall_date": "2024-01-15", "title": f"Recall {i}",
        "description": "Test", "consumer_contact": "",
        "url": "", "last_publish_date": "",
        "product_names": "Widget", "product_types": "Toy",
        "product_categories": "", "number_of_units": "1000",
        "hazard_description": "Choking", "hazard_types": "Choking",
        "remedy_description": "Refund", "remedy_options": "",
        "manufacturer_names": "ACME" if i % 2 == 0 else "BETA",
        "manufacturer_countries": "China",
        "retailer_names": "", "importer_names": "",
        "distributor_names": "", "image_urls": "",
        "normalized_manufacturer": "ACME" if i % 2 == 0 else "BETA",
        "fiscal_year": 2024, "units_numeric": 1000,
        "quality_score": 0.9,
    } for i in range(10)]
    upsert_recalls_batch(recalls, conn=conn)

    incidents = [{
        "report_number": f"INC{i:03d}", "report_date": "2024-03-01",
        "publication_date": "", "submitter_category": "Consumer",
        "product_description": "Widget", "product_category": "Toys",
        "product_subcategory": "", "product_type": "",
        "product_code": "", "manufacturer_name": "ACME",
        "brand_name": "", "model_name": "", "serial_number": "",
        "upc": "", "date_manufactured": "",
        "retailer_name": "", "retailer_state": "",
        "purchase_date": "", "incident_description": "Broke",
        "city": "", "state": "CO", "zip_code": "",
        "location": "", "severity": "Injury",
        "victim_sex": "", "victim_age": "",
        "company_comments": "", "associated_reports": "",
        "normalized_manufacturer": "ACME", "fiscal_year": 2024,
        "quality_score": 0.7,
    } for i in range(5)]
    upsert_incidents_batch(incidents, conn=conn)

    conn.commit()
    build_cross_links(conn)
    build_manufacturer_profiles(conn)

    yield conn
    conn.close()


class TestExportAll:
    def test_exports_files(self, db_with_data, tmp_path, monkeypatch):
        import src.export.exporter as exp
        monkeypatch.setattr(exp, "EXPORT_DIR", tmp_path)

        counts = export_all(db_with_data)
        assert counts["recalls_csv"] == 10
        assert counts["incidents_csv"] == 5
        assert counts["profiles_csv"] == 2  # ACME and BETA

        # Check files exist
        assert (tmp_path / "recalls.csv").exists()
        assert (tmp_path / "incidents.csv").exists()
        assert (tmp_path / "manufacturer_profiles.csv").exists()
        assert (tmp_path / "summary.md").exists()

    def test_summary_md_content(self, db_with_data, tmp_path, monkeypatch):
        import src.export.exporter as exp
        monkeypatch.setattr(exp, "EXPORT_DIR", tmp_path)

        export_all(db_with_data)
        summary = (tmp_path / "summary.md").read_text()
        assert "CPSC Product Safety Tracker" in summary
        assert "Nathan Goldberg" in summary
        assert "10" in summary  # recalls count

    def test_profiles_json(self, db_with_data, tmp_path, monkeypatch):
        import src.export.exporter as exp
        monkeypatch.setattr(exp, "EXPORT_DIR", tmp_path)

        export_all(db_with_data)
        import json
        with open(tmp_path / "manufacturer_profiles.json") as f:
            profiles = json.load(f)
        assert len(profiles) == 2
        assert all("compliance_score" in p for p in profiles)
