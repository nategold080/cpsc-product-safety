"""Tests for Hazard-to-Diagnosis Validation (Enrichment 3)."""

import sqlite3

import pytest

from src.storage.database import init_db, upsert_recalls_batch, upsert_incidents_batch, insert_neiss_batch
from src.validation.hazard_validator import (
    load_hazard_map,
    extract_hazard_type,
    build_hazard_validation,
    load_hazard_map_to_db,
)


@pytest.fixture
def db_conn():
    conn = sqlite3.connect(":memory:")
    init_db(conn=conn)
    yield conn
    conn.close()


class TestHazardMapLoading:
    def test_load_hazard_map_from_config(self):
        """Test loading the actual hazard-diagnosis map config."""
        import os
        yaml_path = os.path.join(os.path.dirname(__file__), "..", "config", "hazard_diagnosis_map.yaml")
        if not os.path.exists(yaml_path):
            pytest.skip("Config not available")

        hazard_map = load_hazard_map(yaml_path)
        assert len(hazard_map) >= 10
        assert "Fire Hazard" in hazard_map
        assert "Fall Hazard" in hazard_map
        assert "Choking Hazard" in hazard_map

    def test_fire_hazard_codes(self):
        import os
        yaml_path = os.path.join(os.path.dirname(__file__), "..", "config", "hazard_diagnosis_map.yaml")
        if not os.path.exists(yaml_path):
            pytest.skip("Config not available")

        hazard_map = load_hazard_map(yaml_path)
        fire_codes = hazard_map["Fire Hazard"]
        assert 41 in fire_codes  # Burns - Thermal
        assert 42 in fire_codes  # Burns - Scald

    def test_load_to_db(self, db_conn):
        import os
        yaml_path = os.path.join(os.path.dirname(__file__), "..", "config", "hazard_diagnosis_map.yaml")
        if not os.path.exists(yaml_path):
            pytest.skip("Config not available")

        count = load_hazard_map_to_db(db_conn, yaml_path)
        assert count > 30  # We have 14 hazard types × multiple codes each

        # Verify fire hazard entries
        rows = db_conn.execute(
            "SELECT COUNT(*) FROM hazard_diagnosis_map WHERE hazard_type = 'Fire Hazard'"
        ).fetchone()
        assert rows[0] >= 3


class TestHazardTypeExtraction:
    def test_fire_hazard(self):
        assert extract_hazard_type("Fire Hazard") == "Fire Hazard"
        assert extract_hazard_type("Product poses a fire hazard") == "Fire Hazard"
        assert extract_hazard_type("Overheating and fire risk") == "Fire Hazard"

    def test_electrical_shock(self):
        assert extract_hazard_type("Electrical Shock Hazard") == "Electrical Shock Hazard"
        assert extract_hazard_type("Risk of electric shock") == "Electrical Shock Hazard"
        assert extract_hazard_type("Electrocution risk") == "Electrical Shock Hazard"

    def test_choking(self):
        assert extract_hazard_type("Choking Hazard") == "Choking Hazard"
        assert extract_hazard_type("Small parts pose a choking risk") == "Choking Hazard"
        assert extract_hazard_type("Aspiration risk for children") == "Choking Hazard"

    def test_fall(self):
        assert extract_hazard_type("Fall Hazard") == "Fall Hazard"
        assert extract_hazard_type("Product can tip or fall") == "Fall Hazard"

    def test_laceration(self):
        assert extract_hazard_type("Laceration Hazard") == "Laceration Hazard"
        assert extract_hazard_type("Sharp edges can cut users") == "Laceration Hazard"

    def test_tip_over(self):
        assert extract_hazard_type("Tip-Over Hazard") == "Tip-Over Hazard"
        assert extract_hazard_type("Tipping hazard") == "Tip-Over Hazard"

    def test_poisoning(self):
        assert extract_hazard_type("Lead poisoning hazard") == "Poisoning Hazard"
        assert extract_hazard_type("Toxic substance risk") == "Poisoning Hazard"

    def test_burn(self):
        assert extract_hazard_type("Burn Hazard") == "Burn Hazard"
        assert extract_hazard_type("Thermal burn risk") == "Burn Hazard"

    def test_drowning(self):
        assert extract_hazard_type("Drowning Hazard") == "Drowning Hazard"
        assert extract_hazard_type("Submersion risk") == "Drowning Hazard"

    def test_strangulation(self):
        assert extract_hazard_type("Strangulation Hazard") == "Strangulation Hazard"

    def test_empty(self):
        assert extract_hazard_type("") == ""
        assert extract_hazard_type(None) == ""

    def test_unknown(self):
        assert extract_hazard_type("General safety concern") == ""

    def test_priority_ordering(self):
        """Electrical shock should match before fire for 'shock' descriptions."""
        result = extract_hazard_type("Electrical shock and fire hazard")
        assert result == "Electrical Shock Hazard"


class TestHazardValidation:
    def _setup_test_data(self, db_conn):
        """Set up recalls, incidents, and NEISS data for validation testing."""
        recalls = [{
            "recall_id": "R1", "recall_number": "23-001",
            "recall_date": "2023-01-01", "title": "Test Fire Recall",
            "description": "Fire hazard test", "consumer_contact": "",
            "url": "", "last_publish_date": "",
            "product_names": "Space Heater", "product_types": "Heaters",
            "product_categories": "", "number_of_units": "1000",
            "hazard_description": "Product overheats, posing a fire hazard",
            "hazard_types": "Fire Hazard",
            "remedy_description": "Refund", "remedy_options": "",
            "manufacturer_names": "TestCo",
            "manufacturer_countries": "USA",
            "retailer_names": "", "importer_names": "",
            "distributor_names": "", "image_urls": "",
            "normalized_manufacturer": "TESTCO",
            "fiscal_year": 2023, "units_numeric": 1000,
            "quality_score": 0.9,
        }]
        upsert_recalls_batch(recalls, conn=db_conn)

        incidents = [{
            "report_number": "INC001", "report_date": "2023-02-01",
            "publication_date": "", "submitter_category": "Consumer",
            "product_description": "Space Heater", "product_category": "Heaters",
            "product_subcategory": "", "product_type": "",
            "product_code": "363",  # Maps to a NEISS product code
            "manufacturer_name": "TestCo", "brand_name": "",
            "model_name": "", "serial_number": "", "upc": "",
            "date_manufactured": "", "retailer_name": "",
            "retailer_state": "", "purchase_date": "",
            "incident_description": "Heater caught fire",
            "city": "", "state": "CA", "zip_code": "",
            "location": "", "severity": "Injury",
            "victim_sex": "", "victim_age": "",
            "company_comments": "", "associated_reports": "",
            "normalized_manufacturer": "TESTCO",
            "fiscal_year": 2023, "quality_score": 0.8,
        }]
        upsert_incidents_batch(incidents, conn=db_conn)

        # NEISS records with product_1 matching incident product_code
        neiss_records = []
        # 15 burn injuries (matching fire hazard)
        for i in range(15):
            neiss_records.append({
                "cpsc_case_number": f"BURN{i:03d}", "treatment_date": "2023-03-01",
                "age": 40 + i, "sex": "Male", "race": "", "hispanic": "",
                "body_part": 35, "body_part_name": "Hand",
                "diagnosis": 41, "diagnosis_name": "Burns - Thermal",  # Matches fire
                "body_part_2": None, "diagnosis_2": None,
                "disposition": 1, "disposition_name": "Treated/Released",
                "location": 1, "location_name": "Home", "fire_involvement": 1,
                "product_1": 363, "product_1_name": "", "product_2": None,
                "product_3": None, "alcohol": 0, "drug": 0,
                "narrative": "Burned by heater", "stratum": "S", "psu": "10",
                "weight": 50.0, "neiss_year": 2023, "quality_score": 0.85,
            })
        # 5 laceration injuries (unexpected for fire hazard)
        for i in range(5):
            neiss_records.append({
                "cpsc_case_number": f"LAC{i:03d}", "treatment_date": "2023-03-01",
                "age": 30 + i, "sex": "Female", "race": "", "hispanic": "",
                "body_part": 36, "body_part_name": "Finger",
                "diagnosis": 61, "diagnosis_name": "Laceration",
                "body_part_2": None, "diagnosis_2": None,
                "disposition": 1, "disposition_name": "Treated/Released",
                "location": 1, "location_name": "Home", "fire_involvement": 0,
                "product_1": 363, "product_1_name": "", "product_2": None,
                "product_3": None, "alcohol": 0, "drug": 0,
                "narrative": "Cut by heater", "stratum": "S", "psu": "10",
                "weight": 50.0, "neiss_year": 2023, "quality_score": 0.8,
            })
        insert_neiss_batch(neiss_records, conn=db_conn)
        db_conn.commit()

    def test_build_hazard_validation(self, db_conn):
        self._setup_test_data(db_conn)
        count = build_hazard_validation(db_conn)
        assert count >= 1

    def test_validation_results_correct(self, db_conn):
        self._setup_test_data(db_conn)
        build_hazard_validation(db_conn)

        row = db_conn.execute("""
            SELECT * FROM hazard_validation_results
            WHERE manufacturer_normalized = 'TESTCO' AND hazard_type = 'Fire Hazard'
        """).fetchone()
        assert row is not None
        # Columns: mfr, hazard, recalls, injuries, matching, unexpected, rate, status
        total_injuries = row[3]
        matching = row[4]
        unexpected = row[5]
        match_rate = row[6]

        assert total_injuries == 20  # 15 burns + 5 lacerations
        assert matching == 15  # Burns match fire hazard
        assert unexpected == 5  # Lacerations don't match
        assert match_rate == 0.75  # 15/20

    def test_validation_status_confirmed(self, db_conn):
        self._setup_test_data(db_conn)
        build_hazard_validation(db_conn)

        row = db_conn.execute("""
            SELECT validation_status FROM hazard_validation_results
            WHERE manufacturer_normalized = 'TESTCO' AND hazard_type = 'Fire Hazard'
        """).fetchone()
        assert row[0] == "confirmed"  # match_rate >= 0.3

    def test_no_data_no_crash(self, db_conn):
        count = build_hazard_validation(db_conn)
        assert count == 0


class TestHazardValidationEdgeCases:
    def test_multiple_hazard_types(self, db_conn):
        """Manufacturer with multiple hazard types should get separate validations."""
        recalls = []
        for hazard in ["Fire Hazard", "Laceration Hazard"]:
            recalls.append({
                "recall_id": f"R-{hazard}", "recall_number": f"23-{hazard[:3]}",
                "recall_date": "2023-01-01", "title": f"Test {hazard}",
                "description": "", "consumer_contact": "",
                "url": "", "last_publish_date": "",
                "product_names": "Product", "product_types": "",
                "product_categories": "", "number_of_units": "",
                "hazard_description": f"Poses a {hazard.lower()}",
                "hazard_types": hazard,
                "remedy_description": "", "remedy_options": "",
                "manufacturer_names": "MultiCo",
                "manufacturer_countries": "",
                "retailer_names": "", "importer_names": "",
                "distributor_names": "", "image_urls": "",
                "normalized_manufacturer": "MULTICO",
                "fiscal_year": 2023, "units_numeric": None,
                "quality_score": 0.5,
            })
        upsert_recalls_batch(recalls, conn=db_conn)

        incidents = [{
            "report_number": "MULTI-INC", "report_date": "2023-01-01",
            "publication_date": "", "submitter_category": "",
            "product_description": "Product", "product_category": "",
            "product_subcategory": "", "product_type": "",
            "product_code": "999",
            "manufacturer_name": "MultiCo", "brand_name": "",
            "model_name": "", "serial_number": "", "upc": "",
            "date_manufactured": "", "retailer_name": "",
            "retailer_state": "", "purchase_date": "",
            "incident_description": "", "city": "", "state": "",
            "zip_code": "", "location": "", "severity": "",
            "victim_sex": "", "victim_age": "",
            "company_comments": "", "associated_reports": "",
            "normalized_manufacturer": "MULTICO",
            "fiscal_year": 2023, "quality_score": 0.5,
        }]
        upsert_incidents_batch(incidents, conn=db_conn)

        neiss = [{
            "cpsc_case_number": f"MULTI{i}", "treatment_date": "2023-01-01",
            "age": 30, "sex": "Male", "race": "", "hispanic": "",
            "body_part": 35, "body_part_name": "Hand",
            "diagnosis": 41, "diagnosis_name": "Burns - Thermal",
            "body_part_2": None, "diagnosis_2": None,
            "disposition": 1, "disposition_name": "Treated/Released",
            "location": 1, "location_name": "Home", "fire_involvement": 0,
            "product_1": 999, "product_1_name": "", "product_2": None,
            "product_3": None, "alcohol": 0, "drug": 0,
            "narrative": "test", "stratum": "S", "psu": "10",
            "weight": 50.0, "neiss_year": 2023, "quality_score": 0.8,
        } for i in range(20)]
        insert_neiss_batch(neiss, conn=db_conn)
        db_conn.commit()

        count = build_hazard_validation(db_conn)
        assert count >= 1  # At least fire hazard should validate
