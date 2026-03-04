"""Tests for NEISS product code resolution (Enrichment 1)."""

import os
import sqlite3
import tempfile

import pytest
import yaml

from src.storage.database import (
    init_db,
    insert_product_codes_batch,
    insert_neiss_batch,
    load_product_codes_from_yaml,
    update_neiss_product_names,
    get_product_code_name,
    get_stats,
)


@pytest.fixture
def db_conn():
    """Create an in-memory database connection for testing."""
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    init_db(conn=conn)
    yield conn
    conn.close()


@pytest.fixture
def sample_product_codes():
    """Sample product codes for testing."""
    return [
        {"product_code": 1807, "product_name": "Floors or flooring materials",
         "category": "Home Structures & Construction", "is_deleted": 0,
         "is_child_related": 0, "is_outdoor": 0},
        {"product_code": 1842, "product_name": "Stairs or steps",
         "category": "Home Structures & Construction", "is_deleted": 0,
         "is_child_related": 0, "is_outdoor": 0},
        {"product_code": 5040, "product_name": "Bicycles or accessories",
         "category": "Sports & Recreation", "is_deleted": 0,
         "is_child_related": 0, "is_outdoor": 1},
        {"product_code": 1541, "product_name": "High chairs",
         "category": "Child Nursery Equipment", "is_deleted": 0,
         "is_child_related": 1, "is_outdoor": 0},
        {"product_code": 100, "product_name": "General household appliances",
         "category": "General Household Appliances", "is_deleted": 1,
         "is_child_related": 0, "is_outdoor": 0},
    ]


@pytest.fixture
def sample_neiss_records():
    """Sample NEISS records for testing product code resolution."""
    return [
        {"cpsc_case_number": "TEST001", "treatment_date": "2023-01-15",
         "age": 45, "sex": "Male", "race": "", "hispanic": "",
         "body_part": 75, "body_part_name": "Head", "diagnosis": 62,
         "diagnosis_name": "Concussions", "body_part_2": None,
         "diagnosis_2": None, "disposition": 1, "disposition_name": "Treated/Released",
         "location": 1, "location_name": "Home", "fire_involvement": 0,
         "product_1": 1807, "product_1_name": "", "product_2": None,
         "product_3": None, "alcohol": 0, "drug": 0,
         "narrative": "Fell on floor", "stratum": "S", "psu": "10",
         "weight": 50.5, "neiss_year": 2023, "quality_score": 0.85},
        {"cpsc_case_number": "TEST002", "treatment_date": "2023-02-20",
         "age": 12, "sex": "Male", "race": "", "hispanic": "",
         "body_part": 33, "body_part_name": "Lower Arm", "diagnosis": 58,
         "diagnosis_name": "Fracture", "body_part_2": None,
         "diagnosis_2": None, "disposition": 1, "disposition_name": "Treated/Released",
         "location": 4, "location_name": "Other Public Property",
         "fire_involvement": 0, "product_1": 5040, "product_1_name": "",
         "product_2": None, "product_3": None, "alcohol": 0, "drug": 0,
         "narrative": "Fell off bicycle", "stratum": "S", "psu": "10",
         "weight": 120.0, "neiss_year": 2023, "quality_score": 0.9},
        {"cpsc_case_number": "TEST003", "treatment_date": "2023-03-10",
         "age": 2, "sex": "Female", "race": "", "hispanic": "",
         "body_part": 75, "body_part_name": "Head", "diagnosis": 50,
         "diagnosis_name": "Contusions/Abrasions", "body_part_2": None,
         "diagnosis_2": None, "disposition": 1, "disposition_name": "Treated/Released",
         "location": 1, "location_name": "Home", "fire_involvement": 0,
         "product_1": 1541, "product_1_name": "", "product_2": None,
         "product_3": None, "alcohol": 0, "drug": 0,
         "narrative": "Fell from high chair", "stratum": "S", "psu": "10",
         "weight": 80.0, "neiss_year": 2023, "quality_score": 0.8},
    ]


class TestProductCodeInsertion:
    def test_insert_product_codes(self, db_conn, sample_product_codes):
        insert_product_codes_batch(sample_product_codes, conn=db_conn)
        count = db_conn.execute("SELECT COUNT(*) FROM neiss_product_codes").fetchone()[0]
        assert count == 5

    def test_product_code_fields(self, db_conn, sample_product_codes):
        insert_product_codes_batch(sample_product_codes, conn=db_conn)
        row = db_conn.execute(
            "SELECT * FROM neiss_product_codes WHERE product_code = 1807"
        ).fetchone()
        assert row is not None
        # product_code, product_name, category, is_deleted, is_child_related, is_outdoor
        assert row[1] == "Floors or flooring materials"
        assert row[2] == "Home Structures & Construction"
        assert row[3] == 0  # is_deleted
        assert row[4] == 0  # is_child_related
        assert row[5] == 0  # is_outdoor

    def test_deleted_code(self, db_conn, sample_product_codes):
        insert_product_codes_batch(sample_product_codes, conn=db_conn)
        row = db_conn.execute(
            "SELECT is_deleted FROM neiss_product_codes WHERE product_code = 100"
        ).fetchone()
        assert row[0] == 1

    def test_child_related_flag(self, db_conn, sample_product_codes):
        insert_product_codes_batch(sample_product_codes, conn=db_conn)
        row = db_conn.execute(
            "SELECT is_child_related FROM neiss_product_codes WHERE product_code = 1541"
        ).fetchone()
        assert row[0] == 1

    def test_outdoor_flag(self, db_conn, sample_product_codes):
        insert_product_codes_batch(sample_product_codes, conn=db_conn)
        row = db_conn.execute(
            "SELECT is_outdoor FROM neiss_product_codes WHERE product_code = 5040"
        ).fetchone()
        assert row[0] == 1

    def test_upsert_replaces(self, db_conn, sample_product_codes):
        insert_product_codes_batch(sample_product_codes, conn=db_conn)
        # Update a code
        updated = [{"product_code": 1807, "product_name": "Floors UPDATED",
                     "category": "Updated Cat", "is_deleted": 0,
                     "is_child_related": 0, "is_outdoor": 0}]
        insert_product_codes_batch(updated, conn=db_conn)
        row = db_conn.execute(
            "SELECT product_name FROM neiss_product_codes WHERE product_code = 1807"
        ).fetchone()
        assert row[0] == "Floors UPDATED"


class TestProductCodeLookup:
    def test_get_product_code_name(self, db_conn, sample_product_codes):
        insert_product_codes_batch(sample_product_codes, conn=db_conn)
        name = get_product_code_name(1807, conn=db_conn)
        assert name == "Floors or flooring materials"

    def test_get_unknown_code(self, db_conn, sample_product_codes):
        insert_product_codes_batch(sample_product_codes, conn=db_conn)
        name = get_product_code_name(9999, conn=db_conn)
        assert name == ""

    def test_get_deleted_code(self, db_conn, sample_product_codes):
        insert_product_codes_batch(sample_product_codes, conn=db_conn)
        name = get_product_code_name(100, conn=db_conn)
        assert name == "General household appliances"


class TestNEISSProductNameResolution:
    def test_update_neiss_product_names(self, db_conn, sample_product_codes,
                                         sample_neiss_records):
        insert_product_codes_batch(sample_product_codes, conn=db_conn)
        insert_neiss_batch(sample_neiss_records, conn=db_conn)
        db_conn.commit()

        updated = update_neiss_product_names(conn=db_conn)
        assert updated == 3

        # Verify each record
        row = db_conn.execute(
            "SELECT product_1_name FROM neiss_injuries WHERE cpsc_case_number = 'TEST001'"
        ).fetchone()
        assert row[0] == "Floors or flooring materials"

        row = db_conn.execute(
            "SELECT product_1_name FROM neiss_injuries WHERE cpsc_case_number = 'TEST002'"
        ).fetchone()
        assert row[0] == "Bicycles or accessories"

        row = db_conn.execute(
            "SELECT product_1_name FROM neiss_injuries WHERE cpsc_case_number = 'TEST003'"
        ).fetchone()
        assert row[0] == "High chairs"

    def test_update_preserves_unmatched(self, db_conn, sample_neiss_records):
        """Records with no matching product code should keep empty product name."""
        insert_neiss_batch(sample_neiss_records, conn=db_conn)
        db_conn.commit()

        # No product codes loaded, so nothing should be updated
        updated = update_neiss_product_names(conn=db_conn)
        assert updated == 0

    def test_category_aggregation(self, db_conn, sample_product_codes,
                                   sample_neiss_records):
        insert_product_codes_batch(sample_product_codes, conn=db_conn)
        insert_neiss_batch(sample_neiss_records, conn=db_conn)
        db_conn.commit()
        update_neiss_product_names(conn=db_conn)

        # Test category join query
        rows = db_conn.execute("""
            SELECT pc.category, COUNT(*) as count
            FROM neiss_injuries ni
            JOIN neiss_product_codes pc ON ni.product_1 = pc.product_code
            GROUP BY pc.category ORDER BY count DESC
        """).fetchall()
        assert len(rows) >= 2
        # Home Structures should have 1 record (floors)
        cats = {r[0]: r[1] for r in rows}
        assert "Home Structures & Construction" in cats
        assert cats["Home Structures & Construction"] == 1

    def test_child_related_query(self, db_conn, sample_product_codes,
                                  sample_neiss_records):
        insert_product_codes_batch(sample_product_codes, conn=db_conn)
        insert_neiss_batch(sample_neiss_records, conn=db_conn)
        db_conn.commit()

        rows = db_conn.execute("""
            SELECT COUNT(*) FROM neiss_injuries ni
            JOIN neiss_product_codes pc ON ni.product_1 = pc.product_code
            WHERE pc.is_child_related = 1
        """).fetchone()
        assert rows[0] == 1  # Only high chair record


class TestYAMLLoading:
    def test_load_from_yaml(self, db_conn):
        """Test loading product codes from YAML config file."""
        yaml_data = {
            "neiss_product_codes": {
                1807: {
                    "product_name": "Floors or flooring materials",
                    "category": "Home Structures & Construction",
                    "is_deleted": False,
                    "is_child_related": False,
                    "is_outdoor": False,
                },
                5040: {
                    "product_name": "Bicycles",
                    "category": "Sports & Recreation",
                    "is_deleted": False,
                    "is_child_related": False,
                    "is_outdoor": True,
                },
            }
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(yaml_data, f)
            tmp_path = f.name

        try:
            count = load_product_codes_from_yaml(tmp_path, conn=db_conn)
            assert count == 2

            row = db_conn.execute(
                "SELECT product_name FROM neiss_product_codes WHERE product_code = 1807"
            ).fetchone()
            assert row[0] == "Floors or flooring materials"
        finally:
            os.unlink(tmp_path)

    def test_load_actual_config(self, db_conn):
        """Test loading the actual config file."""
        yaml_path = os.path.join(
            os.path.dirname(__file__), "..", "config", "neiss_product_codes.yaml"
        )
        if not os.path.exists(yaml_path):
            pytest.skip("Config file not available")

        count = load_product_codes_from_yaml(yaml_path, conn=db_conn)
        assert count > 1000  # We have ~1700 codes
        assert count < 3000  # Sanity check

        # Verify key codes exist
        for code in [1807, 1842, 5040, 464, 611]:
            row = db_conn.execute(
                "SELECT product_name FROM neiss_product_codes WHERE product_code = ?",
                (code,)
            ).fetchone()
            assert row is not None, f"Product code {code} not found"
            assert len(row[0]) > 0, f"Product code {code} has empty name"


class TestStatsIntegration:
    def test_stats_includes_product_data(self, db_conn, sample_product_codes,
                                          sample_neiss_records):
        insert_product_codes_batch(sample_product_codes, conn=db_conn)
        insert_neiss_batch(sample_neiss_records, conn=db_conn)
        db_conn.commit()
        update_neiss_product_names(conn=db_conn)
        db_conn.commit()

        stats = get_stats(conn=db_conn)
        assert "neiss_with_product_name" in stats
        assert stats["neiss_with_product_name"] == 3
        assert "product_categories" in stats
        assert stats["product_categories"] >= 2


class TestWeightedEstimates:
    def test_weighted_national_estimate_by_product(self, db_conn,
                                                     sample_product_codes,
                                                     sample_neiss_records):
        insert_product_codes_batch(sample_product_codes, conn=db_conn)
        insert_neiss_batch(sample_neiss_records, conn=db_conn)
        db_conn.commit()

        rows = db_conn.execute("""
            SELECT product_1, SUM(weight) as est
            FROM neiss_injuries GROUP BY product_1 ORDER BY est DESC
        """).fetchall()
        assert len(rows) == 3
        # Bicycle has highest weight (120.0)
        assert rows[0][0] == 5040
        assert rows[0][1] == 120.0

    def test_weighted_estimate_by_category(self, db_conn, sample_product_codes,
                                            sample_neiss_records):
        insert_product_codes_batch(sample_product_codes, conn=db_conn)
        insert_neiss_batch(sample_neiss_records, conn=db_conn)
        db_conn.commit()

        rows = db_conn.execute("""
            SELECT pc.category, SUM(ni.weight) as est
            FROM neiss_injuries ni
            JOIN neiss_product_codes pc ON ni.product_1 = pc.product_code
            GROUP BY pc.category ORDER BY est DESC
        """).fetchall()
        assert len(rows) >= 2
