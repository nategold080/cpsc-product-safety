"""Tests for quality scoring."""

import pytest
from src.validation.quality import (
    score_recall, score_incident, score_neiss, score_penalty,
    score_import_violation, compute_compliance_score, assign_risk_tier,
)


class TestScoreRecall:
    def test_complete_record(self):
        record = {
            "recall_number": "24-001", "recall_date": "2024-01-01",
            "title": "Test", "description": "Desc",
            "product_names": "Widget", "hazard_description": "Choking",
            "manufacturer_names": "ACME", "number_of_units": "1000",
            "remedy_description": "Refund", "manufacturer_countries": "China",
        }
        score = score_recall(record)
        assert score == 1.0

    def test_empty_record(self):
        assert score_recall({}) == 0.0

    def test_partial_record(self):
        record = {"recall_number": "24-001", "manufacturer_names": "ACME"}
        score = score_recall(record)
        assert 0.0 < score < 1.0
        assert score == pytest.approx(0.25, abs=0.01)


class TestScoreIncident:
    def test_complete_record(self):
        record = {
            "report_number": "INC001", "report_date": "2024-01-01",
            "product_description": "Widget", "product_category": "Toys",
            "manufacturer_name": "ACME", "incident_description": "Broke",
            "state": "CO", "severity": "Injury",
            "product_code": "1234", "brand_name": "Acme",
            "model_name": "W100",
        }
        score = score_incident(record)
        assert score == 1.0

    def test_empty_record(self):
        assert score_incident({}) == 0.0


class TestScoreNeiss:
    def test_complete_record(self):
        record = {
            "cpsc_case_number": "CASE1", "treatment_date": "2023-06-15",
            "age": 25, "sex": "Male", "body_part": 35,
            "diagnosis": 61, "disposition": 1,
            "product_1": 1234, "narrative": "Cut hand",
            "weight": 45.2,
        }
        score = score_neiss(record)
        assert score == 1.0

    def test_empty_record(self):
        assert score_neiss({}) == 0.0


class TestScorePenalty:
    def test_complete_record(self):
        record = {
            "penalty_id": "PEN1", "firm_name": "ACME",
            "penalty_type": "civil", "penalty_date": "2024-01-01",
            "fine_amount": 100000, "act": "CPSA",
            "recall_number": "24-001", "release_title": "Test",
        }
        score = score_penalty(record)
        assert score == 1.0


class TestScoreImportViolation:
    def test_complete_record(self):
        record = {
            "nov_date": "2024-01-01", "product_name": "Widget",
            "violation_type": "Lead", "citation": "16 CFR 1303",
            "firm_name": "ACME", "country": "China",
            "domestic_action": "Recall", "cbp_action": "SEIZE",
        }
        score = score_import_violation(record)
        assert score == 1.0


class TestComplianceScore:
    def test_clean_manufacturer(self):
        profile = {
            "total_recalls": 0, "total_fines": 0.0,
            "total_incidents": 0, "total_import_violations": 0,
        }
        assert compute_compliance_score(profile) == 1.0

    def test_heavy_violator(self):
        profile = {
            "total_recalls": 20, "total_fines": 15_000_000,
            "total_incidents": 100, "total_import_violations": 60,
        }
        score = compute_compliance_score(profile)
        assert score < 0.1

    def test_moderate_risk(self):
        profile = {
            "total_recalls": 3, "total_fines": 50_000,
            "total_incidents": 10, "total_import_violations": 3,
        }
        score = compute_compliance_score(profile)
        assert 0.3 < score < 0.8


class TestAssignRiskTier:
    def test_low(self):
        assert assign_risk_tier(0.9) == "LOW"

    def test_medium(self):
        assert assign_risk_tier(0.6) == "MEDIUM"

    def test_high(self):
        assert assign_risk_tier(0.4) == "HIGH"

    def test_critical(self):
        assert assign_risk_tier(0.1) == "CRITICAL"

    def test_boundaries(self):
        assert assign_risk_tier(0.8) == "LOW"
        assert assign_risk_tier(0.5) == "MEDIUM"
        assert assign_risk_tier(0.3) == "HIGH"
        assert assign_risk_tier(0.29) == "CRITICAL"
