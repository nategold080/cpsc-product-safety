"""Tests for penalty scraper and parser."""

import pytest
from src.scrapers.penalties import parse_penalty, parse_all_penalties


class TestParsePenalty:
    def test_basic_civil(self):
        raw = {
            "PenaltyID": "123",
            "RecallNo": "24-001",
            "Firm": "ACME Corporation",
            "PenaltyDate": "2024-01-15",
            "Act": "CPSA",
            "Fine": "$1,200,000",
            "FiscalYear": 2024,
            "ReleaseTitle": "ACME Corp Penalty",
            "ReleaseURL": "https://example.com",
            "CompanyID": "C1",
            "ProductTypes": [{"Type": "Toys", "CategoryID": "123"}],
        }
        result = parse_penalty(raw, "civil")
        assert result["penalty_id"] == "123"
        assert result["firm_name"] == "ACME Corporation"
        assert result["normalized_firm"] == "ACME"
        assert result["fine_amount"] == 1200000.0
        assert result["penalty_type"] == "civil"
        assert result["fiscal_year"] == 2024
        assert result["product_types"] == "Toys"

    def test_criminal(self):
        raw = {"PenaltyID": "456", "Firm": "Bad Corp", "Fine": "500000"}
        result = parse_penalty(raw, "criminal")
        assert result["penalty_type"] == "criminal"
        assert result["fine_amount"] == 500000.0

    def test_empty_fine(self):
        raw = {"PenaltyID": "789", "Firm": "Test", "Fine": ""}
        result = parse_penalty(raw, "civil")
        assert result["fine_amount"] == 0.0

    def test_missing_penalty_id(self):
        raw = {"RecallNo": "24-001", "Firm": "Test Corp"}
        result = parse_penalty(raw, "civil")
        assert "civil" in result["penalty_id"]

    def test_quality_score(self):
        raw = {
            "PenaltyID": "100", "Firm": "ACME",
            "PenaltyDate": "2024-01-01", "Fine": "$100,000",
            "Act": "CPSA", "RecallNo": "24-001",
            "ReleaseTitle": "Test",
        }
        result = parse_penalty(raw, "civil")
        assert result["quality_score"] == 1.0


class TestParseAllPenalties:
    def test_combines_civil_and_criminal(self):
        civil = [{"PenaltyID": "1", "Firm": "A", "Fine": "100"}]
        criminal = [{"PenaltyID": "2", "Firm": "B", "Fine": "200"}]
        results = parse_all_penalties(civil, criminal)
        assert len(results) == 2
        types = {r["penalty_type"] for r in results}
        assert types == {"civil", "criminal"}

    def test_empty_inputs(self):
        results = parse_all_penalties([], [])
        assert len(results) == 0
