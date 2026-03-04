"""Tests for recalls scraper and parser."""

import pytest
from src.scrapers.recalls import parse_recall_api_record, _join_nested


class TestJoinNested:
    def test_empty_list(self):
        assert _join_nested([], "Name") == ""

    def test_single_item(self):
        assert _join_nested([{"Name": "Widget"}], "Name") == "Widget"

    def test_multiple_items(self):
        result = _join_nested([{"Name": "A"}, {"Name": "B"}], "Name")
        assert result == "A | B"

    def test_missing_field(self):
        result = _join_nested([{"Other": "X"}], "Name")
        assert result == ""

    def test_none_items(self):
        assert _join_nested(None, "Name") == ""


class TestParseRecallApiRecord:
    def test_basic_record(self):
        raw = {
            "RecallID": 12345,
            "RecallNumber": "24-074",
            "RecallDate": "2024-03-15",
            "Title": "Test Recall",
            "Description": "Test description",
            "URL": "https://example.com",
            "Products": [{"Name": "Widget", "Type": "Toy", "NumberOfUnits": "1,000"}],
            "Hazards": [{"Name": "Choking hazard", "HazardType": "Choking"}],
            "Remedies": [{"Name": "Full refund"}],
            "Manufacturers": [{"Name": "ACME Corp"}],
            "ManufacturerCountries": [{"Country": "China"}],
            "Retailers": [{"Name": "Walmart"}],
            "Images": [],
        }
        result = parse_recall_api_record(raw)
        assert result["recall_id"] == "12345"
        assert result["recall_number"] == "24-074"
        assert result["recall_date"] == "2024-03-15"
        assert result["product_names"] == "Widget"
        assert result["hazard_description"] == "Choking hazard"
        assert result["manufacturer_names"] == "ACME Corp"
        assert result["normalized_manufacturer"] == "ACME"
        assert result["units_numeric"] == 1000
        assert result["quality_score"] > 0

    def test_empty_record(self):
        result = parse_recall_api_record({})
        assert result["recall_id"] == ""
        assert result["quality_score"] == 0.0

    def test_multiple_manufacturers(self):
        raw = {
            "RecallID": 1,
            "RecallNumber": "24-001",
            "Manufacturers": [
                {"Name": "ACME Corp"},
                {"Name": "XYZ Industries"},
            ],
        }
        result = parse_recall_api_record(raw)
        assert "ACME Corp" in result["manufacturer_names"]
        assert "XYZ Industries" in result["manufacturer_names"]
        # Normalized uses first manufacturer
        assert result["normalized_manufacturer"] == "ACME"

    def test_units_parsing(self):
        raw = {
            "RecallID": 2,
            "RecallNumber": "24-002",
            "Products": [{"Name": "Toy", "NumberOfUnits": "About 1.2 million"}],
        }
        result = parse_recall_api_record(raw)
        assert result["units_numeric"] == 1200000

    def test_fiscal_year_extraction(self):
        raw = {
            "RecallID": 3,
            "RecallNumber": "24-003",
            "RecallDate": "2023-11-15",
        }
        result = parse_recall_api_record(raw)
        assert result["fiscal_year"] == 2024  # November is FY+1


class TestParseRecallsCsv:
    """Tests for CSV parsing (requires mock data)."""

    def test_csv_record_parsing(self, tmp_path):
        """Test parsing a mock CSV file."""
        from src.scrapers.recalls import parse_recalls_csv

        csv_content = (
            'Recall Number,Product Safety Warning Number,Date,Product Safety Warning Date,'
            'Recall Heading,Name of product,Description,Hazard Description,Consumer Action,'
            'Original Product Safety Warning Announcement,Remedy Type,Units,Incidents,Remedy,'
            'Sold At Label,Sold At,Importers,Manufacturers,Distributors,Manufactured In,'
            'Custom Label,Custom Field\n'
            '24-001,,January 15 2024,,Test Recall,Widget,A toy widget,'
            'Choking hazard,Stop using,,,1000,5,Full refund,,Walmart,,'
            'ACME Corp,,China,,\n'
        )
        csv_path = tmp_path / "test.csv"
        csv_path.write_text(csv_content)

        records = parse_recalls_csv(str(csv_path))
        assert len(records) == 1
        assert records[0]["recall_number"] == "24-001"
        assert records[0]["normalized_manufacturer"] == "ACME"
