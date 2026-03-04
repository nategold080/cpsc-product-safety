"""Tests for incident report parser."""

import pytest
from src.scrapers.incidents import _normalize_column, parse_incidents


class TestNormalizeColumn:
    def test_direct_match(self):
        assert _normalize_column("Report No.") == "report_number"

    def test_case_insensitive(self):
        assert _normalize_column("REPORT NO.") == "report_number"

    def test_manufacturer_name(self):
        assert _normalize_column("Manufacturer/Importer/Private Labeler Name") == "manufacturer_name"

    def test_victim_severity(self):
        assert _normalize_column("(Primary) Victim Severity") == "severity"

    def test_unknown_column(self):
        result = _normalize_column("Custom Field")
        assert result == "custom_field"


class TestParseIncidents:
    def test_basic_csv(self, tmp_path):
        csv_content = (
            'Report No.,Report Date,Publication Date,Category of Submitter,'
            'Product Description,Product Category,Product Sub Category,Product Type,'
            'Product Code,Manufacturer/Importer/Private Labeler Name,Brand Name,'
            'Model Name or Number,Serial Number,UPC,Date Manufactured,Retailer,'
            'Retailer State,Purchase Date,Incident Description,City,State,Zip,'
            'Location,(Primary) Victim Severity,Victim\'s Sex,Victim\'s Age,'
            'Company Comments,Associated Report Numbers\n'
            'INC001,2024-03-01,2024-03-15,Consumer,Widget toy,Toys,,,'
            '1234,ACME Corp,Acme,W100,,,,,,,"Broke apart and child choked",'
            'Denver,CO,80202,Home,Injury,Female,3,,,\n'
        )
        csv_path = tmp_path / "incidents.csv"
        csv_path.write_text(csv_content)

        records = parse_incidents(str(csv_path))
        assert len(records) == 1
        assert records[0]["report_number"] == "INC001"
        assert records[0]["manufacturer_name"] == "ACME Corp"
        assert records[0]["normalized_manufacturer"] == "ACME"
        assert records[0]["severity"] == "Injury"
        assert records[0]["quality_score"] > 0

    def test_empty_csv(self, tmp_path):
        csv_path = tmp_path / "empty.csv"
        csv_path.write_text("Report No.,Report Date\n")
        records = parse_incidents(str(csv_path))
        assert len(records) == 0

    def test_missing_report_number_skipped(self, tmp_path):
        csv_content = (
            'Report No.,Report Date,Product Description\n'
            ',2024-01-01,Widget\n'
            'INC002,2024-01-02,Gadget\n'
        )
        csv_path = tmp_path / "test.csv"
        csv_path.write_text(csv_content)

        records = parse_incidents(str(csv_path))
        assert len(records) == 1
        assert records[0]["report_number"] == "INC002"

    def test_quality_scoring(self, tmp_path):
        csv_content = (
            'Report No.,Report Date,Product Description,Product Category,'
            'Manufacturer/Importer/Private Labeler Name,Incident Description,'
            'State,(Primary) Victim Severity,Product Code,Brand Name,Model Name or Number\n'
            'INC003,2024-01-01,Widget,Toys,ACME,Broke,CO,Injury,1234,Acme,W100\n'
        )
        csv_path = tmp_path / "test.csv"
        csv_path.write_text(csv_content)

        records = parse_incidents(str(csv_path))
        assert records[0]["quality_score"] == 1.0
