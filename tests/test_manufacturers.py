"""Tests for manufacturer name normalization."""

import pytest
from src.normalization.manufacturers import (
    normalize_manufacturer, parse_units, extract_fiscal_year,
)


class TestNormalizeManufacturer:
    def test_empty(self):
        assert normalize_manufacturer("") == ""
        assert normalize_manufacturer(None) == ""

    def test_basic_uppercase(self):
        assert normalize_manufacturer("acme corp") == "ACME"

    def test_strips_llc(self):
        assert normalize_manufacturer("ACME LLC") == "ACME"

    def test_strips_inc(self):
        assert normalize_manufacturer("ACME INC.") == "ACME"

    def test_strips_corporation(self):
        assert normalize_manufacturer("ACME CORPORATION") == "ACME"

    def test_strips_limited(self):
        assert normalize_manufacturer("ACME LIMITED") == "ACME"

    def test_strips_multiple_suffixes(self):
        assert normalize_manufacturer("ACME HOLDINGS GROUP LLC") == "ACME"

    def test_preserves_descriptive_words(self):
        result = normalize_manufacturer("ABC CONSULTING LLC")
        assert "CONSULTING" in result

    def test_preserves_services(self):
        result = normalize_manufacturer("XYZ SERVICES INC")
        assert "SERVICES" in result

    def test_preserves_technology(self):
        result = normalize_manufacturer("DEF TECHNOLOGY CORP")
        assert "TECHNOLOGY" in result

    def test_removes_dba(self):
        result = normalize_manufacturer("ACME CORP DBA ACME WIDGETS")
        assert "WIDGETS" not in result
        assert "ACME" in result

    def test_removes_parenthetical(self):
        result = normalize_manufacturer("ACME (USA) INC")
        assert "USA" not in result
        assert "ACME" in result

    def test_removes_the_prefix(self):
        result = normalize_manufacturer("THE ACME COMPANY")
        assert result == "ACME"

    def test_expands_intl(self):
        result = normalize_manufacturer("ABC INTL LLC")
        assert "INTERNATIONAL" in result

    def test_expands_tech(self):
        result = normalize_manufacturer("ABC TECH LLC")
        assert "TECHNOLOGY" in result

    def test_expands_mfg(self):
        result = normalize_manufacturer("ABC MFG CO")
        assert "MANUFACTURING" in result

    def test_strips_punctuation(self):
        result = normalize_manufacturer("ACME, INC.")
        assert "ACME" == result

    def test_collapses_whitespace(self):
        result = normalize_manufacturer("  ACME   PRODUCTS   LLC  ")
        assert "  " not in result
        assert result.startswith("ACME")

    def test_real_names_ikea(self):
        result = normalize_manufacturer("IKEA North America Services LLC")
        assert "IKEA" in result
        assert "NORTH AMERICA SERVICES" in result

    def test_real_names_samsung(self):
        result = normalize_manufacturer("Samsung Electronics America, Inc.")
        assert "SAMSUNG" in result
        assert "ELECTRONICS" in result
        assert "AMERICA" in result

    def test_real_names_amazon(self):
        result = normalize_manufacturer("Amazon.com, Inc.")
        assert "AMAZON" in result

    def test_handles_international_suffixes(self):
        result = normalize_manufacturer("BMW AG")
        assert result == "BMW"

    def test_handles_gmbh(self):
        result = normalize_manufacturer("BOSCH GMBH")
        assert result == "BOSCH"

    def test_hyphenated_name(self):
        result = normalize_manufacturer("SMITH-WESSON LLC")
        assert "SMITH" in result
        assert "WESSON" in result


class TestParseUnits:
    def test_simple_number(self):
        assert parse_units("1000") == 1000

    def test_comma_separated(self):
        assert parse_units("1,200,000") == 1200000

    def test_about_prefix(self):
        assert parse_units("About 5,000") == 5000

    def test_approximately(self):
        assert parse_units("Approximately 3,200") == 3200

    def test_million(self):
        assert parse_units("1.2 million") == 1200000

    def test_thousand(self):
        assert parse_units("5 thousand") == 5000

    def test_empty(self):
        assert parse_units("") is None
        assert parse_units(None) is None

    def test_no_number(self):
        assert parse_units("unknown") is None


class TestExtractFiscalYear:
    def test_iso_date_jan(self):
        assert extract_fiscal_year("2024-01-15") == 2024

    def test_iso_date_oct(self):
        assert extract_fiscal_year("2023-10-15") == 2024

    def test_month_day_year(self):
        assert extract_fiscal_year("January 15, 2024") == 2024

    def test_october(self):
        assert extract_fiscal_year("October 5, 2023") == 2024

    def test_slash_date(self):
        assert extract_fiscal_year("1/15/2024") == 2024

    def test_slash_date_october(self):
        assert extract_fiscal_year("10/15/2023") == 2024

    def test_empty(self):
        assert extract_fiscal_year("") is None
        assert extract_fiscal_year(None) is None

    def test_year_only(self):
        assert extract_fiscal_year("2024") == 2024
