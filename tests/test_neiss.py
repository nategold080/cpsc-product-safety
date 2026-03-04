"""Tests for NEISS data parser."""

import pytest
from src.scrapers.neiss import (
    parse_neiss_tsv, DISPOSITION_CODES, LOCATION_CODES,
    SEX_CODES, BODY_PART_CODES, DIAGNOSIS_CODES,
)


class TestNeissCodeLookups:
    def test_disposition_codes(self):
        assert DISPOSITION_CODES[1] == "Treated/Released"
        assert DISPOSITION_CODES[8] == "Fatality"

    def test_location_codes(self):
        assert LOCATION_CODES[1] == "Home"
        assert LOCATION_CODES[8] == "Place of Recreation/Sports"

    def test_sex_codes(self):
        assert SEX_CODES[1] == "Male"
        assert SEX_CODES[2] == "Female"

    def test_body_part_codes(self):
        assert BODY_PART_CODES[35] == "Hand"
        assert BODY_PART_CODES[75] == "Head"

    def test_diagnosis_codes(self):
        assert DIAGNOSIS_CODES[58] == "Fracture"
        assert DIAGNOSIS_CODES[61] == "Laceration"


class TestParseNeissTsv:
    def test_basic_parsing(self, tmp_path):
        tsv_content = (
            "CPSC_Case_Number\tTreatment_Date\tAge\tSex\tRace\tHispanic\t"
            "Body_Part\tDiagnosis\tOther_Diagnosis\tBody_Part_2\tDiagnosis_2\t"
            "Other_Diagnosis_2\tDisposition\tLocation\tFire_Involvement\t"
            "Product_1\tProduct_2\tProduct_3\tAlcohol\tDrug\t"
            "Narrative_1\tStratum\tPSU\tWeight\n"
            "230001234\t6/15/2023\t25\t1\t2\t0\t"
            "35\t61\t\t0\t0\t"
            "\t1\t1\t0\t"
            "1234\t0\t0\t0\t0\t"
            "CUT HAND WITH KNIFE\tS\t1\t45.2\n"
        )
        tsv_path = tmp_path / "neiss2023.tsv"
        tsv_path.write_text(tsv_content)

        records = parse_neiss_tsv(str(tsv_path))
        assert len(records) == 1
        r = records[0]
        assert r["cpsc_case_number"] == "230001234"
        assert r["age"] == 25
        assert r["sex"] == "Male"
        assert r["body_part"] == 35
        assert r["body_part_name"] == "Hand"
        assert r["diagnosis"] == 61
        assert r["diagnosis_name"] == "Laceration"
        assert r["disposition"] == 1
        assert r["disposition_name"] == "Treated/Released"
        assert r["location"] == 1
        assert r["location_name"] == "Home"
        assert r["product_1"] == 1234
        assert r["narrative"] == "CUT HAND WITH KNIFE"
        assert r["weight"] == 45.2
        assert r["neiss_year"] == 2023
        assert r["quality_score"] > 0

    def test_empty_file(self, tmp_path):
        tsv_path = tmp_path / "neiss2020.tsv"
        tsv_path.write_text("CPSC_Case_Number\tAge\n")
        records = parse_neiss_tsv(str(tsv_path))
        assert len(records) == 0

    def test_missing_case_number_skipped(self, tmp_path):
        tsv_content = (
            "CPSC_Case_Number\tAge\tSex\tBody_Part\tDiagnosis\tDisposition\t"
            "Location\tProduct_1\tNarrative_1\tWeight\tTreatment_Date\n"
            "\t25\t1\t35\t61\t1\t1\t1234\tTest\t45.2\t6/15/2023\n"
            "230002\t30\t2\t76\t58\t1\t1\t5678\tFell\t52.1\t6/16/2023\n"
        )
        tsv_path = tmp_path / "neiss2023.tsv"
        tsv_path.write_text(tsv_content)

        records = parse_neiss_tsv(str(tsv_path))
        assert len(records) == 1
        assert records[0]["cpsc_case_number"] == "230002"

    def test_year_from_filename(self, tmp_path):
        tsv_content = (
            "CPSC_Case_Number\tAge\tSex\tBody_Part\tDiagnosis\tDisposition\t"
            "Location\tProduct_1\tNarrative_1\tWeight\tTreatment_Date\n"
            "240001\t25\t1\t35\t61\t1\t1\t1234\tTest\t45.2\t3/1/2024\n"
        )
        tsv_path = tmp_path / "neiss2024.tsv"
        tsv_path.write_text(tsv_content)

        records = parse_neiss_tsv(str(tsv_path))
        assert records[0]["neiss_year"] == 2024
