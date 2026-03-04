# Problems Tracker — CPSC Product Safety Tracker

## P1: SaferProducts CSV disclaimer row before header — DONE
The incidents CSV starts with a disclaimer line before the actual column headers. Parser now scans for the header row starting with "Report No" before initializing the DictReader.

## P2: DictReader None key with extra CSV values — DONE
When CSV rows have more values than headers, DictReader stores extras under a None key as a list. Fixed by skipping None keys and handling list values via str join.

## P3: Import violations Excel URL changes with date — DONE
The CPSC NOV data file URL includes a date stamp that changes. Parser tries multiple URL patterns and caches the download. Currently using 2026-02-19 version with 39,261 records across 6 sheets. 13,902 unique after dedup.

## P4: Manufacturer names in recalls include location suffix — OPEN
Many recall API records include the manufacturer's city/state in the name field (e.g., "Polaris Industries Inc., of Medina, Minn."). The normalization strips punctuation and legal suffixes but the location text persists, leading to duplicate profiles for same manufacturer with different location text.

## P5: NEISS product codes not resolved to names — DONE
Parsed the 2025 NEISS Product Code Comparability Table PDF using pdfplumber. Extracted 1,721 product codes (848 active, 873 deleted) with categories, child-related flags, and outdoor flags. Loaded into `neiss_product_codes` table and updated 2,031,804 NEISS records with resolved product names. Added Product Injury Analysis dashboard tab, 18 new tests, YAML config at `config/neiss_product_codes.yaml`.

## P6: Country names in import violations not standardized — OPEN
Import violations have inconsistent country names (e.g., "China" vs "CHINA" vs "china"). Could normalize to uppercase but some entries have non-standard formats. Minor impact on analysis since most are clearly identifiable.

## P7: FDA device events API query syntax — DONE
OpenFDA uses `(field:value)+(field:value)` syntax for AND queries, not `+AND+`. Initial queries returned 0 results. Fixed search queries to use correct parenthesized syntax. Also fixed `_normalize_outcome` to handle list-type `patient_outcome` fields (API returns lists, not comma-separated strings).

## P8: Hazard validation used wrong column (hazard_types vs hazard_description) — DONE
`build_hazard_validation` was querying `hazard_types` which is always empty in the CPSC recalls API data. The actual hazard data is in `hazard_description`. Fixed to use `hazard_description`. Now produces 245 validation records across 14 hazard types.

## P9: FTC enforcement data not accessible — BLOCKED
FTC Cases & Proceedings page (ftc.gov/legal-library/browse/cases-proceedings) is JavaScript-rendered with aggressive bot protection. Returns 478 bytes to automated requests (vs 1MB+ in a real browser). No JSON API, no RSS feed, no bulk data download available on data.gov. Enrichment 4 (FTC Enforcement Actions) cannot proceed without either: (a) a browser automation tool (Playwright/Selenium), or (b) a manual CSV export from the FTC site. Logged as BLOCKED — lower priority enrichment, project already client-ready without it.
