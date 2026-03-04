# CPSC Product Safety Tracker — Project Documentation

## Overview
Cross-linked database of consumer product safety data from 5 CPSC sources: recalls (9,683), incident reports (65,776), NEISS emergency room injuries (2,041,090), penalties (395), and import violations (13,902). Includes 24,715 manufacturer compliance profiles with composite risk scores.

## Architecture

### Data Pipeline
```
CPSC Recalls API JSON → parse_recall_api_record() → upsert_recalls_batch()
SaferProducts ZIP/CSV → parse_incidents() → upsert_incidents_batch()
NEISS TSV → parse_neiss_tsv() → insert_neiss_batch()
Penalty API JSON → parse_all_penalties() → upsert_penalties_batch()
Import Violations XLSX → parse_violations() → insert_import_violations_batch()
→ build_cross_links() → build_manufacturer_profiles() → export_all()
```

### Database Schema (SQLite, WAL mode)
- `recalls` — 9,683 records, PK: recall_id
- `incidents` — 65,776 records, PK: report_number
- `neiss_injuries` — 2,041,090 records, PK: injury_id (auto)
- `penalties` — 395 records, PK: penalty_id
- `import_violations` — 13,902 records, PK: violation_id (auto)
- `manufacturer_profiles` — 24,715 records, PK: profile_id (auto)
- `cross_links` — 45,563 records
- `neiss_product_codes` — reference table

### Key Modules
- `src/scrapers/recalls.py` — CPSC Recalls REST API + bulk CSV
- `src/scrapers/incidents.py` — SaferProducts.gov bulk ZIP/CSV download and parsing
- `src/scrapers/neiss.py` — NEISS TSV download and parsing (2019-2024)
- `src/scrapers/penalties.py` — CPSC civil + criminal penalty API
- `src/scrapers/violations.py` — Import violations Excel parsing (multi-sheet)
- `src/normalization/manufacturers.py` — Name normalization, units parsing, fiscal year
- `src/normalization/cross_linker.py` — Cross-linking engine and profile builder
- `src/validation/quality.py` — Quality scoring (per-record) and compliance scoring (per-manufacturer)
- `src/storage/database.py` — SQLite schema, upsert functions, stats
- `src/export/exporter.py` — CSV, JSON, Markdown exports
- `src/cli.py` — Click CLI with pipeline, scrape-*, crosslink, export, stats, dashboard

### Technical Notes
- SaferProducts.gov incidents CSV has a disclaimer row before the header — parser skips it
- Recalls API returns nested JSON arrays — flattened to pipe-delimited strings
- NEISS uses statistical weights for national injury estimates (multiply weight × count)
- Import violations Excel URL changes periodically (dated filenames)
- Manufacturer names in recalls often include city/state suffix
- CORP_SUFFIXES limited to legal entity designations only (not descriptive words)
- Cross-linking uses O(N) exact normalized name matching (hash lookup)

## CLI Commands
```bash
python3 -m src.cli init              # Initialize database
python3 -m src.cli scrape-recalls    # Download recalls from API
python3 -m src.cli scrape-incidents  # Download incident reports
python3 -m src.cli scrape-neiss      # Download NEISS injury data
python3 -m src.cli scrape-penalties  # Download penalties
python3 -m src.cli scrape-violations # Download import violations
python3 -m src.cli crosslink         # Build cross-links and profiles
python3 -m src.cli export            # Generate exports
python3 -m src.cli stats             # Show statistics
python3 -m src.cli pipeline          # Run everything
python3 -m src.cli dashboard         # Launch Streamlit
```

## Data Source URLs
- Recalls API: `https://www.saferproducts.gov/RestWebServices/Recall?format=json`
- Incidents: `https://www.saferproducts.gov/SPDB.zip`
- NEISS: `https://www.cpsc.gov/cgibin/NEISSQuery/Data/Archived%20Data/{year}/neiss{year}.tsv`
- Penalties: `https://www.saferproducts.gov/RestWebServices/Penalty?penaltytype={civil|criminal}&format=json`
- Import Violations: `https://www.cpsc.gov/s3fs-public/CPSC-NOV-DATA-{date}.xlsx`
