# CPSC Product Safety Tracker — Methodology

## Data Sources

### 1. CPSC Recalls REST API
- **Source:** Consumer Product Safety Commission
- **URL:** https://www.saferproducts.gov/RestWebServices/Recall
- **Format:** JSON, no authentication required
- **Coverage:** 1973–2026 (9,683 recall campaigns)
- **Key fields:** Recall ID/number, date, title, description, products (names, types, categories, units), hazards, remedies, manufacturers, countries of origin, retailers, importers

### 2. SaferProducts.gov Incident Reports
- **Source:** CPSC Public Consumer Product Safety Information Database
- **URL:** https://www.saferproducts.gov/SPDB.zip (bulk export)
- **Format:** CSV within ZIP (90 MB)
- **Coverage:** 2011–2026 (65,776 reports)
- **Key fields:** Report number, date, product category/subcategory/type/code, manufacturer, brand, model, incident description, victim severity/sex/age, location, state

### 3. NEISS (National Electronic Injury Surveillance System)
- **Source:** CPSC via hospital emergency room sample
- **URL:** https://www.cpsc.gov/cgibin/NEISSQuery/Data/Archived%20Data/{year}/neiss{year}.tsv
- **Format:** TSV, annual files
- **Coverage:** 2019–2024 (2,041,090 sample records, ~76M national estimate)
- **Key fields:** Case number, treatment date, age, sex, body part, diagnosis, disposition, location, product codes (1–3), narrative, statistical weight

### 4. CPSC Penalty API
- **Source:** CPSC enforcement data
- **URLs:** https://www.saferproducts.gov/RestWebServices/Penalty?penaltytype=civil and criminal
- **Format:** JSON, no authentication required
- **Coverage:** 1984–present (365 civil + 30 criminal = 395 penalties)
- **Key fields:** Penalty ID, firm name, penalty type, date, act, fine amount, recall number, product types

### 5. CPSC Import Violations (Notice of Violation)
- **Source:** CPSC compliance testing
- **URL:** https://www.cpsc.gov/s3fs-public/CPSC-NOV-DATA-{date}.xlsx
- **Format:** Excel (multiple sheets, 2.9 MB)
- **Coverage:** Multi-year (13,902 unique violations)
- **Key fields:** NOV date, product name, model, violation type, citation (16 CFR), firm name/address/city, country, domestic/CBP action

## Data Pipeline

### Stage 1: Download & Cache
- All raw data cached in `data/raw/` to avoid re-downloading
- HTTP requests use polite User-Agent with contact email
- 2-second delays between API requests
- Recalls fetched year-by-year (1973–2026) for manageable batches

### Stage 2: Parse & Normalize
- Recalls API: Nested JSON arrays flattened to pipe-delimited strings
- Incidents CSV: Disclaimer row skipped, column name variations mapped via alias dictionary
- NEISS TSV: Tab-delimited, numeric codes resolved to human-readable names
- Penalties JSON: Dollar amounts parsed from formatted strings
- Import Violations Excel: Multi-sheet parsing, date normalization from datetime objects

### Stage 3: Manufacturer Name Normalization
- Uppercase and strip whitespace
- Remove DBA/FKA/AKA clauses
- Remove parenthetical content
- Strip "THE" prefix
- Remove punctuation (preserve spaces and hyphens)
- Expand abbreviations (INTL→INTERNATIONAL, TECH→TECHNOLOGY, MFG→MANUFACTURING)
- Strip legal entity suffixes only (LLC, INC, CORP, LTD, etc.) — 3 passes
- Collapse whitespace

### Stage 4: Quality Scoring
Each record scored 0.0–1.0 based on weighted field completeness:
- **Recalls:** 10 components (manufacturer 15%, hazard 15%, date 10%, etc.)
- **Incidents:** 11 components (manufacturer 15%, description 15%, severity 10%, etc.)
- **NEISS:** 10 components (product 15%, narrative 15%, case number 10%, etc.)
- **Penalties:** 8 components (firm 20%, fine 20%, date 10%, etc.)
- **Import Violations:** 8 components (firm 20%, violation type 15%, product 15%, etc.)

### Stage 5: Cross-Linking
45,563 cross-links created via 4 linking functions:
- **Recalls → Penalties:** Direct recall number match (1.0 confidence) + manufacturer name (0.85)
- **Recalls → Incidents:** Normalized manufacturer name match (0.80)
- **Penalties → Incidents:** Normalized firm name match (0.80)
- **Import Violations → Recalls:** Normalized firm name match (0.80)

### Stage 6: Manufacturer Profile Construction
24,715 unified profiles built by aggregating across all sources:
- Recall counts, units recalled, hazard types, product types
- Incident counts, severities, product categories
- Penalty counts, total fines, penalty types
- Import violation counts, violation types, countries

### Compliance Score Formula
Composite score (0.0–1.0), higher = better compliance:
- **Recall frequency (30%):** 0 recalls = 1.0; 1–2 = 0.7; 3–5 = 0.4; 6–10 = 0.2; 11+ = 0.05
- **Penalty severity (25%):** $0 = 1.0; <$100K = 0.7; <$1M = 0.4; <$10M = 0.2; $10M+ = 0.05
- **Incident volume (25%):** 0 = 1.0; 1–5 = 0.7; 6–20 = 0.4; 21–50 = 0.2; 51+ = 0.05
- **Import violations (20%):** 0 = 1.0; 1–5 = 0.7; 6–20 = 0.4; 21–50 = 0.2; 51+ = 0.05

Risk tiers: LOW (≥0.8), MEDIUM (≥0.5), HIGH (≥0.3), CRITICAL (<0.3)

## Coverage and Limitations

### Known Limitations
- Recalls API returns varying manufacturer name formats (some include city/state in name)
- Incident reports are self-reported by consumers — not independently verified
- NEISS is a probability sample of ~100 hospitals — individual records are weighted for national estimates
- Import violations Excel URL changes periodically with new date stamps
- Cross-linking uses exact normalized name matching only (no fuzzy matching for performance)
- Some manufacturers may appear as separate profiles due to name variations
- Country names in import violations are not standardized (e.g., "China" vs "CHINA")

### Data Quality
- Recall quality average: typically >0.80 (API provides structured data)
- Incident quality varies by submitter category
- NEISS records are highly complete (hospital-recorded data)
- Penalty records are complete (CPSC enforcement data)

## Reproducibility
The entire pipeline is reproducible from public sources:
```bash
python3 -m src.cli pipeline
```

---

Built by Nathan Goldberg | nathanmauricegoldberg@gmail.com
