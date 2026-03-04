# CPSC Product Safety — Data Enrichment Plan

## Overview
This enrichment transforms the CPSC tracker from a "recall and incident database" into a **comprehensive product safety intelligence platform** by resolving opaque NEISS product codes (unlocking 2M+ injury records), integrating FDA adverse event data for cross-agency analysis, mapping hazard types to actual injury diagnoses, and adding FTC enforcement for deceptive safety claims.

**Current state:** 9,683 recalls, 65,776 incidents, 2,041,090 NEISS injuries, 395 penalties, 13,902 import violations, 24,715 manufacturer profiles — but NEISS product codes are opaque numeric IDs (cannot interpret injury data by product type), ZERO cross-agency data, and no hazard-to-injury validation.

**Target state:** Human-readable NEISS product categories, FDA FAERS cross-linked adverse events, hazard-to-diagnosis validation mapping, and FTC enforcement data revealing deceptive safety claims.

---

## Enrichment 1: NEISS Product Code Resolution (HIGHEST PRIORITY — Unlocks 2M Records)

### What It Adds
- Human-readable product names for all 2,041,090 NEISS injury records
- Currently, NEISS records contain numeric product codes (1-2000+) but NO product descriptions
- Product category analysis: "Which product types cause the most injuries?"
- Cross-link: NEISS product categories to recall product types for same manufacturer
- Example insight: "Whirlpool's 3,578 incidents — 892 are washing machine injuries (product code 1245)"

### Data Source
- **CPSC NEISS Coding Manual / Comparability Table** — CPSC publishes the product code-to-name mapping
- URL: `https://www.cpsc.gov/Research--Statistics/NEISS-Injury-Data/` (look for "NEISS Coding Manual" or "Product Code Comparability Table")
- The mapping has ~2,000 product codes organized hierarchically
- Format: PDF (requires parsing) or potentially available as structured data from CPSC
- Alternative: Extract product names from NEISS query tool responses at `https://www.cpsc.gov/cgibin/NEISSQuery/`

### Product Code Structure
NEISS product codes follow a hierarchical pattern:
```
0100-0199: Sports & Recreation Equipment
0200-0299: Home Structures & Construction Materials
0300-0399: Home Furnishings & Fixtures
0400-0499: Personal Use Items
0500-0599: Packaging & Containers
0600-0699: Toys
0700-0799: Tools & Workshop Equipment
0800-0899: General Household Appliances
0900-0999: Space Heating, Cooling & Ventilation
1000-1099: Home Communication, Entertainment & Hobby
1100-1199: Yard & Garden Equipment
1200-1299: Home/Family Maintenance
1300-1399: General Apparel
1400-1499: Footwear
1500-1599: Personal Accessories
1600-1699: Not Used
1700-1799: Chemicals & Chemical Products
1800-1899: Child Nursery Equipment & Supplies
```

### Database Schema

```sql
CREATE TABLE neiss_product_codes (
    product_code INTEGER PRIMARY KEY,
    product_name TEXT NOT NULL,
    product_category TEXT,            -- Top-level category (Sports, Home, Toys, etc.)
    product_subcategory TEXT,         -- Sub-category
    is_child_related INTEGER DEFAULT 0,
    is_outdoor INTEGER DEFAULT 0,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX idx_product_category ON neiss_product_codes(product_category);
CREATE INDEX idx_product_child ON neiss_product_codes(is_child_related);
```

### Implementation Steps
1. Obtain NEISS product code mapping (try CPSC website first, then manual PDF parsing)
2. Build `config/neiss_product_codes.yaml` with all ~2,000 product code mappings
3. Load into `neiss_product_codes` table
4. Join NEISS injury records to product codes for human-readable analysis
5. Add product category breakdown to manufacturer profiles
6. Add "Product Injury Analysis" dashboard tab:
   - Top 20 product categories by injury count
   - Product category × diagnosis heatmap
   - Manufacturer × product category injury breakdown
   - Weighted national injury estimates by product category
7. Update quality scoring: add `has_product_name_resolved` component for NEISS records
8. Add tests for product code lookup and category aggregation

### Expected Impact
- Resolves ALL 2,041,090 NEISS records from opaque codes to readable product names
- Enables: "Bicycles cause 500K estimated injuries/year" type insights
- Enables manufacturer-to-product-injury cross-tabulation (currently impossible)
- Makes NEISS data actually interpretable and queryable by product type

---

## Enrichment 2: FDA Adverse Event Reporting System (FAERS) Integration (HIGH PRIORITY)

### What It Adds
- FDA adverse event reports for consumer product-adjacent categories
- Cross-agency analysis: products with BOTH CPSC recalls AND FDA adverse events
- Manufacturer accountability across federal agencies
- Categories: cosmetics, dietary supplements, food contact materials, medical devices used in home
- Signal: manufacturers appearing in BOTH systems have systemic safety culture issues

### Data Source
- **OpenFDA API** — Free, well-documented, no authentication required
- Base URL: `https://api.fda.gov/`
- Endpoints:
  - Device adverse events: `https://api.fda.gov/device/event.json`
  - Drug adverse events: `https://api.fda.gov/drug/event.json` (for OTC consumer products)
  - Food adverse events: `https://api.fda.gov/food/event.json`
  - Device recalls: `https://api.fda.gov/device/recall.json`
- Rate limit: 240 requests/minute without API key, 120K/day with free API key
- Format: JSON responses with pagination
- Volume: 18M+ total events; filter to consumer product-relevant categories

### Filtering Strategy
Focus on categories that overlap with CPSC jurisdiction:
```python
FDA_CONSUMER_PRODUCT_CATEGORIES = [
    # Device adverse events (home-use medical devices)
    'home-use',
    'over-the-counter',
    # Food/cosmetic events
    'cosmetics',
    'dietary supplements',
    'food contact substances',
    # Device recalls (consumer electronics, home health)
    'consumer',
    'household',
]
```

### Database Schema

```sql
CREATE TABLE fda_adverse_events (
    event_id TEXT PRIMARY KEY,
    report_date TEXT,
    product_name TEXT,
    product_type TEXT,                -- 'device', 'drug_otc', 'food', 'cosmetic'
    manufacturer_name TEXT,
    manufacturer_normalized TEXT,
    event_type TEXT,                  -- 'injury', 'malfunction', 'death'
    patient_outcome TEXT,             -- 'hospitalization', 'disability', 'death', etc.
    description TEXT,
    source TEXT,                      -- 'consumer', 'healthcare_professional', 'manufacturer'
    quality_score REAL,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE fda_device_recalls (
    recall_id TEXT PRIMARY KEY,
    product_description TEXT,
    reason_for_recall TEXT,
    manufacturer_name TEXT,
    manufacturer_normalized TEXT,
    recall_class TEXT,                -- 'Class I' (most serious), 'Class II', 'Class III'
    recall_status TEXT,
    event_date TEXT,
    quality_score REAL,
    created_at TEXT DEFAULT (datetime('now'))
);

-- Cross-agency manufacturer links
CREATE TABLE cpsc_fda_manufacturer_links (
    cpsc_manufacturer TEXT NOT NULL,
    fda_manufacturer TEXT NOT NULL,
    link_method TEXT,                 -- 'exact', 'fuzzy', 'manual'
    confidence REAL,
    PRIMARY KEY (cpsc_manufacturer, fda_manufacturer)
);

CREATE INDEX idx_fda_events_mfr ON fda_adverse_events(manufacturer_normalized);
CREATE INDEX idx_fda_events_type ON fda_adverse_events(product_type);
CREATE INDEX idx_fda_events_date ON fda_adverse_events(report_date);
CREATE INDEX idx_fda_recalls_mfr ON fda_device_recalls(manufacturer_normalized);
CREATE INDEX idx_fda_recalls_class ON fda_device_recalls(recall_class);
```

### Implementation Steps
1. Build `src/scrapers/fda_downloader.py` — query OpenFDA API endpoints
2. Filter to consumer product-relevant categories
3. Apply same manufacturer name normalization as CPSC (strip suffixes, normalize case)
4. Cross-link FDA manufacturers to CPSC manufacturers via normalized name matching
5. Build `cpsc_fda_manufacturer_links` using existing normalization infrastructure
6. Add "Cross-Agency Analysis" dashboard tab:
   - Manufacturers appearing in BOTH CPSC and FDA systems
   - FDA adverse event trends by product type
   - Cross-agency recall comparison (CPSC vs FDA device recalls)
   - "Multi-Agency Risk Score" combining CPSC + FDA signals
7. Update manufacturer profiles with FDA event counts and cross-agency flags
8. Add tests for FDA API parsing, manufacturer matching, cross-agency aggregation

### Expected Impact
- +50,000-100,000 FDA adverse event records (filtered to consumer products)
- +5,000-10,000 FDA device recalls
- Cross-agency manufacturer linking for multi-signal risk assessment
- New narrative: "Manufacturers with safety issues across BOTH CPSC and FDA"

---

## Enrichment 3: Hazard-to-Diagnosis Validation Mapping (MEDIUM PRIORITY)

### What It Adds
- Structured mapping between recall hazard types and NEISS injury diagnoses
- Validates recall claims: "Company said product causes 'fire hazard' — NEISS data shows thermal burns for same manufacturer"
- Hazard prediction: which hazard types lead to which injury patterns?
- Root cause analysis: do certain product defects cause unexpected injury types?

### Mapping Logic
Build a reference table linking recall hazard categories to expected NEISS diagnosis codes:

```python
HAZARD_TO_DIAGNOSIS_MAP = {
    'Fire Hazard': {
        'expected_diagnoses': ['Burns, thermal', 'Burns, chemical', 'Smoke inhalation'],
        'neiss_codes': [41, 42, 43, 48, 49],  # Burn diagnosis codes
    },
    'Electrical Shock Hazard': {
        'expected_diagnoses': ['Burns, electrical', 'Electrocution', 'Electric shock'],
        'neiss_codes': [44, 46],
    },
    'Laceration Hazard': {
        'expected_diagnoses': ['Laceration', 'Amputation', 'Avulsion'],
        'neiss_codes': [59, 60, 61],
    },
    'Choking Hazard': {
        'expected_diagnoses': ['Aspiration', 'Ingestion', 'Asphyxiation', 'Submersion'],
        'neiss_codes': [68, 69, 70],
    },
    'Fall Hazard': {
        'expected_diagnoses': ['Fracture', 'Contusion', 'Concussion', 'Strain/sprain'],
        'neiss_codes': [57, 52, 62, 64],
    },
    'Entrapment Hazard': {
        'expected_diagnoses': ['Crushing', 'Amputation', 'Contusion'],
        'neiss_codes': [53, 60, 52],
    },
    'Poisoning Hazard': {
        'expected_diagnoses': ['Poisoning', 'Chemical burn', 'Ingestion'],
        'neiss_codes': [65, 43, 69],
    },
    'Tip-Over Hazard': {
        'expected_diagnoses': ['Fracture', 'Contusion', 'Concussion', 'Internal organ injury'],
        'neiss_codes': [57, 52, 62, 63],
    },
}
```

### Database Schema

```sql
CREATE TABLE hazard_diagnosis_map (
    hazard_type TEXT NOT NULL,
    neiss_diagnosis_code INTEGER NOT NULL,
    diagnosis_name TEXT,
    expected_match INTEGER DEFAULT 1,  -- 1 = expected, 0 = unexpected
    PRIMARY KEY (hazard_type, neiss_diagnosis_code)
);

-- Validation results: do NEISS injuries match recall hazards for same manufacturer?
CREATE TABLE hazard_validation_results (
    manufacturer_normalized TEXT NOT NULL,
    hazard_type TEXT NOT NULL,
    total_recalls_with_hazard INTEGER,
    total_neiss_injuries INTEGER,
    matching_diagnosis_count INTEGER,    -- Injuries matching expected diagnosis
    unexpected_diagnosis_count INTEGER,  -- Injuries NOT matching expected diagnosis
    match_rate REAL,                     -- matching / total
    validation_status TEXT,              -- 'confirmed', 'unexpected_pattern', 'insufficient_data'
    PRIMARY KEY (manufacturer_normalized, hazard_type)
);

CREATE INDEX idx_validation_mfr ON hazard_validation_results(manufacturer_normalized);
CREATE INDEX idx_validation_status ON hazard_validation_results(validation_status);
```

### Implementation Steps
1. Build `config/hazard_diagnosis_map.yaml` with the mapping table
2. Load into `hazard_diagnosis_map` table
3. For each manufacturer with recalls: extract hazard types from recall data
4. Cross-reference against NEISS injuries for same manufacturer (via product code + manufacturer match)
5. Compute match rate: what % of injuries match the expected diagnosis for the stated hazard?
6. Flag manufacturers with "unexpected_pattern" (stated hazard doesn't match actual injuries)
7. Add "Hazard Validation" dashboard section:
   - Hazard type → diagnosis heatmap
   - Manufacturers with highest unexpected injury patterns
   - Validation rate by product category
8. Add tests for mapping logic and validation calculations

### Expected Impact
- Validation layer for ALL 9,683 recalls
- Identifies manufacturers whose stated hazards don't match actual injury patterns
- Enables investigative journalism: "Company says fire hazard, but injuries are lacerations — what's really happening?"

---

## Enrichment 4: FTC Enforcement Actions for Deceptive Safety Claims (LOWER PRIORITY)

### What It Adds
- FTC enforcement actions against manufacturers for deceptive product safety claims
- Cross-link: manufacturers with BOTH CPSC recalls AND FTC deceptive marketing cases
- Pattern detection: companies marketing "safe" products that later get recalled
- Settlement amounts and consent decree terms

### Data Source
- **FTC Cases and Proceedings** — publicly searchable
- URL: `https://www.ftc.gov/legal-library/browse/cases-proceedings`
- Format: HTML pages with structured case metadata
- Filter by: consumer protection, advertising, product safety claims
- Volume: ~200-500 relevant cases (product safety + deceptive claims)
- Alternative: FTC data feed or bulk download if available

### Database Schema

```sql
CREATE TABLE ftc_enforcement (
    case_id TEXT PRIMARY KEY,
    case_name TEXT,
    defendant_names TEXT,              -- JSON array of company names
    defendant_normalized TEXT,         -- Primary defendant normalized
    case_type TEXT,                    -- 'deceptive_advertising', 'product_safety', 'unfair_practices'
    violation_description TEXT,
    settlement_amount REAL,
    consent_decree INTEGER DEFAULT 0,
    case_date TEXT,
    case_status TEXT,                  -- 'settled', 'pending', 'dismissed'
    linked_manufacturer TEXT,          -- FK to manufacturer_profiles
    quality_score REAL,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX idx_ftc_defendant ON ftc_enforcement(defendant_normalized);
CREATE INDEX idx_ftc_type ON ftc_enforcement(case_type);
CREATE INDEX idx_ftc_date ON ftc_enforcement(case_date);
CREATE INDEX idx_ftc_linked ON ftc_enforcement(linked_manufacturer);
```

### Implementation Steps
1. Build `src/scrapers/ftc_downloader.py` — scrape FTC cases and proceedings
2. Filter to product safety, deceptive advertising, and consumer protection categories
3. Extract defendant names, settlement amounts, case types
4. Normalize defendant names and cross-link to CPSC manufacturer profiles
5. Add "Regulatory Cross-Reference" dashboard section
6. Update manufacturer profiles with FTC enforcement flags
7. Add tests

### Expected Impact
- +200-500 FTC enforcement records
- Cross-agency linking: CPSC + FDA + FTC for comprehensive manufacturer risk
- Deceptive marketing detection: "Said safe, got recalled"

---

## Dashboard Enhancements

### New Tabs to Add
1. **Product Injury Analysis** — NEISS injuries by resolved product category, top products by weighted injury estimate, product × diagnosis heatmap
2. **Cross-Agency Analysis** — Manufacturers in CPSC + FDA, cross-agency risk scores, comparative recall analysis
3. **Hazard Validation** — Recall hazard types vs actual injuries, unexpected pattern alerts, validation rates
4. **Regulatory Overview** — Combined CPSC + FDA + FTC enforcement by manufacturer

### Enhanced Existing Tabs
- **Manufacturer Profiles** — Add: product category injuries, FDA event count, FTC enforcement, cross-agency risk score
- **National Overview** — Add KPIs: product categories tracked, FDA adverse events, cross-agency manufacturers
- **Recall Explorer** — Add: hazard validation status, matching NEISS injury data

---

## Updated Quality Scoring

```python
QUALITY_WEIGHTS = {
    'has_manufacturer_name': 0.10,
    'has_product_description': 0.10,
    'has_hazard_type': 0.08,
    'has_remedy_description': 0.08,
    'has_date': 0.06,
    'has_affected_count': 0.06,
    'has_product_code_resolved': 0.10,   # NEW — NEISS product name available
    'has_cross_agency_data': 0.10,        # NEW — FDA match exists
    'has_hazard_validation': 0.08,        # NEW — hazard-diagnosis validated
    'has_penalties_data': 0.06,
    'has_incident_data': 0.08,
    'has_source_url': 0.05,
    'has_state': 0.05,
}
```

---

## Export Updates
- neiss_product_codes.csv — Product code lookup table
- fda_adverse_events.csv — FDA adverse events (consumer product subset)
- fda_device_recalls.csv — FDA device recalls
- hazard_validation.csv — Hazard-to-diagnosis validation results
- ftc_enforcement.csv — FTC deceptive safety claim cases
- Updated manufacturer_profiles.csv with cross-agency fields
- Updated summary.md with cross-agency statistics

---

## Test Requirements (Target: 50+ new tests)
- NEISS product code lookup and resolution
- Product category hierarchy parsing
- FDA API response parsing (device events, drug events, food events, device recalls)
- FDA manufacturer name normalization and CPSC cross-linking
- Hazard-to-diagnosis mapping logic
- Validation rate calculations
- FTC case parsing
- Cross-agency manufacturer matching
- Dashboard data queries for new sections
- Export format validation
- Weighted national estimate calculations with product categories

---

## Priority Order
1. **NEISS Product Codes** — Unlocks 2M+ records, highest single-item impact, relatively easy
2. **FDA FAERS Integration** — Adds whole new agency, well-documented free API
3. **Hazard-to-Diagnosis Mapping** — Validates existing data, creates unique analytical capability
4. **FTC Enforcement** — Adds deceptive marketing dimension, smaller dataset but high narrative value

---

## Expected Outcome
- From "recall and incident database" to "product safety intelligence platform"
- NEISS product codes make 2M+ injury records actually usable (currently opaque)
- Cross-agency linking: CPSC + FDA + FTC for multi-signal manufacturer risk assessment
- Hazard validation creates unique analytical capability nobody else offers
- High-value for: product liability lawyers, consumer advocacy groups, insurance underwriters, investigative journalists, CPSC researchers, retail buyers
