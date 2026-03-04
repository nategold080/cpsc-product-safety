# CPSC Product Safety Tracker
### Cross-linked database of consumer product recalls, injuries, penalties, and import violations

---

## The Problem
CPSC publishes product safety data across five separate systems — recalls, incident reports, emergency room injuries, penalties, and import violations. No single view connects a manufacturer's recall history to their penalty record, consumer complaints, ER injury data, and border inspection failures. This makes it impossible to identify systematic safety risks or hold repeat offenders accountable.

## The Solution
A unified database that cross-links all five CPSC data sources into manufacturer compliance profiles with composite risk scores.

## By the Numbers

| Metric | Value |
|--------|-------|
| Recall Campaigns | 9,683 (1973–2026) |
| Consumer Incident Reports | 65,776 |
| ER Injury Records (NEISS) | 2,041,090 sample / 76.2M national est. |
| Civil/Criminal Penalties | 395 ($363M total fines) |
| Import Violations | 13,902 |
| Manufacturer Profiles | 24,715 |
| Cross-Links | 45,563 |
| High/Critical Risk Manufacturers | 14 |

## Sample Findings

**Highest-risk manufacturers (composite compliance score):**
- Target: 12 recalls, 261 incidents, $600K fines — Score: 0.22 (Critical)
- Black & Decker: 13 recalls, 170 incidents, $3.3M fines — Score: 0.25 (Critical)
- Polaris Industries: 55 recalls (most of any company), $27.3M total fines
- Whirlpool: 3,578 incident reports, only 3 recalls — potential under-reporting

**Import violation patterns:**
- China: 7,268 violations (52% of total)
- USA domestic: 1,357 violations
- Hong Kong: 331 violations

## Compliance Score Methodology
Each manufacturer scored 0.0–1.0 based on four weighted components:
- Recall frequency (30%) — Number of recall campaigns
- Penalty severity (25%) — Total fines assessed
- Incident volume (25%) — Consumer-reported incidents
- Import violations (20%) — Border inspection failures

Risk tiers: LOW (≥0.8) · MEDIUM (≥0.5) · HIGH (≥0.3) · CRITICAL (<0.3)

## Deliverables
- SQLite database with full cross-linking
- CSV/JSON exports of all tables
- Interactive Streamlit dashboard (8 sections)
- Manufacturer risk profiles with compliance scores
- Full methodology documentation

## Data Sources
All data from public CPSC sources — no licensing restrictions:
1. CPSC Recalls REST API
2. SaferProducts.gov consumer incident database
3. NEISS (National Electronic Injury Surveillance System)
4. CPSC Civil & Criminal Penalty API
5. CPSC Import Violations (Notice of Violation data)

---

**Built by Nathan Goldberg**
nathanmauricegoldberg@gmail.com · [LinkedIn](https://linkedin.com/in/nathanmauricegoldberg)
