# CPSC Product Safety Tracker

Cross-linked database of consumer product recalls, incident reports, emergency room injuries, penalties, and import violations. Includes manufacturer compliance scoring for 24,700+ companies.

## Key Numbers

| Metric | Value |
|--------|-------|
| Recall Campaigns | 9,683 |
| Incident Reports | 65,776 |
| NEISS ER Injuries | 2,041,090 |
| Est. National Injuries | 76,222,867 |
| Civil/Criminal Penalties | 395 |
| Total Fines | $363,019,150 |
| Import Violations | 13,902 |
| Manufacturer Profiles | 24,715 |
| Cross-Links | 45,563 |
| High/Critical Risk | 14 |
| Date Range | 1973–2026 |
| Tests | 116 |

## Data Sources

1. **CPSC Recalls API** — Recall campaigns (1973–2026)
2. **SaferProducts.gov** — Consumer incident/complaint reports (2011–2026)
3. **NEISS** — Emergency room injury data (2019–2024, probability sample)
4. **CPSC Penalty API** — Civil and criminal enforcement penalties
5. **CPSC Import Violations** — Notice of Violation data from border inspections

## Quick Start

```bash
pip install -r requirements.txt

# Run full pipeline
python3 -m src.cli pipeline

# Or run individual stages
python3 -m src.cli init
python3 -m src.cli scrape-recalls
python3 -m src.cli scrape-incidents
python3 -m src.cli scrape-neiss
python3 -m src.cli scrape-penalties
python3 -m src.cli scrape-violations
python3 -m src.cli crosslink
python3 -m src.cli export
python3 -m src.cli stats

# Launch dashboard
python3 -m src.cli dashboard
```

## Compliance Score

Composite risk score (0.0–1.0) based on:
- **Recall frequency (30%)** — Number of recall campaigns
- **Penalty severity (25%)** — Total fines assessed
- **Incident volume (25%)** — Number of consumer-reported incidents
- **Import violations (20%)** — Border inspection failures

Risk tiers: LOW (≥0.8), MEDIUM (≥0.5), HIGH (≥0.3), CRITICAL (<0.3)

## Project Structure

```
src/
├── cli.py                 # Click CLI
├── scrapers/              # Recalls, Incidents, NEISS, Penalties, Violations
├── normalization/         # Manufacturer matching, cross-linking
├── validation/            # Quality scoring, compliance scoring
├── storage/               # SQLite database
├── export/                # CSV, JSON, Markdown exports
└── dashboard/             # Streamlit app
```

---

Built by **Nathan Goldberg** · nathanmauricegoldberg@gmail.com · [LinkedIn](https://linkedin.com/in/nathanmauricegoldberg)
