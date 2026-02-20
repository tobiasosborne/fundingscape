# Fundingscape

**EU Research Funding Landscape Intelligence System for Quantum Technology**

A comprehensive research funding intelligence system that ingests, normalises, and analyses grant data from 20+ funders worldwide. Built for a quantum information theory research group at Leibniz Universität Hannover, but useful for any research group seeking funding intelligence.

This tool replaces services that consulting firms charge €5,000–80,000 for. We built it in an afternoon.

## What's Inside

**3.7 million grants** from 20+ funders across 61 countries, plus 7,194 open/forthcoming/closed calls from the EU Funding & Tenders Portal. All queryable via SQL through DuckDB.

| Source | Records | Coverage |
|--------|---------|----------|
| OpenAIRE Bulk (Zenodo dump) | 3,656,773 | NIH, NSF, UKRI, SNSF, ANR, FWF, NWO, ARC, DFG, AKA, FCT, RCN, WT, TUBITAK, SFI, and hundreds more |
| CORDIS Bulk CSV | 54,884 | All Horizon Europe + Horizon 2020 projects with PI/institution data |
| EU F&T Portal JSON | 7,194 | All open, forthcoming, and closed calls across EU programmes |
| Manual YAML entries | 18 | DFG instruments, VolkswagenStiftung, Humboldt, Carl-Zeiss, AFOSR, ONR, ARL, NATO SPS, Forschungszulage |

### Quantum-Specific Coverage

- **14,796 grants** with "quantum" in the title
- **7.19 billion EUR** in quantum-related funding tracked
- Covers: quantum computing, quantum information, quantum sensing, quantum communication, topological quantum, many-body quantum, quantum error correction
- Top quantum funders: NSF, UKRI/EPSRC, EC/ERC, SNSF, FWF, NWO, ANR, DFG

## Quick Start

```bash
# Clone
git clone https://github.com/tobiasosborne/fundingscape.git
cd fundingscape

# Install (requires Python 3.12+ and uv)
uv sync --all-extras

# Initialize database and load all data
# (downloads ~700 MB of bulk data on first run, then uses cache)
make update

# Run tests
make test

# Generate funding landscape report
make report
```

## Architecture

```
fundingscape/
├── src/fundingscape/
│   ├── models.py          # Pydantic data models (Funder, Call, GrantAward, etc.)
│   ├── db.py              # DuckDB schema, CRUD operations, seed data
│   ├── cache.py           # HTTP caching with ETag/Last-Modified support
│   ├── queries.py         # Analytical SQL queries (deadlines, projections, rankings)
│   ├── report.py          # Markdown report generator
│   ├── update.py          # Pipeline orchestrator
│   └── sources/
│       ├── cordis.py      # CORDIS bulk CSV ingestion (H2020 + Horizon Europe)
│       ├── ft_portal.py   # EU Funding & Tenders Portal JSON
│       ├── openaire.py    # OpenAIRE REST API (quantum-filtered, 10 funders)
│       ├── openaire_bulk.py # OpenAIRE Zenodo bulk dump (3.7M projects, all fields)
│       ├── gepris.py      # DFG GEPRIS web scraper (built, needs live run)
│       └── manual.py      # YAML-based manual entry loader
├── tests/                 # 87 tests, all passing
├── manual/                # YAML files for manual funding entries
├── data/
│   ├── cache/             # HTTP response cache (gitignored)
│   └── db/                # DuckDB database file (gitignored)
├── PLAN.md                # Original implementation plan
├── REPORT.md              # Generated funding landscape report
└── moar-data-issues.md    # 27 expansion issues (tracked via beads)
```

## Key Features

### Idempotent Pipeline
Every data fetch is cached. Re-running `make update` uses HTTP ETags and Last-Modified headers — unchanged data is not re-downloaded. The OpenAIRE bulk dump uses DuckDB's native CSV reader to load 3.66M records in ~10 seconds.

### Analytical Queries
All queries are in `src/fundingscape/queries.py`:

- **Open calls by deadline** — ranked list of upcoming quantum/deep-tech calls
- **Income projection** — model future grant income based on active grants and linear burn rates
- **Top PIs by field** — who has the most funding in quantum computing, by institution
- **Historical trends** — quantum funding over time, by year and funder
- **Gap analysis** — which funders have you never applied to?
- **SME instruments** — calls relevant for Innovailia UG (EIC Accelerator, etc.)

### Example Queries

```python
import duckdb

conn = duckdb.connect('data/db/fundingscape.duckdb')

# What quantum grants can I apply for in the next 6 months?
conn.execute("""
    SELECT call_identifier, title, deadline, framework_programme
    FROM call
    WHERE status IN ('open', 'forthcoming')
    AND deadline >= CURRENT_DATE
    AND deadline <= CURRENT_DATE + INTERVAL 6 MONTH
    AND (title ILIKE '%quantum%'
         OR ARRAY_TO_STRING(topic_keywords, ' ') ILIKE '%quantum%')
    ORDER BY deadline
""").fetchdf()

# Top 10 institutions in topological quantum computing
conn.execute("""
    SELECT pi_institution, pi_country, COUNT(*) as grants,
           SUM(total_funding) as total
    FROM grant_award
    WHERE project_title ILIKE '%topolog%quantum%'
    GROUP BY pi_institution, pi_country
    ORDER BY total DESC LIMIT 10
""").fetchdf()

# How much grant income will LUH have in 2026-2027?
conn.execute("""
    SELECT YEAR(start_date) || '-' || YEAR(end_date) as period,
           COUNT(*) as grants, SUM(total_funding) as total
    FROM grant_award
    WHERE pi_institution ILIKE '%HANNOVER%' AND status = 'active'
    GROUP BY period ORDER BY total DESC
""").fetchdf()
```

## Data Sources in Detail

### CORDIS (cordis_bulk)
- **What**: All EU framework programme projects (FP7, H2020, Horizon Europe)
- **How**: Bulk CSV downloads from `cordis.europa.eu/data/`
- **Fields**: Project title, acronym, abstract, PI institution, country, funding amount, EU contribution, start/end dates, topic keywords, funding scheme (ERC-STG, MSCA-PF, etc.)
- **Auth**: None
- **Update**: Monthly

### EU Funding & Tenders Portal (ft_portal)
- **What**: All open, forthcoming, and closed calls across EU programmes
- **How**: Single JSON file from `ec.europa.eu/.../grantsTenders.json` (~120 MB)
- **Fields**: Call identifier, title, deadlines, status, framework programme, topic keywords, tags
- **Auth**: None
- **Update**: Continuous

### OpenAIRE Bulk (openaire_bulk)
- **What**: The entire OpenAIRE Graph project dataset — every grant from every funder they track
- **How**: `project.tar` from Zenodo (620 MB, DOI: 10.5281/zenodo.3516917)
- **Fields**: Title, code, acronym, funder, jurisdiction, funding stream, amount, currency, dates, keywords
- **Auth**: None
- **Update**: Monthly on Zenodo
- **Funders covered**: NIH, NSF, UKRI, EC, SNSF, FCT, NWO, NHMRC, ARC, DFG, AKA, ANR, RCN, WT, FWF, TUBITAK, SFI, and many more

### OpenAIRE API (openaire)
- **What**: Quantum/deep-tech filtered grants from 10 specific funders
- **How**: REST API at `api.openaire.eu/search/projects`
- **Auth**: None (60 req/hr), optional token for 7,200 req/hr
- **Note**: Largely superseded by the bulk loader but useful for targeted queries

### Manual Entries (manual)
- **What**: Funding instruments without APIs (DFG schemes, foundations, US DoD BAAs, NATO SPS)
- **How**: YAML files in `manual/` directory
- **Files**: `foundations.yaml`, `german_federal.yaml`, `international.yaml`

## Toolchain

| Component | Choice | Why |
|-----------|--------|-----|
| Language | Python 3.12 | Best ecosystem for data ingestion, scraping, validation |
| Database | DuckDB | Analytical SQL, single-file, native CSV import, fast |
| Package manager | uv | Fast, already installed |
| Validation | Pydantic v2 | Strict typing, JSON serialisation |
| HTTP | httpx + custom cache | ETag/Last-Modified support, rate limiting |
| Scraping | BeautifulSoup + lxml | For GEPRIS and other HTML sources |
| Testing | pytest | 87 tests, <2 seconds |
| Issue tracking | beads (bd) | Lightweight, git-native |

## Issue Tracker

We use [beads](https://github.com/anthropics/beads) for issue tracking. 27 issues track the path to 100% quantum funding coverage:

```bash
bd list                    # See all issues
bd show datapipeline-7o2   # Show a specific issue
bd close <id> --reason "..." # Close an issue
```

## Project Status

**Current**: 3,711,657 grants from 20+ funders. 87 tests. Full report generation.

**What works**:
- Complete grant database queryable via SQL
- Deadline calendar for open quantum calls
- Income projection for LUH from Horizon Europe grants
- Institutional ranking by quantum funding
- Historical quantum funding trends (2015-2026)
- SME-relevant calls for Innovailia UG
- Automated report generation (`make report`)

**Known gaps** (tracked as beads issues):
- BMBF Förderkatalog (German domestic projects — site was under maintenance)
- DFG GEPRIS live scraping (scraper built, not yet run at scale)
- DFG funding amounts missing in OpenAIRE data
- ESA tenders (not in CORDIS or OpenAIRE)
- Japan KAKEN database
- VC/corporate funding data

## License

Apache License 2.0. See [LICENSE](LICENSE).

## Contributing

Issues are tracked via `bd` (beads). See `moar-data-issues.md` for the full list of planned data source integrations.

To add a new data source:
1. Create `src/fundingscape/sources/your_source.py` with a `fetch_and_load(conn)` function
2. Add tests in `tests/test_sources/test_your_source.py`
3. Wire it into `src/fundingscape/update.py`
4. Run `make test` and `make update`
