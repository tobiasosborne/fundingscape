# Handoff: Next Steps for Fundingscape

## Session Summary

Built a complete research funding intelligence system from scratch, then hardened data quality:

1. **Phase 0**: Researched 30+ data sources across EU, German, foundation, and international funders
2. **Phase 1**: Built infrastructure (DuckDB, pydantic models, HTTP cache, 71 tests)
3. **Phase 2**: Integrated CORDIS (54K grants), F&T Portal (7K calls), manual entries (18)
4. **Phase 3**: Added OpenAIRE API (15K quantum grants from 10 funders)
5. **Phase 4**: Added OpenAIRE bulk dump (3.66M grants from ALL funders worldwide)
6. **Phase 5**: Set up git, GitHub repo, Apache license, beads issue tracker with 27 issues
7. **Phase 6**: Cross-source deduplication — 54,301 OpenAIRE↔CORDIS duplicates flagged via soft `dedup_of` column
8. **Phase 7**: Data integrity fixes — date anomalies (4,433), country codes (7,476), currency codes (6,367), funder linkage (3.69M grants → 35 funders)
9. **Phase 8**: DFG GEPRIS scraper — updated selectors for current site, pagination support, wired into pipeline with GEPRIS↔OpenAIRE dedup
10. **Phase 9**: BMBF Förderkatalog scraper — reverse-engineered JSP form-based search, POST pagination, 286 quantum projects (€420M)
11. **Phase 10**: Resumable bulk scrapers — standalone CLI scripts with checkpoint/resume for full GEPRIS (152K) and Förderkatalog (268K) dumps
12. **Phase 11**: Completed bulk scrapes — fixed GEPRIS session cookies, pagination params, and total-count extraction; ran both scrapers to completion
13. **Phase 12**: Data normalization — PI names (106K cleaned), institution names (59K normalized), pi_country="EU" nulled (115K), funder linkage (420K), GEPRIS↔OpenAIRE dedup (31K), aggregate records flagged (210)
14. **Phase 13**: DFG funding estimation — extracted programme types from GEPRIS abstracts, built lookup table of typical annual rates by programme type, estimated funding for 86,626 records (~64.6B EUR total). Added `total_funding_estimated` and `funding_estimate_method` columns.
15. **Phase 14**: PI and institution enrichment — downloaded H2020 ERC PI XLSX (7,811 PI names added to CORDIS records); expanded GEPRIS institution parser to extract from "Host", "Co-Applicant Institution", etc. (8,817 additional institutions)
16. **Phase 15**: ROR integration — downloaded ROR v2.2 data dump (121,920 organizations), built offline matching index with exact + fuzzy (RapidFuzz) matching, matched 2,659 unique institution names → 80,780 grant records with canonical `ror_id`. Built GEPRIS person page scraper and Förderkatalog detail page scraper (ready to run).
17. **Phase 16**: Förderkatalog detail scrape completed — fetched all 268,164 detail pages (240K cached from prior partial run + 28K new). Extracted 45,717 abstracts total (17% of FK grants; most older entries lack descriptions). 9 failures, fully checkpointed and resumable.
18. **Phase 17**: Data quality uplevel — built `scripts/quality_audit.py` (reusable per-source NULL/anomaly/dedup audit). Built `fundingscape.currency` module with ECB annual reference rates 1995-2026 for 15 currencies + 22 unit tests; added `total_funding_eur` column to `grant_award` and populated 2,327,684 rows. Fixed 3 EIC SME outliers (€2.5B → null, clear 1000x decimal-shift errors). Extended QA matcher with 110+ German keyword variants across 50+ chemistry/materials/QC apps; tightened 6 over-matched patterns; broadened 13 under-matched. QA matches: 5,633 → 6,351 (+12.7%); unmatched apps: 7 → 3 (corpus gaps in Quantum kernels / Bin packing / Warehouse picking).
19. **Phase 18**: OpenAIRE abstract enrichment — discovered the bulk dump's `summary` field had been silently dropped by the loader. Built `scripts/enrich_openaire_abstracts.py` to re-extract from the cached tar (no API calls). Recovered 401,862 abstracts in 73 seconds: UKRI (175K, 100% coverage), EC (89K), ARC (32K), ANR (24K), NHMRC (23K), NWO (19K), WT (12K). NIH/NSF/DFG/SNSF have no summaries in the OpenAIRE bulk format. Updated `openaire_bulk.py` loader to extract summaries on future re-runs. Quantum-related OpenAIRE coverage: 12,206 → 17,421 grants (+43%). Refactored `qa_funding.py:compute_funding_links` to use a single-scan candidate-table approach. **QA matches: 6,351 → 11,889 (+87%); TAM: 6.30B → 10.73B EUR (+70%); unmatched apps: 3 → 1.**

Final state: **4,046,972 unique grants** (4,132,533 total, 85,351 deduped, 210 aggregates), **7,194 calls**, **252 tests**, **~1,340 MB database**, **1.16T EUR canonical funding (EUR-normalized)**, **401,862 OpenAIRE abstracts (11% coverage, 100% for UKRI/ARC subsets)**.

---

## Immediate Next Steps (High Impact, Low Effort)

### ~~1. Run DFG GEPRIS Scraper~~ DONE (Phase 8)
Scraper updated with current GEPRIS selectors (`div.results h2 a`, `h1.facelift`, `span.name`), pagination via `hitsPerPage`/`index` params, 22 tests. Wired into `update.py` with GEPRIS↔OpenAIRE DFG dedup in `dedup.py`. Run with `max_detail_pages=N` to control scope.

### ~~2. BMBF Förderkatalog~~ DONE (Phase 9-10)
Full scraper at `src/fundingscape/sources/foerderkatalog.py`. Reverse-engineered JSP POST form: session init, search with `suche.themaSuche[0]`, pagination via `suche.listrowfrom`/`suche.listrowpersite`. Key discovery: `suche.lfdVhb=N` must be set to include completed projects (default shows only running = 34K of 268K). Standalone bulk scraper at `scripts/scrape_foerderkatalog.py` with checkpoint/resume, 1000 rows/page.

### ~~1. Finish Förderkatalog Bulk Scrape~~ DONE (Phase 11)
**Result**: 268,164 / 268,164 (100%), 0 failures. 267,403 with funding amounts, 268,164 with institutions, 0 PI names (Förderkatalog doesn't expose PIs).

### ~~2. Run GEPRIS Bulk Scrape~~ DONE (Phase 11)
**Result**: 152,712 projects loaded (152,707 cached, 5 failures). 106,462 with PI names, 30,985 with institutions, all with titles. Three bugs fixed in `scrape_gepris.py`: session cookie persistence (JSESSIONID), missing `task=doKatalog` pagination parameter, and total-count extraction from pagination links.

### ~~3. DFG Funding Amounts~~ DONE (Phase 13)
**Result**: DFG does not publish per-project funding amounts on GEPRIS. Estimated 86,626 records using programme-type lookup table (22 programme types with typical annual rates). Values stored in `total_funding_estimated` column with `funding_estimate_method='programme_type'` for traceability.

### ~~4. ERC PI-Specific Download~~ DONE (Phase 14)
**Result**: Downloaded `cordis-h2020-erc-pi.xlsx`, parsed 7,811 unique ERC PIs, matched and enriched all CORDIS H2020 ERC grant records with PI names. Cached at `data/cache/cordis/cordis-h2020-erc-pi.xlsx`, auto-downloaded by `run_dedup()`.

---

## Medium-Term Steps (Next Session)

### 5. Migrate OpenAIRE to Graph API v1
**Issue**: Part of `datapipeline-7o2` (closed, but could be improved)
**Impact**: Cleaner data, cursor-based pagination, 7200 req/hr with auth

The current API integration uses the legacy search API. The Graph API v1 at `https://api.openaire.eu/graph/v1/projects` has better JSON structure and cursor-based pagination for >10K results. Register for a token at `https://develop.openaire.eu/personal-token` for 120x higher rate limit.

### 6. Batch Insert Optimization
**Issue**: `datapipeline-tbc`
**Effort**: 1 hour
**Impact**: Reduce CORDIS load time from ~15 min to ~30 sec

Currently CORDIS uses row-by-row upserts (slow for 55K records). Convert to the same TSV staging + DuckDB bulk load approach used by `openaire_bulk.py`. The pattern is proven and loads 3.7M records in 10 seconds.

### 7. Crossref + ROR Integration
**Issue**: `datapipeline-agx`
**Effort**: 1 hour
**Impact**: Normalise funder and institution names across all sources

Currently "LEIBNIZ UNIVERSITAET HANNOVER" vs "Leibniz Universität Hannover" vs "LUH" are different strings. ROR (https://ror.org/) provides canonical institution IDs. Crossref Funder Registry provides canonical funder IDs. Both have free REST APIs.

### 8. Simpler.Grants.gov API
**Issue**: `datapipeline-7e8`
**Effort**: 1.5 hours
**Impact**: Replace manual AFOSR/ONR/ARL entries with live API data

Endpoint: `POST https://api.simpler.grants.gov/v1/opportunities/search`
Free API key, 60 req/min, 10K req/day. Search for DoD quantum BAAs automatically.

---

## Longer-Term Roadmap

### National Funder APIs (each ~1 hour)
These funders have proper APIs and would add rich data:
- **UKRI Gateway to Research** (`datapipeline-4zd`): REST API, JSON/XML, best national funder API
- **SNSF P3** (`datapipeline-fgi`): REST API, JSON, Swiss quantum
- **ANR** (`datapipeline-4gu`): Recently launched API, French Plan Quantique
- **FWF** (`datapipeline-lbt`): Research Radar API, Austrian quantum
- **NSF Award Search** (`datapipeline-r5z`): JSON API, US quantum
- **DOE/OSTI** (`datapipeline-idl`): OSTI API, US quantum centres
- **SWECRIS** (`datapipeline-fur`): Swedish API, Wallenberg WACQT

### Supplementary Sources
- **ESA tenders** (`datapipeline-gcu`): Quantum satellites, QKD, quantum clocks
- **Simons Foundation** (`datapipeline-lb0`): Major quantum physics funder
- **Japan KAKEN** (`datapipeline-ttf`): 500K grants, Moonshot quantum programme
- **Keep.eu** (`datapipeline-tvi`): Interreg cross-border quantum projects
- **CORDIS SPARQL** (`datapipeline-y7p`): Network mapping, federated Wikidata queries

### Enrichment & Analysis
- ~~**Deduplication**: Same grants appear in CORDIS and OpenAIRE — deduplicate by project code~~ **DONE** (Phase 6-7)
- **Relevance scoring**: Score each call against the group's research profile
- **Network mapping**: Who collaborates with whom in quantum computing?
- **Publication linking**: OpenAlex/OpenAIRE link grants to publications — measure research output per EUR
- **EUR conversion**: Add `total_funding_eur` column with historical exchange rates for cross-currency comparison

### Commercial Sources (if budget allows)
- **Dimensions API**: Most comprehensive commercial grant database, would cover most gaps
- **Crunchbase/Dealroom**: VC deal tracking for quantum startups

---

## Technical Notes

### Database
- DuckDB single file at `data/db/fundingscape.duckdb` (~1,340 MB)
- Schema defined in `src/fundingscape/db.py`
- Tables: `grant_award`, `call`, `funder`, `funding_instrument`, `eligibility_profile`, `data_source`, `change_log`
- View: `grant_award_deduped` — canonical grants only (`WHERE dedup_of IS NULL`), use this for all queries
- Key columns added: `dedup_of` (soft dedup flag), `funder_id` (links to funder table, 99.5% coverage)
- Use `duckdb.connect('data/db/fundingscape.duckdb')` to query directly

### Caching
- All HTTP responses cached in `data/cache/` with metadata
- ETag and Last-Modified headers used for conditional requests
- Re-running the pipeline uses HTTP 304 (Not Modified) — no redundant downloads
- OpenAIRE bulk tar (620 MB) cached at `data/cache/openaire/project.tar`

### Rate Limiting
- CORDIS bulk downloads: no rate limit (static files)
- F&T Portal: no rate limit (single JSON file)
- OpenAIRE API: 60 req/hr unauthenticated, 7200 req/hr with token
- GEPRIS: 2.5s delay between requests (be respectful, no API)
- Förderkatalog: 2.5s delay, 1000 rows/page, ~5s per page
- Simpler.Grants.gov: 60 req/min, 10K req/day

### Key File Locations
- Database: `data/db/fundingscape.duckdb`
- Cache: `data/cache/`
- Förderkatalog checkpoint: `data/cache/foerderkatalog/checkpoint.json`
- Förderkatalog detail checkpoint: `data/cache/foerderkatalog/detail_checkpoint.json`
- GEPRIS checkpoint: `data/cache/gepris/bulk_checkpoint.json`
- OpenAIRE dump: `data/cache/openaire/project.tar`
- CORDIS CSVs: `data/cache/cordis/`
- Manual entries: `manual/*.yaml`
- Generated report: `REPORT.md`

### Dropbox Backup
- Location: `Dropbox/Projects/Computers/fundingscape/` (5.2 GB)
- Contents: DB, checkpoints, CORDIS/ROR data, GEPRIS + FK caches as tarballs
- Restore caches: `tar xf gepris_cache.tar -C data/cache/` (same for FK)

### Environment
- Python 3.12.3, uv 0.9.17
- DuckDB 1.4.4, pydantic 2.12.5, httpx 0.28.1
- 167 tests, all passing in <6 seconds
- GitHub: https://github.com/tobiasosborne/fundingscape
- Data integrity pipeline: `src/fundingscape/dedup.py` (date cleanup, country/currency normalization, funder linkage, cross-source dedup — all idempotent)
- Issues: `bd list` (6 P1 closed, ~37 open)

---

## Commands Reference

```bash
make update     # Run full pipeline (CORDIS + F&T Portal + Manual + OpenAIRE Bulk)
make test       # Run 167 tests

# Resumable bulk scrapers (run from any machine with DB + checkpoint files)
uv run python scripts/scrape_foerderkatalog.py          # resume full 268K Förderkatalog
uv run python scripts/scrape_foerderkatalog.py --status  # check progress
uv run python scripts/scrape_gepris.py --listing-only    # phase 1: collect 152K project IDs
uv run python scripts/scrape_gepris.py --details-only    # phase 2: fetch detail pages
uv run python scripts/scrape_gepris.py --status          # check progress
make report     # Generate REPORT.md
make clean      # Delete database and cache

bd list         # List all issues
bd show <id>    # Show issue details
bd close <id>   # Close an issue

# Direct database query
uv run python3 -c "
import duckdb
conn = duckdb.connect('data/db/fundingscape.duckdb')
print(conn.execute('SELECT COUNT(*) FROM grant_award_deduped').fetchone())
conn.close()
"

# Run data integrity pipeline (dedup + normalization)
uv run python3 -c "
from fundingscape.db import get_connection
from fundingscape.dedup import run_dedup
conn = get_connection()
print(run_dedup(conn))
conn.close()
"
```
