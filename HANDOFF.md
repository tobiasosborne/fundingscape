# Handoff: Next Steps for Fundingscape

## Session Summary

Built a complete research funding intelligence system from scratch in one session:

1. **Phase 0**: Researched 30+ data sources across EU, German, foundation, and international funders
2. **Phase 1**: Built infrastructure (DuckDB, pydantic models, HTTP cache, 71 tests)
3. **Phase 2**: Integrated CORDIS (54K grants), F&T Portal (7K calls), manual entries (18)
4. **Phase 3**: Added OpenAIRE API (15K quantum grants from 10 funders)
5. **Phase 4**: Added OpenAIRE bulk dump (3.66M grants from ALL funders worldwide)
6. **Phase 5**: Set up git, GitHub repo, Apache license, beads issue tracker with 27 issues

Final state: **3,711,657 grants**, **7,194 calls**, **87 tests**, **471 MB database**.

---

## Immediate Next Steps (High Impact, Low Effort)

### 1. Run DFG GEPRIS Scraper
**Issue**: `datapipeline-29h`
**Effort**: 30 minutes (scraper is already built)
**Impact**: Fills the biggest German gap — adds ~200K DFG projects with PI names, institutions, and funding schemes

The scraper exists at `src/fundingscape/sources/gepris.py`. It just needs to be run against the live site. Be respectful with rate limiting (2.5s between requests). Start with quantum keywords, then expand.

```bash
uv run python3 -c "
from fundingscape.db import get_connection
from fundingscape.sources.gepris import fetch_and_load
conn = get_connection()
fetch_and_load(conn, max_detail_pages=500)
conn.close()
"
```

**Note**: DFG GEPRIS has no official API. The scraper uses BeautifulSoup to parse HTML. Page structure may change — staleness detection tests should be added.

### 2. BMBF Förderkatalog
**Issue**: `datapipeline-am7`
**Effort**: 2-3 hours
**Impact**: 110K+ German federal research projects

The site was under maintenance until Feb 23, 2026. A scraper skeleton exists at `src/fundingscape/sources/foerderkatalog.py` (placeholder). The site uses JSP with form-based POST requests. The research agent noted it may have a bulk XML export — investigate first before building a scraper.

Key URL: `https://foerderportal.bund.de/foekat/jsp/SucheAction.do?actionMode=searchmask`

### 3. Fix DFG Funding Amounts
**Current issue**: OpenAIRE has 32,198 DFG grants but most show €0 funding. The DFG doesn't report funding amounts to OpenAIRE. Two options:
- Scrape amounts from GEPRIS detail pages (per step 1)
- Cross-reference GEPRIS data with OpenAIRE by project code to enrich records

### 4. ERC PI-Specific Download
**Issue**: `datapipeline-bvx`
**Effort**: 30 minutes
**Impact**: PI names for all H2020 ERC grants, enabling panel-level success rate analysis

Download `https://cordis.europa.eu/data/cordis-h2020-erc-pi.xlsx` and parse. This gives PI names, panels (PE2 for physics), and institutions for every ERC grant in H2020.

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
- **Deduplication**: Same grants appear in CORDIS and OpenAIRE — deduplicate by project code
- **Relevance scoring**: Score each call against the group's research profile
- **Network mapping**: Who collaborates with whom in quantum computing?
- **Publication linking**: OpenAlex/OpenAIRE link grants to publications — measure research output per EUR

### Commercial Sources (if budget allows)
- **Dimensions API**: Most comprehensive commercial grant database, would cover most gaps
- **Crunchbase/Dealroom**: VC deal tracking for quantum startups

---

## Technical Notes

### Database
- DuckDB single file at `data/db/fundingscape.duckdb` (471 MB)
- Schema defined in `src/fundingscape/db.py`
- Tables: `grant_award`, `call`, `funder`, `funding_instrument`, `eligibility_profile`, `data_source`, `change_log`
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
- Simpler.Grants.gov: 60 req/min, 10K req/day

### Key File Locations
- Database: `data/db/fundingscape.duckdb`
- Cache: `data/cache/`
- OpenAIRE dump: `data/cache/openaire/project.tar`
- CORDIS CSVs: `data/cache/cordis/`
- Manual entries: `manual/*.yaml`
- Generated report: `REPORT.md`

### Environment
- Python 3.12.3, uv 0.9.17
- DuckDB 1.4.4, pydantic 2.12.5, httpx 0.28.1
- 87 tests, all passing in <2 seconds
- GitHub: https://github.com/tobiasosborne/fundingscape
- Issues: `bd list` (27 issues, 26 open)

---

## Commands Reference

```bash
make update     # Run full pipeline (CORDIS + F&T Portal + Manual + OpenAIRE Bulk)
make test       # Run 87 tests
make report     # Generate REPORT.md
make clean      # Delete database and cache

bd list         # List all issues
bd show <id>    # Show issue details
bd close <id>   # Close an issue

# Direct database query
uv run python3 -c "
import duckdb
conn = duckdb.connect('data/db/fundingscape.duckdb')
print(conn.execute('SELECT COUNT(*) FROM grant_award').fetchone())
conn.close()
"
```
