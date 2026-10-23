# Critique: Data Collection Transparency in `pipeline/fetch.py`

The main problem is that the module can't tell a user whether the data is real. The live path collects nothing, and synthetic data is stored in the same table as real data with nothing to tell them apart.

## Critical issues

### 1. The live scraper is a stub that looks like it works
- `fetch_live_price_data` always returns an empty DataFrame.
- When tables are found, it logs "Found %d tables… Parsing..." and then returns empty. The log implies parsing happens, and none does.
- The module docstring says it "Scrapes daily market bulletins". In practice it never ingests a single real record.
- `run_ingestion` then logs "No new live records to ingest today." That makes a permanently broken scraper look like a quiet day.

### 2. Synthetic data has no provenance
- `generate_seed_data` produces random-walk prices, and its rows have the same schema as real ones.
- There is no `source`, `is_synthetic` or `ingested_at` column. Downstream dashboards and forecasts can't separate fabricated prices from DA data.
- "Realistic, statistically calibrated" suggests the numbers were fitted to real data. They weren't. `base_price` and `volatility` are hand-picked constants with no cited source.
- Rows are labelled `market_scope: "NCR Wet Markets"`, which attributes invented figures to a real market.

### 3. Synthetic data can overwrite real data
- `upsert_commodity_prices` is used for both modes. Running `--seed` after live data exists could silently replace real rows for the same date and commodity.
- The seed window ends at `today`, so it overlaps real data by design.

## Significant issues

### 4. Real HTTP failures are logged as expected behaviour
- Non-200 responses, timeouts and connection errors all log at `WARNING` or `INFO` and return an empty frame.
- `run_ingestion` returns `0` for all of these, so a caller or cron job can't distinguish "no bulletin" from "site down" from "parser unimplemented".
- Sunday is the only case that is truly expected, and it is handled correctly.

### 5. The fetch isn't tied to the requested date
- `target_date` is only used for the Sunday check and in log messages.
- The request is always for the landing page `DA_PRICE_MONITORING_URL`. Nothing confirms the content matches `target_date`.
- `BANTAY_PRESYO_URL` is defined and never used.

### 6. There is no record of what was fetched
- Raw responses aren't saved, and neither are URL, status code, content hash or fetch timestamp.
- Nobody can audit or reproduce a value, and there's nothing to re-parse if the parser changes.

### 7. The browser User-Agent hides who is scraping
- It impersonates Chrome on Windows.
- A public-data scraper should identify itself with a project name and contact.
- The code never checks `robots.txt` or the site's terms.
- There is no rate limiting or retry policy.

### 8. Unexplained data-shape assumptions
- Every group uses the same flat `market_scope`.
- Low and high bounds are invented from a uniform 4–10% spread.
- The comment "typically 3-10% spread" has no citation.

## Minor issues
- `np.random.seed` and `random.seed` set global state. Use a local `np.random.default_rng(seed)`.
- `run_ingestion` never closes `con`.
- `generate_seed_data` is called without `end_date`, so output depends on the current day. Two runs on different days don't give the same data for the same `seed`.
- The `--seed` help text says "realistic", which compounds the provenance problem.

---

## Suggested fixes

### Priority 1: Provenance columns
Add these columns to the `commodity_prices` table and the DataFrames:

| Column | Example values | Purpose |
|---|---|---|
| `source` | `da_amas`, `synthetic_seed` | Where the row came from |
| `is_synthetic` | `true` / `false` | Easy filter for dashboards and models |
| `source_url` | bulletin URL, or `null` | Link back to the original |
| `fetched_at` | UTC timestamp | When it was collected |

Dashboards should either exclude synthetic rows by default or show a visible "contains synthetic data" banner.

### Priority 2: Protect real data from seed overwrites
- On a seed run, insert only where no row exists for `(date, commodity, variety, market_scope)`.
- Alternatively, keep seed data in a separate table (`commodity_prices_synthetic`) and union it in views.
- Also make `--seed` refuse to run against a non-empty database unless `--force` is passed.

### Priority 3: Return a structured fetch result
```python
from dataclasses import dataclass
from enum import Enum

class FetchStatus(Enum):
    OK = "ok"
    SUNDAY = "sunday"
    NOT_PUBLISHED = "not_published"
    HTTP_ERROR = "http_error"
    NETWORK_ERROR = "network_error"
    PARSER_NOT_IMPLEMENTED = "parser_not_implemented"

@dataclass
class FetchResult:
    status: FetchStatus
    data: pd.DataFrame
    source_url: str | None = None
    detail: str = ""
```
- `run_ingestion` logs and reacts per status.
- The CLI exits non-zero for `HTTP_ERROR`, `NETWORK_ERROR` and `PARSER_NOT_IMPLEMENTED`, so schedulers see real failures.
- `SUNDAY` and `NOT_PUBLISHED` exit 0.

### Priority 4: Honest wording
- Docstring: "Live scraping is **not yet implemented**; the module currently only generates synthetic placeholder data."
- Replace "realistic" and "calibrated" with "synthetic placeholder data with illustrative prices".
- Change the "Found %d tables… Parsing..." log to a `WARNING`: "Parser not implemented; no data ingested."
- Rename the `market_scope` value for seed rows to something like `"Synthetic (NCR-style)"`, or rely on `is_synthetic`.

### Priority 5: Persist raw responses
- Save each fetched response to `data/raw/<source>/<YYYY-MM-DD>.html` (or `.pdf`).
- Record the SHA-256 hash, HTTP status and fetch time in a `fetch_log` table.
- Re-parsing then doesn't require re-fetching.

### Priority 6: Responsible scraping
- Use an identifying User-Agent, e.g. `PH-Commodity-Tracker/1.0 (+contact-url-or-email)`.
- Check `robots.txt` (`urllib.robotparser`) and document the DA terms of use and data licence in the README.
- Add a small retry with backoff, and a delay between requests.
- Decide which site is the source of truth: use `BANTAY_PRESYO_URL` or remove it.

### Priority 7: Document assumptions
- Add a `docs/data_sources.md` listing what is real, what is synthetic, and the origin of each `base_price` and `volatility`.
- Cite the source of the spread assumption, or label it "illustrative".

### Minor cleanups
- Use `rng = np.random.default_rng(seed)` and `random.Random(seed)` instead of global seeding.
- Wrap `con` in `try/finally` (or a context manager) so it closes.
- Pass an explicit `end_date` in `run_ingestion` (or fix a default) for reproducibility.
- Reword the `--seed` help text to "synthetic placeholder data".

## Suggested implementation order
1. Provenance columns, plus `run_ingestion` marking seed rows (Priorities 1 and 2).
2. `FetchResult` and CLI exit codes (Priority 3).
3. Wording fixes (Priority 4).
4. Raw-response persistence and `fetch_log` (Priority 5).
5. Scraping etiquette and docs (Priorities 6 and 7).
