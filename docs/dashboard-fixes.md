# Philippine Commodity Price Tracker: Fix List

Reviewed: 2026-11-11 · Latest bulletin at review time: 2026-11-10
Note: reviewed from the page text and data only. Plotly charts and treemap interactions were not visible, so check those manually.

---

## 1. Data integrity (high priority)

- [x] **Investigate the Sardines (Tamban) +41.69% day-over-day jump**
  - 1-day is +41.69%, but 7-day is only +1.44% and 30-day is -1.42%.
  - Implied previous-day price is about ₱167, against about ₱235-240 a week and a month ago.
  - 30d SMA (₱196) is far below the current price (₱237), and 30d CV is 15.5%, so the series looks like it flips between two levels.
  - *Investigation Result (2026-11-12)*: Verified directly against the official DA bulletin `Daily-Price-Index-November-9-2026.pdf`. Page 1 explicitly recorded `['Sardines (Tamban), Local', 'Medium, Fresh or Chilled', '167.27']` and Nov 8 recorded `246.92`. No PDF parsing or row-alignment error occurred; the DA bulletin itself contained this rapid price oscillation.
- [x] **Handle provisional bulletins**
  - The Nov 10 file is a provisional `DPI-AFC` bulletin, and the audit log says no finalized Daily Price Index has replaced it.
  - *Implemented*: Added `is_provisional` flag in `v_commodity_analytics`, `v_latest_snapshot`, and bulletin audit queries. Added "PROVISIONAL" badges in sidebar, KPI strip, and governance register.
  - Re-ingest and overwrite once the finalized bulletin is published (supported via `daily_update.py` finalized candidate check).

## 2. Wrong labels and misleading numbers

- [x] **"64.0% daily net advances" is mislabeled**
  - 32 of 50 is the advance share, not net advances.
  - *Implemented*: Renamed in `templates/components/kpi_strip.html` to clearly report `64.0% advancers (32 of 50)`.
- [x] **Category averages mix units** (treemap headers)
  - "Basic Goods Avg ₱69.42/kg / pc" averages eggs (per piece), oil (per 1L bottle), and salt and sugar (per kg).
  - Vegetables includes Native Pechay priced per bundle.
  - *Implemented*: Replaced mixed-unit averages in treemap cluster headers with mathematically sound, unit-invariant category indices and item counts (e.g. `8 items · Index 104.8`).
- [x] **Commodity count mismatch**
  - The meta description says 48, but the page shows 50 (8 + 5 + 9 + 6 + 3 + 5 + 14).
  - *Implemented*: Updated `meta-description`, `og:description`, `twitter:description`, and JSON-LD structured metadata in `templates/base.html` to dynamically bind to `{{ kpis.total_commodities }}` (50) so it cannot drift.
- [x] **Default treemap view to 1-day change**
  - Previously opened on 7-day change.
  - *Implemented*: Set default active treemap horizon and pill button to 1-Day Change (DoD), rendering DoD % deltas and styling on page load. Removed redundant horizon text label.

## 3. Pipeline reliability

- [x] **Fix the ~3,400 s runs**
  - Two runs on 2026-11-07 took about 57 minutes against the usual 15-60 s.
  - That looks like a hang or timeout waiting on the DA portal.
  - Add request timeouts and retries with backoff.
  - Record these runs as `WARN` or `TIMEOUT`, not `NO NEW DATA`.
  - *Implemented*: Configured granular `httpx.Timeout(timeout=15.0, connect=5.0, read=10.0, write=5.0)` across catalog crawler and candidate downloaders. Added consecutive network/timeout error short-circuiting (aborts candidate loops after 2 consecutive connection failures rather than attempting all permutations). Backfilled historical long runs in `pipeline_execution_logs.csv` as `TIMEOUT`.
- [x] **Distinguish "no new data" from "source unreachable"** in the audit log status values.
  - *Implemented*: Added `TIMEOUT` and `SOURCE_UNREACHABLE` status classifications to `run_daily_update` when candidate date requests encounter timeouts or connection drops. Added amber `.status-pill-warning` pill styles to table views and `dashboard.js`.
- [x] **Consider a data freshness alert** if no successful ingest happens within N days.
  - *Implemented*: Added automated freshness evaluation in `pipeline/dashboard/queries.py` (`is_stale`, `days_since_update` if >3 days since latest bulletin). Added a visual warning banner in `overview.html` and badge in `sidebar.html` that triggers dynamically if data is stale.

## 4. Methodology and documentation

- [ ] **Clarify gap handling**
  - The page says missing days are forward-filled, but 612 bulletins over a 620-day span means 8 dates are absent: several Sundays in March 2025 and Apr 17-19, 2025.
  - State which dates are missing and which are filled, and make the text match the data.
- [ ] **Verify how change metrics are computed**
  - Check whether 1-day, 7-day, and 30-day changes and the SMAs use calendar offsets or row offsets.
  - The two differ around gaps. Document the choice either way.
- [ ] **Document the effect of forward-filling** on volatility and CV (it understates both) and on 1-day change (artificial zeros).
- [ ] **Add a provisional bulletin handling note** to the Data Governance section (how provisional DPI-AFC values are treated).

## 5. Branding and wording

- [x] **Drop "Official" from the title and meta tags**
  - The data is official (from the DA), but the tracker is not.
  - Suggested wording: "Independent tracker of DA Daily Price Index data".
  - *Implemented*: Updated `<title>`, `<meta name="title">`, `<meta name="description">`, Open Graph, and Twitter metadata in `templates/base.html` and `README.md` to: "Philippine Commodity Price Tracker — Independent Tracker of DA Daily Price Index Data".
- [x] **Add a short disclaimer** such as "Not affiliated with the Department of Agriculture. Data sourced from public DA bulletins."
  - *Implemented*: Added prominent disclaimers in `templates/base.html` footer, `templates/views/governance.html`, and `README.md`: "Not affiliated with the Department of Agriculture. Data sourced from the public Department of Agriculture (DA) price monitoring portal."
- [x] **Reconsider "Bantay Presyo" as a title**, since it is a DA program name and could imply affiliation. Keep it only as a subtitle if you keep it at all.
  - *Implemented*: Removed "Bantay Presyo" as primary title across titles, navbar, metadata, and JSON-LD schema. Retained solely as a descriptive search tag in keywords.

## 6. Nice-to-have polish

- [x] Show units next to prices in the treemap consistently (₱/kg, ₱/bundle, ₱/piece).
  - *Implemented*: Formatted prices to 2 decimals for small items (< ₱20, such as chicken egg ₱8.50) and integer for larger items, alongside explicit units (`/kg`, `/piece`, `/bundle`, `/bottle (1L)`) in `templates/views/overview.html` and `templates/assets/dashboard.css`.
- [ ] **Add a "last successful update" timestamp near the top, using the pipeline's latest SUCCESS run**
  - *(Removed from header navbar and sidebar per user preference)*.
- [x] Link the audit log rows to the commit or run that produced each update.
  - *Implemented*: Added direct links in `templates/views/governance.html` and `dashboard.js` table rendering pointing to GitHub Actions run details (or workflow runs directory). Enabled `GITHUB_RUN_ID` capture in `daily_update.py`.
- [x] Add a short README section on how to reproduce the pipeline locally (DuckDB + Jinja2 + Plotly).
  - *Implemented*: Added a comprehensive "Reproducing the Pipeline Locally (DuckDB + Jinja2 + Plotly)" section to `README.md` with step-by-step commands for testing, warehouse build, static dashboard compilation, and live preview.

---

## Suggested order of work

1. Sardines anomaly + provisional badge (sections 1)
2. Mislabeled "net advances" + unit-mixed averages + 48 vs 50 (section 2)
3. Pipeline timeout handling (section 3)
4. Methodology text and branding wording (sections 4-5)
5. Polish (section 6)
