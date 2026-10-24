# UI Design System & Frontend Standards Bible

> **MANDATORY DIRECTIVE FOR LLMs & AI AGENTS:**
> This document is the single source of truth ("Bible") for the **Philippine Commodity Price Tracker (Bantay Presyo)** user interface. When modifying templates, adding views, creating components, or adjusting styles/charts, you **MUST** strictly reuse the design tokens, CSS classes, component anatomies, Jinja2 filters, and Plotly configurations documented here. **DO NOT** invent new color palettes, ad-hoc inline styles, external CSS frameworks (e.g., Tailwind, Bootstrap), or unapproved component structures.

---

## 1. Design Philosophy & Aesthetic Identity

The dashboard employs a **Modern Institutional Dashboard** aesthetic inspired by **Creative Tim / Material Dashboard** paired with the official **Department of Agriculture (DA) Forest Green** identity and **econometric financial reporting** conventions.

### Core Principles
1. **Institutional Authority & Clarity**: High-contrast slate typography on clean white surfaces (`#ffffff`) over a soft neutral canvas (`#f8f9fa`), anchored by deep DA Forest Green (`#14532d` / `#064e3b`) accents.
2. **Inflation-Semantic Color Coding (Crucial)**:
   - Unlike stock portfolios where green = up, **this is a consumer retail price tracker**.
   - **Price Increases (`+`) are RED (`--increase-*` / `.text-gain`)** — signaling upward retail price pressure / inflation.
   - **Price Decreases (`-`) are GREEN (`--decrease-*` / `.text-drop`)** — signaling consumer price relief / deflation.
   - **Unchanged (`0.00%` or `NULL`) is SLATE GREY (`--neutral-*` / `.text-flat`)**.
3. **Tabular Numeric Precision**: Every price, index value, percentage delta, volatility metric, date value, and count badge **MUST** use `var(--font-mono)` with `font-variant-numeric: tabular-nums` so digits align vertically across rows and cards.
4. **Zero-Framework Vanilla Architecture**: Pure semantic HTML5, modular vanilla CSS (`templates/assets/dashboard.css`), vanilla IIFE JavaScript (`templates/assets/dashboard.js`), Jinja2 Static Site Generation (SSG), and Plotly.js (`2.35.2`).

---

## 2. File & Directory Architecture

All UI source files live in `templates/` and are compiled into `public/` via `pipeline/build_dashboard.py`.

| File Path | Role & Responsibility |
|---|---|
| `templates/base.html` | Root HTML5 document shell, `.app-shell` split layout, footer, asset links, and live-reload watcher script. |
| `templates/dashboard.html` | Extends `base.html`; defines the 4 top-level `<section class="view-panel">` containers. |
| `templates/components/sidebar.html` | Left navigation drawer (`.dashboard-sidebar`), brand header, primary view pills, search box, and collapsible commodity tree. |
| `templates/components/header.html` | Top workspace navbar card (`.dashboard-navbar`), dynamic view kicker/breadcrumbs, commodity stepper/picker, latest bulletin badge, and print button. |
| `templates/components/kpi_strip.html` | 5-card executive summary KPI strip included at the top of the Market Overview view. |
| `templates/views/overview.html` | `#view-overview`: Executive KPI strip, 2-column Top 5 Movers tables, and Macro Category Price Indices chart + summary table. |
| `templates/views/commodity_detail.html` | `#view-commodity`: 7-card compact KPI strip (`#commodity-metrics-strip`) and interactive commodity price + SMA Plotly chart card. |
| `templates/views/census_table.html` | `#view-census`: Master cross-sectional price table (`#master-price-table`) with category filter pills, search input, and sortable columns. |
| `templates/views/governance.html` | `#view-governance`: 4-card governance KPI strip and data provenance/methodology disclosure card. |
| `templates/assets/dashboard.css` | Canonical stylesheet containing all `:root` tokens, layout rules, component classes, print styles, and `@media (max-width: 960px)` responsive rules. |
| `templates/assets/dashboard.js` | Client-side router (`activateView`, URL hash sync), DOM updater for commodity detail cards, table sorter/filter, and Plotly chart controller. |
| `pipeline/dashboard/filters.py` | Jinja2 custom filters (`currency`, `percent`, `badge`, `format_date`, `format_num`, `slugify`). |
| `pipeline/dashboard/queries.py` | DuckDB SQL queries and server-side Plotly figure builder (`CATEGORY_PALETTE`, `fetch_category_indices`). |

> **Rule:** Never edit `public/index.html`, `public/assets/css/dashboard.css`, or `public/assets/js/dashboard.js` directly. Edit the sources in `templates/` and `pipeline/dashboard/` and run the build pipeline.

---

## 3. CSS Design Tokens (`:root`)

Always reference `:root` CSS custom properties defined in `templates/assets/dashboard.css` instead of hardcoding hex values.

### 3.1 Surface & Border Tokens
| Token | Value | Usage |
|---|---|---|
| `--bg-page` | `#f8f9fa` | App shell and `.main-workspace` background canvas |
| `--bg-surface` | `#ffffff` | Primary card, sidebar, and navbar surface |
| `--bg-subtle` | `#f0fdf4` | Soft green tint for hover states and date badges |
| `--border-color` | `#e9ecef` | Standard dividers, table wrappers, and card headers |
| `--border-card` | `rgba(226, 232, 240, 0.85)` | Outer border for `.card`, `.kpi-card`, and `.dashboard-navbar` |
| `--border-dark` | `#d2d6da` | Form inputs (`.search-input`, `.sidebar-search-input`) and hover borders |

### 3.2 Typography Color Tokens
| Token | Value | Usage |
|---|---|---|
| `--text-heading` | `#252f40` | Primary titles, KPI values, commodity names, bold highlights |
| `--text-main` | `#344767` | Default body text, table cells, group toggles |
| `--text-secondary` | `#67748e` | Secondary labels, inactive nav buttons, methodology prose |
| `--text-muted` | `#8392ab` | Uppercase kickers, table headers (`th`), units, context labels, muted captions |

### 3.3 Brand Palette (Department of Agriculture Forest Green)
| Token | Value | Usage |
|---|---|---|
| `--da-green-950` | `#042f1a` | Gradient dark stop for `.brand-badge` and `.kpi-icon-box`, date badge value |
| `--da-green-900` | `#064e3b` | Mapped to `--navy-900`; dark hover states and section subheadings |
| `--da-green-800` | `#14532d` | Mapped to `--navy-800` and `--primary-blue`; active nav buttons, active filter pills, primary action buttons |
| `--da-green-700` | `#15803d` | Mapped to `--navy-700`; decrease/relief icon gradients, bulletin icon |
| `--da-green-600` | `#16a34a` | Mapped to `--accent-blue`; kickers, breadcrumb links, input focus rings |
| `--da-green-500` | `#22c55e` | Bright accent green |
| `--da-green-50` | `#f0fdf4` | Subtle row hover (`tr.clickable-row:hover td`) and active sidebar item background |

### 3.4 Financial & Semantic Indicator Tokens
| Direction | Text Token | Background Token | Border Token | Usage |
|---|---|---|---|---|
| **Price Increase (`▲`)** | `--increase-text` (`#991b1b`) | `--increase-bg` (`#fef2f2`) | `--increase-border` (`#fecaca`) | `.badge-increase`, `.status-pill-increase`, `.dir-up` |
| **Price Decrease (`▼`)** | `--decrease-text` (`#166534`) | `--decrease-bg` (`#f0fdf4`) | `--decrease-border` (`#bbf7d0`) | `.badge-decrease`, `.status-pill-decrease`, `.dir-down` |
| **Neutral / Flat (`—`)** | `--neutral-text` (`#475569`) | `--neutral-bg` (`#f8fafc`) | `--neutral-border` (`#e2e8f0`) | `.badge-neutral`, `.dir-flat` |

> **KPI Headline Text Classes:** Inside `.kpi-value`, use `.text-gain` (`#b91c1c`, bold) for price increases, `.text-drop` (`#15803d`, bold) for price decreases, and `.text-flat` (`var(--text-muted)`, bold) for unchanged values.

### 3.5 Elevation & Layout Tokens
| Token | Value | Usage |
|---|---|---|
| `--sidebar-width` | `280px` | Fixed width of `.dashboard-sidebar` on desktop |
| `--shadow-card` | `0 4px 6px -1px rgba(0, 0, 0, 0.05), 0 2px 4px -1px rgba(0, 0, 0, 0.03)` | Default resting shadow for `.card`, `.kpi-card`, `.dashboard-navbar` |
| `--shadow-badge` | `0 4px 10px 0 rgba(20, 83, 45, 0.25)` | Elevated green glow on `.brand-badge`, `.kpi-icon-box`, and `.sidebar-nav-btn.active` |
| `--shadow-dropdown` | `0 10px 15px -3px rgba(0, 0, 0, 0.1), 0 4px 6px -2px rgba(0, 0, 0, 0.05)` | Floating popovers/dropdowns |

---

## 4. Typography & Formatting Standards

### 4.1 Font Stacks
- **Sans-Serif (`--font-sans`)**: `-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif`
  - Used for all UI chrome, headings, labels, prose, category pills (`category-pill`), and status pills (`status-pill`).
- **Monospace (`--font-mono`)**: `ui-monospace, SFMono-Regular, "SF Mono", Menlo, Monaco, Consolas, "Liberation Mono", monospace`
  - Always paired with `font-variant-numeric: tabular-nums`.
  - Used for all `.kpi-value` (unless `.kpi-value-text` is applied), `.badge`, `.font-mono` table cells, `.sidebar-item-meta`, `.sidebar-group-count`, and `.date-val`.

### 4.2 Type Scale Reference
| Element / Class | Size | Weight | Transform / Tracking | Font Stack |
|---|---|---|---|---|
| `.navbar-title` (`h1`) | `20px` | `700` | `-0.01em` | `--font-sans` |
| `.card-title` (`h2`) | `16px` | `700` | `-0.01em` | `--font-sans` |
| `.kpi-value` | `23px` | `700` | `tabular-nums`, line-height `1.2` | `--font-mono` |
| `.kpi-value.kpi-value-compact` | `19.5px` | `700` | `tabular-nums` | `--font-mono` |
| `.kpi-value.kpi-value-text` | `20px` | `700` | `-0.01em` (for text KPIs like `"NCR Wet Markets"`) | `--font-sans` |
| `.methodology-text` | `13.5px` | `400` | line-height `1.65` | `--font-sans` |
| `.formal-table` body | `13px` | `400` (`600` on `.commodity-cell`) | — | `--font-sans` / `--font-mono` |
| `.kpi-label` / `.navbar-kicker` | `11px` | `700` | `uppercase`, `0.04em`–`0.05em` | `--font-sans` |
| `.sidebar-section-title` / `.formal-table th` | `10.5px` | `700` | `uppercase`, `0.05em`–`0.06em` | `--font-sans` |

### 4.3 Number & Currency Formatting Conventions
All server-side Jinja2 templates and client-side JS updaters must format values identically:
1. **Currency (`₱`)**: Prefix with `₱`, comma thousands separator, 2 decimal places (`₱1,234.56` or `₱45.00`). Missing/null values render as an em-dash (`—`).
   - Jinja2: `{{ val | currency }}`
   - JS: `` meta.price !== null ? `₱${meta.price.toFixed(2)}` : "—" ``
2. **Percentages (`%`)**: Explicit `+` sign for positive numbers (`> 0.0001`), `-` for negative (`< -0.0001`), `0.00%` for flat (`abs <= 0.0001`), 2 decimal places.
   - Jinja2: `{{ val | badge }}` or `{{ val | percent }}`
   - JS: `formatBadgeHtml(val, true)` or `formatKpiHeadlineChange(val)`
3. **Directional Arrows**:
   - Upward: `▲` (`U+25B2`)
   - Downward: `▼` (`U+25BC`)
   - Flat/Null: `—` (`U+2014`)
   - Sortable Table Header Idle: `⇅` (`U+21C5`)

---

## 5. Layout & Spatial Architecture

### 5.1 App-Shell Split Layout
The viewport is locked (`body { height: 100vh; overflow: hidden; }`) with two independent scrollable regions inside `.app-shell`:
1. **Left Sidebar (`.dashboard-sidebar`)**: Fixed `280px` width, `height: 100%`, `overflow-y: auto`, `border-right: 1px solid var(--border-color)`.
2. **Right Main Workspace (`.main-workspace`)**: `flex: 1`, `height: 100%`, `overflow-y: auto`, `padding: 20px 28px 32px`.
   - Inside `.main-workspace`, all top-level blocks (`.dashboard-navbar`, `.workspace-content`, `.dashboard-footer`) are constrained to `max-width: 1400px; width: 100%; margin-left: auto; margin-right: auto;`.

### 5.2 Multi-View SPA Switching Pattern
All views are pre-rendered into `public/index.html` inside `<main class="workspace-content">` as `<section id="view-<name>" class="view-panel">`:
- Inactive views have `display: none`.
- The active view has `.view-panel.active` (`display: block`).
- Navigation is handled by `activateView(viewName, commName, updateHash)` in `templates/assets/dashboard.js` and synchronized with URL hashes:
  - `#overview` → `#view-overview`
  - `#commodity/<slug>` → `#view-commodity`
  - `#census` → `#view-census`
  - `#governance` → `#view-governance`

### 5.3 Grid Systems
Use ONLY the established CSS Grid classes for multi-card layouts:
- `.kpi-grid`: Default executive KPI grid (`repeat(auto-fit, minmax(215px, 1fr))`, `gap: 16px`, `margin-bottom: 20px`). Used for the 5-card Overview KPI strip.
- `.kpi-grid.kpi-grid-7`: Dense 7-card strip for Commodity Detail (`repeat(auto-fit, minmax(155px, 1fr))`, `gap: 12px`). Pair with `.kpi-card-compact` and `.kpi-value-compact`.
- `.kpi-grid.kpi-grid-4`: 4-card strip for Data Governance (`repeat(auto-fit, minmax(230px, 1fr))`, `gap: 16px`).
- `.grid-2col`: Side-by-side content cards (`repeat(auto-fit, minmax(440px, 1fr))`, `gap: 16px`, `margin-bottom: 20px`). Collapses to `1fr` at `@media (max-width: 960px)`.

---

## 6. Canonical Component Blueprints

When creating or modifying UI elements, copy these exact HTML structures and class combinations.

### 6.1 Standard KPI Card (`.kpi-card`)
Used in Overview (`.kpi-grid`) and Governance (`.kpi-grid.kpi-grid-4`). Includes a top-right `.kpi-icon-box` (42×42px rounded gradient square with 20×20px white SVG icon), a horizontal divider (`.kpi-card-divider`), and a bottom `.kpi-detail` row.

```html
<div class="kpi-card">
  <div class="kpi-card-body">
    <div class="kpi-card-header">
      <div class="kpi-card-info">
        <span class="kpi-label">Metric Label</span>
        <div class="kpi-value">
          1,234
          <span class="kpi-unit">Unit</span>
        </div>
      </div>
      <!-- Modifiers for icon box: default (DA green), .kpi-icon-gain (red), .kpi-icon-drop (green) -->
      <div class="kpi-icon-box" aria-hidden="true">
        <svg viewBox="0 0 24 24" width="22" height="22" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
          <!-- Feather/Lucide stroke path -->
        </svg>
      </div>
    </div>
    <hr class="kpi-card-divider">
    <div class="kpi-detail">
      <span class="kpi-highlight">Bold Stat</span>
      <span class="kpi-context">supporting context text</span>
    </div>
  </div>
</div>
```

### 6.2 Compact KPI Card (`.kpi-card.kpi-card-compact`)
Used in 7-column strips (`.kpi-grid.kpi-grid-7`) where horizontal space is tight. Omits `.kpi-icon-box` and uses `.kpi-value-compact`.

```html
<div class="kpi-card kpi-card-compact">
  <div class="kpi-card-body">
    <div class="kpi-card-info">
      <span class="kpi-label">Prevailing Price</span>
      <div class="kpi-value kpi-value-compact" id="box-price">₱52.00</div>
    </div>
    <hr class="kpi-card-divider">
    <div class="kpi-detail" id="box-unit">
      <span class="kpi-context">Retailing per</span> <span class="kpi-highlight">kg</span>
    </div>
  </div>
</div>
```

### 6.3 Content Card (`.card`)
All charts, tables, and prose sections **MUST** be wrapped in a `.card` with a `.card-header` and `.card-body`.

> **STRICT RULE — No Eyebrows or Subtitles on Tables & Charts:**
> Never place eyebrow/kicker tags (`.card-tag`) or subtitles (`.card-subtitle`) inside `.card-title-group` on table or chart cards. Also never set an internal Plotly `layout.title`. Keep `.card-title-group` strictly to a single `<h2 class="card-title">` element.

```html
<div class="card">
  <div class="card-header">
    <div class="card-title-group">
      <h2 class="card-title">Section Headline Title</h2>
    </div>
    <!-- Optional right-side header accessory: .status-pill, .timeframe-pills, or counter -->
    <span class="status-pill status-pill-increase">▲ Price Advances</span>
  </div>
  <div class="card-body">
    <!-- Card content here -->
  </div>
</div>
```
> **Full-Bleed Table Exception:** When a `.card` contains *only* a table (such as the Top 5 Movers cards in `overview.html`), set `<div class="card-body" style="padding: 0;">` and `<div class="table-container" style="border: none; border-radius: 0;">` so table row borders sit flush with the card edges. When a table sits inside a card alongside other content (like a toolbar or a chart), keep standard `.card-body` padding and default `.table-container` borders.

### 6.4 Badges vs. Pills (Strict Semantic Separation)
Never confuse numeric table badges with metadata/status pills:

| Component Class | Font Family | Border Radius | Purpose & Example |
|---|---|---|---|
| `.badge` (`.badge-increase`, `.badge-decrease`, `.badge-neutral`) | `--font-mono` (`tabular-nums`) | `6px` | **Numeric deltas inside tables**. Always includes `<span class="badge-arrow">▲/▼/—</span>`. Generated via `{{ val \| badge }}` or `formatBadgeHtml()`. |
| `.category-pill` | `--font-sans` | `999px` (pill) | **Commodity group tags** inside table cells (e.g., `<span class="category-pill">Rice</span>`). Neutral `#f1f5f9` background. |
| `.status-pill` (`.status-pill-increase`, `.status-pill-decrease`) | `--font-sans` | `999px` (pill) | **Card header semantic labels** (e.g., `▲ Price Advances`, `Zero Synthetic Records Assertion`). |
| `.btn-filter` | `--font-sans` | `8px` | **Interactive category filter buttons** in `.table-toolbar`. Uses `.active` (`--da-green-800` fill). |
| `.btn-timeframe` | `--font-sans` | `6px` (inside `8px` `.timeframe-pills` track) | **Segmented chart timeframe controls** (`1M`, `3M`, `6M`, `1Y`, `ALL`). Uses `.active` (white elevated pill). |

### 6.5 Formal Data Tables (`.formal-table`)
All tables must be wrapped in `.table-container` and use `.formal-table`:
- **Alignment Rules**:
  - Text columns (Commodity name, Category): default left-aligned. Commodity name cell **must** have `class="commodity-cell"`.
  - Unit column: `class="text-center font-mono"`.
  - All numeric/price/percentage/volatility columns (`th` and `td`): **MUST** be right-aligned (`class="text-right"`).
  - Raw numeric `td` cells (not using `.badge`) **MUST** include `font-mono` (`class="text-right font-mono"`).
- **Interactive Rows**:
  - Rows that navigate to a commodity detail view on click must have `class="clickable-row"` and `data-target-commodity="{{ row.commodity }}"`.
- **Sortable Columns**:
  - Add `class="sortable"` (or `class="text-right sortable"`) and `data-sort="<key>"` to `<th>`, ending the label with ` ⇅`.
  - Add `data-val="{{ row.metric or -999999 }}"` to numeric `<td>` cells so `dashboard.js` can sort nulls deterministically.

---

## 7. Data Visualization & Plotly.js Standards

All interactive charts use **Plotly.js (`v2.35.2`)** with a unified institutional theme.

### 7.1 Shared Plotly Layout Configuration
Every Plotly chart (whether serialized in Python via `pipeline/dashboard/queries.py` or rendered client-side in `templates/assets/dashboard.js`) **MUST** use these exact layout and config settings:

```javascript
const layout = {
  template: "plotly_white",
  hovermode: "x unified",
  margin: { l: 55, r: 20, t: 30, b: 40 },
  xaxis: {
    title: "",
    showgrid: true,
    gridcolor: "#f1f5f9",
    zeroline: false,
  },
  yaxis: {
    title: "Price (₱ / kg)", // or "Price Index (Base 100)"
    showgrid: true,
    gridcolor: "#f1f5f9",
    tickprefix: "₱", // omit tickprefix on Base-100 index charts
    zeroline: false,
  },
  legend: {
    orientation: "h",
    yanchor: "bottom",
    y: 1.02,
    xanchor: "right",
    x: 1,
    font: { size: 11 },
  },
};

const config = {
  responsive: true,
  displayModeBar: "hover",
  displaylogo: false,
};
```

### 7.2 Commodity Time-Series Trace Styling (`renderCommodityChart`)
When plotting an individual commodity's price history (`#commodity-trend-plot`, height `460px`):
1. **Prevailing Retail Price**: Solid line, color `#166534` (DA Deep Green), `width: 2.4`.
2. **7-Day SMA**: Dashed line (`dash: "dash"`), color `#d97706` (Harvest Amber), `width: 1.8`.
3. **30-Day SMA**: Dotted line (`dash: "dot"`), color `#64748b` (Slate Grey), `width: 1.5`.

### 7.3 Macro Category Palette (`CATEGORY_PALETTE`)
When plotting multi-category comparisons (`#category-index-plot`, height `430px`), always use the canonical `CATEGORY_PALETTE` from `pipeline/dashboard/queries.py`:
- **Rice**: `#15803d` (DA Emerald Green)
- **Meat and Poultry**: `#b91c1c` (Crimson / Deep Red)
- **Fish**: `#0284c7` (Ocean Blue / Teal)
- **Vegetables**: `#16a34a` (Agriculture Leaf Green)
- **Fruits**: `#d97706` (Harvest Amber)
- **Spices and Condiments**: `#7c3aed` (Spice Purple)
- **Basic Goods**: `#475569` (Slate Grey)

---

## 8. Iconography Standards

- **Icon Set**: Inline **Feather / Lucide** style SVG icons (`viewBox="0 0 24 24"`, `fill="none"`, `stroke="currentColor"`, `stroke-width="2"`, `stroke-linecap="round"`, `stroke-linejoin="round"`).
- **Accessibility**: Always add `aria-hidden="true"` to decorative SVGs, and provide `title` and `aria-label` attributes on icon-only buttons (e.g., `#btn-sidebar-toggle`, `.btn-print`).
- **Standard Sizes**:
  - `.kpi-icon-box svg`: `20px`×`20px` (inside `42px`×`42px` box)
  - `.brand-badge .brand-icon`: `20px`×`20px` (inside `36px`×`36px` box)
  - `.sidebar-nav-btn .nav-icon`: `18px`×`18px`
  - `.navbar-date-badge .bulletin-icon`: `17px`×`17px`
  - `.group-chevron`: `14px`×`14px` (`stroke-width="2.5"`, rotates `90deg` when parent `.sidebar-group` has `.expanded`)

---

## 9. Responsive & Print Requirements

1. **Mobile / Tablet Breakpoint (`@media (max-width: 960px)`)**:
   - `.dashboard-sidebar` becomes a fixed off-canvas drawer (`transform: translateX(-100%)`, `z-index: 120`) toggled via `#btn-sidebar-toggle` adding `.mobile-open`.
   - `#btn-sidebar-toggle` becomes visible (`display: inline-flex`).
   - `.grid-2col` collapses to a single column (`1fr`).
   - Calling `activateView()` **must** always close the mobile drawer (`sidebar.classList.remove("mobile-open")`).
2. **Print / PDF Bulletin Mode (`@media print`)**:
   - Triggered via the top-right print button (`window.print()`).
   - Automatically hides interactive navigation chrome: `.dashboard-sidebar`, `.btn-navbar-action`, `.btn-sidebar-toggle`, `.navbar-commodity-controls`, and `.table-toolbar`.
   - Expands `.app-shell` and `.main-workspace` to natural document flow (`height: auto !important; overflow: visible !important;`) and removes shadows for clean monochrome/archival PDF output.

---

## 10. Strict Do's and Don'ts for LLMs (Anti-Drift Checklist)

### DO:
- **DO** use `var(--da-green-800)` for primary active states and action buttons.
- **DO** use **Red** (`.badge-increase`, `.text-gain`, `.kpi-icon-gain`) for price **increases** and **Green** (`.badge-decrease`, `.text-drop`, `.kpi-icon-drop`) for price **decreases**.
- **DO** apply `.font-mono` (or use `.kpi-value` / `.badge`) on every numeric figure, price, percentage, and date.
- **DO** right-align (`class="text-right"`) both the `<th>` and `<td>` of every numeric column in `.formal-table`.
- **DO** use Jinja2 filters (`| currency`, `| badge`, `| format_num`, `| format_date`, `| slugify`) when rendering values in HTML templates.
- **DO** update `templates/components/header.html` and `activateView()` in `templates/assets/dashboard.js` if adding a new top-level `.view-panel` so the unified navbar kicker (`#navbar-view-tag`) and title (`#navbar-view-title`) stay synchronized.

### DO NOT:
- **DO NOT** introduce Tailwind CSS, Bootstrap, Material UI, FontAwesome, or external Google Fonts CDNs. The system uses zero-latency native system font stacks and inline SVGs.
- **DO NOT** create dark-mode overrides, neon gradients, glassmorphism blurs, or unapproved color variables.
- **DO NOT** color positive price changes green — in an inflation/retail price tracker, rising food prices are red (`▲ +x.xx%`) and falling food prices are green (`▼ -x.xx%`).
- **DO NOT** use `.badge` for text categories (use `.category-pill` or `.status-pill`), and do not use `.category-pill` for numeric deltas (use `.badge`).
- **DO NOT** put eyebrows/kickers (`.card-tag`) or subtitles (`.card-subtitle`) on table and chart cards, and do not add titles/subtitles inside Plotly `layout.title` — only a single `<h2 class="card-title">` is permitted inside `.card-title-group`.
- **DO NOT** add a separate page header inside a `.view-panel` — the unified `.dashboard-navbar` in `templates/components/header.html` serves as the single top header across all views.
- **DO NOT** edit files inside `public/` directly — they are overwritten whenever `pipeline/build_dashboard.py` runs.
