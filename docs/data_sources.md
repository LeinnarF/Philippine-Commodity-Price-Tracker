# Data Sources & Provenance Documentation

This document defines the origin, structure, and integrity guarantees for data used in the **Philippine Commodity Price Tracker**.

---

## 1. Data Integrity & Provenance Policy

**100% Official Government Data Guarantee**:
- All records in `data/commodities.duckdb` are parsed directly from official Department of Agriculture price monitoring Daily Price Index bulletins.
- Zero synthetic data is used in the database.
- Every observation links directly to its source bulletin URL (`source_url`) and raw PDF archive (`raw_file_path`).

| Field | Production Value | Description |
|---|---|---|
| **`source`** | `"da_amas"` | Philippine Department of Agriculture price monitoring |
| **`is_synthetic`** | `FALSE` | Explicit boolean flag confirming authentic government observation |
| **`source_url`** | `https://www.da.gov.ph/wp-content/uploads/...` | Permanent URL to the published PDF bulletin |
| **`market_scope`** | `"NCR Wet Markets"` | Survey coverage across major public wet markets in Metro Manila |

---

## 2. Official Data Source: Department of Agriculture Price Monitoring

### Overview
- **Publishing Agency**: Department of Agriculture (DA) price monitoring.
- **Portal**: [DA Price Monitoring Updates](https://www.da.gov.ph/price-monitoring/) / [Bantay Presyo](https://bantaypresyo.da.gov.ph/).
- **Bulletin Format**: "Daily Price Index" 8-page PDF document published every government workday (Monday through Saturday, excluding Sundays and Philippine regular holidays).
- **Survey Scope**: Prevailing retail prices collected across surveyed major wet markets in Metro Manila (NCR), including Commonwealth Market, Mega Q-Mart, Quinta Market, and Muñoz Market.

### Commodity Categories Covered
1. **Commercial Rice & Cereals**: Regular Milled, Well Milled, Premium, Special, and Corn (Local & Imported).
2. **Fish Products**: Bangus (Medium & Large), Tilapia, Galunggong (Local & Imported), Alumahan, and other marine fish.
3. **Meat & Poultry**: Pork Kasim, Pork Liempo, Pork Loin, Beef Brisket, Beef Rump, Beef Loin, Whole Dressed Chicken, Chicken Cuts, and Chicken Eggs.
4. **Lowland & Highland Vegetables**: Ampalaya, Sitao, Eggplant, Native Pechay, Squash, Cabbage, Carrots, White Potato, Sayote, Baguio Beans.
5. **Spices & Condiments**: Red Onion (Local & Imported), White Onion, Garlic, Ginger, and Chili (Siling Haba & Siling Labuyo).
6. **Fruits**: Calamansi, Lakatan Banana, Latundan Banana, Papaya, and Carabao Mango.
7. **Other Basic Goods**: Refined Sugar, Washed Sugar, Brown Sugar, and Cooking Oil (Palm & Coconut).

---

## 3. Ingestion & Raw Payload Archiving

Every bulletin downloaded by the pipeline is permanently archived:
1. **Raw PDF Archive**: Saved to `data/raw/da_amas/<YYYY-MM-DD>.pdf`.
2. **Cryptographic Verification**: SHA-256 hash is computed and recorded.
3. **Audit Log (`fetch_log`)**: Immutable record in DuckDB tracking:
   - `target_date`: Market observation date.
   - `url`: Official publication URL on `da.gov.ph`.
   - `http_status`: HTTP response status code (e.g. 200, 404).
   - `content_sha256`: Checksum of the downloaded bulletin.
   - `raw_file_path`: Local archive file path.
   - `status`: Execution status (`OK`, `SUNDAY`, `NOT_PUBLISHED`, `HTTP_ERROR`, `NETWORK_ERROR`).
   - `records_ingested`: Number of commodity rows extracted.
   - `detail`: Diagnostic notes and parser outcomes.
   - `fetched_at`: Timestamp of ingestion.
