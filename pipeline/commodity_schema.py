"""
Core Domain Commodity Schema & Panel Normalization for Philippine Commodity Price Tracker.

Defines the authoritative 48-commodity panel definitions, category classifications,
retail units, verified official clerical anomaly corrections, and standardization engine.
Pure observed price reporting: unobserved/missing items remain blank/null (NO forward-filling).
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

logger = logging.getLogger("commodity_schema")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CLEAN_CSV = PROJECT_ROOT / "data" / "cleaned_dataset.csv"

# Production 6-column schema (5 analytical dimensions + 1 source audit link)
CLEAN_COLUMNS = [
    "date",
    "commodity_group",
    "commodity",
    "unit",
    "price_prevailing",
    "source_url",
]

# Verified typographical/clerical anomalies in official government bulletins and their corrections.
# Format: (date, commodity): (corrected_price, rationale)
DATA_ANOMALY_CORRECTIONS: dict[tuple[str, str], tuple[float, str]] = {
    ("2026-09-10", "Chicken Egg (Medium White)"): (
        8.13,
        "Human entry error in official Department of Agriculture price monitoring bulletin (printed 200.00/pc instead of prevailing 8.13/pc; aligned with 2026-09-09 and 2026-09-11 prices).",
    ),
}


def get_commodity_definitions(
    egg_category: str = "Basic Goods",
    include_corn: bool = True,
) -> list[dict[str, Any]]:
    """
    Returns the comprehensive commodity matching rules and target metadata for all 48 tracked commodities.
    """
    definitions: list[dict[str, Any]] = [
        # =====================================================================
        # 1. RICE
        # =====================================================================
        {
            "category": "Rice",
            "commodity": "Regular Milled Rice",
            "unit": "kg",
            "matcher": lambda df, is_num: (
                (is_num & df["variety"].isin(["Regular Milled"]))
                | (~is_num & df["commodity"].isin(["Regular Milled"]))
            ),
            "description": "Core staple rice (20-40% bran streak)",
        },
        {
            "category": "Rice",
            "commodity": "Well Milled Rice",
            "unit": "kg",
            "matcher": lambda df, is_num: (
                (is_num & df["variety"].isin(["Well Milled"]))
                | (~is_num & df["commodity"].isin(["Well Milled"]))
            ),
            "description": "Core staple rice (1-19% bran streak)",
        },
        {
            "category": "Rice",
            "commodity": "Premium Rice",
            "unit": "kg",
            "matcher": lambda df, is_num: (
                (is_num & df["variety"].isin(["Premium"]))
                | (~is_num & df["commodity"].isin(["Premium"]))
            ),
            "description": "High grade commercial rice (5% broken)",
        },

        # =====================================================================
        # 2. FISH
        # =====================================================================
        {
            "category": "Fish",
            "commodity": "Bangus (Large)",
            "unit": "kg",
            "matcher": lambda df, is_num: (
                (is_num & (df["variety"] == "Bangus") & df["unit"].str.contains("Large", na=False))
                | (~is_num & (df["commodity"] == "Bangus, Large"))
                | (~is_num & (df["commodity"] == "Bangus") & df["variety"].str.contains("Large", na=False))
            ),
            "description": "Large milkfish (1-2 pcs/kg)",
        },
        {
            "category": "Fish",
            "commodity": "Bangus (Medium)",
            "unit": "kg",
            "matcher": lambda df, is_num: (
                (is_num & (df["variety"] == "Bangus") & df["unit"].str.contains("Medium", na=False))
                | (~is_num & (df["commodity"] == "Bangus, Medium"))
                | (~is_num & (df["commodity"] == "Bangus") & df["variety"].str.contains("Medium", na=False))
            ),
            "description": "Medium milkfish (3-4 pcs/kg)",
        },
        {
            "category": "Fish",
            "commodity": "Tilapia",
            "unit": "kg",
            "matcher": lambda df, is_num: (
                (is_num & df["variety"].isin(["Tilapia"]))
                | (~is_num & df["commodity"].isin(["Tilapia"]))
            ),
            "description": "Fresh medium freshwater tilapia",
        },
        {
            "category": "Fish",
            "commodity": "Galunggong (Local)",
            "unit": "kg",
            "matcher": lambda df, is_num: (
                (is_num & df["variety"].isin(["Galunggong (Local)"]))
                | (~is_num & df["commodity"].isin(["Galunggong (Local)", "Galunggong, Local"]))
            ),
            "description": "Local round scad (male, medium)",
        },
        {
            "category": "Fish",
            "commodity": "Sardines (Tamban)",
            "unit": "kg",
            "matcher": lambda df, is_num: (
                (is_num & df["variety"].isin(["Sardines (Tamban)"]))
                | (~is_num & df["commodity"].isin(["Sardines (Tamban)"]))
            ),
            "description": "Fresh tamban sardines",
        },

        # =====================================================================
        # 3. MEAT AND POULTRY
        # =====================================================================
        {
            "category": "Meat and Poultry",
            "commodity": "Whole Chicken",
            "unit": "kg",
            "matcher": lambda df, is_num: (
                (is_num & df["variety"].isin(["Whole Chicken"]))
                | (~is_num & df["commodity"].isin(["Whole Chicken", "Whole Chicken (fully dressed)", "Whole Chicken, Local"]))
            ),
            "description": "Fully dressed local broiler chicken",
        },
        {
            "category": "Meat and Poultry",
            "commodity": "Pork Belly (Liempo)",
            "unit": "kg",
            "matcher": lambda df, is_num: (
                (is_num & df["variety"].isin(["Pork Belly/Liempo (fresh or chilled)"]))
                | (~is_num & df["commodity"].isin(["Pork Belly", "Pork Belly (Liempo), Local", "Pork Belly/Liempo (fresh or chilled)"]))
            ),
            "description": "Fresh pork belly / liempo cut",
        },
        {
            "category": "Meat and Poultry",
            "commodity": "Pork Kasim (Picnic Shoulder)",
            "unit": "kg",
            "matcher": lambda df, is_num: (
                (is_num & df["variety"].isin(["Local Pork Picnic Shoulder (Kasim)", "Pork Picnic Shoulder (Kasim), Local"]))
                | (~is_num & df["commodity"].isin(["Pork Picnic Shoulder (Kasim), Local", "Pork Ham/Kasim (fresh or chilled)", "Pork Ham (Kasim), Local"]))
                | (~is_num & (df["commodity"] == "Pork Ham") & (df["variety"] == "Kasim"))
            ),
            "description": "Local pork picnic shoulder (front cut)",
        },
        {
            "category": "Meat and Poultry",
            "commodity": "Pork Pigue (Hind Leg / Ham)",
            "unit": "kg",
            "matcher": lambda df, is_num: (
                (is_num & df["variety"].isin(["Pork Hind Leg/Pigue (fresh or chilled)", "Pork Hind Leg (Pigue), Local"]))
                | (~is_num & df["commodity"].isin(["Pork Hind Leg (Pigue), Local"]))
            ),
            "description": "Local pork ham / hind leg cut",
        },
        {
            "category": "Meat and Poultry",
            "commodity": "Beef Brisket",
            "unit": "kg",
            "matcher": lambda df, is_num: (
                (is_num & df["variety"].isin(["Beef Brisket, Local"]))
                | (~is_num & df["commodity"].isin(["Beef Brisket", "Beef Brisket, Local"]))
            ),
            "description": "Local beef brisket (meat with bones)",
        },
        {
            "category": "Meat and Poultry",
            "commodity": "Beef Rump",
            "unit": "kg",
            "matcher": lambda df, is_num: (
                (is_num & df["variety"].isin(["Beef Rump, Local"]))
                | (~is_num & df["commodity"].isin(["Beef Rump", "Beef Rump, Local"]))
            ),
            "description": "Local lean beef rump / tapadera",
        },

        # =====================================================================
        # 4. VEGETABLES (COMBINED LOWLAND, HIGHLAND, & CORN)
        # =====================================================================
        {
            "category": "Vegetables",
            "commodity": "Tomato",
            "unit": "kg",
            "matcher": lambda df, is_num: (
                (is_num & df["variety"].isin(["Tomato"]))
                | (~is_num & df["commodity"].isin(["Tomato"]))
            ),
            "description": "Fresh ripe cooking tomatoes",
        },
        {
            "category": "Vegetables",
            "commodity": "Eggplant",
            "unit": "kg",
            "matcher": lambda df, is_num: (
                (is_num & df["variety"].isin(["Eggplant"]))
                | (~is_num & df["commodity"].isin(["Eggplant"]))
            ),
            "description": "Long purple lowland eggplant",
        },
        {
            "category": "Vegetables",
            "commodity": "Ampalaya (Bitter Gourd)",
            "unit": "kg",
            "matcher": lambda df, is_num: (
                (is_num & df["variety"].isin(["Ampalaya"]))
                | (~is_num & df["commodity"].isin(["Ampalaya"]))
            ),
            "description": "Standard bitter gourd",
        },
        {
            "category": "Vegetables",
            "commodity": "Squash",
            "unit": "kg",
            "matcher": lambda df, is_num: (
                (is_num & df["variety"].isin(["Squash"]))
                | (~is_num & df["commodity"].isin(["Squash"]))
            ),
            "description": "Kalabasa / squash",
        },
        {
            "category": "Vegetables",
            "commodity": "Native Pechay",
            "unit": "bundle",
            "matcher": lambda df, is_num: (
                (is_num & df["variety"].isin(["Native Pechay", "Pechay (Native)"]))
                | (~is_num & df["commodity"].isin(["Native Pechay", "Pechay (Native)"]))
            ),
            "description": "Native green pechay (bundled)",
        },
        {
            "category": "Vegetables",
            "commodity": "Cabbage (Scorpio)",
            "unit": "kg",
            "matcher": lambda df, is_num: (
                (is_num & df["variety"].isin(["Cabbage (Scorpio), Local"]))
                | (~is_num & df["commodity"].isin(["Cabbage (Scorpio)", "Cabbage (Scorpio), Local"]))
            ),
            "description": "Scorpio head cabbage (750g-1kg)",
        },
        {
            "category": "Vegetables",
            "commodity": "Cabbage (Rare Ball)",
            "unit": "kg",
            "matcher": lambda df, is_num: (
                (is_num & df["variety"].isin(["Cabbage (Rare Ball), Local"]))
                | (~is_num & df["commodity"].isin(["Cabbage (Rare Ball)", "Cabbage (Rare Ball), Local"]))
            ),
            "description": "Rare ball cabbage (510g-1kg)",
        },
        {
            "category": "Vegetables",
            "commodity": "Cabbage (Wonder Ball)",
            "unit": "kg",
            "matcher": lambda df, is_num: (
                (is_num & df["variety"].isin(["Cabbage (Wonder Ball), Local"]))
                | (~is_num & df["commodity"].isin(["Cabbage (Wonder Ball)", "Cabbage (Wonder Ball), Local"]))
            ),
            "description": "Wonder ball cabbage (510g-1kg)",
        },
        {
            "category": "Vegetables",
            "commodity": "Carrots",
            "unit": "kg",
            "matcher": lambda df, is_num: (
                (is_num & df["variety"].isin(["Carrots, Local"]))
                | (~is_num & df["commodity"].isin(["Carrots", "Carrots, Local"]))
            ),
            "description": "Local highland carrots",
        },
        {
            "category": "Vegetables",
            "commodity": "White Potato",
            "unit": "kg",
            "matcher": lambda df, is_num: (
                (is_num & df["variety"].isin(["White Potato, Local"]))
                | (~is_num & df["commodity"].isin(["White Potato", "White Potato, Local"]))
            ),
            "description": "Local white table potato",
        },
        {
            "category": "Vegetables",
            "commodity": "Bell Pepper (Red)",
            "unit": "kg",
            "matcher": lambda df, is_num: (
                (is_num & df["variety"].isin(["Bell Pepper (Red), Local"]))
                | (~is_num & df["commodity"].isin(["Bell Pepper (Red)", "Bell Pepper (Red), Local"]))
            ),
            "description": "Medium local red bell pepper",
        },
        {
            "category": "Vegetables",
            "commodity": "Bell Pepper (Green)",
            "unit": "kg",
            "matcher": lambda df, is_num: (
                (is_num & df["variety"].isin(["Bell Pepper (Green), Local"]))
                | (~is_num & df["commodity"].isin(["Bell Pepper (Green)", "Bell Pepper (Green), Local"]))
            ),
            "description": "Medium local green bell pepper",
        },

        # =====================================================================
        # 5. SPICES AND CONDIMENTS
        # =====================================================================
        {
            "category": "Spices and Condiments",
            "commodity": "Red Onion",
            "unit": "kg",
            "matcher": lambda df, is_num: (
                (is_num & df["variety"].isin(["Red Onion", "Red Onion, Local"]))
                | (~is_num & df["commodity"].isin(["Red Onion", "Red Onion, Local"]))
            ),
            "description": "Local red bulb onion",
        },
        {
            "category": "Spices and Condiments",
            "commodity": "Garlic (Imported)",
            "unit": "kg",
            "matcher": lambda df, is_num: (
                (is_num & df["variety"].isin(["Garlic (Imported)", "Garlic, Imported"]))
                | (~is_num & df["commodity"].isin(["Garlic (Imported)", "Garlic, Imported"]))
            ),
            "description": "Standard imported white garlic",
        },
        {
            "category": "Spices and Condiments",
            "commodity": "Garlic (Native)",
            "unit": "kg",
            "matcher": lambda df, is_num: (
                (is_num & df["variety"].isin(["Garlic (Native)", "Garlic, Native/Local"]))
                | (~is_num & df["commodity"].isin(["Garlic (Native)", "Garlic, Native/Local"]))
            ),
            "description": "Native Philippine garlic",
        },
        {
            "category": "Spices and Condiments",
            "commodity": "Ginger",
            "unit": "kg",
            "matcher": lambda df, is_num: (
                (is_num & df["variety"].isin(["Ginger", "Ginger (Local)", "Ginger, Local"]))
                | (~is_num & df["commodity"].isin(["Ginger", "Ginger (Local)", "Ginger, Local"]))
            ),
            "description": "Fairly well-matured local ginger",
        },
        {
            "category": "Spices and Condiments",
            "commodity": "Calamansi",
            "unit": "kg",
            "matcher": lambda df, is_num: (
                (is_num & df["variety"].isin(["Calamansi"]))
                | (~is_num & df["commodity"].isin(["Calamansi"]))
            ),
            "description": "Fresh Philippine calamansi lime",
        },

        # =====================================================================
        # 6. FRUITS
        # =====================================================================
        {
            "category": "Fruits",
            "commodity": "Banana (Lakatan)",
            "unit": "kg",
            "matcher": lambda df, is_num: (
                (is_num & df["variety"].isin(["Banana (Lakatan)"]))
                | (~is_num & df["commodity"].isin(["Banana (Lakatan)"]))
            ),
            "description": "Table banana lakatan (8-10 pcs/kg)",
        },
        {
            "category": "Fruits",
            "commodity": "Banana (Latundan)",
            "unit": "kg",
            "matcher": lambda df, is_num: (
                (is_num & df["variety"].isin(["Banana (Latundan)"]))
                | (~is_num & df["commodity"].isin(["Banana (Latundan)"]))
            ),
            "description": "Table banana latundan (10-12 pcs/kg)",
        },
        {
            "category": "Fruits",
            "commodity": "Banana (Saba)",
            "unit": "kg",
            "matcher": lambda df, is_num: (
                (is_num & df["variety"].isin(["Banana (Saba)"]))
                | (~is_num & df["commodity"].isin(["Banana (Saba)"]))
            ),
            "description": "Cooking banana saba",
        },
        {
            "category": "Fruits",
            "commodity": "Mango (Carabao)",
            "unit": "kg",
            "matcher": lambda df, is_num: (
                (is_num & df["variety"].isin(["Mango (Carabao)"]))
                | (~is_num & df["commodity"].isin(["Mango (Carabao)"]))
            ),
            "description": "Ripe Carabao mango (3-4 pcs/kg)",
        },
        {
            "category": "Fruits",
            "commodity": "Papaya",
            "unit": "kg",
            "matcher": lambda df, is_num: (
                (is_num & df["variety"].isin(["Papaya"]))
                | (~is_num & df["commodity"].isin(["Papaya"]))
            ),
            "description": "Solo ripe papaya",
        },
        {
            "category": "Fruits",
            "commodity": "Melon",
            "unit": "kg",
            "matcher": lambda df, is_num: (
                (is_num & df["variety"].isin(["Melon"]))
                | (~is_num & df["commodity"].isin(["Melon"]))
            ),
            "description": "Cantaloupe / sweet melon",
        },
        {
            "category": "Fruits",
            "commodity": "Watermelon",
            "unit": "kg",
            "matcher": lambda df, is_num: (
                (is_num & df["variety"].isin(["Watermelon"]))
                | (~is_num & df["commodity"].isin(["Watermelon"]))
            ),
            "description": "Fresh whole watermelon",
        },
        {
            "category": "Fruits",
            "commodity": "Avocado",
            "unit": "kg",
            "matcher": lambda df, is_num: (
                (is_num & df["variety"].isin(["Avocado"]))
                | (~is_num & df["commodity"].isin(["Avocado"]))
            ),
            "description": "Standard local avocado",
        },
        {
            "category": "Fruits",
            "commodity": "Pomelo",
            "unit": "kg",
            "matcher": lambda df, is_num: (
                (is_num & df["variety"].isin(["Pomelo"]))
                | (~is_num & df["commodity"].isin(["Pomelo"]))
            ),
            "description": "Standard table pomelo / suha",
        },

        # =====================================================================
        # 7. BASIC GOODS (INCLUDING CHICKEN EGG & STRICT 1L OILS)
        # =====================================================================
        {
            "category": egg_category,
            "commodity": "Chicken Egg (Medium White)",
            "unit": "piece",
            "matcher": lambda df, is_num: (
                (is_num & df["variety"].isin(["Chicken Egg (White, Medium)"]))
                | (~is_num & df["commodity"].isin(["Chicken Egg (White, Medium)"]))
            ),
            "description": "Standard medium white table egg (RA 7581 Basic Necessity)",
        },
        {
            "category": "Basic Goods",
            "commodity": "Refined Sugar",
            "unit": "kg",
            "matcher": lambda df, is_num: (
                (is_num & df["variety"].isin(["Sugar Refined", "Sugar (Refined)"]))
                | (~is_num & df["commodity"].isin(["Sugar Refined", "Sugar (Refined)"]))
            ),
            "description": "Pure white refined cane sugar",
        },
        {
            "category": "Basic Goods",
            "commodity": "Brown Sugar",
            "unit": "kg",
            "matcher": lambda df, is_num: (
                (is_num & df["variety"].isin(["Sugar Brown", "Sugar (Brown)"]))
                | (~is_num & df["commodity"].isin(["Sugar Brown", "Sugar (Brown)"]))
            ),
            "description": "Standard unrefined brown cane sugar",
        },
        {
            "category": "Basic Goods",
            "commodity": "Washed Sugar",
            "unit": "kg",
            "matcher": lambda df, is_num: (
                (is_num & df["variety"].isin(["Sugar Washed", "Sugar (Washed)"]))
                | (~is_num & df["commodity"].isin(["Sugar Washed", "Sugar (Washed)"]))
            ),
            "description": "Semi-refined washed cane sugar",
        },
        {
            "category": "Basic Goods",
            "commodity": "Cooking Oil (Palm)",
            "unit": "bottle (1L)",
            "matcher": lambda df, is_num: (
                (is_num & (df["variety"] == "Cooking Oil (Palm)") & (df["unit"].str.contains("1 Liter|1,000 ml|1L|1000ml|1 Liter/bottle", case=False, na=False)))
                | (~is_num & (df["commodity"] == "Cooking Oil (Palm)") & (df["variety"].str.contains("1 Liter|1,000 ml|1L|1000ml", case=False, na=False) | df["unit"].str.contains("1L|1 Liter|1,000 ml", case=False, na=False)))
            ),
            "description": "Refined palm cooking oil (1-Liter bottle)",
        },
        {
            "category": "Basic Goods",
            "commodity": "Cooking Oil (Coconut)",
            "unit": "bottle (1L)",
            "matcher": lambda df, is_num: (
                (is_num & (df["variety"] == "Cooking Oil (Coconut)") & (df["unit"].str.contains("1 Liter|1,000 ml|1L|1000ml|1 Liter/bottle", case=False, na=False)))
                | (~is_num & (df["commodity"] == "Cooking Oil (Coconut)") & (df["variety"].str.contains("1 Liter|1,000 ml|1L|1000ml", case=False, na=False) | df["unit"].str.contains("1L|1 Liter|1,000 ml", case=False, na=False)))
            ),
            "description": "Pure coconut cooking oil (1-Liter bottle)",
        },
    ]

    # Optional Corn placement in Vegetables
    if include_corn:
        corn_defs = [
            {
                "category": "Vegetables",
                "commodity": "Yellow Corn",
                "unit": "kg",
                "matcher": lambda df, is_num: (
                    (is_num & df["variety"].isin(["Corn (Yellow)"]))
                    | (~is_num & df["commodity"].isin(["Corn (Yellow)"]))
                ),
                "description": "Whole yellow grain corn",
            },
            {
                "category": "Vegetables",
                "commodity": "White Corn",
                "unit": "kg",
                "matcher": lambda df, is_num: (
                    (is_num & df["variety"].isin(["Corn (White)"]))
                    | (~is_num & df["commodity"].isin(["Corn (White)"]))
                ),
                "description": "White edible flint corn",
            },
        ]
        definitions.extend(corn_defs)

    return definitions


def standardize_bulletin_records(
    df_raw: pd.DataFrame,
    target_date: str,
    source_url: str = "",
) -> list[dict[str, Any]]:
    """
    Standardizes raw bulletin observations into the strict 48-commodity panel format.
    Pure observed price reporting: unobserved/missing items remain blank/null (NO forward-filling).
    Applies official anomaly corrections where applicable.
    """
    # 1. Deduplicate raw survey records: strictly date, commodity_group, commodity, variety
    # Favor later rows in duplicate cases (e.g. Local Commercial Rice over Imported Commercial Rice)
    dedup_cols = ["date", "commodity_group", "commodity", "variety"]
    available_dedup = [c for c in dedup_cols if c in df_raw.columns]
    if available_dedup:
        df_raw = df_raw.drop_duplicates(subset=available_dedup, keep="last")

    # 2. Check if commodity column is numeric (indicator for the 104 shifted dates)
    is_num = df_raw["commodity"].astype(str).str.isdigit()

    definitions = get_commodity_definitions(egg_category="Basic Goods", include_corn=True)
    records: list[dict[str, Any]] = []

    for item in definitions:
        mask = item["matcher"](df_raw, is_num)
        sub = df_raw[mask]

        if not sub.empty:
            price = sub["price_prevailing"].mean()
            price_val = round(float(price), 2) if pd.notna(price) else np.nan
        else:
            price_val = np.nan

        # Apply official verified typographical/clerical anomaly corrections
        correction = DATA_ANOMALY_CORRECTIONS.get((target_date, item["commodity"]))
        if correction is not None:
            price_val = correction[0]

        records.append({
            "date": target_date,
            "commodity_group": item["category"],
            "commodity": item["commodity"],
            "unit": item["unit"],
            "price_prevailing": price_val,
            "source_url": source_url or "",
        })

    return records


def validate_cleaned_panel(df: pd.DataFrame) -> None:
    """
    Validates panel integrity:
    - Exactly CLEAN_COLUMNS present in correct order
    - Rectangular Cartesian dimensions: total_rows == unique_dates * 48
    - Zero duplicate (date, commodity) rows
    - 48 unique commodities across 7 commodity groups
    - Prices strictly positive (> 0) when observed
    """
    missing_cols = [c for c in CLEAN_COLUMNS if c not in df.columns]
    if missing_cols:
        raise ValueError(f"Cleaned dataset missing required columns: {missing_cols}")

    n_dates = df["date"].nunique()
    n_commodities = df["commodity"].nunique()
    n_groups = df["commodity_group"].nunique()
    total_rows = len(df)
    expected_rows = n_dates * 48

    if n_commodities != 48:
        raise ValueError(f"Expected 48 commodities, found {n_commodities}")

    if n_groups != 7:
        raise ValueError(f"Expected 7 commodity groups, found {n_groups}")

    if total_rows != expected_rows:
        raise ValueError(
            f"Panel is not rectangular: {total_rows} total rows != {n_dates} dates * 48 commodities ({expected_rows})"
        )

    dups = df.duplicated(subset=["date", "commodity"]).sum()
    if dups > 0:
        raise ValueError(f"Found {dups} duplicate (date, commodity) pairs in cleaned panel!")

    # Observed prices must be strictly positive
    observed = df.loc[df["price_prevailing"].notnull(), "price_prevailing"]
    if (observed <= 0).any():
        invalid_cnt = (observed <= 0).sum()
        raise ValueError(f"Found {invalid_cnt} rows with price_prevailing <= 0!")
