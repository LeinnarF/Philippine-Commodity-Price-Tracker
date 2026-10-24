"""
Selected Commodity Data Pipeline for Philippine Commodity Price Tracker.
Filters, standardizes, and structures the selected 48 dashboard commodities from
raw Department of Agriculture price monitoring retail price records (data/raw_da_amas_prices.csv) into a unified panel
dataset (data/selected_da_amas_prices.csv).

Features:
- Standardizes commodity names, categories, and retail units across all 451 monitoring dates.
- Resolves the 104-date PDF row-index shift (April 9, 2025 to September 22, 2025).
- Separates Bangus into Large and Medium size tiers.
- Decouples Pork Kasim (Picnic Shoulder) from Pork Pigue (Ham / Hind Leg).
- Isolates 1-Liter bottles for Cooking Oil (Palm & Coconut).
- Classifies Chicken Egg into Basic Goods (per RA 7581 Price Act).
- Consolidates vegetables (Lowland, Highland, and Corn) into a single Vegetables category.
- Drops min and max columns to keep focus on prevailing prices.
- Produces a complete date-commodity panel grid (451 dates x 48 commodities = 21,648 rows).
  If a price is unobserved in a specific timeframe, it is left blank for downstream imputation.
"""

from __future__ import annotations

import argparse
import logging
import time
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("select_commodities")

DEFAULT_RAW_CSV = Path(__file__).resolve().parent.parent / "data" / "raw_da_amas_prices.csv"
DEFAULT_SELECTED_CSV = Path(__file__).resolve().parent.parent / "data" / "selected_da_amas_prices.csv"

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
    Returns the comprehensive commodity matching rules and target metadata.
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


def select_commodities(
    input_csv: str | Path = DEFAULT_RAW_CSV,
    output_csv: str | Path = DEFAULT_SELECTED_CSV,
    egg_category: str = "Basic Goods",
    include_corn: bool = True,
    ffill_sporadic: bool = True,
) -> pd.DataFrame:
    """
    Extracts the selected 48 commodities from raw prices and structures them
    into a complete date-commodity panel grid. If a price does not exist in
    that timeframe, it is left blank.
    """
    input_path = Path(input_csv)
    output_path = Path(output_csv)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if not input_path.exists():
        raise FileNotFoundError(f"Raw prices CSV not found at: {input_path}")

    start_time = time.time()
    logger.info("==================================================")
    logger.info("Philippine Commodity Price Tracker: Commodity Selection")
    logger.info("Input Raw CSV      : %s", input_path.resolve())
    logger.info("Output Selected CSV: %s", output_path.resolve())
    logger.info("Egg Category       : %s", egg_category)
    logger.info("Include Corn       : %s", include_corn)
    logger.info("==================================================")

    logger.info("Loading raw CSV...")
    df_raw = pd.read_csv(input_path)
    all_dates = sorted(df_raw["date"].unique())
    logger.info("Loaded %d raw rows across %d dates.", len(df_raw), len(all_dates))

    # Precompute mask for the 104 shifted dates
    is_num = df_raw["commodity"].astype(str).str.isdigit()

    definitions = get_commodity_definitions(
        egg_category=egg_category,
        include_corn=include_corn,
    )
    logger.info("Processing %d selected commodities...", len(definitions))

    # Map each date to default raw_file and source_url metadata
    date_meta = df_raw.groupby("date").agg(
        raw_file=("raw_file", "first"),
        source_url=("source_url", "first"),
    ).to_dict(orient="index")

    # Aggregate matched survey rows per (date, commodity)
    matched_chunks: list[pd.DataFrame] = []
    for item in definitions:
        mask = item["matcher"](df_raw, is_num)
        sub = df_raw[mask].copy()

        if sub.empty:
            logger.warning("No records matched for commodity: %s", item["commodity"])
            continue

        daily = sub.groupby("date").agg(
            price_prevailing=("price_prevailing", "mean"),
            raw_count=("price_prevailing", "count"),
            source_url=("source_url", "first"),
            raw_file=("raw_file", "first"),
        ).reset_index()

        daily["commodity_group"] = item["category"]
        daily["commodity"] = item["commodity"]
        daily["unit"] = item["unit"]
        daily["price_prevailing"] = daily["price_prevailing"].round(2)
        matched_chunks.append(daily)

    matched_df = pd.concat(matched_chunks, ignore_index=True)

    # Build complete rectangular Cartesian grid: all_dates x all_selected_commodities
    grid_records = []
    for d in all_dates:
        for item in definitions:
            grid_records.append({
                "date": d,
                "commodity_group": item["category"],
                "commodity": item["commodity"],
                "unit": item["unit"],
                "market_scope": "NCR Wet Markets",
                "source": "da_amas",
                "is_synthetic": False,
            })
    grid_df = pd.DataFrame(grid_records)

    # Left join to preserve every date-commodity pair
    selected_df = pd.merge(
        grid_df,
        matched_df[["date", "commodity", "price_prevailing", "raw_count", "raw_file", "source_url"]],
        on=["date", "commodity"],
        how="left",
    )

    # Populate raw_file and source_url for unobserved days
    for d, meta in date_meta.items():
        missing_mask = (selected_df["date"] == d) & selected_df["raw_file"].isnull()
        selected_df.loc[missing_mask, "raw_file"] = meta["raw_file"]
        selected_df.loc[missing_mask, "source_url"] = meta["source_url"]

    selected_df["raw_count"] = selected_df["raw_count"].fillna(0).astype(int)

    # Apply verified data anomaly corrections
    for (d, comm), (corrected_price, rationale) in DATA_ANOMALY_CORRECTIONS.items():
        mask = (selected_df["date"] == d) & (selected_df["commodity"] == comm)
        if mask.any():
            old_price = selected_df.loc[mask, "price_prevailing"].iloc[0]
            if pd.notna(old_price) and old_price != corrected_price:
                selected_df.loc[mask, "price_prevailing"] = corrected_price
                logger.info(
                    "Applied anomaly correction for '%s' on %s: ₱%.2f -> ₱%.2f (%s)",
                    comm, d, old_price, corrected_price, rationale,
                )
            elif pd.isna(old_price):
                selected_df.loc[mask, "price_prevailing"] = corrected_price

    # Apply forward fill for sporadic gaps while leaving leading day gap blank
    if ffill_sporadic:
        logger.info("Applying forward fill (ffill) to sporadic 1-2 day gaps...")
        selected_df = selected_df.sort_values(by=["commodity", "date"]).reset_index(drop=True)
        # Forward fill all commodities with sporadic gaps, preserving leading gap for Pork Pigue
        non_leading_mask = selected_df["commodity"] != "Pork Pigue (Hind Leg / Ham)"
        selected_df.loc[non_leading_mask, "price_prevailing"] = (
            selected_df[non_leading_mask]
            .groupby("commodity")["price_prevailing"]
            .ffill()
        )

    # Reorder columns without min and max
    columns = [
        "date",
        "commodity_group",
        "commodity",
        "unit",
        "price_prevailing",
        "market_scope",
        "source",
        "is_synthetic",
        "raw_count",
        "raw_file",
        "source_url",
    ]
    selected_df = selected_df[columns].sort_values(
        by=["date", "commodity_group", "commodity"]
    ).reset_index(drop=True)

    # Deduplication assertion: strictly 1 row per (date, commodity)
    dups = selected_df.duplicated(subset=["date", "commodity"]).sum()
    if dups > 0:
        logger.error("Deduplication error: %d duplicate rows found!", dups)
        raise ValueError("Duplicate (date, commodity) pairs present in grid.")

    # Write output CSV (blank price for missing entries)
    selected_df.to_csv(output_path, index=False, encoding="utf-8")
    elapsed = time.time() - start_time
    file_size_mb = output_path.stat().st_size / (1024 * 1024)
    missing_count = selected_df["price_prevailing"].isnull().sum()

    logger.info("==================================================")
    logger.info("Selection Complete in %.2f seconds!", elapsed)
    logger.info("  Total Panel Rows     : %d rows (%d dates x %d items)", len(selected_df), len(all_dates), len(definitions))
    logger.info("  Observed Prices      : %d rows", len(selected_df) - missing_count)
    logger.info("  Blank (Unobserved)   : %d rows (left blank for filling)", missing_count)
    logger.info("  Standard Categories  : %d groups", selected_df["commodity_group"].nunique())
    logger.info("  Selected Commodities : %d items", selected_df["commodity"].nunique())
    logger.info("  Output CSV File      : %s (%.2f MB)", output_path.resolve(), file_size_mb)
    logger.info("==================================================")

    # Print summary by category
    summary = selected_df.groupby("commodity_group").agg(
        Commodity_Count=("commodity", "nunique"),
        Total_Rows=("date", "count"),
        Observed_Rows=("price_prevailing", lambda s: s.notnull().sum()),
        Blank_Rows=("price_prevailing", lambda s: s.isnull().sum()),
        Min_Price=("price_prevailing", "min"),
        Max_Price=("price_prevailing", "max"),
    ).reset_index()
    logger.info("Category Distribution:\n%s", summary.to_string(index=False))

    return selected_df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Filter and structure the 48 selected commodities into a panel dataset."
    )
    parser.add_argument(
        "--input",
        "-i",
        type=str,
        default=str(DEFAULT_RAW_CSV),
        help=f"Path to input raw CSV (default: {DEFAULT_RAW_CSV})",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=str,
        default=str(DEFAULT_SELECTED_CSV),
        help=f"Path to output selected CSV (default: {DEFAULT_SELECTED_CSV})",
    )
    parser.add_argument(
        "--egg-category",
        type=str,
        default="Basic Goods",
        choices=["Basic Goods", "Meat and Poultry"],
        help="Category to assign Chicken Egg (default: 'Basic Goods')",
    )
    parser.add_argument(
        "--no-corn",
        action="store_true",
        help="Exclude Yellow and White Corn from the dataset",
    )
    parser.add_argument(
        "--no-ffill",
        action="store_true",
        help="Disable forward-fill for sporadic 1-2 day gaps (leaves all unobserved days blank)",
    )

    args = parser.parse_args()

    select_commodities(
        input_csv=args.input,
        output_csv=args.output,
        egg_category=args.egg_category,
        include_corn=not args.no_corn,
        ffill_sporadic=not args.no_ffill,
    )
